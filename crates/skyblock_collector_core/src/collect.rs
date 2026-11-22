//! The pollers. Each archives every response it receives, then spools new snapshots for the
//! ingest worker. A secondary collector has no spool and only archives.

use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::{self, Either};
use futures::stream;
use serde::Deserialize;
use time::OffsetDateTime;
use tokio::sync::Notify;
use tokio::{task, time as tokio_time};
use tokio_util::sync::CancellationToken;

use crate::archive::{self, Archive};
use crate::feed;
use crate::http::{Client, FetchError, Fetched};
use crate::model::{self, AuctionsPage, Envelope};
use crate::spool::{Job, Meta, Priority, Spool};
use crate::status::{FetchRow, Status};

/// What every poller shares.
#[derive(Debug)]
pub struct Ctx {
    /// The connection for every poll but full sweeps.
    pub client: Client,
    /// A separate connection for full sweeps, so their bulk downloads never queue ahead of the
    /// sales and bazaar polls on one HTTP/2 connection.
    pub sweep_client: Client,
    /// Where every response goes first.
    #[cfg_attr(
        dylint_lib = "trellis_lints",
        expect(
            shared_ownership,
            reason = "each append runs on a blocking thread that outlives the poll's borrow, and \
                      shutdown finishes the archive after the pollers stop"
        )
    )]
    pub archive: Arc<Archive>,
    /// Where new snapshots wait for the database. None on a secondary collector.
    #[cfg_attr(
        dylint_lib = "trellis_lints",
        expect(
            shared_ownership,
            reason = "the ingest worker drains the spool the pollers fill, and each push runs on a \
                      blocking thread that outlives the poll's borrow"
        )
    )]
    pub spool: Option<Arc<Spool>>,
    /// The collector's runtime state.
    pub status: Status,
    /// Wakes the ingest worker after each spooled snapshot.
    #[cfg_attr(
        dylint_lib = "trellis_lints",
        expect(
            shared_ownership,
            reason = "the pollers notify and the ingest worker waits on the one `Notify`, each in \
                      a task of its own"
        )
    )]
    pub wake: Arc<Notify>,
    /// Cancelled when the collector stops.
    pub shutdown: CancellationToken,
    /// How long from the start of one full sweep to the next.
    pub sweep_interval: Duration,
}

/// Pages beyond page 0 read when every listing on the earlier pages changed since the last
/// snapshot. Page 0 alone normally covers about 17 minutes of changes.
const MAX_DELTA_PAGES: usize = 10;
const SWEEP_CONCURRENCY: usize = 8;

/// The latest snapshot of a feed as a poll read it.
struct SnapshotRead {
    poll: Poll,
    fetched: Option<Fetched>,
}

impl Ctx {
    /// Fetches `url`, archives the response, and records the attempt. Returns the response for
    /// any HTTP status, or None when no response arrived.
    async fn fetch(&self, feed: &'static str, url: &str) -> Option<Fetched> {
        self.status.feed(feed, |s| {
            s.last_attempt_at = Some(OffsetDateTime::now_utc());
        });
        let client = if feed == feed::SWEEP {
            &self.sweep_client
        } else {
            &self.client
        };
        match client.get(url, timeout_for(feed)).await {
            Ok(fetched) => {
                let api_last_updated = serde_json::from_slice::<Envelope>(&fetched.body)
                    .ok()
                    .and_then(|e| e.last_updated)
                    .and_then(model::ms_to_time);
                self.archive(feed, &fetched).await;
                self.status.record_fetch(FetchRow {
                    feed,
                    url: url.to_owned(),
                    started_at: fetched.started_at,
                    duration_ms: fetched.duration_ms,
                    http_status: Some(fetched.status),
                    body_bytes: Some(fetched.body.len()),
                    api_last_updated,
                    cache_age_s: fetched.header_secs("age"),
                    error: None,
                });
                self.status.feed(feed, |s| {
                    if fetched.ok() {
                        s.last_success_at = Some(fetched.started_at);
                        s.consecutive_failures = 0;
                    } else {
                        s.consecutive_failures += 1;
                        s.last_error = Some(format!("HTTP {}", fetched.status));
                    }
                });
                Some(fetched)
            }
            Err(FetchError {
                started_at,
                duration_ms,
                error,
            }) => {
                let message = format!("{error:#}");
                tracing::warn!(feed, url, error = %message, "request failed");
                self.status.record_fetch(FetchRow {
                    feed,
                    url: url.to_owned(),
                    started_at,
                    duration_ms,
                    http_status: None,
                    body_bytes: None,
                    api_last_updated: None,
                    cache_age_s: None,
                    error: Some(message.clone()),
                });
                self.status.feed(feed, |s| {
                    s.consecutive_failures += 1;
                    s.last_error = Some(message);
                });
                None
            }
        }
    }

    /// Fetches a feed's latest snapshot from `path`. Hypixel sometimes answers 200 without a
    /// snapshot, such as an empty body, and Cloudflare then serves that answer to every request for
    /// the rest of the period, so a retry cannot help. Only then is the unversioned copy asked
    /// once.
    async fn fetch_snapshot(
        &self,
        feed: &'static str,
        path: &str,
        cadence: &Cadence,
    ) -> SnapshotRead {
        let fetched = self.fetch(feed, &self.client.url(path)).await;
        self.note_implausible_stamp(feed, fetched.as_ref(), cadence);
        let poll = poll_of(fetched.as_ref(), cadence, now_ms());
        if matches!(poll, Poll::Failed { .. })
            && fetched.as_ref().is_some_and(Fetched::ok)
            && let Some(url) = self.client.fallback_url(path)
        {
            let fallback = self.fetch(feed, &url).await;
            self.note_implausible_stamp(feed, fallback.as_ref(), cadence);
            let fallback_poll = poll_of(fallback.as_ref(), cadence, now_ms());
            if !matches!(fallback_poll, Poll::Failed { .. }) {
                tracing::info!(feed, url, "read the snapshot from the fallback URL");
                return SnapshotRead {
                    poll: fallback_poll,
                    fetched: fallback,
                };
            }
        }
        SnapshotRead { poll, fetched }
    }

    /// Logs a snapshot stamp [`too_far_ahead`] and shows it as the feed's last error in `/status`,
    /// since the poll then fails although the request itself succeeded.
    fn note_implausible_stamp(
        &self,
        feed: &'static str,
        fetched: Option<&Fetched>,
        cadence: &Cadence,
    ) {
        let now = now_ms();
        if let Some(f) = fetched
            && let Some(stamp) = snapshot_stamp(f)
            && too_far_ahead(stamp, cadence, now)
        {
            tracing::warn!(
                feed,
                url = f.url,
                stamp,
                now,
                "ignored a snapshot stamped far past the local clock"
            );
            self.status.feed(feed, |s| {
                s.last_error = Some(format!(
                    "ignored a snapshot stamped {stamp}, far past the local clock ({now})"
                ));
            });
        }
    }

    async fn archive(&self, feed: &'static str, fetched: &Fetched) {
        let line = archive::encode_line(Some(self.archive.run()), feed, fetched);
        let archive = self.archive.clone();
        let at = fetched.started_at;
        let result = task::spawn_blocking(move || archive.append(feed, at, &line)).await;
        let error = match result {
            Ok(Ok(())) => return,
            Ok(Err(e)) => e.to_string(),
            Err(e) => e.to_string(),
        };
        tracing::error!(feed, error = %error, "could not write to the raw archive");
        self.status.archive_error(format!("{feed}: {error}"));
    }

    async fn spool(&self, job: Job, fetched_at: OffsetDateTime, body: Vec<u8>, priority: Priority) {
        let Some(spool) = self.spool.clone() else {
            return;
        };
        let meta = Meta { job, fetched_at };
        let kind = format!("{:?}", meta.job);
        let result = task::spawn_blocking(move || spool.push(&meta, &body, priority)).await;
        match result {
            Ok(Ok(())) => self.wake.notify_one(),
            Ok(Err(e)) => {
                tracing::error!(job = kind, error = %e, "could not spool a snapshot; it is only in the raw archive");
                self.status.ingest(|s| s.spool_refusals += 1);
            }
            Err(e) => {
                tracing::error!(job = kind, error = %e, "spool task failed");
                self.status.ingest(|s| s.spool_refusals += 1);
            }
        }
    }

    /// Waits for `wait`. False when shutdown came first.
    async fn sleep(&self, wait: Duration) -> bool {
        let cancelled = pin!(self.shutdown.cancelled());
        let slept = pin!(tokio_time::sleep(wait));
        matches!(future::select(cancelled, slept).await, Either::Right(_))
    }
}

/// Request timeouts, each under its feed's refresh period: sales and page 0 refresh every 60 s, the
/// bazaar every 20 s.
fn timeout_for(feed: &str) -> Duration {
    Duration::from_secs(match feed {
        feed::BAZAAR => 12,
        feed::ENDED => 20,
        feed::AUCTIONS => 25,
        feed::SWEEP => 45,
        _ => 60,
    })
}

/// Schedules polls of a feed that publishes a snapshot every `period`, read through Cloudflare.
///
/// Cloudflare keeps each copy for one period and shares it with every client. A request that
/// arrives after the copy expired makes Cloudflare fetch from Hypixel; if Hypixel has not published
/// the next snapshot yet, that fetch gets the old one, Cloudflare serves it for a whole period, and
/// the new snapshot is never seen. So:
///
/// - no request goes out in the first sixth of the period after the next expected stamp (the
///   floor), so a fetch our request causes gets that snapshot rather than locking in the old one;
///   and
/// - after each response the next request waits until the copy it saw expires, which its `age`
///   header gives, because asking sooner only returns the same copy.
///
/// Another client can still make Cloudflare fetch too early; that snapshot is then lost to
/// everyone, and `feed_gaps` shows it. With no other client, each fetch we cause starts a new copy
/// lifetime, so the fetch time drifts later by the response time every period and one snapshot is
/// skipped each time the drift passes a whole period: about once in (period / response time)
/// periods. A copy cannot be fetched again before it expires, so no schedule avoids that.
#[derive(Debug, Clone)]
pub struct Cadence {
    period_ms: i64,
    floor_ms: i64,
    retry_ms: i64,
    last: Option<i64>,
    failures: u32,
}

/// What one poll found.
#[derive(Debug)]
pub enum Poll {
    /// A snapshot newer than any seen.
    New {
        /// The snapshot's stamp in Unix milliseconds.
        stamp: i64,
        /// The response's `age` header in seconds.
        age_s: Option<u64>,
    },
    /// A snapshot already seen, or an older one.
    Same {
        /// The response's `age` header in seconds.
        age_s: Option<u64>,
    },
    /// No snapshot: no response, an error status, or a 200 without one.
    Failed {
        /// The response's `retry-after` header in seconds.
        retry_after_s: Option<u64>,
    },
}

impl Cadence {
    /// A schedule for a feed that publishes every `period`, with no snapshot seen yet.
    #[must_use]
    pub fn new(period: Duration) -> Self {
        let period_ms = i64::try_from(period.as_millis()).unwrap_or(i64::MAX);
        Self {
            period_ms,
            floor_ms: (period_ms / 6).max(1_000),
            retry_ms: (period_ms / 10).clamp(1_000, 3_000),
            last: None,
            failures: 0,
        }
    }

    /// The stamp of the newest snapshot seen, in Unix milliseconds.
    #[must_use]
    pub fn last(&self) -> Option<i64> {
        self.last
    }

    /// Moves `t` past the floor when it falls in the first sixth of the period after the stamp
    /// that follows the last one seen. A later `t` is already behind, so it is left alone to
    /// catch up.
    fn outside_floor(&self, t: i64) -> i64 {
        match self.last {
            Some(last)
                if (last.saturating_add(self.period_ms)
                    ..last.saturating_add(self.period_ms + self.floor_ms))
                    .contains(&t) =>
            {
                last.saturating_add(self.period_ms + self.floor_ms)
            }
            _ => t,
        }
    }

    /// Classifies a response's snapshot stamp as new or already seen.
    #[must_use]
    pub fn classify(&self, last_updated: i64, age_s: Option<u64>) -> Poll {
        if self.last.is_none_or(|last| last_updated > last) {
            Poll::New {
                stamp: last_updated,
                age_s,
            }
        } else {
            Poll::Same { age_s }
        }
    }

    /// The latest the copy a response came from can expire. `age` is whole seconds since
    /// Cloudflare fetched it, so it was fetched no later than `sent - age`; at age 0 the fetch may
    /// be the one our request caused, which finished before the response arrived at `now`. A copy
    /// older than a period is kept longer than usual (a Cloudflare rule, or stale serving while
    /// Hypixel is down); when it ends is unknown, so the retry is at the usual pace.
    fn copy_expires(&self, sent_ms: i64, now_ms: i64, age_s: Option<u64>) -> i64 {
        match age_s {
            None => sent_ms + self.retry_ms,
            Some(0) => now_ms + self.period_ms,
            Some(age)
                if i64::try_from(age.saturating_mul(1_000)).unwrap_or(i64::MAX)
                    >= self.period_ms =>
            {
                sent_ms + self.retry_ms
            }
            Some(age) => {
                sent_ms - i64::try_from(age.saturating_mul(1_000)).unwrap_or(0) + self.period_ms
            }
        }
    }

    /// Records the result of a poll sent at `sent_ms`, and returns how long to wait from `now_ms`
    /// before the next one.
    pub fn observe(&mut self, sent_ms: i64, now_ms: i64, poll: &Poll) -> Duration {
        let target = match *poll {
            Poll::New { stamp, age_s } => {
                self.last = Some(stamp);
                self.failures = 0;
                self.copy_expires(sent_ms, now_ms, age_s)
                    .max(stamp.saturating_add(self.period_ms + self.floor_ms))
            }
            Poll::Same { age_s } => {
                self.failures = 0;
                self.copy_expires(sent_ms, now_ms, age_s)
            }
            Poll::Failed { retry_after_s } => {
                self.failures = self.failures.saturating_add(1);
                let backoff = 1_000_i64 << self.failures.min(4);
                let cap = (self.period_ms / 2).clamp(5_000, 30_000);
                let wait = retry_after_s
                    .map_or(backoff, |s| {
                        i64::try_from(s.saturating_mul(1_000)).unwrap_or(i64::MAX)
                    })
                    .min(cap);
                now_ms + wait
            }
        };
        let at = self.outside_floor(target.max(now_ms + 250));
        Duration::from_millis(
            u64::try_from((at - now_ms).clamp(250, 2 * self.period_ms)).unwrap_or(250),
        )
    }
}

fn now_ms() -> i64 {
    i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000).unwrap_or(i64::MAX)
}

/// The snapshot stamp of a 200 response, or None when the body holds no snapshot.
fn snapshot_stamp(fetched: &Fetched) -> Option<i64> {
    match serde_json::from_slice::<Envelope>(&fetched.body) {
        Ok(Envelope {
            success: Some(true) | None,
            last_updated: Some(stamp),
        }) if fetched.ok() => Some(stamp),
        _ => None,
    }
}

/// Whether `stamp` is too far past the local clock at `now_ms` to be a real snapshot: more than
/// [`model::MAX_STAMP_LEAD_MS`] or a period ahead, whichever is longer.
fn too_far_ahead(stamp: i64, cadence: &Cadence, now_ms: i64) -> bool {
    stamp > now_ms.saturating_add(cadence.period_ms.max(model::MAX_STAMP_LEAD_MS))
}

/// How a response polled at `now_ms` counts. A stamp [`too_far_ahead`] can't be a real snapshot,
/// and taking it as the newest would make every real one after it look old, so it counts as a
/// failed poll, retried as one.
fn poll_of(fetched: Option<&Fetched>, cadence: &Cadence, now_ms: i64) -> Poll {
    match fetched {
        Some(f) if f.ok() => match snapshot_stamp(f) {
            Some(stamp) if too_far_ahead(stamp, cadence, now_ms) => Poll::Failed {
                retry_after_s: None,
            },
            Some(stamp) => cadence.classify(stamp, f.header_secs("age")),
            None => Poll::Failed {
                retry_after_s: None,
            },
        },
        Some(f) => Poll::Failed {
            retry_after_s: f.header_secs("retry-after"),
        },
        None => Poll::Failed {
            retry_after_s: None,
        },
    }
}

/// Polls a feed that publishes every `period` and spools each new snapshot as `job`.
///
/// # Errors
///
/// None so far: it returns `Ok` at its next wait after shutdown starts, once any fetch in flight finishes.
#[cfg_attr(
    dylint_lib = "trellis_lints",
    expect(
        shared_ownership,
        reason = "each poller runs as a spawned task the supervisor restarts, so it owns a share of \
                  the context rather than a borrow"
    )
)]
pub async fn snapshots(
    ctx: Arc<Ctx>,
    feed: &'static str,
    path: &str,
    period: Duration,
    job: Job,
    priority: Priority,
) -> anyhow::Result<()> {
    let mut cadence = Cadence::new(period);
    loop {
        ctx.status.beat(task_name(feed));
        let sent = now_ms();
        let SnapshotRead { poll, fetched } = ctx.fetch_snapshot(feed, path, &cadence).await;
        if let (Poll::New { stamp, .. }, Some(f)) = (&poll, fetched) {
            ctx.status.feed(feed, |s| {
                s.last_new_snapshot_at = Some(f.started_at);
                s.api_last_updated = model::ms_to_time(*stamp);
            });
            ctx.spool(job.clone(), f.started_at, f.body.to_vec(), priority)
                .await;
        }
        let wait = cadence.observe(sent, now_ms(), &poll);
        if !ctx.sleep(wait).await {
            return Ok(());
        }
    }
}

/// Polls page 0 of the active auctions every snapshot, reading further pages while every listing
/// on them changed since the previous snapshot.
///
/// # Errors
///
/// None so far: it returns `Ok` at its next wait after shutdown starts, once any fetch in flight finishes.
#[cfg_attr(
    dylint_lib = "trellis_lints",
    expect(
        shared_ownership,
        reason = "each poller runs as a spawned task the supervisor restarts, so it owns a share of \
                  the context rather than a borrow"
    )
)]
pub async fn auction_changes(ctx: Arc<Ctx>) -> anyhow::Result<()> {
    let mut cadence = Cadence::new(Duration::from_secs(60));
    loop {
        ctx.status.beat("auctions");
        let sent = now_ms();
        let previous = cadence.last();
        let SnapshotRead { poll, fetched } = ctx
            .fetch_snapshot(feed::AUCTIONS, "skyblock/auctions", &cadence)
            .await;
        if let (Poll::New { stamp, .. }, Some(first)) = (&poll, fetched) {
            let fetched_at = first.started_at;
            let mut bodies = vec![first.body.to_vec()];
            let mut page = 0;
            while page < MAX_DELTA_PAGES
                && previous.is_some_and(|p| fully_changed(bodies.last(), p))
            {
                page += 1;
                let url = ctx.client.url(&format!("skyblock/auctions?page={page}"));
                match ctx.fetch(feed::AUCTIONS, &url).await {
                    Some(f) if snapshot_stamp(&f).is_some() => bodies.push(f.body.to_vec()),
                    _ => break,
                }
            }
            ctx.status.feed(feed::AUCTIONS, |s| {
                s.last_new_snapshot_at = Some(fetched_at);
                s.api_last_updated = model::ms_to_time(*stamp);
            });
            ctx.spool(
                Job::AuctionsDelta,
                fetched_at,
                json_array(&bodies),
                Priority::Optional,
            )
            .await;
        }
        let wait = cadence.observe(sent, now_ms(), &poll);
        if !ctx.sleep(wait).await {
            return Ok(());
        }
    }
}

/// True when every listing on the page changed after `previous`, so older changes may be on the
/// next page.
fn fully_changed(body: Option<&Vec<u8>>, previous: i64) -> bool {
    body.and_then(|b| serde_json::from_slice::<AuctionsPage>(b).ok())
        .and_then(|p| p.auctions.iter().map(|a| a.last_updated.unwrap_or(0)).min())
        .is_some_and(|oldest| oldest > previous)
}

fn json_array(bodies: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bodies.iter().map(Vec::len).sum::<usize>() + bodies.len() + 2);
    out.push(b'[');
    for (i, body) in bodies.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(body);
    }
    out.push(b']');
    out
}

struct SweepPage {
    stamp: Option<i64>,
    total_pages: Option<i64>,
    body: Option<Vec<u8>>,
}

/// Reads every auction page on an interval. Cloudflare caches each `?page=N` copy separately and
/// for up to 300 s, so a sweep often mixes snapshots; the two-sweep rule in the database copes.
///
/// # Errors
///
/// None so far: a failed sweep is logged, and it returns `Ok` at its next wait after shutdown starts, once any fetch in flight finishes.
#[cfg_attr(
    dylint_lib = "trellis_lints",
    expect(
        shared_ownership,
        reason = "each poller runs as a spawned task the supervisor restarts, so it owns a share of \
                  the context rather than a borrow"
    )
)]
pub async fn sweeps(ctx: Arc<Ctx>) -> anyhow::Result<()> {
    loop {
        ctx.status.beat("sweep");
        let started = Instant::now();
        if let Err(e) = sweep_once(&ctx).await {
            tracing::warn!(error = %format!("{e:#}"), "sweep failed");
        }
        let wait = ctx.sweep_interval.saturating_sub(started.elapsed());
        let mut left = wait;
        while !left.is_zero() {
            let step = left.min(Duration::from_secs(30));
            if !ctx.sleep(step).await {
                return Ok(());
            }
            ctx.status.beat("sweep");
            left -= step;
        }
        if ctx.shutdown.is_cancelled() {
            return Ok(());
        }
    }
}

async fn sweep_page(ctx: &Ctx, page: i64) -> SweepPage {
    let url = ctx.client.url(&format!("skyblock/auctions?page={page}"));
    ctx.status.beat("sweep");
    match ctx.fetch(feed::SWEEP, &url).await {
        Some(f) if f.ok() => {
            let envelope: Option<AuctionsEnvelope> = serde_json::from_slice(&f.body).ok();
            SweepPage {
                stamp: envelope.as_ref().and_then(|e| e.last_updated),
                total_pages: envelope.and_then(|e| e.total_pages),
                body: Some(f.body.to_vec()),
            }
        }
        _ => SweepPage {
            stamp: None,
            total_pages: None,
            body: None,
        },
    }
}

#[derive(Deserialize)]
struct AuctionsEnvelope {
    #[serde(rename = "lastUpdated")]
    last_updated: Option<i64>,
    #[serde(rename = "totalPages")]
    total_pages: Option<i64>,
}

/// Reads one sweep and spools it, unless shutdown starts during the wait before the retry, when the
/// pages read so far stay only in the archive. Pages that failed get one more try. Stale pages are not
/// re-read: Cloudflare keeps each `?page=N` copy for up to 300 s, so a quick re-read returns the
/// same copy.
async fn sweep_once(ctx: &Ctx) -> anyhow::Result<()> {
    use futures::StreamExt as _;

    let fetched_at = OffsetDateTime::now_utc();
    let first = sweep_page(ctx, 0).await;
    let total = first
        .total_pages
        .filter(|t| (1..=1_000).contains(t))
        .ok_or_else(|| anyhow::anyhow!("page 0 did not report totalPages"))?;
    let mut pages: Vec<SweepPage> = Vec::with_capacity(usize::try_from(total).unwrap_or(0));
    pages.push(first);
    let rest: Vec<SweepPage> = stream::iter(1..total)
        .map(|p| sweep_page(ctx, p))
        .buffered(SWEEP_CONCURRENCY)
        .collect()
        .await;
    pages.extend(rest);

    let failed: Vec<usize> = (0..pages.len())
        .filter(|&i| pages[i].body.is_none())
        .collect();
    if !failed.is_empty() {
        if !ctx.sleep(Duration::from_secs(5)).await {
            return Ok(());
        }
        let retried: Vec<(usize, SweepPage)> = stream::iter(failed)
            .map(|i| async move { (i, sweep_page(ctx, i64::try_from(i).unwrap_or(0)).await) })
            .buffered(SWEEP_CONCURRENCY)
            .collect()
            .await;
        for (i, page) in retried {
            pages[i] = page;
        }
    }

    let stamps: Vec<Option<i64>> = pages.iter().map(|p| p.stamp).collect();
    let complete = pages
        .iter()
        .all(|p| p.body.is_some() && p.total_pages == Some(total));
    let consistent = complete && stamps.iter().all(|s| *s == stamps[0]);
    let bodies: Vec<Vec<u8>> = pages.into_iter().filter_map(|p| p.body).collect();
    if bodies.is_empty() {
        anyhow::bail!("no page could be read");
    }
    tracing::info!(
        pages = bodies.len(),
        total,
        complete,
        consistent,
        "sweep read"
    );
    ctx.status.feed(feed::SWEEP, |s| {
        s.last_new_snapshot_at = Some(fetched_at);
        s.api_last_updated = stamps
            .iter()
            .flatten()
            .max()
            .copied()
            .and_then(model::ms_to_time);
    });
    ctx.spool(
        Job::AuctionsSweep {
            complete,
            consistent,
        },
        fetched_at,
        json_array(&bodies),
        Priority::Optional,
    )
    .await;
    Ok(())
}

/// Resources that change rarely. Each is fetched on its interval and stored when it changed. With
/// `election_only`, as on a secondary collector, only the election, whose vote counts are lost
/// once missed: every other resource can be read again later.
///
/// # Errors
///
/// None so far: it returns `Ok` at its next wait after shutdown starts, once any fetch in flight finishes.
#[cfg_attr(
    dylint_lib = "trellis_lints",
    expect(
        shared_ownership,
        reason = "each poller runs as a spawned task the supervisor restarts, so it owns a share of \
                  the context rather than a borrow"
    )
)]
pub async fn slow(ctx: Arc<Ctx>, election_only: bool) -> anyhow::Result<()> {
    let all: [(&'static str, &str, Duration, Job); 4] = [
        (
            feed::ELECTION,
            "resources/skyblock/election",
            Duration::from_secs(60),
            Job::Election,
        ),
        (
            feed::FIRESALES,
            "skyblock/firesales",
            Duration::from_secs(300),
            resource(feed::FIRESALES),
        ),
        (
            feed::ITEMS,
            "resources/skyblock/items",
            Duration::from_secs(3_600),
            resource(feed::ITEMS),
        ),
        (
            feed::COLLECTIONS,
            "resources/skyblock/collections",
            Duration::from_secs(3_600),
            resource(feed::COLLECTIONS),
        ),
    ];
    let resources = if election_only { &all[..1] } else { &all[..] };
    let mut due = vec![Instant::now(); resources.len()];
    loop {
        ctx.status.beat("slow");
        for (i, (feed, path, every, job)) in resources.iter().enumerate() {
            if Instant::now() < due[i] {
                continue;
            }
            let fetched = ctx.fetch(feed, &ctx.client.url(path)).await;
            match fetched {
                Some(f) if f.ok() => {
                    ctx.status
                        .feed(feed, |s| s.last_new_snapshot_at = Some(f.started_at));
                    let priority = if *feed == feed::ELECTION {
                        Priority::Essential
                    } else {
                        Priority::Optional
                    };
                    ctx.spool(job.clone(), f.started_at, f.body.to_vec(), priority)
                        .await;
                    due[i] = Instant::now() + *every;
                }
                _ => due[i] = Instant::now() + Duration::from_secs(30),
            }
        }
        if !ctx.sleep(Duration::from_secs(5)).await {
            return Ok(());
        }
    }
}

fn resource(name: &str) -> Job {
    Job::Resource {
        name: name.to_owned(),
    }
}

/// The heartbeat name for a feed's poller.
fn task_name(feed: &'static str) -> &'static str {
    match feed {
        feed::ENDED => "ended",
        feed::BAZAAR => "bazaar",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeSet, HashMap};

    use bytes::Bytes;
    use serde_json::Value;

    const MIN: i64 = 60_000;

    #[test]
    fn a_new_snapshot_waits_for_the_floor_of_the_next_period() {
        let mut c = Cadence::new(Duration::from_secs(60));
        let stamp = 1_000 * MIN;
        let p = c.classify(stamp, Some(2));
        let wait = c.observe(stamp + 10_000, stamp + 10_150, &p);
        assert_eq!(
            wait,
            Duration::from_millis(70_000 - 10_150),
            "seen at +10 s in a copy Cloudflare fetched 2 s earlier, which expires at +68 s, so \
             the floor at +70 s is later"
        );
    }

    #[test]
    fn a_new_snapshot_waits_for_its_copy_to_expire_when_that_is_later() {
        let mut c = Cadence::new(Duration::from_secs(60));
        let stamp = 1_000 * MIN;
        let p = c.classify(stamp, Some(5));
        let wait = c.observe(stamp + 25_000, stamp + 25_100, &p);
        assert_eq!(
            wait,
            Duration::from_millis(80_000 - 25_100),
            "Cloudflare fetched this copy at +20 s, so nothing new shows before +80 s"
        );
    }

    /// Another client made Cloudflare fetch at +68 s and got the old snapshot again: the next one
    /// cannot be seen before that copy expires at +128 s.
    #[test]
    fn the_same_snapshot_waits_for_its_copy_to_expire() {
        let mut c = Cadence::new(Duration::from_secs(60));
        let stamp = 1_000 * MIN;
        let p = c.classify(stamp, Some(0));
        c.observe(stamp + 10_000, stamp + 10_100, &p);
        let p = c.classify(stamp, Some(2));
        let wait = c.observe(stamp + 70_000, stamp + 70_100, &p);
        assert_eq!(wait, Duration::from_millis(128_000 - 70_100));
    }

    #[test]
    fn retries_and_backoff_never_land_in_the_floor() {
        let mut c = Cadence::new(Duration::from_secs(20));
        let stamp = 1_000_000;
        let p = c.classify(stamp, Some(0));
        c.observe(stamp + 4_000, stamp + 4_000, &p);
        let wait = c.observe(
            stamp + 19_000,
            stamp + 19_000,
            &Poll::Failed {
                retry_after_s: None,
            },
        );
        assert_eq!(
            wait,
            Duration::from_millis(20_000 + 3_333 - 19_000),
            "a failure 1 s before the next stamp would retry 1 s after it, so it waits for the \
             floor"
        );
        let wait = c.observe(stamp + 39_500, stamp + 39_500, &Poll::Same { age_s: None });
        assert_eq!(
            wait,
            Duration::from_millis(2_000),
            "once a whole period behind, a retry goes out after the retry interval to catch up"
        );
    }

    /// A copy older than a period ends at an unknown time, so retries keep the usual pace
    /// rather than hammering.
    #[test]
    fn a_copy_older_than_a_period_retries_at_the_usual_pace() {
        let mut c = Cadence::new(Duration::from_secs(60));
        let stamp = 1_000 * MIN;
        let p = c.classify(stamp, Some(0));
        c.observe(stamp + 10_000, stamp + 10_100, &p);
        let p = c.classify(stamp, Some(150));
        let wait = c.observe(stamp + 200_000, stamp + 200_100, &p);
        assert_eq!(wait, Duration::from_millis(3_000 - 100));
    }

    /// A stamp at either end of `i64`, from a broken or hostile API answer, must not overflow:
    /// release builds check overflow and abort, which would stop the whole collector.
    #[test]
    fn extreme_stamps_schedule_without_overflow() {
        for stamp in [i64::MAX, i64::MIN, i64::MAX - 1] {
            let mut cadence = Cadence::new(Duration::from_secs(60));
            let poll = cadence.classify(stamp, Some(0));
            let wait = cadence.observe(1_000 * MIN, 1_000 * MIN + 100, &poll);
            assert!(
                (Duration::from_millis(250)..=Duration::from_secs(120)).contains(&wait),
                "stamp {stamp}: the wait {wait:?} should stay within the usual bounds"
            );
            let wait = cadence.observe(
                1_000 * MIN + 200,
                1_000 * MIN + 300,
                &Poll::Failed {
                    retry_after_s: None,
                },
            );
            assert!(
                wait <= Duration::from_secs(120),
                "stamp {stamp}: a failure after it should schedule normally, not wait {wait:?}"
            );
        }
    }

    /// A 200 answer whose body holds `stamp`, as the API sends a snapshot.
    fn answer(stamp: i64) -> Fetched {
        Fetched {
            url: "https://example.invalid/v2/skyblock/bazaar".to_owned(),
            started_at: OffsetDateTime::UNIX_EPOCH,
            duration_ms: 1,
            status: 200,
            headers: HashMap::new(),
            body: Bytes::from(format!(r#"{{"success":true,"lastUpdated":{stamp}}}"#)),
        }
    }

    /// One absurd stamp from a broken or hostile API answer must not stop the feed: it is a failed
    /// poll, and the real snapshots after it still count as new.
    #[test]
    fn a_stamp_far_past_the_clock_fails_and_later_real_snapshots_still_count() {
        let mut cadence = Cadence::new(Duration::from_secs(60));
        let now = 1_000 * MIN;
        let real = poll_of(Some(&answer(now - 2_000)), &cadence, now);
        assert!(
            matches!(real, Poll::New { .. }),
            "a current stamp is a new snapshot"
        );
        cadence.observe(now - 1_000, now, &real);
        for absurd in [i64::MAX, now + 11 * MIN] {
            let poll = poll_of(Some(&answer(absurd)), &cadence, now);
            assert!(
                matches!(poll, Poll::Failed { .. }),
                "a stamp of {absurd}, more than ten minutes past the clock, is a failed poll"
            );
            cadence.observe(now, now + 10, &poll);
        }
        let next = poll_of(Some(&answer(now + 58_000)), &cadence, now + 60_000);
        assert!(
            matches!(next, Poll::New { .. }),
            "the next real snapshot still counts as new"
        );
        let lagging = poll_of(Some(&answer(now + 5 * MIN)), &cadence, now);
        assert!(
            matches!(lagging, Poll::New { .. }),
            "a stamp minutes ahead of a lagging local clock is still taken"
        );
    }

    #[test]
    fn failures_back_off_but_never_past_half_a_period() {
        let mut c = Cadence::new(Duration::from_secs(60));
        let failed = Poll::Failed {
            retry_after_s: None,
        };
        let mut waits = Vec::new();
        for _ in 0..6 {
            waits.push(c.observe(0, 0, &failed));
        }
        assert_eq!(waits[0], Duration::from_secs(2));
        assert_eq!(waits[1], Duration::from_secs(4));
        assert!(waits.iter().all(|w| *w <= Duration::from_secs(30)));
        let w = c.observe(
            0,
            0,
            &Poll::Failed {
                retry_after_s: Some(600),
            },
        );
        assert_eq!(w, Duration::from_secs(30));
    }

    /// The copy Cloudflare holds: when it fetched it, and the snapshot stamp it holds.
    #[derive(Clone, Copy)]
    struct CdnCopy {
        fetched: i64,
        stamp: i64,
    }

    /// What a request to [`Cdn`] gets: the snapshot stamp, and the `age` header in seconds.
    struct CdnResponse {
        stamp: i64,
        age_s: u64,
    }

    /// One Cloudflare copy, kept for a period and fetched again by the first request after it
    /// expires, as measured. Hypixel stamps snapshot k at k * period plus up to 50 ms of jitter
    /// and can serve it `lag` later.
    struct Cdn {
        period: i64,
        lag: i64,
        copy: Option<CdnCopy>,
    }

    impl Cdn {
        fn stamp(&self, k: i64) -> i64 {
            k * self.period + (k * 37).rem_euclid(101) - 50
        }

        fn published_at(&self, t: i64) -> i64 {
            let mut k = (t + 50) / self.period;
            while self.stamp(k) + self.lag > t {
                k -= 1;
            }
            self.stamp(k)
        }

        fn request(&mut self, t: i64) -> CdnResponse {
            let expired = self.copy.is_none_or(|copy| t >= copy.fetched + self.period);
            if expired {
                self.copy = Some(CdnCopy {
                    fetched: t,
                    stamp: self.published_at(t),
                });
            }
            let copy = self.copy.unwrap();
            CdnResponse {
                stamp: copy.stamp,
                age_s: u64::try_from((t - copy.fetched) / 1_000).unwrap(),
            }
        }
    }

    /// Round 6's model: with no other client, our own requests are the ones Cloudflare fetches
    /// for. No request lands in the floor, and snapshots are skipped only when the fetch time has
    /// drifted by a whole period: at most once per (period / response time) periods.
    #[test]
    fn alone_behind_the_cache_losses_stay_within_the_drift_bound() {
        const LATENCY: i64 = 150;
        const PERIODS: i64 = 1_000;
        for (period, lag) in [(60_000, 7_000), (20_000, 2_000)] {
            for start in [3_217, 9_900, 41_234, 59_999] {
                let mut cdn = Cdn {
                    period,
                    lag,
                    copy: None,
                };
                let mut c = Cadence::new(Duration::from_millis(u64::try_from(period).unwrap()));
                let mut seen = BTreeSet::new();
                let mut t = 1_000 * period + start;
                let end = t + PERIODS * period;
                while t < end {
                    if let Some(last) = c.last() {
                        assert!(
                            !(last + period..last + period + period / 6).contains(&t),
                            "request in the floor"
                        );
                    }
                    let response = cdn.request(t);
                    seen.insert(response.stamp);
                    let p = c.classify(response.stamp, Some(response.age_s));
                    let now = t + LATENCY;
                    t = now + i64::try_from(c.observe(t, now, &p).as_millis()).unwrap();
                }
                let first_k = 1_000 + start / period + 2;
                let last_k = (end - lag) / period - 1;
                let missed = (first_k..=last_k)
                    .filter(|&k| !seen.contains(&cdn.stamp(k)))
                    .count();
                let bound = usize::try_from(PERIODS * LATENCY / period + 1).unwrap();
                assert!(
                    missed <= bound,
                    "period {period}, start {start}: missed {missed}, bound {bound}"
                );
            }
        }
    }

    #[test]
    fn page_fully_changed_only_when_every_listing_is_newer() {
        let page =
            br#"{"auctions":[{"uuid":"a","last_updated":200},{"uuid":"b","last_updated":150}]}"#
                .to_vec();
        assert!(fully_changed(Some(&page), 100));
        assert!(!fully_changed(Some(&page), 150));
        assert!(!fully_changed(None, 100));
    }

    #[test]
    fn json_array_joins_bodies() {
        let joined = json_array(&[b"{\"a\":1}".to_vec(), b"{\"b\":2}".to_vec()]);
        let value: Value = serde_json::from_slice(&joined).unwrap();
        assert_eq!(value[1]["b"], 2);
    }
}
