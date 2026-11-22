//! Rebuilds database rows from the raw archive. Writes are idempotent, so replaying files that
//! are already in the database only fills in what is missing.
//!
//! Some writes depend on order: a listing's `first_seen_at` is fixed by its first write. So the
//! feeds replay merged in the order the live collector wrote them: a page-0 snapshot when it was
//! fetched, and a sweep once its last page was.
//!
//! A backfill writes only the sales, bazaar, page-0, and election snapshots the database lacks,
//! such as a secondary collector's copies of snapshots the main collector missed.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use serde::Deserialize;
use sqlx::PgPool;
use time::{Date, OffsetDateTime};
use tokio::time as tokio_time;

use crate::archive::{self, Record};
use crate::db::{Db, JobError};
use crate::spool::Job;
use crate::{feed, model};

/// Archived sweep pages fetched within this long of a sweep's first page belong to that sweep.
/// A sweep, retries included, takes about a minute; sweeps start 5 minutes apart.
const SWEEP_WINDOW: time::Duration = time::Duration::seconds(150);

/// What a replay or backfill did with the records it read.
#[derive(Default, Debug)]
pub struct Summary {
    /// Archive lines read.
    pub records: u64,
    /// Database writes made.
    pub written: u64,
    /// Records that weren't written: error responses, feeds with no database form, repeated
    /// page-0 snapshots, snapshots stamped far past their fetch, and in a backfill, snapshots the
    /// database already has.
    pub skipped: u64,
    /// Records the database refused, or whose body isn't what its feed sends.
    pub rejected: u64,
}

fn job_for(feed_name: &str) -> Option<Job> {
    Some(match feed_name {
        feed::ENDED => Job::Ended,
        feed::AUCTIONS => Job::AuctionsDelta,
        feed::BAZAAR => Job::Bazaar,
        feed::ELECTION => Job::Election,
        feed::ITEMS | feed::COLLECTIONS | feed::FIRESALES => Job::Resource {
            name: feed_name.to_owned(),
        },
        _ => return None,
    })
}

/// Whether a snapshot the pollers schedule by stamp (sales, the bazaar, and page 0 of the
/// auctions, not the later pages) holds a stamp more than [`model::MAX_STAMP_LEAD_MS`] past the
/// time its response arrived, `fetched_at` plus `duration_ms`, as the live collector compares. The
/// live collector ignores such an answer, though it archives it, so replay skips it too: taken as
/// the newest page 0, it would make replay skip every real one after it.
fn stamped_far_past(job: &Job, body: &[u8], fetched_at: OffsetDateTime, duration_ms: u64) -> bool {
    if !matches!(job, Job::Ended | Job::AuctionsDelta | Job::Bazaar) {
        return false;
    }
    let Ok(envelope) = serde_json::from_slice::<PageEnvelope>(body) else {
        return false;
    };
    if *job == Job::AuctionsDelta && !matches!(envelope.page, None | Some(0)) {
        return false;
    }
    let arrived_ms = i64::try_from(fetched_at.unix_timestamp_nanos() / 1_000_000)
        .unwrap_or(i64::MAX)
        .saturating_add(i64::try_from(duration_ms).unwrap_or(i64::MAX));
    envelope
        .last_updated
        .is_some_and(|stamp| stamp > arrived_ms.saturating_add(model::MAX_STAMP_LEAD_MS))
}

/// Whether `body` is a page-0 snapshot no newer than `last_page_zero`, the last one written: the
/// live collector wrote each only once. Otherwise a page-0 snapshot becomes the last one written.
fn repeats_page_zero(last_page_zero: &mut Option<i64>, job: &Job, body: &[u8]) -> bool {
    if *job != Job::AuctionsDelta {
        return false;
    }
    let envelope = serde_json::from_slice::<PageEnvelope>(body).ok();
    if let Some(PageEnvelope {
        page: None | Some(0),
        last_updated: Some(stamp),
        ..
    }) = envelope
    {
        if last_page_zero.is_some_and(|last| stamp <= last) {
            return true;
        }
        *last_page_zero = Some(stamp);
    }
    false
}

#[derive(Deserialize)]
struct PageEnvelope {
    page: Option<i64>,
    #[serde(rename = "totalPages")]
    total_pages: Option<i64>,
    #[serde(rename = "lastUpdated")]
    last_updated: Option<i64>,
}

/// One archived page of a sweep.
struct SweepPage {
    total_pages: Option<i64>,
    last_updated: Option<i64>,
    body: Vec<u8>,
}

/// The pages of one archived sweep, keeping the last copy of each page as the live sweep did.
struct SweepGroup {
    path: PathBuf,
    started: OffsetDateTime,
    last_page_at: OffsetDateTime,
    pages: BTreeMap<i64, SweepPage>,
}

impl SweepGroup {
    fn into_write(self) -> Write {
        let total = self.pages.values().filter_map(|p| p.total_pages).max();
        let complete = total.is_some_and(|t| {
            (0..t).all(|p| self.pages.contains_key(&p))
                && self.pages.values().all(|p| p.total_pages == Some(t))
        });
        let stamps: HashSet<_> = self.pages.values().map(|p| p.last_updated).collect();
        let consistent = complete && stamps.len() == 1;
        let mut body = vec![b'['];
        for (i, page) in self.pages.into_values().enumerate() {
            if i > 0 {
                body.push(b',');
            }
            body.extend_from_slice(&page.body);
        }
        body.push(b']');
        Write {
            path: self.path,
            written_at: self.last_page_at,
            job: Job::AuctionsSweep {
                complete,
                consistent,
            },
            fetched_at: self.started,
            body,
        }
    }
}

/// One database write the live collector made, rebuilt from the archive.
struct Write {
    path: PathBuf,
    /// When the live collector made it, which orders writes across feeds.
    written_at: OffsetDateTime,
    job: Job,
    fetched_at: OffsetDateTime,
    body: Vec<u8>,
}

type Lines = Box<dyn Iterator<Item = io::Result<Vec<u8>>>>;

/// The archive file being read, and its remaining lines.
struct OpenFile {
    path: PathBuf,
    lines: Lines,
}

/// The run of the record read before, which marks restarts. Records gained a run with a new
/// binary, so a change to or from none is a restart too.
#[derive(Clone, Copy)]
enum PreviousRun {
    /// No record has been read yet.
    NoRecord,
    /// The previous record's run, None for a record from before records had one.
    Record(Option<i64>),
}

/// The writes in one feed's archive files, read in order across days. A restart empties the live
/// collector's last page-0 snapshot and drops the sweep it was in. Records name the process that
/// fetched them, which marks restarts; older records do not, and for them another file within the
/// same day is the only sign of one. At UTC midnight the archive starts a new file while the
/// collector keeps running, so a sweep running then continues into it.
struct FeedWrites {
    files: VecDeque<PathBuf>,
    current: Option<OpenFile>,
    run: PreviousRun,
    /// The newest page-0 snapshot read since the collector last started. The live collector writes
    /// page 0 only when its snapshot is newer than the last one it fetched, while the archive keeps
    /// every fetch; a repeat written after a sweep would clear the sweep's missing flags.
    last_page_zero: Option<i64>,
    sweep: Option<SweepGroup>,
    last_day: Option<PathBuf>,
    /// The next record is the first of a file in the same day directory as the previous file.
    at_same_day_file_start: bool,
    /// A record read past a write returned first, to process on the next call.
    held: Option<Vec<u8>>,
}

impl FeedWrites {
    fn new(files: Vec<PathBuf>) -> Self {
        Self {
            files: files.into(),
            current: None,
            run: PreviousRun::NoRecord,
            last_page_zero: None,
            sweep: None,
            last_day: None,
            at_same_day_file_start: false,
            held: None,
        }
    }

    /// The next non-empty line across the feed's files, or None after the last file.
    fn next_line(&mut self, summary: &mut Summary) -> Result<Option<Vec<u8>>> {
        use anyhow::Context as _;

        loop {
            if self.current.is_none() {
                let Some(path) = self.files.pop_front() else {
                    return Ok(None);
                };
                tracing::info!(file = %path.display(), "replaying");
                let lines = archive::read_lines(&path)
                    .with_context(|| format!("opening {}", path.display()))?;
                let day = path.parent().map(Path::to_path_buf);
                self.at_same_day_file_start = day.is_some() && day == self.last_day;
                self.last_day = day;
                self.current = Some(OpenFile {
                    path,
                    lines: Box::new(lines),
                });
            }
            let file = self.current.as_mut().expect("opened above");
            let Some(line) = file.lines.next() else {
                self.current = None;
                continue;
            };
            let line = line.with_context(|| format!("reading {}", file.path.display()))?;
            if line.is_empty() {
                continue;
            }
            summary.records += 1;
            return Ok(Some(line));
        }
    }

    fn next(&mut self, summary: &mut Summary) -> Result<Option<Write>> {
        use anyhow::Context as _;

        loop {
            let line = if let Some(line) = self.held.take() {
                line
            } else {
                let Some(line) = self.next_line(summary)? else {
                    return Ok(self.sweep.take().map(SweepGroup::into_write));
                };
                line
            };
            let path = &self
                .current
                .as_ref()
                .expect("a record comes from an open file")
                .path;
            let record: Record<'_> = serde_json::from_slice(&line).with_context(|| {
                format!("a line in {} is not an archive record", path.display())
            })?;

            let restarted = match (self.run, record.run) {
                (PreviousRun::Record(before), now) if before.is_some() || now.is_some() => {
                    before != now
                }
                _ => self.at_same_day_file_start,
            };
            self.at_same_day_file_start = false;
            self.run = PreviousRun::Record(record.run);
            if restarted {
                self.last_page_zero = None;
                if let Some(group) = self.sweep.take() {
                    self.held = Some(line);
                    return Ok(Some(group.into_write()));
                }
            }

            if record.status != 200 {
                summary.skipped += 1;
                continue;
            }

            if record.feed == feed::SWEEP {
                let body = record.body_bytes()?;
                let Ok(envelope) = serde_json::from_slice::<PageEnvelope>(&body) else {
                    summary.rejected += 1;
                    continue;
                };
                if self
                    .sweep
                    .as_ref()
                    .is_some_and(|g| record.fetched_at - g.started > SWEEP_WINDOW)
                {
                    let done = self.sweep.take().expect("checked above");
                    self.held = Some(line);
                    return Ok(Some(done.into_write()));
                }
                let group = self.sweep.get_or_insert_with(|| SweepGroup {
                    path: path.clone(),
                    started: record.fetched_at,
                    last_page_at: record.fetched_at,
                    pages: BTreeMap::new(),
                });
                group.last_page_at = record.fetched_at;
                group.pages.insert(
                    envelope.page.unwrap_or(0),
                    SweepPage {
                        total_pages: envelope.total_pages,
                        last_updated: envelope.last_updated,
                        body,
                    },
                );
                continue;
            }

            let Some(job) = job_for(&record.feed) else {
                summary.skipped += 1;
                continue;
            };
            let mut body = record.body_bytes()?;
            if stamped_far_past(&job, &body, record.fetched_at, record.duration_ms)
                || repeats_page_zero(&mut self.last_page_zero, &job, &body)
            {
                summary.skipped += 1;
                continue;
            }
            if job == Job::AuctionsDelta {
                body.insert(0, b'[');
                body.push(b']');
            }
            return Ok(Some(Write {
                path: path.clone(),
                written_at: record.fetched_at,
                job,
                fetched_at: record.fetched_at,
                body,
            }));
        }
    }
}

/// Replays archive files with every feed merged in the order the live collector wrote it.
///
/// # Errors
///
/// Fails when a file can't be read or holds a line that isn't a record, when a write fails
/// through its retries, or at once when a day's partitions can't be created or, for a backfill,
/// when looking up which snapshots the database has fails. The writes before it are committed.
pub async fn replay_files(pool: &PgPool, db: &Db, files: &[PathBuf]) -> Result<Summary> {
    replay(pool, db, files, None).await
}

/// Like [`replay_files`], but writes only the sales, bazaar, page-0, and election snapshots that
/// are not in the database yet.
///
/// # Errors
///
/// Fails as [`replay_files`] does.
pub async fn backfill_files(pool: &PgPool, db: &Db, files: &[PathBuf]) -> Result<Summary> {
    replay(pool, db, files, Some(Missing::default())).await
}

/// One feed's snapshot, by its stamp in Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SnapshotKey {
    feed: &'static str,
    stamp: i64,
}

/// Which snapshots the database lacks. Each snapshot is looked up once, so the later pages of a
/// missing page-0 snapshot are written with it. The election is stored only when its content
/// changes, so an election snapshot is wanted when none was stored with its `lastUpdated`; the
/// write itself then skips it if its content matches the one stored before it. An election
/// snapshot is fetched after it was published and indexed by when it was fetched, so the lookup
/// starts a minute before its `lastUpdated` to allow for the clocks of this host and Hypixel's to
/// differ.
#[derive(Default)]
struct Missing(HashMap<SnapshotKey, bool>);

impl Missing {
    async fn wanted(&mut self, pool: &PgPool, job: &Job, body: &[u8]) -> Result<bool> {
        let feed_name = match job {
            Job::Ended => feed::ENDED,
            Job::Bazaar => feed::BAZAAR,
            Job::AuctionsDelta => feed::AUCTIONS,
            Job::Election => feed::ELECTION,
            _ => return Ok(false),
        };
        let stamp = if *job == Job::AuctionsDelta {
            serde_json::from_slice::<Vec<PageEnvelope>>(body)
                .ok()
                .and_then(|pages| pages.first().and_then(|p| p.last_updated))
        } else {
            serde_json::from_slice::<PageEnvelope>(body)
                .ok()
                .and_then(|p| p.last_updated)
        };
        let Some(stamp) = stamp else {
            return Ok(false);
        };
        let key = SnapshotKey {
            feed: feed_name,
            stamp,
        };
        if let Some(&wanted) = self.0.get(&key) {
            return Ok(wanted);
        }
        let stored: bool = if *job == Job::Election {
            sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM election_snapshots
                 WHERE fetched_at >= $1 - interval '1 minute' AND fetched_at < $1 + interval '1 day'
                   AND api_last_updated = $1)",
            )
            .bind(model::ms_to_time(stamp))
            .fetch_one(pool)
            .await?
        } else {
            sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM feed_snapshots WHERE feed = $1 AND api_last_updated = $2)",
            )
            .bind(feed_name)
            .bind(model::ms_to_time(stamp))
            .fetch_one(pool)
            .await?
        };
        self.0.insert(key, !stored);
        Ok(!stored)
    }
}

/// Replays `files`, or with `missing`, backfills them. Paths run
/// `<feed>/<day>/<feed>-<day>-<creation time>`, so within a feed path order is write order.
async fn replay(
    pool: &PgPool,
    db: &Db,
    files: &[PathBuf],
    mut missing: Option<Missing>,
) -> Result<Summary> {
    let mut feeds: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    for file in files {
        let feed_name = file
            .parent()
            .and_then(Path::parent)
            .and_then(Path::file_name)
            .map_or_default(|n| n.to_string_lossy().into_owned());
        feeds.entry(feed_name).or_default().push(file.clone());
    }
    let mut streams: Vec<FeedWrites> = feeds
        .into_values()
        .map(|mut files| {
            files.sort();
            FeedWrites::new(files)
        })
        .collect();
    let mut summary = Summary::default();
    let mut partitions = HashSet::new();
    let mut heads = Vec::with_capacity(streams.len());
    for stream in &mut streams {
        heads.push(stream.next(&mut summary)?);
    }
    while let Some(i) = heads
        .iter()
        .enumerate()
        .filter_map(|(i, h)| h.as_ref().map(|w| (i, w.written_at)))
        .min_by_key(|&(_, at)| at)
        .map(|(i, _)| i)
    {
        let w = heads[i].take().expect("chosen above");
        if let Some(missing) = &mut missing
            && !missing.wanted(pool, &w.job, &w.body).await?
        {
            summary.skipped += 1;
            heads[i] = streams[i].next(&mut summary)?;
            continue;
        }
        ensure_day(pool, &mut partitions, w.fetched_at.date()).await?;
        write(
            pool,
            db,
            &w.path,
            &w.job,
            w.fetched_at,
            &w.body,
            &mut summary,
        )
        .await?;
        drop(w);
        heads[i] = streams[i].next(&mut summary)?;
    }
    Ok(summary)
}

async fn ensure_day(pool: &PgPool, days: &mut HashSet<Date>, day: Date) -> Result<()> {
    if days.insert(day) {
        sqlx::query("SELECT ensure_partitions($1 - 1, $1 + 1)")
            .bind(day)
            .execute(pool)
            .await?;
    }
    Ok(())
}

/// Writes one record. A transient error such as a lock conflict or a dropped connection is
/// retried before the replay gives up.
async fn write(
    pool: &PgPool,
    db: &Db,
    path: &Path,
    job: &Job,
    fetched_at: OffsetDateTime,
    body: &[u8],
    summary: &mut Summary,
) -> Result<()> {
    let mut attempt = 0;
    loop {
        match db.write(pool, job, fetched_at, body).await {
            Ok(_) => summary.written += 1,
            Err(JobError::Invalid(e)) => {
                summary.rejected += 1;
                tracing::warn!(file = %path.display(), job = ?job, error = %format!("{e:#}"), "record rejected");
            }
            Err(JobError::Retry(e)) if attempt < 5 => {
                attempt += 1;
                tracing::warn!(file = %path.display(), attempt, error = %format!("{e:#}"), "retrying a record");
                tokio_time::sleep(Duration::from_secs(1 << attempt)).await;
                continue;
            }
            Err(JobError::Retry(e)) => {
                return Err(e.context("database unavailable during replay"));
            }
        }
        return Ok(());
    }
}
