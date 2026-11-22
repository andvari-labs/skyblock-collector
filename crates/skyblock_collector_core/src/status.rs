//! Shared runtime state, served as JSON on `/status`, and the liveness check on `/health`.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::{Json, Router, routing};
use serde::Serialize;
use serde_json::Value;
use time::OffsetDateTime;
use time::serde::rfc3339::option as rfc3339_option;

use crate::archive;

/// A fetch waiting to be written to `fetch_log`. Kept in memory so a database outage does not
/// stop collection; past `MAX_PENDING_FETCHES` (100,000) the oldest rows are dropped.
#[derive(Clone, Debug)]
pub struct FetchRow {
    /// The feed fetched.
    pub feed: &'static str,
    /// The URL requested.
    pub url: String,
    /// When the request was sent.
    pub started_at: OffsetDateTime,
    /// How long the request took.
    pub duration_ms: u64,
    /// The HTTP status, or None when no response arrived.
    pub http_status: Option<u16>,
    /// The size of the body, or None when no response arrived.
    pub body_bytes: Option<usize>,
    /// The snapshot stamp the body held, if any.
    pub api_last_updated: Option<OffsetDateTime>,
    /// The response's `age` header in seconds, if any.
    pub cache_age_s: Option<u64>,
    /// Why no response arrived.
    pub error: Option<String>,
}

const MAX_PENDING_FETCHES: usize = 100_000;

/// One feed's status, as `/status` shows it.
#[derive(Serialize, Default, Clone, Debug)]
pub struct FeedStatus {
    /// When the feed was last requested.
    #[serde(with = "rfc3339_option")]
    pub last_attempt_at: Option<OffsetDateTime>,
    /// When the feed last answered 200.
    #[serde(with = "rfc3339_option")]
    pub last_success_at: Option<OffsetDateTime>,
    /// When the feed last delivered a snapshot the collector had not seen.
    #[serde(with = "rfc3339_option")]
    pub last_new_snapshot_at: Option<OffsetDateTime>,
    /// The stamp of the newest snapshot seen.
    #[serde(with = "rfc3339_option")]
    pub api_last_updated: Option<OffsetDateTime>,
    /// Failed requests since the last 200.
    pub consecutive_failures: u32,
    /// Why the last failed request failed.
    pub last_error: Option<String>,
}

/// The ingest worker's status, as `/status` shows it.
#[derive(Serialize, Default, Clone, Debug)]
pub struct IngestStatus {
    /// The last database operation succeeded.
    pub database_ok: bool,
    /// Why the last failed database operation failed.
    pub last_database_error: Option<String>,
    /// When a job was last committed.
    #[serde(with = "rfc3339_option")]
    pub last_commit_at: Option<OffsetDateTime>,
    /// Jobs waiting in the spool.
    pub pending_jobs: usize,
    /// Bytes waiting in the spool.
    pub pending_bytes: u64,
    /// Jobs moved to `failed/`.
    pub quarantined_jobs: usize,
    /// Jobs committed since the start.
    pub jobs_written: u64,
    /// Snapshots the spool refused since the start.
    pub spool_refusals: u64,
    /// `fetch_log` rows dropped since the start.
    pub fetch_rows_dropped: u64,
}

#[derive(Default, Debug)]
struct Inner {
    feeds: BTreeMap<&'static str, FeedStatus>,
    ingest: IngestStatus,
    heartbeats: BTreeMap<&'static str, OffsetDateTime>,
    fetches: VecDeque<FetchRow>,
    archive_errors: u64,
    last_archive_error: Option<String>,
}

/// The collector's runtime state. Clones share it.
#[derive(Clone, Debug)]
pub struct Status {
    #[cfg_attr(
        dylint_lib = "trellis_lints",
        expect(
            shared_ownership,
            reason = "every poller, the ingest worker and the status server each hold a clone and \
                      update or read the one state"
        )
    )]
    inner: Arc<Mutex<Inner>>,
    started_at: OffsetDateTime,
    /// False when nothing writes `fetch_log`, so fetch rows are not kept.
    keeps_fetches: bool,
}

impl Default for Status {
    fn default() -> Self {
        Self::new()
    }
}

impl Status {
    /// Status for a collector with a database, which keeps fetch rows for `fetch_log`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::default(),
            started_at: OffsetDateTime::now_utc(),
            keeps_fetches: true,
        }
    }

    /// Status for a collector without a database, which drops fetch rows.
    #[must_use]
    pub fn without_fetch_log() -> Self {
        Self {
            keeps_fetches: false,
            ..Self::new()
        }
    }

    fn with<T>(&self, f: impl FnOnce(&mut Inner) -> T) -> T {
        f(&mut self.inner.lock().expect("status lock poisoned"))
    }

    /// Records that `task` is alive now.
    pub fn beat(&self, task: &'static str) {
        self.with(|s| s.heartbeats.insert(task, OffsetDateTime::now_utc()));
    }

    /// Updates one feed's status.
    pub fn feed(&self, feed: &'static str, f: impl FnOnce(&mut FeedStatus)) {
        self.with(|s| f(s.feeds.entry(feed).or_default()));
    }

    /// Updates the ingest worker's status.
    pub fn ingest(&self, f: impl FnOnce(&mut IngestStatus)) {
        self.with(|s| f(&mut s.ingest));
    }

    /// Counts a failed archive write and keeps its message.
    pub fn archive_error(&self, error: String) {
        self.with(|s| {
            s.archive_errors += 1;
            s.last_archive_error = Some(error);
        });
    }

    /// Keeps a fetch for `fetch_log`, dropping the oldest past `MAX_PENDING_FETCHES` (100,000). A status
    /// without a fetch log drops it.
    pub fn record_fetch(&self, row: FetchRow) {
        if !self.keeps_fetches {
            return;
        }
        self.with(|s| {
            if s.fetches.len() >= MAX_PENDING_FETCHES {
                s.fetches.pop_front();
                s.ingest.fetch_rows_dropped += 1;
            }
            s.fetches.push_back(row);
        });
    }

    /// Takes every kept fetch, oldest first.
    #[must_use]
    pub fn take_fetches(&self) -> Vec<FetchRow> {
        self.with(|s| s.fetches.drain(..).collect())
    }

    /// Puts back rows that could not be written, ahead of newer ones.
    pub fn return_fetches(&self, rows: Vec<FetchRow>) {
        self.with(|s| {
            for row in rows.into_iter().rev() {
                if s.fetches.len() >= MAX_PENDING_FETCHES {
                    s.ingest.fetch_rows_dropped += 1;
                    continue;
                }
                s.fetches.push_front(row);
            }
        });
    }

    /// Every task has reported within `max_age_s`.
    ///
    /// # Errors
    ///
    /// Names the first task in `expected` that never reported or last reported too long ago.
    pub fn alive(&self, max_age_s: i64, expected: &[&'static str]) -> Result<(), String> {
        let now = OffsetDateTime::now_utc();
        self.with(|s| {
            for task in expected {
                match s.heartbeats.get(task) {
                    Some(t) if (now - *t).whole_seconds() <= max_age_s => {}
                    Some(t) => return Err(format!("{task} last reported at {t}")),
                    None => return Err(format!("{task} has not reported")),
                }
            }
            Ok(())
        })
    }

    fn snapshot(&self) -> Value {
        self.with(|s| {
            serde_json::json!({
                "version": env!("CARGO_PKG_VERSION"),
                "started_at": archive::format_time(self.started_at),
                "feeds": s.feeds,
                "ingest": s.ingest,
                "archive_errors": s.archive_errors,
                "last_archive_error": s.last_archive_error,
                "pending_fetch_rows": s.fetches.len(),
                "heartbeats": s.heartbeats.iter()
                    .map(|(k, v)| (*k, archive::format_time(*v)))
                    .collect::<BTreeMap<_, _>>(),
            })
        })
    }
}

/// The tasks of the full collector.
pub const TASKS: &[&str] = &["ended", "auctions", "sweep", "bazaar", "slow", "ingest"];
/// The tasks of a secondary collector.
pub const SECONDARY_TASKS: &[&str] = &["ended", "auctions", "bazaar", "slow"];

/// Serves `/status`, and `/health`, which fails when one of `tasks` stops reporting.
pub fn router(status: Status, tasks: &'static [&'static str]) -> Router {
    Router::new()
        .route(
            "/health",
            routing::get(move |State(s): State<Status>| async move { health(&s, tasks) }),
        )
        .route(
            "/status",
            routing::get(|State(s): State<Status>| async move { Json(s.snapshot()) }),
        )
        .with_state(status)
}

fn health(status: &Status, tasks: &[&'static str]) -> Response {
    use axum::response::IntoResponse as _;

    match status.alive(180, tasks) {
        Ok(()) => (StatusCode::OK, "ok\n".to_owned()).into_response(),
        Err(reason) => (StatusCode::SERVICE_UNAVAILABLE, format!("{reason}\n")).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(n: u64) -> FetchRow {
        FetchRow {
            feed: "ended",
            url: String::new(),
            started_at: OffsetDateTime::UNIX_EPOCH,
            duration_ms: n,
            http_status: Some(200),
            body_bytes: None,
            api_last_updated: None,
            cache_age_s: None,
            error: None,
        }
    }

    #[test]
    fn returned_fetches_go_back_in_order_ahead_of_new_ones() {
        let status = Status::new();
        status.record_fetch(row(1));
        status.record_fetch(row(2));
        let taken = status.take_fetches();
        status.record_fetch(row(3));
        status.return_fetches(taken);
        let order: Vec<_> = status
            .take_fetches()
            .iter()
            .map(|r| r.duration_ms)
            .collect();
        assert_eq!(order, vec![1, 2, 3]);
    }

    #[test]
    fn a_status_without_a_fetch_log_keeps_no_fetches() {
        let status = Status::without_fetch_log();
        status.record_fetch(row(1));
        assert!(status.take_fetches().is_empty());
    }

    #[test]
    fn health_requires_every_task() {
        let status = Status::new();
        assert!(status.alive(60, &["ended"]).is_err());
        status.beat("ended");
        assert!(status.alive(60, &["ended"]).is_ok());
    }
}
