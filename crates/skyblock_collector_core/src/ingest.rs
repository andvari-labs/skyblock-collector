//! The single writer: drains the spool into Postgres in order, and keeps partitions ahead.

use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::{self, Either};
use sqlx::PgPool;
use time::OffsetDateTime;
use tokio::sync::Notify;
use tokio::time as tokio_time;
use tokio_util::sync::CancellationToken;

use crate::db::{self, Db, JobError};
use crate::spool::Spool;
use crate::status::Status;

/// The ingest worker and what it shares with the pollers.
#[derive(Debug)]
pub struct Ingest {
    /// The spool the pollers fill.
    #[cfg_attr(
        dylint_lib = "trellis_lints",
        expect(
            shared_ownership,
            reason = "the pollers push to the spool this worker drains, each from a task of its own"
        )
    )]
    pub spool: Arc<Spool>,
    /// The collector's runtime state.
    pub status: Status,
    /// Notified by the pollers after each spooled snapshot.
    #[cfg_attr(
        dylint_lib = "trellis_lints",
        expect(
            shared_ownership,
            reason = "the pollers notify and this worker waits on the one `Notify`, each in a task \
                      of its own"
        )
    )]
    pub wake: Arc<Notify>,
    /// The Postgres connection URL.
    pub database_url: String,
}

const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// How often the worker makes sure the coming days' partitions exist.
const PARTITION_CHECK_INTERVAL: time::Duration = time::Duration::hours(1);

/// How soon a failed partition check runs again. Partitions are made days ahead, so a failure
/// waits this long rather than retrying on every wake, since each try briefly blocks the parent
/// table while it waits for a lock.
const PARTITION_RETRY: time::Duration = time::Duration::minutes(5);

impl Ingest {
    /// Connects, creating the schema in a new database, then writes spooled jobs until
    /// `shutdown`. A database outage leaves the jobs spooled and retries with backoff.
    ///
    /// # Errors
    ///
    /// None so far: database failures are retried, and it returns `Ok` once shutdown starts.
    pub async fn run(self, shutdown: CancellationToken) -> anyhow::Result<()> {
        let pool = self.connect(&shutdown).await;
        let Some(pool) = pool else { return Ok(()) };
        let db = Db::default();
        let mut backoff = Duration::from_secs(1);
        let mut last_partition_check: Option<OffsetDateTime> = None;
        loop {
            self.status.beat("ingest");
            if last_partition_check
                .is_none_or(|t| OffsetDateTime::now_utc() - t > PARTITION_CHECK_INTERVAL)
            {
                last_partition_check = Some(match ensure_partitions(&pool).await {
                    Ok(()) => OffsetDateTime::now_utc(),
                    Err(e) => {
                        tracing::warn!(error = %e, "could not create upcoming partitions; retrying in 5 minutes");
                        OffsetDateTime::now_utc() - (PARTITION_CHECK_INTERVAL - PARTITION_RETRY)
                    }
                });
            }
            let healthy = match self.drain(&pool, &db, &shutdown).await {
                Ok(()) => true,
                Err(e) => {
                    tracing::warn!(error = %e, "database write failed; jobs stay spooled");
                    self.status.ingest(|s| {
                        s.database_ok = false;
                        s.last_database_error = Some(format!("{e:#}"));
                    });
                    false
                }
            };
            self.update_counts();
            if healthy {
                backoff = Duration::from_secs(1);
            } else {
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            let wait = if healthy {
                Duration::from_secs(5)
            } else {
                backoff
            };
            let cancelled = pin!(shutdown.cancelled());
            let woken = pin!(async {
                if healthy {
                    self.wake.notified().await;
                } else {
                    future::pending::<()>().await;
                }
            });
            let slept = pin!(tokio_time::sleep(wait));
            if let Either::Left(_) = future::select(cancelled, future::select(woken, slept)).await {
                return Ok(());
            }
        }
    }

    async fn connect(&self, shutdown: &CancellationToken) -> Option<PgPool> {
        let mut backoff = Duration::from_secs(1);
        loop {
            self.status.beat("ingest");
            let attempt = async {
                let pool = db::connect(&self.database_url).await?;
                db::ensure_schema(&pool).await?;
                anyhow::Ok(pool)
            };
            match attempt.await {
                Ok(pool) => {
                    self.status.ingest(|s| s.database_ok = true);
                    tracing::info!("connected to Postgres");
                    return Some(pool);
                }
                Err(e) => {
                    tracing::warn!(error = %format!("{e:#}"), "Postgres not ready; collection continues into the spool");
                    self.status.ingest(|s| {
                        s.database_ok = false;
                        s.last_database_error = Some(format!("{e:#}"));
                    });
                }
            }
            let cancelled = pin!(shutdown.cancelled());
            let slept = pin!(tokio_time::sleep(backoff));
            if let Either::Left(_) = future::select(cancelled, slept).await {
                return None;
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// Writes every pending job, then the fetch log. Stops at the first retryable failure so jobs
    /// keep their order.
    async fn drain(
        &self,
        pool: &PgPool,
        db: &Db,
        shutdown: &CancellationToken,
    ) -> anyhow::Result<()> {
        for path in self.spool.pending()? {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            self.status.beat("ingest");
            let entry = match Spool::read(&path) {
                Ok(entry) => entry,
                Err(e) => {
                    tracing::error!(path = %path.display(), error = %e, "unreadable job quarantined");
                    self.spool.quarantine(&path)?;
                    continue;
                }
            };
            match db
                .write(pool, &entry.meta.job, entry.meta.fetched_at, &entry.body)
                .await
            {
                Ok(outcome) => {
                    Spool::complete(&entry)?;
                    tracing::debug!(feed = outcome.feed, rows = outcome.rows, api_last_updated = ?outcome.api_last_updated, "written");
                    self.status.ingest(|s| {
                        s.database_ok = true;
                        s.last_commit_at = Some(OffsetDateTime::now_utc());
                        s.jobs_written += 1;
                    });
                }
                Err(JobError::Invalid(e)) => {
                    tracing::error!(path = %path.display(), job = ?entry.meta.job, error = %format!("{e:#}"),
                        "job can never be written; quarantined (the raw archive still has it)");
                    self.spool.quarantine(&path)?;
                }
                Err(JobError::Retry(e)) => return Err(e),
            }
        }
        let rows = self.status.take_fetches();
        if let Err(e) = db::write_fetches(pool, &rows).await {
            match JobError::from(e) {
                JobError::Invalid(e) => {
                    tracing::error!(rows = rows.len(), error = %format!("{e:#}"), "fetch log rows can never be written; dropped");
                    self.status
                        .ingest(|s| s.fetch_rows_dropped += rows.len() as u64);
                }
                JobError::Retry(e) => {
                    self.status.return_fetches(rows);
                    return Err(e);
                }
            }
        }
        self.status.ingest(|s| s.database_ok = true);
        Ok(())
    }

    fn update_counts(&self) {
        let pending = self.spool.pending().map_or(0, |p| p.len());
        let bytes = self.spool.bytes().unwrap_or(0);
        let quarantined = self.spool.quarantined().unwrap_or(0);
        self.status.ingest(|s| {
            s.pending_jobs = pending;
            s.pending_bytes = bytes;
            s.quarantined_jobs = quarantined;
        });
    }
}

/// Creates the next days' partitions. Creating one waits for every open transaction on its table,
/// including a reader's idle notebook session, and blocks writes while it waits, so it gives up
/// after a short lock timeout. Partitions are made days ahead, so a later try is still in time.
async fn ensure_partitions(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL lock_timeout = '2s'")
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "SELECT ensure_partitions((now() AT TIME ZONE 'UTC')::date - 1, (now() AT TIME ZONE 'UTC')::date + 3)",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

/// How long the last write of fetch rows at shutdown may take, connecting included.
const FINAL_WRITE_WITHIN: Duration = Duration::from_secs(5);

/// Writes the fetch rows `status` still holds to `fetch_log` once, at shutdown, after every poller
/// has stopped, so the requests of the last seconds before a stop aren't lost. Spooled snapshots
/// need nothing: they wait on disk for the next start. It gives up after 5 seconds
/// and logs how many rows it couldn't write.
pub async fn write_last_fetches(database_url: &str, status: &Status) {
    let rows = status.take_fetches();
    if rows.is_empty() {
        return;
    }
    let count = rows.len();
    let write = async {
        let pool = db::connect(database_url).await?;
        db::write_fetches(&pool, &rows).await
    };
    match tokio_time::timeout(FINAL_WRITE_WITHIN, write).await {
        Ok(Ok(())) => tracing::info!(rows = count, "wrote the last fetch log rows"),
        Ok(Err(e)) => {
            tracing::warn!(rows = count, error = %e, "could not write the last fetch log rows; dropped");
        }
        Err(_) => {
            tracing::warn!(
                rows = count,
                "writing the last fetch log rows timed out; they may or may not have been written"
            );
        }
    }
}
