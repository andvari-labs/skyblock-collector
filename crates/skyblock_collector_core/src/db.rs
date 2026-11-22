//! Writes each kind of snapshot to Postgres. Every write is idempotent, so the spool and archive
//! replays can repeat any job safely. Writes also tolerate arriving out of order, except that a
//! sweep or sighting only clears an absence flag set before it.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::anyhow;
use serde_json::value::RawValue;
use serde_json::{Map, Value as Json};
use sha2::Sha256;
use sqlx::migrate::{Migration, MigrationType, Migrator};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::model::{
    self, Auction, AuctionsPage, Bazaar, BazaarProduct, EndedPage, ItemBytes, QuickStatus,
};
use crate::nbt;
use crate::spool::Job;
use crate::status::FetchRow;

/// Why a job wasn't written, which says what to do with it.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum JobError {
    /// The database is unreachable or failed transiently. Retry the same job later.
    #[error("database unavailable: {0}")]
    Retry(#[source] anyhow::Error),
    /// The job can never be written as is. Quarantine it; the raw archive still has it.
    #[error("job cannot be written: {0}")]
    Invalid(#[source] anyhow::Error),
}

impl From<sqlx::Error> for JobError {
    fn from(error: sqlx::Error) -> Self {
        use sqlx::error::DatabaseError;

        let permanent = error
            .as_database_error()
            .and_then(DatabaseError::code)
            .is_some_and(|code| fails_every_time(&code));
        if permanent {
            Self::Invalid(error.into())
        } else {
            Self::Retry(error.into())
        }
    }
}

/// Data exceptions (22), integrity violations (23), syntax or schema errors (42), and writes to a
/// closed partition (25006) fail the same way on every attempt. Any other SQLSTATE, such as a lost
/// connection's, may pass later.
fn fails_every_time(sqlstate: &str) -> bool {
    sqlstate == "25006"
        || sqlstate.starts_with("22")
        || sqlstate.starts_with("23")
        || sqlstate.starts_with("42")
}

/// A body that doesn't parse as its feed's JSON, which no retry can write.
fn malformed(error: serde_json::Error) -> JobError {
    JobError::Invalid(error.into())
}

/// Opens a pool of up to 4 connections to `url`.
///
/// # Errors
///
/// Fails at once on a bad URL, a refused login or a missing database, and when the server
/// refuses connections or stays unavailable for 10 s.
pub async fn connect(url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(url)
        .await
}

/// Creates the schema in a new database. sqlx records the schema file's checksum, so on an
/// existing database this does nothing while the file is unchanged and fails if it was edited:
/// a schema change means rebuilding the database from the archive (`crates/skyblock_collector/deploy/README.md`).
///
/// # Errors
///
/// Fails when the database is unreachable, the schema file changed since it was applied, or
/// applying it fails.
pub async fn ensure_schema(pool: &PgPool) -> anyhow::Result<()> {
    use anyhow::Context as _;
    use sqlx::SqlSafeStr as _;

    let schema = Migration::new(
        1,
        "schema".into(),
        MigrationType::Simple,
        include_str!("../schema/0001_schema.sql").into_sql_str(),
        false,
    );
    Migrator::with_migrations(vec![schema])
        .run(pool)
        .await
        .context("creating the schema")
}

/// How often every product gets an order-book row even when its book did not change. Prices
/// (`bazaar_quick`) get a row for every product in every snapshot.
const BOOK_KEYFRAME: time::Duration = time::Duration::minutes(30);

/// What the collector remembers between jobs to skip unchanged rows. It must match what is
/// committed, so every failed job forgets it all.
#[derive(Default, Debug)]
pub struct Db {
    products: Mutex<HashMap<String, i32>>,
    /// Each auction's state as last written, by UUID.
    auctions: Mutex<HashMap<Uuid, AuctionState>>,
    bazaar: Mutex<BazaarMemory>,
}

/// The fields of an auction whose change means a new write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AuctionState {
    last_updated: Option<i64>,
    highest_bid: Option<i64>,
    claimed: Option<bool>,
}

impl AuctionState {
    fn of(auction: &Auction) -> Self {
        Self {
            last_updated: auction.last_updated,
            highest_bid: auction.highest_bid_amount,
            claimed: auction.claimed,
        }
    }
}

#[derive(Default, Debug)]
struct BazaarMemory {
    /// The fingerprint of each product's last written order book.
    books: HashMap<String, u64>,
    book_keyframe: Option<OffsetDateTime>,
    /// The newest snapshot this process wrote. When the newest in the database differs, another
    /// writer such as a backfill added books this memory has not seen.
    last: Option<OffsetDateTime>,
}

fn fingerprint(value: &impl Hash) -> u64 {
    use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};
    BuildHasherDefault::<DefaultHasher>::default().hash_one(value)
}

#[derive(Clone, Copy)]
struct SweepKind {
    complete: bool,
    consistent: bool,
}

/// A listing's newest copy in a job, and the stamp of the page it was on.
#[derive(Clone, Copy)]
struct Sighting<'page> {
    seen: OffsetDateTime,
    auction: &'page Auction,
}

/// A listing to write: its UUID, the stamp of the page it was on, and the listing.
#[derive(Clone, Copy)]
struct Changed<'page> {
    uuid: Uuid,
    seen: OffsetDateTime,
    auction: &'page Auction,
}

/// What a write did.
#[derive(Debug)]
pub struct Outcome {
    /// The feed written.
    pub feed: &'static str,
    /// The snapshot's stamp.
    pub api_last_updated: Option<OffsetDateTime>,
    /// The rows written.
    pub rows: usize,
}

impl Db {
    /// Writes one job in one transaction.
    ///
    /// # Errors
    ///
    /// Fails with [`JobError::Invalid`] when the body isn't its feed's JSON, Hypixel reported a
    /// failure, or Postgres refuses the rows; with [`JobError::Retry`] when Postgres is
    /// unreachable or fails transiently.
    pub async fn write(
        &self,
        pool: &PgPool,
        job: &Job,
        fetched_at: OffsetDateTime,
        body: &[u8],
    ) -> Result<Outcome, JobError> {
        let result = self.write_inner(pool, job, fetched_at, body).await;
        if result.is_err() {
            self.forget_caches();
        }
        result
    }

    async fn write_inner(
        &self,
        pool: &PgPool,
        job: &Job,
        fetched_at: OffsetDateTime,
        body: &[u8],
    ) -> Result<Outcome, JobError> {
        let mut tx = pool.begin().await?;
        let outcome = match job {
            Job::Ended => ended(&mut tx, fetched_at, body).await?,
            Job::AuctionsDelta => self.auctions(&mut tx, fetched_at, body, None).await?,
            Job::AuctionsSweep {
                complete,
                consistent,
            } => {
                let kind = SweepKind {
                    complete: *complete,
                    consistent: *consistent,
                };
                self.auctions(&mut tx, fetched_at, body, Some(kind)).await?
            }
            Job::Bazaar => self.bazaar(&mut tx, fetched_at, body).await?,
            Job::Election => election(&mut tx, fetched_at, body).await?,
            Job::Resource { name } => resource(&mut tx, name, fetched_at, body).await?,
        };
        tx.commit().await?;
        Ok(outcome)
    }

    async fn auctions(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        fetched_at: OffsetDateTime,
        body: &[u8],
        sweep: Option<SweepKind>,
    ) -> Result<Outcome, JobError> {
        let pages: Vec<AuctionsPage> = serde_json::from_slice(body).map_err(malformed)?;
        let generation = pages
            .iter()
            .filter_map(|p| p.last_updated)
            .max()
            .and_then(model::ms_to_time)
            .ok_or_else(|| JobError::Invalid(anyhow!("no page has lastUpdated")))?;
        if pages.iter().any(|p| p.success == Some(false)) {
            return Err(JobError::Invalid(anyhow!("a page reports success=false")));
        }

        let newest = newest_copies(&pages, generation);
        let changed = self.changed_auctions(&newest);
        let written = upsert_auctions(tx, &changed).await?;
        insert_bids(tx, &changed).await?;

        let feed = if sweep.is_some() {
            "auctions_sweep"
        } else {
            "auctions"
        };
        let flagged: Vec<Uuid> = if let Some(kind) = sweep {
            let present: Vec<Uuid> = newest.keys().copied().collect();
            record_sweep(tx, &pages, &present, generation, fetched_at, kind).await?
        } else {
            snapshot(tx, feed, generation, fetched_at, written).await?;
            Vec::new()
        };
        self.remember_auctions(&newest, &changed, &flagged, sweep);
        Ok(Outcome {
            feed,
            api_last_updated: Some(generation),
            rows: written,
        })
    }

    /// The listings whose state differs from the one last written, sorted by UUID, so two writers
    /// (the collector and a restore replay) lock rows in the same order and cannot deadlock.
    fn changed_auctions<'page>(
        &self,
        newest: &HashMap<Uuid, Sighting<'page>>,
    ) -> Vec<Changed<'page>> {
        let known = self.auctions.lock().expect("auction cache poisoned");
        let mut changed: Vec<Changed<'page>> = newest
            .iter()
            .filter(|(uuid, s)| known.get(*uuid) != Some(&AuctionState::of(s.auction)))
            .map(|(uuid, s)| Changed {
                uuid: *uuid,
                seen: s.seen,
                auction: s.auction,
            })
            .collect();
        changed.sort_unstable_by_key(|c| c.uuid);
        changed
    }

    /// Remembers what this job wrote. Cache updates take effect only for rows this transaction
    /// wrote; a failed commit leaves a stale cache entry that merely re-sends the row next time.
    /// Flagged listings leave the cache, so a later sighting is written and clears the flag.
    fn remember_auctions(
        &self,
        newest: &HashMap<Uuid, Sighting<'_>>,
        changed: &[Changed<'_>],
        flagged: &[Uuid],
        sweep: Option<SweepKind>,
    ) {
        let mut known = self.auctions.lock().expect("auction cache poisoned");
        if sweep.is_some_and(|s| s.complete) {
            let present: HashSet<_> = newest.keys().collect();
            known.retain(|uuid, _| present.contains(uuid));
        }
        for c in changed {
            known.insert(c.uuid, AuctionState::of(c.auction));
        }
        for uuid in flagged {
            known.remove(uuid);
        }
    }

    /// Writes a bazaar snapshot. One bazaar writer runs at a time, so two processes cannot both
    /// decide from the same newest snapshot which books to write.
    async fn bazaar(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        fetched_at: OffsetDateTime,
        body: &[u8],
    ) -> Result<Outcome, JobError> {
        let bazaar: Bazaar<'_> = serde_json::from_slice(body).map_err(malformed)?;
        if bazaar.success == Some(false) {
            return Err(JobError::Invalid(anyhow!("bazaar reports success=false")));
        }
        let generation = bazaar
            .last_updated
            .and_then(model::ms_to_time)
            .ok_or_else(|| JobError::Invalid(anyhow!("bazaar has no lastUpdated")))?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('bazaar'))")
            .execute(&mut **tx)
            .await?;
        let seen: Option<(i32,)> =
            sqlx::query_as("SELECT 1 FROM bazaar_snapshots WHERE api_last_updated = $1")
                .bind(generation)
                .fetch_optional(&mut **tx)
                .await?;
        if seen.is_some() {
            return Ok(Outcome {
                feed: "bazaar",
                api_last_updated: Some(generation),
                rows: 0,
            });
        }

        let newest: Option<OffsetDateTime> =
            sqlx::query_scalar("SELECT max(api_last_updated) FROM bazaar_snapshots")
                .fetch_one(&mut **tx)
                .await?;
        let keys = self
            .product_keys(tx, bazaar.products.keys(), fetched_at)
            .await?;
        if newest.is_some_and(|n| n > generation) {
            return late_bazaar(tx, &bazaar, &keys, generation, fetched_at).await;
        }
        let BazaarRows {
            quick: q,
            books,
            book_keyframe,
        } = self.bazaar_rows(&bazaar, &keys, newest, generation);
        insert_quick(tx, &q, generation).await?;
        sqlx::query(
            "INSERT INTO bazaar_books (product_key, api_last_updated, buy_summary, sell_summary)
             SELECT k, $2, b::jsonb, s::jsonb FROM unnest($1::int4[], $3::text[], $4::text[]) AS u(k, b, s)
             ON CONFLICT DO NOTHING",
        )
        .bind(&books.key)
        .bind(generation)
        .bind(&books.buy)
        .bind(&books.sell)
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "INSERT INTO bazaar_snapshots (api_last_updated, fetched_at, product_count, book_rows,
                 book_keyframe)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (api_last_updated) DO NOTHING",
        )
        .bind(generation)
        .bind(fetched_at)
        .bind(i32::try_from(bazaar.products.len()).unwrap_or(i32::MAX))
        .bind(i32::try_from(books.key.len()).unwrap_or(i32::MAX))
        .bind(book_keyframe)
        .execute(&mut **tx)
        .await?;
        let rows = q.key.len() + books.key.len();
        snapshot(tx, "bazaar", generation, fetched_at, bazaar.products.len()).await?;
        Ok(Outcome {
            feed: "bazaar",
            api_last_updated: Some(generation),
            rows,
        })
    }

    /// Decides which order books a snapshot newer than every stored one writes, and remembers
    /// them. Every product gets a price row, and a book row when its book changed or at a
    /// keyframe. It holds the memory's lock throughout, so it runs between awaits.
    fn bazaar_rows(
        &self,
        bazaar: &Bazaar<'_>,
        keys: &HashMap<String, i32>,
        newest: Option<OffsetDateTime>,
        generation: OffsetDateTime,
    ) -> BazaarRows {
        let mut memory = self.bazaar.lock().expect("bazaar memory poisoned");
        let book_keyframe = memory.last != newest
            || memory
                .book_keyframe
                .is_none_or(|last| generation - last >= BOOK_KEYFRAME || generation < last);
        let mut quick = QuickColumns::default();
        let mut books = BookColumns::default();
        for (id, product) in &bazaar.products {
            let key = keys[id];
            let book_print = fingerprint(&(
                product.buy_summary.map(RawValue::get),
                product.sell_summary.map(RawValue::get),
            ));
            if book_keyframe || memory.books.get(id) != Some(&book_print) {
                memory.books.insert(id.clone(), book_print);
                books.push(key, product);
            }
            quick.push(key, product.quick_status.as_ref());
        }
        if book_keyframe {
            memory.book_keyframe = Some(generation);
        }
        memory.last = Some(generation);
        BazaarRows {
            quick,
            books,
            book_keyframe,
        }
    }

    /// The key of each product, adding the products not stored yet. Existing products are looked
    /// up before inserting: an insert that conflicts still uses up a sequence value, so inserting
    /// every product on each cold start would exhaust it.
    async fn product_keys<'a>(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        ids: impl Iterator<Item = &'a String>,
        fetched_at: OffsetDateTime,
    ) -> Result<HashMap<String, i32>, JobError> {
        let ids: Vec<String> = ids.cloned().collect();
        let missing: Vec<String> = {
            let known = self.products.lock().expect("product cache poisoned");
            ids.iter()
                .filter(|id| !known.contains_key(*id))
                .cloned()
                .collect()
        };
        if !missing.is_empty() {
            let lookup =
                "SELECT product_key, product_id FROM bazaar_products WHERE product_id = ANY($1)";
            let found: Vec<(i32, String)> = sqlx::query_as(lookup)
                .bind(&missing)
                .fetch_all(&mut **tx)
                .await?;
            let found_ids: HashSet<&str> = found.iter().map(|(_, id)| id.as_str()).collect();
            let new: Vec<&String> = missing
                .iter()
                .filter(|id| !found_ids.contains(id.as_str()))
                .collect();
            let mut rows = found.clone();
            if !new.is_empty() {
                sqlx::query(
                    "INSERT INTO bazaar_products (product_id, first_seen_at)
                     SELECT unnest($1::text[]), $2 ON CONFLICT (product_id) DO NOTHING",
                )
                .bind(&new)
                .bind(fetched_at)
                .execute(&mut **tx)
                .await?;
                let added: Vec<(i32, String)> = sqlx::query_as(lookup)
                    .bind(&new)
                    .fetch_all(&mut **tx)
                    .await?;
                rows.extend(added);
            }
            let mut known = self.products.lock().expect("product cache poisoned");
            for (key, id) in rows {
                known.insert(id, key);
            }
        }
        let known = self.products.lock().expect("product cache poisoned");
        ids.into_iter()
            .map(|id| {
                known
                    .get(&id)
                    .map(|key| (id.clone(), *key))
                    .ok_or_else(|| JobError::Retry(anyhow!("bazaar product {id} has no key")))
            })
            .collect()
    }

    /// Forgets what the caches remember, for when a transaction that changed it rolled back.
    fn forget_caches(&self) {
        self.products
            .lock()
            .expect("product cache poisoned")
            .clear();
        self.auctions
            .lock()
            .expect("auction cache poisoned")
            .clear();
        *self.bazaar.lock().expect("bazaar memory poisoned") = BazaarMemory::default();
    }
}

/// Each listing's newest copy across the pages. A listing can appear on two pages when it moves
/// during a read.
fn newest_copies(
    pages: &[AuctionsPage],
    generation: OffsetDateTime,
) -> HashMap<Uuid, Sighting<'_>> {
    let mut newest: HashMap<Uuid, Sighting<'_>> = HashMap::new();
    for page in pages {
        let seen = page
            .last_updated
            .and_then(model::ms_to_time)
            .unwrap_or(generation);
        for auction in &page.auctions {
            let Some(uuid) = model::parse_uuid(Some(&auction.uuid)) else {
                continue;
            };
            let replace = newest.get(&uuid).is_none_or(|prev| {
                auction.last_updated.unwrap_or(0) > prev.auction.last_updated.unwrap_or(0)
            });
            if replace {
                newest.insert(uuid, Sighting { seen, auction });
            }
        }
    }
    newest
}

/// Records a sweep: clears the flags of the listings it saw, flags the ones a complete sweep
/// missed, and stores the sweep's row. Returns the listings it flagged.
///
/// Pages are cached separately, so a sweep can mix snapshots and miss a listing that moved across
/// a page boundary. The oldest page bounds what the sweep can vouch for. Only a sighting newer than
/// a flag clears it, so replaying an older sweep leaves a later flag alone. A listing is gone once
/// two complete sweeps in a row miss it: the first miss records when, and the second confirms it.
async fn record_sweep(
    tx: &mut Transaction<'_, Postgres>,
    pages: &[AuctionsPage],
    present: &[Uuid],
    generation: OffsetDateTime,
    fetched_at: OffsetDateTime,
    kind: SweepKind,
) -> Result<Vec<Uuid>, JobError> {
    let oldest = pages
        .iter()
        .filter_map(|p| p.last_updated)
        .min()
        .and_then(model::ms_to_time)
        .unwrap_or(generation);
    let mut flagged: Vec<Uuid> = Vec::new();
    sqlx::query(
        "UPDATE auctions SET missing_since = NULL, absent_since = NULL
                 WHERE uuid = ANY($1)
                   AND ((absent_since IS NULL AND missing_since < $2) OR absent_since < $2)",
    )
    .bind(present)
    .bind(oldest)
    .execute(&mut **tx)
    .await?;
    if kind.complete {
        flagged = sqlx::query_scalar(
            "WITH gone AS (
                         SELECT uuid FROM auctions
                         WHERE absent_since IS NULL AND first_seen_at < $1
                         EXCEPT SELECT unnest($2::uuid[])
                     )
                     UPDATE auctions a SET
                         absent_since = CASE WHEN a.missing_since < $1 THEN a.missing_since END,
                         missing_since = coalesce(a.missing_since, $1)
                     FROM gone WHERE a.uuid = gone.uuid
                     RETURNING a.uuid",
        )
        .bind(oldest)
        .bind(present)
        .fetch_all(&mut **tx)
        .await?;
    }
    let (missing, absent): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE absent_since IS NULL), count(*) FILTER (WHERE absent_since IS NOT NULL)
                 FROM auctions WHERE uuid = ANY($1)",
    )
    .bind(&flagged)
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO auction_sweeps (api_last_updated, oldest_page_at, fetched_at, pages,
                     auction_count, complete, consistent, marked_missing, marked_absent)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (api_last_updated) DO NOTHING",
    )
    .bind(generation)
    .bind(oldest)
    .bind(fetched_at)
    .bind(i32::try_from(pages.len()).unwrap_or(i32::MAX))
    .bind(i32::try_from(present.len()).unwrap_or(i32::MAX))
    .bind(kind.complete)
    .bind(kind.consistent)
    .bind(i32::try_from(missing).unwrap_or(i32::MAX))
    .bind(i32::try_from(absent).unwrap_or(i32::MAX))
    .execute(&mut **tx)
    .await?;
    Ok(flagged)
}

/// Writes a bazaar snapshot older than the newest one stored, such as one a backfill found.
/// Books are stored only when they change, so a book row here changes the book every later
/// snapshot shows until the next row. So the snapshot gets a row for each product whose book
/// differs from the one in force before it, and the next stored snapshot gets the old book back
/// for each such product it has no row of its own for. The query's `at_next` clause runs though
/// nothing reads it, since Postgres runs every data-modifying WITH clause, read or not.
async fn late_bazaar(
    tx: &mut Transaction<'_, Postgres>,
    bazaar: &Bazaar<'_>,
    keys: &HashMap<String, i32>,
    generation: OffsetDateTime,
    fetched_at: OffsetDateTime,
) -> Result<Outcome, JobError> {
    let mut q = QuickColumns::default();
    let mut books = BookColumns::default();
    for (id, product) in &bazaar.products {
        let key = keys[id];
        q.push(key, product.quick_status.as_ref());
        books.push(key, product);
    }
    insert_quick(tx, &q, generation).await?;
    let book_rows: i64 = sqlx::query_scalar(
        "WITH s AS (
             SELECT k, b::jsonb AS buy, sl::jsonb AS sell
             FROM unnest($1::int4[], $2::text[], $3::text[]) AS u(k, b, sl)
         ), next AS (
             SELECT min(api_last_updated) AS at FROM bazaar_snapshots WHERE api_last_updated > $4
         ), changed AS (
             SELECT s.k, s.buy, s.sell, p.buy_summary AS old_buy, p.sell_summary AS old_sell,
                    p.api_last_updated IS NOT NULL AS known
             FROM s LEFT JOIN LATERAL (
                 SELECT buy_summary, sell_summary, api_last_updated FROM bazaar_books
                 WHERE product_key = s.k AND api_last_updated < $4
                   AND api_last_updated > $4 - interval '2 hours'
                 ORDER BY api_last_updated DESC LIMIT 1
             ) p ON true
             WHERE p.api_last_updated IS NULL
                OR (s.buy, s.sell) IS DISTINCT FROM (p.buy_summary, p.sell_summary)
         ), at_stamp AS (
             INSERT INTO bazaar_books (product_key, api_last_updated, buy_summary, sell_summary)
             SELECT k, $4, buy, sell FROM changed
             ON CONFLICT DO NOTHING RETURNING 1
         ), at_next AS (
             INSERT INTO bazaar_books (product_key, api_last_updated, buy_summary, sell_summary)
             SELECT c.k, next.at, c.old_buy, c.old_sell FROM changed c, next
             WHERE c.known AND next.at IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM bazaar_books b
                               WHERE b.product_key = c.k AND b.api_last_updated = next.at)
             ON CONFLICT DO NOTHING
         )
         SELECT count(*) FROM at_stamp",
    )
    .bind(&books.key)
    .bind(&books.buy)
    .bind(&books.sell)
    .bind(generation)
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO bazaar_snapshots (api_last_updated, fetched_at, product_count, book_rows,
             book_keyframe)
         VALUES ($1, $2, $3, $4, false)
         ON CONFLICT (api_last_updated) DO NOTHING",
    )
    .bind(generation)
    .bind(fetched_at)
    .bind(i32::try_from(bazaar.products.len()).unwrap_or(i32::MAX))
    .bind(i32::try_from(book_rows).unwrap_or(i32::MAX))
    .execute(&mut **tx)
    .await?;
    snapshot(tx, "bazaar", generation, fetched_at, bazaar.products.len()).await?;
    Ok(Outcome {
        feed: "bazaar",
        api_last_updated: Some(generation),
        rows: q.key.len() + usize::try_from(book_rows).unwrap_or(0),
    })
}

/// The rows of a bazaar write that a snapshot newer than every stored one makes.
struct BazaarRows {
    quick: QuickColumns,
    books: BookColumns,
    book_keyframe: bool,
}

/// `bazaar_books` rows, a column each.
#[derive(Default)]
struct BookColumns {
    key: Vec<i32>,
    buy: Vec<Option<String>>,
    sell: Vec<Option<String>>,
}

impl BookColumns {
    fn push(&mut self, key: i32, product: &BazaarProduct<'_>) {
        self.key.push(key);
        self.buy
            .push(product.buy_summary.map(|r| r.get().to_owned()));
        self.sell
            .push(product.sell_summary.map(|r| r.get().to_owned()));
    }
}

#[derive(Default)]
struct QuickColumns {
    key: Vec<i32>,
    buy_price: Vec<Option<f64>>,
    sell_price: Vec<Option<f64>>,
    buy_volume: Vec<Option<i64>>,
    sell_volume: Vec<Option<i64>>,
    buy_week: Vec<Option<i64>>,
    sell_week: Vec<Option<i64>>,
    buy_orders: Vec<Option<i32>>,
    sell_orders: Vec<Option<i32>>,
}

impl QuickColumns {
    fn push(&mut self, key: i32, s: Option<&QuickStatus>) {
        self.key.push(key);
        self.buy_price.push(s.and_then(|s| s.buy_price));
        self.sell_price.push(s.and_then(|s| s.sell_price));
        self.buy_volume
            .push(model::whole(s.and_then(|s| s.buy_volume)));
        self.sell_volume
            .push(model::whole(s.and_then(|s| s.sell_volume)));
        self.buy_week
            .push(model::whole(s.and_then(|s| s.buy_moving_week)));
        self.sell_week
            .push(model::whole(s.and_then(|s| s.sell_moving_week)));
        self.buy_orders
            .push(int32(model::whole(s.and_then(|s| s.buy_orders))));
        self.sell_orders
            .push(int32(model::whole(s.and_then(|s| s.sell_orders))));
    }
}

async fn insert_quick(
    tx: &mut Transaction<'_, Postgres>,
    q: &QuickColumns,
    generation: OffsetDateTime,
) -> Result<(), JobError> {
    sqlx::query(
        "INSERT INTO bazaar_quick (product_key, api_last_updated, buy_price, sell_price,
             buy_volume, sell_volume, buy_moving_week, sell_moving_week, buy_orders, sell_orders)
         SELECT k, $2, bp, sp, bv, sv, bw, sw, bo, so
         FROM unnest($1::int4[], $3::float8[], $4::float8[], $5::int8[], $6::int8[],
                     $7::int8[], $8::int8[], $9::int4[], $10::int4[])
              AS u(k, bp, sp, bv, sv, bw, sw, bo, so)
         ON CONFLICT DO NOTHING",
    )
    .bind(&q.key)
    .bind(generation)
    .bind(&q.buy_price)
    .bind(&q.sell_price)
    .bind(&q.buy_volume)
    .bind(&q.sell_volume)
    .bind(&q.buy_week)
    .bind(&q.sell_week)
    .bind(&q.buy_orders)
    .bind(&q.sell_orders)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn int32(v: Option<i64>) -> Option<i32> {
    v.and_then(|v| i32::try_from(v).ok())
}

async fn snapshot(
    tx: &mut Transaction<'_, Postgres>,
    feed: &str,
    generation: OffsetDateTime,
    fetched_at: OffsetDateTime,
    rows: usize,
) -> Result<(), JobError> {
    sqlx::query(
        "INSERT INTO feed_snapshots (feed, api_last_updated, fetched_at, row_count)
         VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
    )
    .bind(feed)
    .bind(generation)
    .bind(fetched_at)
    .bind(i32::try_from(rows).unwrap_or(i32::MAX))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The three stored forms of an item plus the fields pulled out of it.
#[derive(Default)]
struct ItemColumns {
    id: Vec<Option<String>>,
    uuid: Vec<Option<String>>,
    count: Vec<Option<i32>>,
    attributes: Vec<Option<String>>,
    nbt: Vec<Option<String>>,
    bytes: Vec<Option<Vec<u8>>>,
    error: Vec<Option<String>>,
}

impl ItemColumns {
    fn push(&mut self, item: Option<&ItemBytes>) {
        let decoded = item.map(|b| {
            let gzipped = nbt::decode_base64(b.base64());
            let parsed = gzipped
                .as_ref()
                .map_err(ToString::to_string)
                .and_then(|g| nbt::decode_gzipped(g).map_err(|e| e.to_string()));
            (gzipped.ok(), parsed)
        });
        match decoded {
            Some((bytes, Ok(item))) => {
                self.id.push(item.item_id);
                self.uuid.push(item.item_uuid);
                self.count.push(item.count);
                self.attributes.push(item.attributes.map(|a| a.to_string()));
                self.nbt.push(Some(item.nbt.to_string()));
                self.bytes.push(bytes);
                self.error.push(None);
            }
            Some((bytes, Err(error))) => {
                self.id.push(None);
                self.uuid.push(None);
                self.count.push(None);
                self.attributes.push(None);
                self.nbt.push(None);
                self.bytes.push(bytes);
                self.error.push(Some(error));
            }
            None => {
                self.id.push(None);
                self.uuid.push(None);
                self.count.push(None);
                self.attributes.push(None);
                self.nbt.push(None);
                self.bytes.push(None);
                self.error.push(Some("no item_bytes".into()));
            }
        }
    }
}

fn unknown_json(map: &Map<String, Json>) -> Option<String> {
    (!map.is_empty()).then(|| Json::Object(map.clone()).to_string())
}

fn uuid_list(values: &[String]) -> String {
    Json::Array(
        values
            .iter()
            .filter_map(|v| model::parse_uuid(Some(v)))
            .map(|u| Json::String(u.to_string()))
            .collect(),
    )
    .to_string()
}

async fn ended(
    tx: &mut Transaction<'_, Postgres>,
    fetched_at: OffsetDateTime,
    body: &[u8],
) -> Result<Outcome, JobError> {
    let page: EndedPage = serde_json::from_slice(body).map_err(malformed)?;
    if page.success == Some(false) {
        return Err(JobError::Invalid(anyhow!(
            "auctions_ended reports success=false"
        )));
    }
    let generation = page
        .last_updated
        .and_then(model::ms_to_time)
        .ok_or_else(|| JobError::Invalid(anyhow!("auctions_ended has no lastUpdated")))?;

    let mut ids = Vec::new();
    let mut seller = Vec::new();
    let mut seller_profile = Vec::new();
    let mut buyer = Vec::new();
    let mut buyer_profile = Vec::new();
    let mut sold_at = Vec::new();
    let mut price = Vec::new();
    let mut bin = Vec::new();
    let mut unknown = Vec::new();
    let mut items = ItemColumns::default();
    for sale in &page.auctions {
        let (Some(id), Some(at), Some(p)) = (
            model::parse_uuid(Some(&sale.auction_id)),
            sale.timestamp.and_then(model::ms_to_time),
            sale.price,
        ) else {
            continue;
        };
        ids.push(id);
        seller.push(model::parse_uuid(sale.seller.as_deref()));
        seller_profile.push(model::parse_uuid(sale.seller_profile.as_deref()));
        buyer.push(model::parse_uuid(sale.buyer.as_deref()));
        buyer_profile.push(model::parse_uuid(sale.buyer_profile.as_deref()));
        sold_at.push(at);
        price.push(p);
        bin.push(sale.bin.unwrap_or(false));
        unknown.push(unknown_json(&sale.unknown));
        items.push(sale.item_bytes.as_ref());
    }
    if ids.len() < page.auctions.len() {
        tracing::warn!(
            skipped = page.auctions.len() - ids.len(),
            "sales without an id, time, or price were not written; the archive keeps them"
        );
    }
    let rows = ids.len();
    sqlx::query(
        "INSERT INTO sales (auction_uuid, seller, seller_profile, buyer, buyer_profile, sold_at,
             price, bin, item_id, item_uuid, item_count, item_attributes, item_nbt, item_bytes,
             item_decode_error, api_unknown, api_last_updated, collected_at)
         SELECT id, s, sp, b, bp, at, p, bin, iid, iu, ic, ia::jsonb, inbt::jsonb, ib, ie, unk::jsonb, $17, $18
         FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[], $5::uuid[], $6::timestamptz[],
                     $7::int8[], $8::bool[], $9::text[], $10::text[], $11::int4[], $12::text[],
                     $13::text[], $14::bytea[], $15::text[], $16::text[])
              AS u(id, s, sp, b, bp, at, p, bin, iid, iu, ic, ia, inbt, ib, ie, unk)
         ON CONFLICT (auction_uuid, sold_at) DO NOTHING",
    )
    .bind(&ids)
    .bind(&seller)
    .bind(&seller_profile)
    .bind(&buyer)
    .bind(&buyer_profile)
    .bind(&sold_at)
    .bind(&price)
    .bind(&bin)
    .bind(&items.id)
    .bind(&items.uuid)
    .bind(&items.count)
    .bind(&items.attributes)
    .bind(&items.nbt)
    .bind(&items.bytes)
    .bind(&items.error)
    .bind(&unknown)
    .bind(generation)
    .bind(fetched_at)
    .execute(&mut **tx)
    .await?;
    snapshot(tx, "auctions_ended", generation, fetched_at, rows).await?;
    Ok(Outcome {
        feed: "auctions_ended",
        api_last_updated: Some(generation),
        rows,
    })
}

/// Inserts new listings and updates changed ones, then stores the item of each new one. Returns
/// the rows inserted or updated. A listing's `first_seen_at` never changes after the insert,
/// since `auction_items` is keyed by it.
async fn upsert_auctions(
    tx: &mut Transaction<'_, Postgres>,
    rows: &[Changed<'_>],
) -> Result<usize, JobError> {
    if rows.is_empty() {
        return Ok(0);
    }
    let c = AuctionColumns::of(rows);
    let returned: Vec<(Uuid, bool)> = sqlx::query_as(
        "INSERT INTO auctions (uuid, seller, seller_profile, coop, started_at, ends_at, item_name,
             search_text, category, categories, tier, bin, starting_bid, highest_bid, bid_count,
             claimed, claimed_bidders, auction_last_updated, item_id, item_uuid, item_count,
             api_unknown, first_seen_at, last_changed_at)
         SELECT id, s, sp,
                ARRAY(SELECT jsonb_array_elements_text(co::jsonb)::uuid),
                st, en, nm, ex, ca,
                ARRAY(SELECT jsonb_array_elements_text(cs::jsonb)),
                ti, bin, sb, hb, bc, cl,
                ARRAY(SELECT jsonb_array_elements_text(cb::jsonb)::uuid),
                lu, iid, iu, ic, unk::jsonb, seen, seen
         FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::text[], $5::timestamptz[],
                     $6::timestamptz[], $7::text[], $8::text[], $9::text[], $10::text[],
                     $11::text[], $12::bool[], $13::int8[], $14::int8[], $15::int4[], $16::bool[],
                     $17::text[], $18::timestamptz[], $19::text[], $20::text[], $21::int4[],
                     $22::text[], $23::timestamptz[])
              AS u(id, s, sp, co, st, en, nm, ex, ca, cs, ti, bin, sb, hb, bc, cl, cb, lu,
                   iid, iu, ic, unk, seen)
         ON CONFLICT (uuid) DO UPDATE SET
             highest_bid = greatest(auctions.highest_bid, excluded.highest_bid),
             bid_count = greatest(auctions.bid_count, excluded.bid_count),
             claimed = coalesce(auctions.claimed, false) OR coalesce(excluded.claimed, false),
             claimed_bidders = CASE
                 WHEN cardinality(excluded.claimed_bidders) > cardinality(auctions.claimed_bidders)
                 THEN excluded.claimed_bidders ELSE auctions.claimed_bidders END,
             auction_last_updated = greatest(auctions.auction_last_updated, excluded.auction_last_updated),
             api_unknown = coalesce(excluded.api_unknown, auctions.api_unknown),
             last_changed_at = greatest(auctions.last_changed_at, excluded.last_changed_at),
             missing_since = CASE
                 WHEN auctions.missing_since < excluded.last_changed_at THEN NULL
                 ELSE auctions.missing_since END,
             absent_since = CASE
                 WHEN auctions.absent_since < excluded.last_changed_at THEN NULL
                 ELSE auctions.absent_since END
         WHERE excluded.auction_last_updated IS DISTINCT FROM auctions.auction_last_updated
            OR excluded.highest_bid IS DISTINCT FROM auctions.highest_bid
            OR excluded.claimed IS DISTINCT FROM auctions.claimed
            OR auctions.absent_since < excluded.last_changed_at
            OR auctions.missing_since < excluded.last_changed_at
         RETURNING uuid, (xmax = 0) AS inserted",
    )
    .bind(&c.uuid)
    .bind(&c.seller)
    .bind(&c.seller_profile)
    .bind(&c.coop)
    .bind(&c.started_at)
    .bind(&c.ends_at)
    .bind(&c.item_name)
    .bind(&c.search_text)
    .bind(&c.category)
    .bind(&c.categories)
    .bind(&c.tier)
    .bind(&c.bin)
    .bind(&c.starting_bid)
    .bind(&c.highest_bid)
    .bind(&c.bid_count)
    .bind(&c.claimed)
    .bind(&c.claimed_bidders)
    .bind(&c.last_updated)
    .bind(&c.items.id)
    .bind(&c.items.uuid)
    .bind(&c.items.count)
    .bind(&c.unknown)
    .bind(&c.seen)
    .fetch_all(&mut **tx)
    .await?;
    let written = returned.len();
    let inserted: HashSet<Uuid> = returned
        .into_iter()
        .filter_map(|(uuid, new)| new.then_some(uuid))
        .collect();
    insert_items(tx, &c, &inserted).await?;
    Ok(written)
}

/// Stores the item of each listing in `inserted`, the UUIDs the upsert returned with
/// `xmax = 0`, which marks an insert rather than an update.
async fn insert_items(
    tx: &mut Transaction<'_, Postgres>,
    c: &AuctionColumns,
    inserted: &HashSet<Uuid>,
) -> Result<(), JobError> {
    if !inserted.is_empty() {
        let pick = |i: usize| inserted.contains(&c.uuid[i]);
        let idx: Vec<usize> = (0..c.uuid.len()).filter(|&i| pick(i)).collect();
        let take = |v: &Vec<Option<String>>| idx.iter().map(|&i| v[i].clone()).collect::<Vec<_>>();
        sqlx::query(
            "INSERT INTO auction_items (uuid, first_seen_at, item_lore, item_attributes, item_nbt,
                 item_bytes, item_decode_error)
             SELECT id, seen, lo, ia::jsonb, inbt::jsonb, ib, ie
             FROM unnest($1::uuid[], $2::timestamptz[], $3::text[], $4::text[], $5::text[],
                         $6::bytea[], $7::text[]) AS u(id, seen, lo, ia, inbt, ib, ie)
             ON CONFLICT DO NOTHING",
        )
        .bind(idx.iter().map(|&i| c.uuid[i]).collect::<Vec<_>>())
        .bind(idx.iter().map(|&i| c.seen[i]).collect::<Vec<_>>())
        .bind(take(&c.item_lore))
        .bind(take(&c.items.attributes))
        .bind(take(&c.items.nbt))
        .bind(
            idx.iter()
                .map(|&i| c.items.bytes[i].clone())
                .collect::<Vec<_>>(),
        )
        .bind(take(&c.items.error))
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

#[derive(Default)]
struct AuctionColumns {
    uuid: Vec<Uuid>,
    seller: Vec<Option<Uuid>>,
    seller_profile: Vec<Option<Uuid>>,
    coop: Vec<String>,
    started_at: Vec<Option<OffsetDateTime>>,
    ends_at: Vec<Option<OffsetDateTime>>,
    item_name: Vec<Option<String>>,
    item_lore: Vec<Option<String>>,
    search_text: Vec<Option<String>>,
    category: Vec<Option<String>>,
    categories: Vec<String>,
    tier: Vec<Option<String>>,
    bin: Vec<bool>,
    starting_bid: Vec<Option<i64>>,
    highest_bid: Vec<Option<i64>>,
    bid_count: Vec<i32>,
    claimed: Vec<Option<bool>>,
    claimed_bidders: Vec<String>,
    last_updated: Vec<Option<OffsetDateTime>>,
    unknown: Vec<Option<String>>,
    seen: Vec<OffsetDateTime>,
    items: ItemColumns,
}

impl AuctionColumns {
    fn of(rows: &[Changed<'_>]) -> Self {
        let mut c = Self::default();
        for row in rows {
            let a = row.auction;
            c.uuid.push(row.uuid);
            c.seller.push(model::parse_uuid(a.auctioneer.as_deref()));
            c.seller_profile
                .push(model::parse_uuid(a.profile_id.as_deref()));
            c.coop.push(uuid_list(&a.coop));
            c.started_at.push(a.start.and_then(model::ms_to_time));
            c.ends_at.push(a.end.and_then(model::ms_to_time));
            c.item_name.push(a.item_name.clone());
            c.item_lore.push(a.item_lore.clone());
            c.search_text.push(a.extra.clone());
            c.category.push(a.category.clone());
            c.categories
                .push(Json::from(a.categories.clone()).to_string());
            c.tier.push(a.tier.clone());
            c.bin.push(a.bin.unwrap_or(false));
            c.starting_bid.push(a.starting_bid);
            c.highest_bid.push(a.highest_bid_amount);
            c.bid_count
                .push(i32::try_from(a.bids.len()).unwrap_or(i32::MAX));
            c.claimed.push(a.claimed);
            c.claimed_bidders.push(uuid_list(&a.claimed_bidders));
            c.last_updated
                .push(a.last_updated.and_then(model::ms_to_time));
            c.unknown.push(unknown_json(&a.unknown));
            c.seen.push(row.seen);
            c.items.push(a.item_bytes.as_ref());
        }
        c
    }
}

async fn insert_bids(
    tx: &mut Transaction<'_, Postgres>,
    rows: &[Changed<'_>],
) -> Result<(), JobError> {
    let (mut auction, mut bidder, mut profile, mut amount, mut at) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for row in rows {
        for bid in &row.auction.bids {
            let (Some(value), Some(time)) = (bid.amount, bid.timestamp.and_then(model::ms_to_time))
            else {
                continue;
            };
            auction.push(row.uuid);
            bidder.push(model::parse_uuid(bid.bidder.as_deref()));
            profile.push(model::parse_uuid(bid.profile_id.as_deref()));
            amount.push(value);
            at.push(time);
        }
    }
    if auction.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO bids (auction_uuid, bidder, bidder_profile, amount, bid_at)
         SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::int8[], $5::timestamptz[])
         ON CONFLICT DO NOTHING",
    )
    .bind(&auction)
    .bind(&bidder)
    .bind(&profile)
    .bind(&amount)
    .bind(&at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Hashes a resource without its `lastUpdated`, which can change while the content does not.
fn content_hash(payload: &Json) -> Vec<u8> {
    use sha2::Digest as _;

    let mut content = payload.clone();
    if let Some(map) = content.as_object_mut() {
        map.remove("lastUpdated");
    }
    Sha256::digest(serde_json::to_vec(&content).expect("JSON values serialize")).to_vec()
}

fn last_updated(payload: &Json) -> Option<OffsetDateTime> {
    payload
        .get("lastUpdated")
        .and_then(Json::as_i64)
        .and_then(model::ms_to_time)
}

/// Writes an election snapshot when its content changed, with its vote counts, and records the
/// term it shows.
///
/// One election writer runs at a time, so two processes, such as a spool drain and a backfill,
/// cannot both decide from the same neighbours: the content stored just before this read, and the
/// generations of the nearest stored snapshots on each side. Cloudflare can serve a copy up to 5
/// minutes old, and a backfill merges reads from another host. A read whose generation falls
/// outside the stored ones around it would record a revert, so it is ignored, the term included:
/// of two such reads, the one stored first wins. A restore normally stores them in the same
/// order, since it backfills after replaying the archive.
async fn election(
    tx: &mut Transaction<'_, Postgres>,
    fetched_at: OffsetDateTime,
    body: &[u8],
) -> Result<Outcome, JobError> {
    let payload: Json = serde_json::from_slice(body).map_err(malformed)?;
    if payload.get("success") == Some(&Json::Bool(false)) {
        return Err(JobError::Invalid(anyhow!("election reports success=false")));
    }
    let hash = content_hash(&payload);
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('election'))")
        .execute(&mut **tx)
        .await?;
    let generation = last_updated(&payload);

    let (latest, before, after): (
        Option<Vec<u8>>,
        Option<OffsetDateTime>,
        Option<OffsetDateTime>,
    ) = sqlx::query_as(
        "SELECT
             (SELECT payload_sha256 FROM election_snapshots WHERE fetched_at <= $1
              ORDER BY fetched_at DESC LIMIT 1),
             (SELECT api_last_updated FROM election_snapshots
              WHERE fetched_at <= $1 AND api_last_updated IS NOT NULL
              ORDER BY fetched_at DESC LIMIT 1),
             (SELECT api_last_updated FROM election_snapshots
              WHERE fetched_at > $1 AND api_last_updated IS NOT NULL
              ORDER BY fetched_at LIMIT 1)",
    )
    .bind(fetched_at)
    .fetch_one(&mut **tx)
    .await?;
    if let Some(generation) = generation
        && (before.is_some_and(|b| generation < b) || after.is_some_and(|a| generation > a))
    {
        return Ok(Outcome {
            feed: "election",
            api_last_updated: Some(generation),
            rows: 0,
        });
    }
    let changed = latest.is_none_or(|h| h != hash);
    if changed {
        sqlx::query(
            "INSERT INTO election_snapshots (api_last_updated, fetched_at, payload_sha256, payload)
             VALUES ($1, $2, $3, $4::jsonb)",
        )
        .bind(generation)
        .bind(fetched_at)
        .bind(&hash)
        .bind(payload.to_string())
        .execute(&mut **tx)
        .await?;
        if let Some(current) = payload.get("current") {
            election_votes(tx, current, fetched_at).await?;
        }
    }
    if let Some(mayor) = payload.get("mayor") {
        mayor_term(tx, mayor, fetched_at).await?;
    }
    Ok(Outcome {
        feed: "election",
        api_last_updated: generation,
        rows: usize::from(changed),
    })
}

async fn election_votes(
    tx: &mut Transaction<'_, Postgres>,
    current: &Json,
    observed_at: OffsetDateTime,
) -> Result<(), JobError> {
    let Some(year) = current
        .get("year")
        .and_then(Json::as_i64)
        .and_then(|y| i32::try_from(y).ok())
    else {
        return Ok(());
    };
    let candidates = current
        .get("candidates")
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_default();
    let (mut key, mut name, mut votes, mut perks) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for c in &candidates {
        let Some(k) = c.get("key").and_then(Json::as_str) else {
            continue;
        };
        key.push(k.to_owned());
        name.push(c.get("name").and_then(Json::as_str).map(str::to_owned));
        votes.push(c.get("votes").and_then(Json::as_i64));
        perks.push(c.get("perks").map(Json::to_string));
    }
    sqlx::query(
        "INSERT INTO election_votes (election_year, candidate_key, observed_at, candidate_name, votes, perks)
         SELECT $1, k, $2, n, v, p::jsonb FROM unnest($3::text[], $4::text[], $5::int8[], $6::text[]) AS u(k, n, v, p)
         ON CONFLICT DO NOTHING",
    )
    .bind(year)
    .bind(observed_at)
    .bind(&key)
    .bind(&name)
    .bind(&votes)
    .bind(&perks)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Records the term the payload shows. An observation older than the newest one stored, such as a
/// backfilled snapshot, only widens the observed range and leaves the term's details as they are.
async fn mayor_term(
    tx: &mut Transaction<'_, Postgres>,
    mayor: &Json,
    observed_at: OffsetDateTime,
) -> Result<(), JobError> {
    let Some(year) = mayor
        .pointer("/election/year")
        .and_then(Json::as_i64)
        .and_then(|y| i32::try_from(y).ok())
    else {
        return Ok(());
    };
    let text = |p: &str| mayor.pointer(p).and_then(Json::as_str).map(str::to_owned);
    let json = |p: &str| mayor.pointer(p).map(Json::to_string);
    sqlx::query(
        "INSERT INTO mayor_terms (election_year, term_start, term_end, mayor_key, mayor_name,
             mayor_perks, minister_key, minister_name, minister_perk, election_results,
             first_observed_at, last_observed_at, mayor_votes)
         VALUES ($1, mayor_term_start($1), mayor_term_start($1 + 1), $2, $3, $4::jsonb, $5, $6,
                 $7::jsonb, $8::jsonb, $9, $9,
                 (SELECT try_bigint(c->>'votes') FROM jsonb_array_elements(
                      CASE WHEN jsonb_typeof($8::jsonb) = 'array' THEN $8::jsonb ELSE '[]' END) c
                  WHERE c->>'key' = $2 LIMIT 1))
         ON CONFLICT (election_year) DO UPDATE SET
             mayor_key = excluded.mayor_key, mayor_name = excluded.mayor_name,
             mayor_perks = excluded.mayor_perks, minister_key = excluded.minister_key,
             minister_name = excluded.minister_name, minister_perk = excluded.minister_perk,
             election_results = excluded.election_results, mayor_votes = excluded.mayor_votes,
             last_observed_at = excluded.last_observed_at
         WHERE excluded.last_observed_at >= mayor_terms.last_observed_at",
    )
    .bind(year)
    .bind(text("/key"))
    .bind(text("/name"))
    .bind(json("/perks"))
    .bind(text("/minister/key"))
    .bind(text("/minister/name"))
    .bind(json("/minister/perk"))
    .bind(json("/election/candidates"))
    .bind(observed_at)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE mayor_terms SET first_observed_at = $2
         WHERE election_year = $1 AND first_observed_at > $2",
    )
    .bind(year)
    .bind(observed_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn resource(
    tx: &mut Transaction<'_, Postgres>,
    name: &str,
    fetched_at: OffsetDateTime,
    body: &[u8],
) -> Result<Outcome, JobError> {
    let payload: Json = serde_json::from_slice(body).map_err(malformed)?;
    if payload.get("success") == Some(&Json::Bool(false)) {
        return Err(JobError::Invalid(anyhow!("{name} reports success=false")));
    }
    let generation = last_updated(&payload);
    let inserted = sqlx::query(
        "INSERT INTO resource_snapshots (resource, payload_sha256, api_last_updated, fetched_at, payload)
         VALUES ($1, $2, $3, $4, $5::jsonb) ON CONFLICT DO NOTHING",
    )
    .bind(name)
    .bind(content_hash(&payload))
    .bind(generation)
    .bind(fetched_at)
    .bind(payload.to_string())
    .execute(&mut **tx)
    .await?
    .rows_affected();
    Ok(Outcome {
        feed: "resource",
        api_last_updated: generation,
        rows: usize::try_from(inserted).unwrap_or(0),
    })
}

/// Writes kept fetches to `fetch_log`.
///
/// # Errors
///
/// Fails when Postgres refuses the rows or is unreachable.
pub async fn write_fetches(pool: &PgPool, rows: &[FetchRow]) -> Result<(), sqlx::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let feed: Vec<&str> = rows.iter().map(|r| r.feed).collect();
    let url: Vec<&str> = rows.iter().map(|r| r.url.as_str()).collect();
    let started: Vec<OffsetDateTime> = rows.iter().map(|r| r.started_at).collect();
    let duration: Vec<i32> = rows
        .iter()
        .map(|r| i32::try_from(r.duration_ms).unwrap_or(i32::MAX))
        .collect();
    let status: Vec<Option<i16>> = rows
        .iter()
        .map(|r| r.http_status.and_then(|s| i16::try_from(s).ok()))
        .collect();
    let bytes: Vec<Option<i32>> = rows
        .iter()
        .map(|r| r.body_bytes.and_then(|b| i32::try_from(b).ok()))
        .collect();
    let lu: Vec<Option<OffsetDateTime>> = rows.iter().map(|r| r.api_last_updated).collect();
    let age: Vec<Option<i32>> = rows
        .iter()
        .map(|r| r.cache_age_s.and_then(|a| i32::try_from(a).ok()))
        .collect();
    let error: Vec<Option<&str>> = rows.iter().map(|r| r.error.as_deref()).collect();
    sqlx::query(
        "INSERT INTO fetch_log (feed, url, started_at, duration_ms, http_status, body_bytes,
             api_last_updated, cache_age_s, error)
         SELECT * FROM unnest($1::text[], $2::text[], $3::timestamptz[], $4::int4[], $5::int2[],
                              $6::int4[], $7::timestamptz[], $8::int4[], $9::text[])",
    )
    .bind(&feed)
    .bind(&url)
    .bind(&started)
    .bind(&duration)
    .bind(&status)
    .bind(&bytes)
    .bind(&lu)
    .bind(&age)
    .bind(&error)
    .execute(pool)
    .await?;
    Ok(())
}
