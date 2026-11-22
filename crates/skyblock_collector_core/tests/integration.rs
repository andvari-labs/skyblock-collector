//! Integration tests against a real Postgres. Each test starts a throwaway server through `docker`
//! and gets a database of its own on it; when `TEST_DATABASE_URL` names a server where the user can
//! create databases, the tests use that one instead.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde_json::Value;
use skyblock_collector_core::archive::{self, Archive};
use skyblock_collector_core::db::{self, Db, JobError};
use skyblock_collector_core::http::Fetched;
use skyblock_collector_core::ingest::{self, Ingest};
use skyblock_collector_core::spool::{Job, Meta, Priority, Spool};
use skyblock_collector_core::status::{FetchRow, Status};
use skyblock_collector_core::{model, nbt, replay};
use skyblock_collector_testdb::TestPostgres;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;
use time::macros::datetime;
use tokio::sync::Notify;
use tokio::time as tokio_time;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const AUCTIONS: &[u8] = include_bytes!("fixtures/auctions_page.json");
const ENDED: &[u8] = include_bytes!("fixtures/ended.json");
const BAZAAR_A: &[u8] = include_bytes!("fixtures/bazaar_a.json");
const BAZAAR_B: &[u8] = include_bytes!("fixtures/bazaar_b.json");
const ELECTION: &[u8] = include_bytes!("fixtures/election.json");

/// A database of its own for one test, on a server that lives as long as the test holds it.
struct TestDb {
    pool: PgPool,
    url: String,
    _server: TestPostgres,
}

/// Starts a Postgres server for this test, and creates a database on it with the schema.
async fn test_db() -> TestDb {
    let server = TestPostgres::start()
        .await
        .expect("the test Postgres server should start");
    let url = server
        .create_database()
        .await
        .expect("the test server should create a database");
    let pool = db::connect(&url)
        .await
        .expect("the test database should accept connections");
    db::ensure_schema(&pool)
        .await
        .expect("the schema should apply to an empty database");
    TestDb {
        pool,
        url,
        _server: server,
    }
}

/// The schema's `_sqlx_migrations` row exactly as `sqlx::migrate!("./schema")` wrote it in every
/// database the collector has created. The collector refuses a database whose row differs, so
/// a change here breaks every existing database and restore point.
#[test]
fn the_schema_is_recorded_as_every_existing_database_recorded_it() {
    skyblock_collector_testdb::block_on(async {
        let db = test_db().await;
        let row: (i64, String, bool, String) = sqlx::query_as(
            "SELECT version, description, success, encode(checksum, 'hex') FROM _sqlx_migrations",
        )
        .fetch_one(&db.pool)
        .await
        .expect("the migration should be recorded");
        assert_eq!(
            row,
            (
                1,
                "schema".to_owned(),
                true,
                "50c40b022241744ff492a0be3dfc3edc5aa0e9dbc2373f26c1b8ef4fbfb59cb402a27b36b2c8f21b9d79db744bc3a7fc".to_owned()
            ),
            "version, description and checksum must match the live databases' row"
        );
        db::ensure_schema(&db.pool)
            .await
            .expect("applying the schema again to a database that has it should do nothing");
    });
}

/// A page wrapped in a JSON array, as the collector spools page 0 of the auctions.
fn wrap(page: &[u8]) -> Vec<u8> {
    let mut body = b"[".to_vec();
    body.extend_from_slice(page);
    body.push(b']');
    body
}

/// The first copy of each listing on an auctions page; the fixture repeats some listings.
fn distinct_listings(page: &Value) -> Vec<Value> {
    let mut seen = HashSet::new();
    let mut listings = Vec::new();
    for listing in page["auctions"]
        .as_array()
        .expect("the page lists auctions")
    {
        let uuid = listing["uuid"].as_str().expect("every listing has a uuid");
        if seen.insert(uuid.to_owned()) {
            listings.push(listing.clone());
        }
    }
    listings
}

/// The number of distinct listings in the auctions fixture.
fn fixture_auctions() -> i64 {
    let page: Value = serde_json::from_slice(AUCTIONS).expect("the auctions fixture is JSON");
    i64::try_from(distinct_listings(&page).len()).expect("the fixture's listing count fits an i64")
}

/// An archived response the way the collector fetched it.
fn response(at: OffsetDateTime, status: u16, body: &[u8]) -> Fetched {
    Fetched {
        url: "https://example.invalid".to_owned(),
        started_at: at,
        duration_ms: 1,
        status,
        headers: HashMap::default(),
        body: Bytes::copy_from_slice(body),
    }
}

/// The single count that `sql` selects.
async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_owned()))
        .fetch_one(pool)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

/// The `lastUpdated` time of a fixture page.
fn page_stamp(page: &[u8]) -> OffsetDateTime {
    let v: Value = serde_json::from_slice(page).expect("the page is JSON");
    let millis = v["lastUpdated"]
        .as_i64()
        .expect("the page has a numeric lastUpdated");
    model::ms_to_time(millis).expect("the page's lastUpdated is a valid time")
}

/// Sales written twice are stored once, each with its item decoded and its raw bytes kept.
#[test]
fn sales_are_decoded_and_written_once() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let db = Db::default();
        let fetched = page_stamp(ENDED);
        db.write(&t.pool, &Job::Ended, fetched, ENDED)
            .await
            .unwrap();
        db.write(&t.pool, &Job::Ended, fetched, ENDED)
            .await
            .unwrap();

        assert_eq!(count(&t.pool, "SELECT count(*) FROM sales").await, 40);
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM sales WHERE item_id IS NOT NULL AND item_nbt IS NOT NULL AND item_bytes IS NOT NULL").await,
            40,
            "every real item decodes"
        );
        assert_eq!(
            count(
                &t.pool,
                "SELECT count(*) FROM sales WHERE item_decode_error IS NOT NULL"
            )
            .await,
            0
        );
        assert_eq!(
            count(
                &t.pool,
                "SELECT count(*) FROM feed_snapshots WHERE feed = 'auctions_ended'"
            )
            .await,
            1
        );
        let first: Value = serde_json::from_slice::<Value>(ENDED).unwrap()["auctions"][0].clone();
        let id = Uuid::try_parse(first["auction_id"].as_str().unwrap()).unwrap();
        let bytes: Vec<u8> =
            sqlx::query_scalar("SELECT item_bytes FROM sales WHERE auction_uuid = $1")
                .bind(id)
                .fetch_one(&t.pool)
                .await
                .unwrap();
        assert_eq!(
            bytes,
            nbt::decode_base64(first["item_bytes"].as_str().unwrap()).unwrap(),
            "the raw bytes are the exact gzip Hypixel sent"
        );
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM sales_enriched").await,
            40,
            "the enriched view joins without error"
        );
    });
}

/// Auctions get one item row each and keep their bids. A later snapshot with a higher bid updates
/// the listing but adds no second item row, even when a fresh cache, as after a restart, writes it.
#[test]
fn auctions_write_items_once_and_track_bids() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let db = Db::default();
        let seen = page_stamp(AUCTIONS);
        db.write(&t.pool, &Job::AuctionsDelta, seen, &wrap(AUCTIONS))
            .await
            .unwrap();
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM auctions").await,
            fixture_auctions()
        );
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM auction_items").await,
            fixture_auctions()
        );
        assert!(
            count(&t.pool, "SELECT count(*) FROM bids").await >= 6,
            "three regular auctions with at least two bids each"
        );
        assert_eq!(
            count(
                &t.pool,
                "SELECT count(*) FROM listings WHERE item_attributes IS NOT NULL"
            )
            .await,
            fixture_auctions()
        );
        assert!(
            count(
                &t.pool,
                "SELECT count(*) FROM listings WHERE pet_type IS NOT NULL AND item_tag LIKE 'PET_%'"
            )
            .await
                >= 1
        );

        let mut page: Value = serde_json::from_slice(AUCTIONS).unwrap();
        let auction = &mut page["auctions"][0];
        let uuid = Uuid::try_parse(auction["uuid"].as_str().unwrap()).unwrap();
        let bid = auction["highest_bid_amount"].as_i64().unwrap() + 1_000;
        auction["highest_bid_amount"] = bid.into();
        auction["last_updated"] = (auction["last_updated"].as_i64().unwrap() + 60_000).into();
        page["lastUpdated"] = (page["lastUpdated"].as_i64().unwrap() + 60_000).into();
        let later = serde_json::to_vec(&page).unwrap();
        Db::default()
            .write(
                &t.pool,
                &Job::AuctionsDelta,
                seen + Duration::from_secs(60),
                &wrap(&later),
            )
            .await
            .unwrap();
        let stored: i64 = sqlx::query_scalar("SELECT highest_bid FROM auctions WHERE uuid = $1")
            .bind(uuid)
            .fetch_one(&t.pool)
            .await
            .unwrap();
        assert_eq!(stored, bid, "the later snapshot's higher bid is stored");
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM auction_items").await,
            fixture_auctions(),
            "the later snapshot adds no second item row"
        );

        let first_seen: OffsetDateTime =
            sqlx::query_scalar("SELECT first_seen_at FROM auctions WHERE uuid = $1")
                .bind(uuid)
                .fetch_one(&t.pool)
                .await
                .unwrap();
        assert_eq!(first_seen, seen, "first_seen_at keeps the first sighting");
    });
}

/// A BIN listing still hours from its end is removed, so its disappearance is a cancellation. One
/// complete sweep that misses it flags it, an incomplete one confirms nothing, and a second
/// complete sweep makes it gone since the first miss.
///
/// A gap in `auctions_ended` makes the outcome unknown only when it could hide the sale: the
/// listing was last seen at `seen` and the sweep at +300 s first missed it. A sweep that straddles
/// the gap's start flags the listing with its oldest page, from before the gap, so the test makes
/// the first miss such a sweep and then puts the flags back.
///
/// Replaying an older sweep or page 0 that still listed it leaves the later flags alone, and a
/// later sweep that lists it again clears them.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one listing's whole life, each step checking the database state the step before left"
)]
fn listings_are_gone_after_two_complete_sweeps_miss_them() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let db = Db::default();
        let seen = page_stamp(AUCTIONS);
        db.write(&t.pool, &Job::AuctionsDelta, seen, &wrap(AUCTIONS))
            .await
            .unwrap();

        let base: Value = serde_json::from_slice(AUCTIONS).unwrap();
        let stamp = base["lastUpdated"].as_i64().unwrap();
        let removed = base["auctions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["bin"] == true && a["end"].as_i64().unwrap() > stamp + 3_600_000)
            .unwrap()
            .clone();
        let removed_uuid = Uuid::try_parse(removed["uuid"].as_str().unwrap()).unwrap();
        let sweep_without = |minutes: i64| {
            let mut page = base.clone();
            page["auctions"]
                .as_array_mut()
                .unwrap()
                .retain(|a| a["uuid"] != removed["uuid"]);
            page["lastUpdated"] = (stamp + minutes * 60_000).into();
            wrap(&serde_json::to_vec(&page).unwrap())
        };
        let state = |pool: PgPool| async move {
            sqlx::query_as::<_, (Option<OffsetDateTime>, Option<OffsetDateTime>)>(
                "SELECT missing_since, absent_since FROM auctions WHERE uuid = $1",
            )
            .bind(removed_uuid)
            .fetch_one(&pool)
            .await
            .unwrap()
        };
        let complete = Job::AuctionsSweep {
            complete: true,
            consistent: false,
        };

        let first_miss = seen + Duration::from_secs(300);
        db.write(&t.pool, &complete, first_miss, &sweep_without(5))
            .await
            .unwrap();
        let (missing, absent) = state(t.pool.clone()).await;
        assert_eq!(
            missing,
            Some(seen + Duration::from_secs(300)),
            "one complete sweep that misses it flags it"
        );
        assert_eq!(absent, None, "one complete sweep doesn't make it gone");
        assert_eq!(
            count(
                &t.pool,
                "SELECT count(*) FROM auctions WHERE missing_since IS NOT NULL"
            )
            .await,
            1
        );

        let incomplete = Job::AuctionsSweep {
            complete: false,
            consistent: false,
        };
        db.write(
            &t.pool,
            &incomplete,
            seen + Duration::from_secs(600),
            &sweep_without(10),
        )
        .await
        .unwrap();
        assert_eq!(
            state(t.pool.clone()).await.1,
            None,
            "an incomplete sweep confirms nothing"
        );

        db.write(
            &t.pool,
            &complete,
            seen + Duration::from_mins(15),
            &sweep_without(15),
        )
        .await
        .unwrap();
        let (_, absent) = state(t.pool.clone()).await;
        assert_eq!(
            absent,
            Some(seen + Duration::from_secs(300)),
            "a second complete sweep that misses it makes it gone since the first miss"
        );
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM auction_sweeps").await,
            3
        );
        let outcome: String =
            sqlx::query_scalar("SELECT outcome FROM auction_outcomes WHERE uuid = $1")
                .bind(removed_uuid)
                .fetch_one(&t.pool)
                .await
                .unwrap();
        assert_eq!(
            outcome, "cancelled",
            "it vanished before its end time without a sale"
        );

        let outcome_with_ended_gap = |from: OffsetDateTime, to: OffsetDateTime| {
            let pool = t.pool.clone();
            async move {
                sqlx::query(
                    "INSERT INTO feed_snapshots (feed, api_last_updated, fetched_at, row_count)
                 VALUES ('auctions_ended', $1, $1, 0), ('auctions_ended', $2, $2, 0)",
                )
                .bind(from)
                .bind(to)
                .execute(&pool)
                .await
                .unwrap();
                let outcome: String =
                    sqlx::query_scalar("SELECT outcome FROM auction_outcomes WHERE uuid = $1")
                        .bind(removed_uuid)
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                sqlx::query("DELETE FROM feed_snapshots WHERE feed = 'auctions_ended'")
                    .execute(&pool)
                    .await
                    .unwrap();
                outcome
            }
        };
        let secs = |s: u64| Duration::from_secs(s);
        for (from, to, expected, why) in [
            (
                seen,
                seen + secs(240),
                "unknown",
                "sales went unrecorded while it vanished",
            ),
            (
                seen + secs(200),
                seen + secs(500),
                "unknown",
                "the gap reaches past the sweep that flagged it",
            ),
            (
                seen + secs(400),
                seen + secs(700),
                "cancelled",
                "the sweep flagged it before the gap",
            ),
            (
                seen - secs(300),
                seen - secs(100),
                "cancelled",
                "it was still listed after the gap",
            ),
            (
                seen,
                seen + secs(30),
                "cancelled",
                "a 30-second step is not a gap",
            ),
            (
                seen + secs(1000),
                seen + secs(1300),
                "cancelled",
                "no complete sweep has followed the gap",
            ),
        ] {
            assert_eq!(outcome_with_ended_gap(from, to).await, expected, "{why}");
        }
        let flag_first_miss_at = |at: OffsetDateTime| {
            let pool = t.pool.clone();
            async move {
                sqlx::query(
                    "UPDATE auction_sweeps SET oldest_page_at = $1 WHERE api_last_updated = $2",
                )
                .bind(at)
                .bind(seen + secs(300))
                .execute(&pool)
                .await
                .unwrap();
                sqlx::query(
                    "UPDATE auctions SET missing_since = $1, absent_since = $1 WHERE uuid = $2",
                )
                .bind(at)
                .bind(removed_uuid)
                .execute(&pool)
                .await
                .unwrap();
            }
        };
        flag_first_miss_at(seen + secs(100)).await;
        assert_eq!(
            outcome_with_ended_gap(seen + secs(200), seen + secs(400)).await,
            "unknown",
            "the flagging sweep was still reading pages after the gap began"
        );
        flag_first_miss_at(seen + secs(300)).await;
        assert_eq!(
            count(
                &t.pool,
                "SELECT count(*) FROM auctions WHERE absent_since IS NOT NULL"
            )
            .await,
            1,
            "only the removed listing"
        );

        db.write(
            &t.pool,
            &complete,
            seen + Duration::from_secs(60),
            &wrap(AUCTIONS),
        )
        .await
        .unwrap();
        Db::default()
            .write(&t.pool, &Job::AuctionsDelta, seen, &wrap(AUCTIONS))
            .await
            .unwrap();
        assert_eq!(
            state(t.pool.clone()).await,
            (
                Some(seen + Duration::from_secs(300)),
                Some(seen + Duration::from_secs(300))
            ),
            "an older sighting must not clear a newer flag"
        );

        let mut back = base.clone();
        back["lastUpdated"] = (stamp + 20 * 60_000).into();
        db.write(
            &t.pool,
            &complete,
            seen + Duration::from_mins(20),
            &wrap(&serde_json::to_vec(&back).unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(
            state(t.pool.clone()).await,
            (None, None),
            "listed again in a later sweep, it is no longer flagged"
        );
    });
}

/// Every bazaar snapshot keeps every product's price, but only the order books that changed. Each
/// product's price row in the second snapshot holds that snapshot's values, and its order book as
/// of then, the newest book row at or before it, matches the snapshot's when compared as jsonb,
/// which normalizes numbers such as 90000000.0.
#[test]
fn bazaar_keeps_every_price_and_changed_books() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let db = Db::default();
        let a = page_stamp(BAZAAR_A);
        let b = page_stamp(BAZAAR_B);
        db.write(&t.pool, &Job::Bazaar, a, BAZAAR_A).await.unwrap();
        db.write(&t.pool, &Job::Bazaar, b, BAZAAR_B).await.unwrap();
        db.write(&t.pool, &Job::Bazaar, b, BAZAAR_B).await.unwrap();

        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM bazaar_products").await,
            40
        );
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM bazaar_snapshots").await,
            2
        );
        let first_rows = count(
            &t.pool,
            &format!(
                "SELECT count(*) FROM bazaar_quick WHERE api_last_updated = '{}'",
                archive::format_time(a)
            ),
        )
        .await;
        assert_eq!(first_rows, 40);
        let second_rows = count(
            &t.pool,
            &format!(
                "SELECT count(*) FROM bazaar_quick WHERE api_last_updated = '{}'",
                archive::format_time(b)
            ),
        )
        .await;
        assert_eq!(
            second_rows, 40,
            "every product gets a price row in every snapshot"
        );
        let books = |at: OffsetDateTime| {
            format!(
                "SELECT count(*) FROM bazaar_books WHERE api_last_updated = '{}'",
                archive::format_time(at)
            )
        };
        assert_eq!(
            count(&t.pool, &books(a)).await,
            40,
            "the first order books are a keyframe"
        );
        let changed_books = count(&t.pool, &books(b)).await;
        assert!(
            (1..40).contains(&changed_books),
            "only changed order books get rows, got {changed_books}"
        );

        let bz: Value = serde_json::from_slice(BAZAAR_B).unwrap();
        for (id, product) in bz["products"].as_object().unwrap() {
            let row = sqlx::query(
                "SELECT buy_price, sell_volume FROM bazaar_prices WHERE product_id = $1 AND api_last_updated = $2",
            )
            .bind(id)
            .bind(b)
            .fetch_one(&t.pool)
            .await
            .unwrap();
            let buy: Option<f64> = row.get(0);
            assert_eq!(buy, product["quick_status"]["buyPrice"].as_f64(), "{id}");
            let volume: Option<i64> = row.get(1);
            assert_eq!(
                volume,
                product["quick_status"]["sellVolume"].as_i64(),
                "{id}"
            );
            let same: bool =
                sqlx::query_scalar("SELECT sell_summary = $3::jsonb FROM bazaar_book_at($1, $2)")
                    .bind(id)
                    .bind(b + Duration::from_secs(5))
                    .bind(product["sell_summary"].to_string())
                    .fetch_one(&t.pool)
                    .await
                    .unwrap();
            assert!(same, "{id}");
        }
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM bazaar_orders").await,
            count(&t.pool, "SELECT count(*) FROM bazaar_books").await
        );
    });
}

/// Each write is a fresh process with empty caches, as after a restart, a failed job, or a replay,
/// and none of them takes product keys for products already stored.
#[test]
fn cold_starts_do_not_use_up_product_keys() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let mut bazaar: Value = serde_json::from_slice(BAZAAR_A).unwrap();
        let stamp = bazaar["lastUpdated"].as_i64().unwrap();
        for n in 0..20 {
            bazaar["lastUpdated"] = (stamp + n * 20_000).into();
            let body = serde_json::to_vec(&bazaar).unwrap();
            let at = page_stamp(&body);
            Db::default()
                .write(&t.pool, &Job::Bazaar, at, &body)
                .await
                .unwrap();
        }
        let used: i64 = sqlx::query_scalar(
            "SELECT last_value FROM pg_sequences WHERE sequencename = 'bazaar_products_product_key_seq'",
        )
        .fetch_one(&t.pool)
        .await
        .unwrap();

        assert_eq!(used, 40, "only the 40 new products took keys");
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM bazaar_snapshots").await,
            20
        );
    });
}

/// A candidate's row in `election_candidates`.
struct Candidate {
    name: String,
    won: Option<bool>,
    became_minister: Option<bool>,
    final_rank: Option<i64>,
}

/// An election snapshot, written at its own time since the mayor cycle it shows depends on it,
/// records the term, the votes, the candidates of the finished and the open election, the perks,
/// and the mayor cycle at that time. `mayor_snapshots` lists changes of the mayor, perks, or
/// minister only: new vote counts add an election snapshot but no mayor snapshot, and a perk
/// change adds one.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "checks every table one election snapshot fills, against the same written database"
)]
fn election_records_terms_votes_and_changes_only() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let db = Db::default();
        let now = page_stamp(ELECTION);
        db.write(&t.pool, &Job::Election, now, ELECTION)
            .await
            .unwrap();
        db.write(
            &t.pool,
            &Job::Election,
            now + Duration::from_secs(60),
            ELECTION,
        )
        .await
        .unwrap();
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM election_snapshots").await,
            1
        );

        let payload: Value = serde_json::from_slice(ELECTION).unwrap();
        let year = i32::try_from(payload["mayor"]["election"]["year"].as_i64().unwrap()).unwrap();
        let row = sqlx::query(
            "SELECT mayor_name, term_start, term_end FROM mayor_terms WHERE election_year = $1",
        )
        .bind(year)
        .fetch_one(&t.pool)
        .await
        .unwrap();
        let name: String = row.get(0);
        assert_eq!(name, payload["mayor"]["name"].as_str().unwrap());
        let start: OffsetDateTime = row.get(1);
        let end: OffsetDateTime = row.get(2);
        assert_eq!(end - start, time::Duration::hours(124));
        let votes: Option<i64> =
            sqlx::query_scalar("SELECT mayor_votes FROM mayor_terms WHERE election_year = $1")
                .bind(year)
                .fetch_one(&t.pool)
                .await
                .unwrap();
        let winner_votes = payload["mayor"]["election"]["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["key"] == payload["mayor"]["key"])
            .and_then(|c| c["votes"].as_i64());
        assert_eq!(votes, winner_votes);
        let start_524: OffsetDateTime = sqlx::query_scalar("SELECT mayor_term_start(524)")
            .fetch_one(&t.pool)
            .await
            .unwrap();
        assert_eq!(
            start_524,
            datetime!(2026-11-10 07:15 UTC),
            "year 524's term started 2026-11-10 07:15 UTC, as observed from the API"
        );
        let in_office: i32 =
            sqlx::query_scalar("SELECT mayor_election_year('2026-11-13 06:30+00')")
                .fetch_one(&t.pool)
                .await
                .unwrap();
        assert_eq!(
            in_office, 524,
            "year 524's mayor (Foxy) was in office on 2026-11-13, as observed from the API"
        );
        let candidates = payload["current"]["candidates"]
            .as_array()
            .map_or(0, Vec::len);
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM election_votes").await,
            i64::try_from(candidates).unwrap()
        );

        let open_year = i32::try_from(payload["current"]["year"].as_i64().unwrap()).unwrap();
        let seen = model::ms_to_time(payload["lastUpdated"].as_i64().unwrap()).unwrap();
        let (opens, closes): (OffsetDateTime, OffsetDateTime) =
            sqlx::query_as("SELECT election_opens($1), election_closes($1)")
                .bind(open_year)
                .fetch_one(&t.pool)
                .await
                .unwrap();
        assert!(
            opens < seen && seen < closes,
            "the calendar puts this snapshot inside the open election: {opens} < {seen} < {closes}"
        );
        assert_eq!(
            closes - opens,
            time::Duration::minutes((191 + 88) * 20),
            "191 days of year Y left after Late Summer 27, plus 88 into year Y + 1, at 20 minutes a day"
        );

        let finished: Vec<Candidate> = sqlx::query(
            "SELECT candidate_name, won, became_minister, final_rank FROM election_candidates
         WHERE election_year = $1 ORDER BY final_rank",
        )
        .bind(year)
        .fetch_all(&t.pool)
        .await
        .unwrap()
        .iter()
        .map(|row| Candidate {
            name: row.get(0),
            won: row.get(1),
            became_minister: row.get(2),
            final_rank: row.get(3),
        })
        .collect();
        assert_eq!(
            finished.len(),
            payload["mayor"]["election"]["candidates"]
                .as_array()
                .unwrap()
                .len()
        );
        assert_eq!(finished[0].name, payload["mayor"]["name"].as_str().unwrap());
        assert_eq!(
            (finished[0].won, finished[0].final_rank),
            (Some(true), Some(1))
        );
        let minister = finished
            .iter()
            .find(|c| c.became_minister == Some(true))
            .unwrap();
        assert_eq!(
            minister.name,
            payload["mayor"]["minister"]["name"].as_str().unwrap()
        );
        let open = count(
            &t.pool,
            &format!("SELECT count(*) FROM election_candidates WHERE election_year = {open_year} AND won IS NULL AND last_observed_votes IS NOT NULL AND minister_perk IS NOT NULL"),
        )
        .await;
        assert_eq!(
            open,
            i64::try_from(candidates).unwrap(),
            "the open election's candidates as observed"
        );

        let perks = count(
            &t.pool,
            &format!("SELECT count(*) FROM mayor_perks WHERE election_year = {year}"),
        )
        .await;
        assert_eq!(
            perks,
            i64::try_from(payload["mayor"]["perks"].as_array().unwrap().len() + 1).unwrap(),
            "every mayor perk plus the minister's"
        );
        assert_eq!(
            count(
                &t.pool,
                "SELECT count(*) FROM mayor_perks WHERE description LIKE '%§%'"
            )
            .await,
            0,
            "formatting codes are stripped"
        );

        let leader = payload["current"]["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .max_by_key(|c| c["votes"].as_i64().unwrap())
            .unwrap();
        let context = sqlx::query(
            "SELECT mayor_name, voting_open, leading_candidate_name, leading_share::float8, extract(epoch FROM into_term + until_term_end)::float8
         FROM mayor_context($1)",
        )
        .bind(now + Duration::from_secs(1))
        .fetch_one(&t.pool)
        .await
        .unwrap();
        let mayor: String = context.get(0);
        assert_eq!(
            mayor,
            payload["mayor"]["name"].as_str().unwrap(),
            "the mayor cycle at the time of the snapshot"
        );
        let voting: bool = context.get(1);
        assert!(voting, "voting is open at the time of the snapshot");
        let leading: String = context.get(2);
        assert_eq!(leading, leader["name"].as_str().unwrap());
        let share: f64 = context.get(3);
        let total: i64 = payload["current"]["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["votes"].as_i64().unwrap())
            .sum();
        #[expect(
            clippy::cast_precision_loss,
            reason = "vote counts are far below 2^52, so an f64 holds them exactly"
        )]
        let expected = leader["votes"].as_i64().unwrap() as f64 / total as f64;
        assert!((share - expected).abs() < 1e-9, "{share} vs {expected}");
        let length: f64 = context.get(4);
        assert!(
            (length - 124.0 * 3_600.0).abs() < 1e-6,
            "a term is 124 hours, got {length} s"
        );
        assert_eq!(
            count(
                &t.pool,
                "SELECT count(*) FROM election_standings WHERE rank = 1"
            )
            .await,
            1
        );
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM mayor_snapshots").await,
            1
        );

        let mut votes_moved = payload.clone();
        votes_moved["current"]["candidates"][0]["votes"] =
            (payload["current"]["candidates"][0]["votes"]
                .as_i64()
                .unwrap()
                + 1)
            .into();
        db.write(
            &t.pool,
            &Job::Election,
            now + Duration::from_secs(120),
            &serde_json::to_vec(&votes_moved).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM election_snapshots").await,
            2,
            "new vote counts add an election snapshot"
        );
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM mayor_snapshots").await,
            1,
            "new vote counts add no mayor snapshot"
        );
        let mut perks_changed = votes_moved.clone();
        perks_changed["mayor"]["perks"] =
            serde_json::json!([{"name": "Perkpocalypse", "description": "x"}]);
        db.write(
            &t.pool,
            &Job::Election,
            now + Duration::from_secs(180),
            &serde_json::to_vec(&perks_changed).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM mayor_snapshots").await,
            2,
            "a perk change adds a mayor snapshot"
        );
    });
}

#[test]
fn closed_partitions_refuse_writes_until_reopened() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let db = Db::default();
        let fetched = page_stamp(ENDED);
        let day = fetched.date();
        sqlx::query("SELECT ensure_partitions($1, $1)")
            .bind(day)
            .execute(&t.pool)
            .await
            .unwrap();
        db.write(&t.pool, &Job::Ended, fetched, ENDED)
            .await
            .unwrap();

        let closed: Vec<String> = sqlx::query_scalar("SELECT close_partitions($1 + 1)")
            .bind(day)
            .fetch_all(&t.pool)
            .await
            .unwrap();
        let sales_partition = format!("sales_closed_{}", day.to_string().replace('-', ""));
        assert!(closed.contains(&sales_partition), "{closed:?}");

        let err = Db::default()
            .write(&t.pool, &Job::Ended, fetched, ENDED)
            .await
            .err()
            .unwrap();
        assert!(
            matches!(err, JobError::Invalid(_)),
            "writing that day again is refused, and the refusal is permanent, not a retry: {err:?}"
        );
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM sales").await,
            40,
            "reads of a closed partition still work"
        );

        sqlx::query("SELECT reopen_partition($1)")
            .bind(&sales_partition)
            .execute(&t.pool)
            .await
            .unwrap();
        Db::default()
            .write(&t.pool, &Job::Ended, fetched, ENDED)
            .await
            .unwrap();
    });
}

/// A fetch row for `url`, as a poller records one.
fn fetch_row(url: &str) -> FetchRow {
    FetchRow {
        feed: "bazaar",
        url: url.to_owned(),
        started_at: OffsetDateTime::now_utc(),
        duration_ms: 1,
        http_status: Some(200),
        body_bytes: Some(2),
        api_last_updated: None,
        cache_age_s: None,
        error: None,
    }
}

/// The rows held when the collector stops reach `fetch_log` in one last write, which the binary
/// runs once every poller has stopped.
#[test]
fn the_last_fetch_rows_are_written_at_shutdown() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let status = Status::new();
        status.record_fetch(fetch_row("https://example.invalid/last-1"));
        status.record_fetch(fetch_row("https://example.invalid/last-2"));
        ingest::write_last_fetches(&t.url, &status).await;
        assert_eq!(
            count(
                &t.pool,
                "SELECT count(*) FROM fetch_log WHERE url LIKE 'https://example.invalid/last-%'"
            )
            .await,
            2,
            "both rows held at shutdown should be in fetch_log"
        );
        ingest::write_last_fetches("postgres://nobody:nothing@127.0.0.1:1/none", &status).await;
        status.record_fetch(fetch_row("https://example.invalid/unreachable"));
        ingest::write_last_fetches("postgres://nobody:nothing@127.0.0.1:1/none", &status).await;
    });
}

#[test]
fn ingest_drains_the_spool_and_keeps_jobs_while_the_database_is_down() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let dir = tempfile::tempdir().unwrap();
        let spool = Arc::new(Spool::try_new(dir.path().to_path_buf(), 1 << 30).unwrap());
        let now = page_stamp(ENDED);
        spool
            .push(
                &Meta {
                    job: Job::Ended,
                    fetched_at: now,
                },
                ENDED,
                Priority::Essential,
            )
            .unwrap();
        spool
            .push(
                &Meta {
                    job: Job::Bazaar,
                    fetched_at: now,
                },
                BAZAAR_A,
                Priority::Optional,
            )
            .unwrap();
        spool
            .push(
                &Meta {
                    job: Job::Bazaar,
                    fetched_at: now,
                },
                b"{\"not\":\"bazaar\"}",
                Priority::Optional,
            )
            .unwrap();

        let status = Status::new();
        let shutdown = CancellationToken::new();
        let down = Ingest {
            spool: spool.clone(),
            status: status.clone(),
            wake: Arc::new(Notify::new()),
            database_url: "postgres://nobody:nothing@127.0.0.1:1/none".into(),
        };
        let handle = tokio::spawn(down.run(shutdown.clone()));
        tokio_time::sleep(Duration::from_secs(2)).await;
        shutdown.cancel();
        handle.await.unwrap().unwrap();
        assert_eq!(
            spool.pending().unwrap().len(),
            3,
            "with an unreachable database, nothing is lost"
        );

        let shutdown = CancellationToken::new();
        let up = Ingest {
            spool: spool.clone(),
            status: status.clone(),
            wake: Arc::new(Notify::new()),
            database_url: t.url.clone(),
        };
        let handle = tokio::spawn(up.run(shutdown.clone()));
        for _ in 0..100 {
            if spool.pending().unwrap().is_empty() {
                break;
            }
            tokio_time::sleep(Duration::from_millis(100)).await;
        }
        shutdown.cancel();
        handle.await.unwrap().unwrap();
        assert_eq!(
            spool.pending().unwrap(),
            Vec::<PathBuf>::new(),
            "the ingest drains the spool"
        );
        assert_eq!(
            spool.quarantined().unwrap(),
            1,
            "a job that can never be written is set aside instead of blocking the queue"
        );
        assert_eq!(count(&t.pool, "SELECT count(*) FROM sales").await, 40);
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM bazaar_snapshots").await,
            1
        );
    });
}

/// Every feed's archived responses replay into the database, an error response is skipped, and a
/// two-page sweep, archived page by page as the live sweep does, replays as one whole sweep. A
/// later sweep that lacks one listing makes it gone once two replayed sweeps missed it.
#[test]
fn replay_rebuilds_from_the_archive() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let dir = tempfile::tempdir().unwrap();
        let archive = Archive::try_new(dir.path().to_path_buf(), 3).unwrap();
        let fetched = page_stamp(ENDED);
        for (feed, body) in [
            ("auctions_ended", ENDED),
            ("auctions", AUCTIONS),
            ("bazaar", BAZAAR_A),
            ("election", ELECTION),
        ] {
            let line =
                archive::encode_line(Some(archive.run()), feed, &response(fetched, 200, body));
            archive.append(feed, fetched, &line).unwrap();
        }
        let error = archive::encode_line(
            Some(archive.run()),
            "bazaar",
            &response(fetched, 502, b"<html>bad gateway</html>"),
        );
        archive.append("bazaar", fetched, &error).unwrap();
        let page: Value = serde_json::from_slice(AUCTIONS).unwrap();
        let listings = distinct_listings(&page);
        let (first, second) = listings.split_at(listings.len() / 2);
        let sweep_page = |n: i64, part: &[Value], minutes: i64| {
            let mut p = page.clone();
            p["page"] = n.into();
            p["totalPages"] = 2.into();
            p["auctions"] = Value::Array(part.to_vec());
            p["lastUpdated"] = (page["lastUpdated"].as_i64().unwrap() + minutes * 60_000).into();
            serde_json::to_vec(&p).unwrap()
        };
        let without_last = &second[..second.len() - 1];
        for (minutes, part0, part1) in [
            (5, first, second),
            (10, first, without_last),
            (15, first, without_last),
        ] {
            let at = fetched + Duration::from_secs(u64::try_from(minutes * 60).unwrap());
            for (n, part) in [(0, part0), (1, part1)] {
                let body = sweep_page(n, part, minutes);
                let line = archive::encode_line(
                    Some(archive.run()),
                    "auctions_sweep",
                    &response(at, 200, &body),
                );
                archive.append("auctions_sweep", at, &line).unwrap();
            }
        }
        archive.finish_all().unwrap();

        let files = archive::list_files(dir.path()).unwrap();
        let total = replay::replay_files(&t.pool, &Db::default(), &files)
            .await
            .unwrap();
        assert_eq!(
            (total.records, total.written, total.skipped, total.rejected),
            (11, 7, 1, 0),
            "six sweep pages replay as three whole sweeps"
        );
        assert_eq!(
            count(
                &t.pool,
                "SELECT count(*) FROM auction_sweeps WHERE complete AND consistent AND pages = 2"
            )
            .await,
            3
        );
        assert_eq!(
            count(
                &t.pool,
                "SELECT count(*) FROM auctions WHERE absent_since IS NOT NULL"
            )
            .await,
            1,
            "the listing two replayed sweeps missed is gone"
        );
        assert_eq!(
            count(
                &t.pool,
                "SELECT count(*) FROM feed_snapshots WHERE feed = 'auctions'"
            )
            .await,
            1,
            "sweep pages do not count as page-0 snapshots"
        );
        assert_eq!(count(&t.pool, "SELECT count(*) FROM sales").await, 40);
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM auctions").await,
            fixture_auctions()
        );
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM bazaar_snapshots").await,
            1
        );
        assert_eq!(count(&t.pool, "SELECT count(*) FROM mayor_terms").await, 1);
    });
}

/// A sweep can see a listing before page 0 does, when page 0 missed a snapshot. Live, the sweep
/// is written first and sets `first_seen_at`; replay must keep that order across feeds. Here the
/// sweep reads the snapshot page 0 missed, and page 0 sees the next one 10 seconds later.
#[test]
fn replay_writes_feeds_in_the_order_they_were_fetched() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let dir = tempfile::tempdir().unwrap();
        let archive = Archive::try_new(dir.path().to_path_buf(), 3).unwrap();
        let page: Value = serde_json::from_slice(AUCTIONS).unwrap();
        let stamp = page["lastUpdated"].as_i64().unwrap();
        let fetched = page_stamp(AUCTIONS) + Duration::from_secs(5);
        let with = |p: Option<i64>, last_updated: i64| {
            let mut body = page.clone();
            if let Some(n) = p {
                body["page"] = n.into();
                body["totalPages"] = 1.into();
            }
            body["lastUpdated"] = last_updated.into();
            serde_json::to_vec(&body).unwrap()
        };
        for (feed, at, body) in [
            ("auctions_sweep", fetched, with(Some(0), stamp)),
            (
                "auctions",
                fetched + Duration::from_secs(10),
                with(None, stamp + 60_000),
            ),
        ] {
            let line = archive::encode_line(Some(archive.run()), feed, &response(at, 200, &body));
            archive.append(feed, at, &line).unwrap();
        }
        archive.finish_all().unwrap();

        let files = archive::list_files(dir.path()).unwrap();
        replay::replay_files(&t.pool, &Db::default(), &files)
            .await
            .unwrap();
        let (first, last): (OffsetDateTime, OffsetDateTime) =
            sqlx::query_as("SELECT min(first_seen_at), max(first_seen_at) FROM auctions")
                .fetch_one(&t.pool)
                .await
                .unwrap();
        assert_eq!((first, last), (page_stamp(AUCTIONS), page_stamp(AUCTIONS)));
    });
}

/// An archived page 0 stamped far past its fetch time, from a broken or hostile answer, is skipped
/// as the live collector ignored it, so the real page-0 snapshots after it are still written.
#[test]
fn replay_skips_a_page_zero_stamped_far_past_its_fetch() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let dir = tempfile::tempdir().unwrap();
        let archive = Archive::try_new(dir.path().to_path_buf(), 3).unwrap();
        let page: Value = serde_json::from_slice(AUCTIONS).unwrap();
        let stamp = page["lastUpdated"].as_i64().unwrap();
        let fetched = page_stamp(AUCTIONS) + Duration::from_secs(5);
        let with = |last_updated: i64| {
            let mut body = page.clone();
            body["lastUpdated"] = last_updated.into();
            serde_json::to_vec(&body).unwrap()
        };
        let ahead_at = fetched + Duration::from_secs(61);
        let ahead_ms = i64::try_from(ahead_at.unix_timestamp_nanos() / 1_000_000).unwrap();
        for (at, body) in [
            (fetched, with(stamp)),
            (fetched + Duration::from_secs(60), with(i64::MAX)),
            (ahead_at, with(ahead_ms + 11 * 60_000)),
            (fetched + Duration::from_secs(120), with(stamp + 120_000)),
        ] {
            let line =
                archive::encode_line(Some(archive.run()), "auctions", &response(at, 200, &body));
            archive.append("auctions", at, &line).unwrap();
        }
        archive.finish_all().unwrap();

        let files = archive::list_files(dir.path()).unwrap();
        let summary = replay::replay_files(&t.pool, &Db::default(), &files)
            .await
            .unwrap();
        assert_eq!(
            (summary.written, summary.skipped),
            (2, 2),
            "the two real page-0 snapshots are written and the two stamped far ahead skipped"
        );
        let written: Vec<OffsetDateTime> = sqlx::query_scalar(
            "SELECT api_last_updated FROM feed_snapshots WHERE feed = 'auctions' ORDER BY 1",
        )
        .fetch_all(&t.pool)
        .await
        .unwrap();
        assert_eq!(
            written,
            vec![
                model::ms_to_time(stamp).unwrap(),
                model::ms_to_time(stamp + 120_000).unwrap()
            ],
            "the written snapshots are the two real ones"
        );
    });
}

/// The archive keeps every page-0 fetch, but the live collector writes a snapshot only once. A
/// repeat replayed after a sweep must not clear the missing flag the sweep set.
#[test]
fn replay_skips_repeated_page_zero_snapshots() {
    skyblock_collector_testdb::block_on(async {
        repeat_after_sweep(
            page_stamp(AUCTIONS) + Duration::from_secs(5),
            Process::Same,
            RunNamed {
                before_repeat: true,
                repeat: true,
            },
        )
        .await;
    });
}

/// Archives from before records named their process: a second file within a day is a restart.
#[test]
fn replay_finds_restarts_in_older_archives_by_their_files() {
    skyblock_collector_testdb::block_on(async {
        repeat_after_sweep(
            page_stamp(AUCTIONS) + Duration::from_secs(5),
            Process::Same,
            RunNamed {
                before_repeat: false,
                repeat: false,
            },
        )
        .await;
        repeat_after_sweep(
            page_stamp(AUCTIONS) + Duration::from_secs(5),
            Process::Restarted,
            RunNamed {
                before_repeat: false,
                repeat: false,
            },
        )
        .await;
    });
}

/// A restarted collector starts without a last snapshot, so its first page 0 is written even when
/// it repeats the one before the restart, and clears the flag as it did live.
#[test]
fn replay_writes_the_first_page_zero_after_a_restart() {
    skyblock_collector_testdb::block_on(async {
        repeat_after_sweep(
            page_stamp(AUCTIONS) + Duration::from_secs(5),
            Process::Restarted,
            RunNamed {
                before_repeat: true,
                repeat: true,
            },
        )
        .await;
    });
}

/// A restart that spans UTC midnight is still a restart: the records name the new process.
#[test]
fn replay_writes_the_first_page_zero_after_a_restart_across_midnight() {
    skyblock_collector_testdb::block_on(async {
        repeat_after_sweep(
            next_midnight() - Duration::from_secs(160),
            Process::Restarted,
            RunNamed {
                before_repeat: true,
                repeat: true,
            },
        )
        .await;
    });
}

/// The collector keeps running across UTC midnight, when the archive starts new files, so a
/// repeat just after midnight is still a repeat. The sweep lands just before midnight and the
/// repeat just after.
#[test]
fn replay_skips_repeated_page_zero_snapshots_across_midnight() {
    skyblock_collector_testdb::block_on(async {
        repeat_after_sweep(
            next_midnight() - Duration::from_secs(160),
            Process::Same,
            RunNamed {
                before_repeat: true,
                repeat: true,
            },
        )
        .await;
    });
}

/// The UTC midnight after the auctions fixture's time.
fn next_midnight() -> OffsetDateTime {
    page_stamp(AUCTIONS)
        .date()
        .next_day()
        .expect("the fixture's date has a next day")
        .midnight()
        .assume_utc()
}

/// A sweep that starts shortly before midnight but finishes before it is written then, ahead of
/// later sightings that day, whether or not the next day has records. The complete sweep, its
/// second page a minute stale, misses the listing, and a newer snapshot lists it again, which live
/// cleared the flag the sweep set.
#[test]
fn replay_writes_a_sweep_just_before_midnight_in_order() {
    skyblock_collector_testdb::block_on(async {
        for next_day_record in [false, true] {
            let t = test_db().await;
            let dir = tempfile::tempdir().unwrap();
            let archive = Archive::try_new(dir.path().to_path_buf(), 3).unwrap();
            let page: Value = serde_json::from_slice(AUCTIONS).unwrap();
            let stamp = page["lastUpdated"].as_i64().unwrap();
            let listed = page["auctions"][0]["uuid"].as_str().unwrap().to_owned();
            let body = |sweep_page: Option<i64>, last_updated: i64, keep: bool| {
                let mut b = page.clone();
                if let Some(n) = sweep_page {
                    b["page"] = n.into();
                    b["totalPages"] = 2.into();
                }
                if !keep {
                    b["auctions"]
                        .as_array_mut()
                        .unwrap()
                        .retain(|a| a["uuid"] != listed.as_str());
                }
                if sweep_page == Some(1) {
                    b["auctions"] = serde_json::json!([]);
                }
                b["lastUpdated"] = last_updated.into();
                serde_json::to_vec(&b).unwrap()
            };
            let m = next_midnight();
            let minute = 60_000;
            let mut records = vec![
                (
                    "auctions",
                    m - Duration::from_secs(180),
                    body(None, stamp, true),
                ),
                (
                    "auctions_sweep",
                    m - Duration::from_secs(60),
                    body(Some(0), stamp + 2 * minute, false),
                ),
                (
                    "auctions_sweep",
                    m - Duration::from_secs(59),
                    body(Some(1), stamp + minute, false),
                ),
                (
                    "auctions",
                    m - Duration::from_secs(30),
                    body(None, stamp + 3 * minute, true),
                ),
            ];
            if next_day_record {
                records.push((
                    "auctions",
                    m + Duration::from_secs(30),
                    body(None, stamp + 4 * minute, true),
                ));
            }
            for (feed, at, b) in records {
                let line = archive::encode_line(Some(archive.run()), feed, &response(at, 200, &b));
                archive.append(feed, at, &line).unwrap();
            }
            archive.finish_all().unwrap();
            let files = archive::list_files(dir.path()).unwrap();
            replay::replay_files(&t.pool, &Db::default(), &files)
                .await
                .unwrap();
            let missing: Option<OffsetDateTime> =
                sqlx::query_scalar("SELECT missing_since FROM auctions WHERE uuid = $1::uuid")
                    .bind(&listed)
                    .fetch_one(&t.pool)
                    .await
                    .unwrap();
            assert_eq!(missing, None, "next-day record: {next_day_record}");
        }
    });
}

/// Only a new binary, run by a new process, changes whether records name their run: the switch
/// is a restart either way, even across midnight.
#[test]
fn replay_treats_a_change_in_record_format_as_a_restart() {
    skyblock_collector_testdb::block_on(async {
        repeat_after_sweep(
            next_midnight() - Duration::from_secs(160),
            Process::Restarted,
            RunNamed {
                before_repeat: false,
                repeat: true,
            },
        )
        .await;
        repeat_after_sweep(
            next_midnight() - Duration::from_secs(160),
            Process::Restarted,
            RunNamed {
                before_repeat: true,
                repeat: false,
            },
        )
        .await;
    });
}

/// A restart drops the sweep the collector was in; the new process's sweep is its own.
#[test]
fn replay_ends_a_sweep_at_a_restart() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let dir = tempfile::tempdir().unwrap();
        let page: Value = serde_json::from_slice(AUCTIONS).unwrap();
        let stamp = page["lastUpdated"].as_i64().unwrap();
        let start = page_stamp(AUCTIONS) + Duration::from_secs(5);
        let sweep_page = |n: i64, last_updated: i64| {
            let mut b = page.clone();
            b["page"] = n.into();
            b["totalPages"] = 2.into();
            b["lastUpdated"] = last_updated.into();
            serde_json::to_vec(&b).unwrap()
        };
        let first = Archive::try_new(dir.path().to_path_buf(), 3).unwrap();
        let line = archive::encode_line(
            Some(first.run()),
            "auctions_sweep",
            &response(start, 200, &sweep_page(0, stamp)),
        );
        first.append("auctions_sweep", start, &line).unwrap();
        first.finish_all().unwrap();
        let second = Archive::try_new(dir.path().to_path_buf(), 3).unwrap();
        for (n, secs) in [(0, 30), (1, 31)] {
            let at = start + Duration::from_secs(secs);
            let b = sweep_page(n, stamp + 60_000);
            let line =
                archive::encode_line(Some(second.run()), "auctions_sweep", &response(at, 200, &b));
            second.append("auctions_sweep", at, &line).unwrap();
        }
        second.finish_all().unwrap();

        let files = archive::list_files(dir.path()).unwrap();
        let summary = replay::replay_files(&t.pool, &Db::default(), &files)
            .await
            .unwrap();
        assert_eq!(summary.records, 3);
        let sweeps: Vec<(bool, i32)> =
            sqlx::query_as("SELECT complete, pages FROM auction_sweeps ORDER BY api_last_updated")
                .fetch_all(&t.pool)
                .await
                .unwrap();
        assert_eq!(sweeps, vec![(false, 1), (true, 2)]);
    });
}

/// A sweep running at UTC midnight spans two day files; it replays as the one sweep it was.
#[test]
fn replay_joins_a_sweep_that_spans_midnight() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let dir = tempfile::tempdir().unwrap();
        let archive = Archive::try_new(dir.path().to_path_buf(), 3).unwrap();
        let page: Value = serde_json::from_slice(AUCTIONS).unwrap();
        for (n, at) in [
            (0, next_midnight() - Duration::from_secs(1)),
            (1, next_midnight() + Duration::from_secs(1)),
        ] {
            let mut b = page.clone();
            b["page"] = n.into();
            b["totalPages"] = 2.into();
            let b = serde_json::to_vec(&b).unwrap();
            let line = archive::encode_line(
                Some(archive.run()),
                "auctions_sweep",
                &response(at, 200, &b),
            );
            archive.append("auctions_sweep", at, &line).unwrap();
        }
        archive.finish_all().unwrap();
        let files = archive::list_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2, "one file per day");
        replay::replay_files(&t.pool, &Db::default(), &files)
            .await
            .unwrap();
        let sweeps: Vec<(bool, i32)> = sqlx::query_as("SELECT complete, pages FROM auction_sweeps")
            .fetch_all(&t.pool)
            .await
            .unwrap();
        assert_eq!(sweeps, vec![(true, 2)]);
    });
}

/// Whether the collector restarted between the sweep and the repeated page 0.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Process {
    /// The same process fetched everything.
    Same,
    /// A new process fetched the repeated page 0.
    Restarted,
}

/// Whether archive records name the run that wrote them, as records from newer binaries do.
#[derive(Clone, Copy)]
struct RunNamed {
    /// The records before the repeated page 0.
    before_repeat: bool,
    /// The repeated page 0.
    repeat: bool,
}

/// Archives a page 0 that lists a listing, a newer page 0 that lists it again, a complete sweep
/// whose second page is a minute stale and misses it, and then page 0 fetched again before the
/// next snapshot, so it repeats the newer one. Replays the archive and checks the flag the sweep
/// set: the repeat keeps it, unless a restart made the repeat the new process's first page 0.
async fn repeat_after_sweep(first_fetch: OffsetDateTime, process: Process, runs: RunNamed) {
    let t = test_db().await;
    let dir = tempfile::tempdir().expect("a temporary directory should be created");
    let open_archive =
        || Archive::try_new(dir.path().to_path_buf(), 3).expect("the archive should open");
    let mut archive = open_archive();
    let page: Value = serde_json::from_slice(AUCTIONS).expect("the auctions fixture is JSON");
    let stamp = page["lastUpdated"]
        .as_i64()
        .expect("the auctions fixture has a numeric lastUpdated");
    let gone = page["auctions"][0]["uuid"]
        .as_str()
        .expect("the first fixture auction has a uuid")
        .to_owned();
    let body = |sweep_page: Option<i64>, last_updated: i64, keep_gone: bool| {
        let mut b = page.clone();
        if let Some(n) = sweep_page {
            b["page"] = n.into();
            b["totalPages"] = 2.into();
        }
        if !keep_gone {
            b["auctions"]
                .as_array_mut()
                .expect("the auctions fixture lists auctions")
                .retain(|a| a["uuid"] != gone.as_str());
        }
        if sweep_page == Some(1) {
            b["auctions"] = serde_json::json!([]);
        }
        b["lastUpdated"] = last_updated.into();
        serde_json::to_vec(&b).expect("a JSON value serializes")
    };
    let at = |secs: u64| first_fetch + Duration::from_secs(secs);
    let minute = 60_000;
    for (feed, fetched, b) in [
        ("auctions", at(0), body(None, stamp, true)),
        ("auctions", at(120), body(None, stamp + 2 * minute, true)),
        (
            "auctions_sweep",
            at(150),
            body(Some(0), stamp + 2 * minute, false),
        ),
        (
            "auctions_sweep",
            at(151),
            body(Some(1), stamp + minute, false),
        ),
        ("auctions", at(180), body(None, stamp + 2 * minute, true)),
    ] {
        let repeat = fetched == at(180);
        if process == Process::Restarted && repeat {
            archive.finish_all().expect("the archive finishes");
            archive = open_archive();
        }
        let run = if repeat {
            runs.repeat
        } else {
            runs.before_repeat
        }
        .then(|| archive.run());
        let line = archive::encode_line(run, feed, &response(fetched, 200, &b));
        archive
            .append(feed, fetched, &line)
            .expect("the archive should take the record");
    }
    archive.finish_all().expect("the archive finishes");

    let files = archive::list_files(dir.path()).expect("the archive's files should list");
    let summary = replay::replay_files(&t.pool, &Db::default(), &files)
        .await
        .expect("the archive should replay");
    let missing: Option<OffsetDateTime> =
        sqlx::query_scalar("SELECT missing_since FROM auctions WHERE uuid = $1::uuid")
            .bind(&gone)
            .fetch_one(&t.pool)
            .await
            .expect("the listing should be stored");
    match process {
        Process::Restarted => {
            assert_eq!(
                missing, None,
                "the first page 0 after the restart clears the flag"
            );
            assert_eq!(
                summary.skipped, 0,
                "the first page 0 after the restart is written"
            );
        }
        Process::Same => {
            assert_eq!(
                missing,
                Some(page_stamp(AUCTIONS) + Duration::from_secs(60)),
                "the repeat must not clear the sweep's flag"
            );
            assert_eq!(summary.skipped, 1, "the repeated snapshot");
        }
    }
}

#[test]
fn views_and_helpers_run() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        for sql in [
            "SELECT * FROM listings LIMIT 1",
            "SELECT * FROM auction_outcomes LIMIT 1",
            "SELECT * FROM sales_enriched LIMIT 1",
            "SELECT * FROM feed_gaps LIMIT 1",
            "SELECT * FROM skyblock_items LIMIT 1",
            "SELECT * FROM bazaar_prices LIMIT 1",
            "SELECT * FROM bazaar_orders LIMIT 1",
            "SELECT * FROM bazaar_book_at('X', now())",
            "SELECT try_bigint('12'), try_bigint('x'), try_numeric('1.5'), skyblock_year(now())",
            "SELECT item_tag('ENCHANTED_BOOK', '{\"enchantments\":{\"sharpness\":7}}')",
        ] {
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .execute(&t.pool)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        let tag: String = sqlx::query_scalar(
            "SELECT item_tag('ENCHANTED_BOOK', '{\"enchantments\":{\"sharpness\":7}}')",
        )
        .fetch_one(&t.pool)
        .await
        .unwrap();
        assert_eq!(tag, "ENCHANTMENT_SHARPNESS_7");
        let mut conn = t.pool.acquire().await.unwrap();
        sqlx::query("SET ROLE reader")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query("SELECT count(*) FROM sales_enriched")
            .execute(&mut *conn)
            .await
            .unwrap();
        let denied = sqlx::query("DELETE FROM sales")
            .execute(&mut *conn)
            .await
            .unwrap_err();
        assert!(
            denied.to_string().contains("permission denied"),
            "the read-only role can read but not write: {denied}"
        );
        sqlx::query("RESET ROLE").execute(&mut *conn).await.unwrap();
    });
}

/// The fixture with its `lastUpdated` set to `at`, and every other byte as it was, since order
/// books are compared by their exact text.
fn restamped(body: &[u8], at: OffsetDateTime) -> Vec<u8> {
    let text = str::from_utf8(body).expect("the fixture is UTF-8");
    let key = "\"lastUpdated\":";
    let start = text.find(key).expect("the fixture has a lastUpdated") + key.len();
    let end = start
        + text[start..]
            .find(|c: char| !c.is_ascii_digit())
            .expect("the fixture's lastUpdated is followed by more JSON");
    let ms = at.unix_timestamp_nanos() / 1_000_000;
    format!("{}{ms}{}", &text[..start], &text[end..]).into_bytes()
}

/// Asserts that every product's order book shown just after `at` is the one in `body`.
async fn assert_books_at(pool: &PgPool, at: OffsetDateTime, body: &[u8], label: &str) {
    let bz: Value = serde_json::from_slice(body).expect("the bazaar body is JSON");
    for (id, product) in bz["products"]
        .as_object()
        .expect("the bazaar body has products")
    {
        let same: bool = sqlx::query_scalar(
            "SELECT buy_summary = $3::jsonb AND sell_summary = $4::jsonb FROM bazaar_book_at($1, $2)",
        )
        .bind(id)
        .bind(at + Duration::from_secs(5))
        .bind(product["buy_summary"].to_string())
        .bind(product["sell_summary"].to_string())
        .fetch_one(pool)
        .await
        .unwrap_or_else(|error| panic!("{label}: the book of {id} should be readable: {error}"));
        assert!(same, "{label}: {id}");
    }
}

/// A missed snapshot written late, between two stored ones, changes only the books it shows: the
/// next snapshot still shows its own books, though it stored rows only for what changed then. The
/// books change at the late snapshot and change back at the next one.
#[test]
fn a_late_bazaar_snapshot_leaves_later_books_alone() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let a = page_stamp(BAZAAR_A);
        let (late, next) = (a + Duration::from_secs(20), a + Duration::from_secs(40));
        let late_body = restamped(BAZAAR_B, late);
        let next_body = restamped(BAZAAR_A, next);
        let live = Db::default();
        live.write(&t.pool, &Job::Bazaar, a, BAZAAR_A)
            .await
            .unwrap();
        live.write(&t.pool, &Job::Bazaar, next, &next_body)
            .await
            .unwrap();
        let books_before = count(&t.pool, "SELECT count(*) FROM bazaar_books").await;
        assert_eq!(books_before, 40, "the next snapshot changed nothing");

        Db::default()
            .write(&t.pool, &Job::Bazaar, late, &late_body)
            .await
            .unwrap();

        assert_books_at(&t.pool, a, BAZAAR_A, "first").await;
        assert_books_at(&t.pool, late, &late_body, "late").await;
        assert_books_at(&t.pool, next, &next_body, "next").await;
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM bazaar_snapshots").await,
            3
        );
        assert_eq!(
        count(
            &t.pool,
            "SELECT count(*) FROM bazaar_quick WHERE api_last_updated = (SELECT min(api_last_updated) + interval '20 seconds' FROM bazaar_snapshots)"
        )
        .await,
        40,
        "the late snapshot has every price"
    );
    });
}

/// When another writer stores a newer snapshot, the live collector's memory of the books is out
/// of date, so its next snapshot stores every book.
#[test]
fn a_snapshot_written_by_another_process_resets_the_book_memory() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let a = page_stamp(BAZAAR_A);
        let (other, next) = (a + Duration::from_secs(20), a + Duration::from_secs(40));
        let other_body = restamped(BAZAAR_B, other);
        let next_body = restamped(BAZAAR_A, next);
        let live = Db::default();
        live.write(&t.pool, &Job::Bazaar, a, BAZAAR_A)
            .await
            .unwrap();
        Db::default()
            .write(&t.pool, &Job::Bazaar, other, &other_body)
            .await
            .unwrap();
        live.write(&t.pool, &Job::Bazaar, next, &next_body)
            .await
            .unwrap();

        assert_books_at(&t.pool, other, &other_body, "other").await;
        assert_books_at(&t.pool, next, &next_body, "next").await;
    });
}

/// A backfill writes only the snapshots the database lacks, and every page of a missing page-0
/// snapshot. The database starts with the first of each feed's two snapshots.
#[test]
fn backfill_writes_only_missing_snapshots() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let ended = page_stamp(ENDED);
        let auctions = page_stamp(AUCTIONS);
        let bazaar = page_stamp(BAZAAR_A);
        let minute = Duration::from_secs(60);
        let live = Db::default();
        live.write(&t.pool, &Job::Ended, ended, ENDED)
            .await
            .unwrap();
        live.write(&t.pool, &Job::AuctionsDelta, auctions, &wrap(AUCTIONS))
            .await
            .unwrap();
        live.write(&t.pool, &Job::Bazaar, bazaar, BAZAAR_A)
            .await
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let archive = Archive::hourly(dir.path().to_path_buf(), 3).unwrap();
        let mut second_ended: Value = serde_json::from_slice(ENDED).unwrap();
        for sale in second_ended["auctions"].as_array_mut().unwrap() {
            sale["timestamp"] = (sale["timestamp"].as_i64().unwrap() + 60_000).into();
        }
        let second_ended = restamped(&serde_json::to_vec(&second_ended).unwrap(), ended + minute);
        let mut page_one: Value =
            serde_json::from_slice(&restamped(AUCTIONS, auctions + minute)).unwrap();
        page_one["page"] = 1.into();
        let page_one = serde_json::to_vec(&page_one).unwrap();
        for (feed, at, body) in [
            ("auctions_ended", ended, ENDED.to_vec()),
            ("auctions_ended", ended + minute, second_ended),
            ("auctions", auctions, AUCTIONS.to_vec()),
            (
                "auctions",
                auctions + minute,
                restamped(AUCTIONS, auctions + minute),
            ),
            ("auctions", auctions + minute, page_one),
            ("bazaar", bazaar, BAZAAR_A.to_vec()),
            (
                "bazaar",
                bazaar + Duration::from_secs(20),
                restamped(BAZAAR_B, bazaar + Duration::from_secs(20)),
            ),
            ("items", ended, ELECTION.to_vec()),
        ] {
            let record = archive::encode_line(Some(archive.run()), feed, &response(at, 200, &body));
            archive.append(feed, at, &record).unwrap();
        }
        archive.finish_all().unwrap();

        let files = archive::list_files(dir.path()).unwrap();
        let summary = replay::backfill_files(&t.pool, &Db::default(), &files)
            .await
            .unwrap();
        assert_eq!(
            (
                summary.records,
                summary.written,
                summary.skipped,
                summary.rejected
            ),
            (8, 4, 4, 0),
            "the second sales, both pages of the second page 0, and the second bazaar"
        );
        assert_eq!(count(&t.pool, "SELECT count(*) FROM sales").await, 80);
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM feed_snapshots").await,
            6
        );
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM resource_snapshots").await,
            0,
            "a backfill writes no resource other than the election"
        );
        assert_books_at(
            &t.pool,
            bazaar + Duration::from_secs(20),
            BAZAAR_B,
            "backfilled",
        )
        .await;

        let again = replay::backfill_files(&t.pool, &Db::default(), &files)
            .await
            .unwrap();
        assert_eq!(again.written, 0, "a second backfill finds nothing missing");
    });
}

/// An election snapshot, and the time it was published.
struct Election {
    at: OffsetDateTime,
    body: Vec<u8>,
}

/// The election fixture `minutes` after its own time, with the first candidate `more_votes` ahead
/// and the minister renamed to `minister`.
fn election_at(minutes: i64, more_votes: i64, minister: &str) -> Election {
    let mut v: Value = serde_json::from_slice(ELECTION).expect("the election fixture is JSON");
    let stamp = v["lastUpdated"]
        .as_i64()
        .expect("the election fixture has a numeric lastUpdated")
        + minutes * 60_000;
    v["lastUpdated"] = stamp.into();
    let votes = &mut v["current"]["candidates"][0]["votes"];
    *votes = (votes
        .as_i64()
        .expect("the first candidate has a vote count")
        + more_votes)
        .into();
    v["mayor"]["minister"]["name"] = minister.into();
    Election {
        at: model::ms_to_time(stamp).expect("the snapshot's time is valid"),
        body: serde_json::to_vec(&v).expect("a JSON value serializes"),
    }
}

/// A backfilled election snapshot is older than the newest one stored. It adds its votes, but the
/// term keeps the details seen last, and only its observed range widens.
#[test]
fn a_late_election_snapshot_leaves_the_term_as_last_seen() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let newer = election_at(5, 100, "Newer");
        let older = election_at(0, 0, "Older");
        Db::default()
            .write(&t.pool, &Job::Election, newer.at, &newer.body)
            .await
            .unwrap();
        Db::default()
            .write(&t.pool, &Job::Election, older.at, &older.body)
            .await
            .unwrap();

        let (minister, first, last): (String, OffsetDateTime, OffsetDateTime) = sqlx::query_as(
            "SELECT minister_name, first_observed_at, last_observed_at FROM mayor_terms",
        )
        .fetch_one(&t.pool)
        .await
        .unwrap();
        assert_eq!(minister, "Newer");
        assert_eq!((first, last), (older.at, newer.at));
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM election_snapshots").await,
            2,
            "the late snapshot's votes are kept"
        );
    });
}

/// A backfill writes an election snapshot whose lastUpdated was never stored, and the write keeps
/// it only when its content changed.
#[test]
fn backfill_writes_missing_election_snapshots() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let stored = election_at(0, 0, "Cole");
        let missed = election_at(5, 100, "Cole");
        let same = election_at(10, 100, "Cole");
        Db::default()
            .write(&t.pool, &Job::Election, stored.at, &stored.body)
            .await
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let archive = Archive::hourly(dir.path().to_path_buf(), 3).unwrap();
        let seconds = |s| Duration::from_secs(s);
        for (at, body) in [
            (stored.at + seconds(20), &stored.body),
            (missed.at + seconds(20), &missed.body),
            (same.at + seconds(20), &same.body),
        ] {
            let line =
                archive::encode_line(Some(archive.run()), "election", &response(at, 200, body));
            archive.append("election", at, &line).unwrap();
        }
        archive.finish_all().unwrap();

        let files = archive::list_files(dir.path()).unwrap();
        let summary = replay::backfill_files(&t.pool, &Db::default(), &files)
            .await
            .unwrap();
        assert_eq!(
            (summary.records, summary.skipped),
            (3, 1),
            "the stored snapshot is skipped"
        );
        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM election_snapshots").await,
            2,
            "the missed snapshot is added; the next one repeats its content and is not"
        );
        let missed_payload: Value = serde_json::from_slice(&missed.body).unwrap();
        let candidate = &missed_payload["current"]["candidates"][0];
        let votes: i64 = sqlx::query_scalar(
            "SELECT votes FROM election_votes WHERE observed_at = $1 AND candidate_key = $2",
        )
        .bind(missed.at + seconds(20))
        .bind(candidate["key"].as_str().unwrap())
        .fetch_one(&t.pool)
        .await
        .unwrap();
        assert_eq!(Some(votes), candidate["votes"].as_i64(), "the missed votes");
    });
}

/// Cloudflare can serve the secondary an older copy than the main collector already stored. That
/// read, merged in by a backfill, must not record a revert.
#[test]
fn an_election_read_older_than_the_stored_one_is_ignored() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let first = election_at(0, 0, "Cole");
        let stale = election_at(5, 50, "Cole");
        let newer = election_at(10, 100, "Newer");
        let live = Db::default();
        live.write(&t.pool, &Job::Election, first.at, &first.body)
            .await
            .unwrap();
        live.write(&t.pool, &Job::Election, newer.at, &newer.body)
            .await
            .unwrap();
        Db::default()
            .write(
                &t.pool,
                &Job::Election,
                newer.at + Duration::from_secs(30),
                &stale.body,
            )
            .await
            .unwrap();
        assert!(
            stale.at < newer.at,
            "the secondary read an older generation 30 s after the main collector stored the newer one"
        );

        assert_eq!(
            count(&t.pool, "SELECT count(*) FROM election_snapshots").await,
            2
        );
        let minister: String = sqlx::query_scalar("SELECT minister_name FROM mayor_terms")
            .fetch_one(&t.pool)
            .await
            .unwrap();
        assert_eq!(minister, "Newer");
    });
}

/// The mirror case: the secondary read a newer generation just before the main collector's first
/// read after an outage, which a stale cache served an older one. The main collector's read was
/// stored first, and placing the newer read before it would record a revert, so it is ignored.
#[test]
fn an_election_read_newer_than_the_next_stored_one_is_ignored() {
    skyblock_collector_testdb::block_on(async {
        let t = test_db().await;
        let first = election_at(0, 0, "Cole");
        let lagging = election_at(5, 50, "Cole");
        let newer = election_at(10, 100, "Cole");
        let live = Db::default();
        live.write(&t.pool, &Job::Election, first.at, &first.body)
            .await
            .unwrap();
        let back = newer.at + Duration::from_secs(60);
        live.write(&t.pool, &Job::Election, back, &lagging.body)
            .await
            .unwrap();
        Db::default()
            .write(
                &t.pool,
                &Job::Election,
                back - Duration::from_secs(30),
                &newer.body,
            )
            .await
            .unwrap();
        assert!(
            lagging.at < newer.at,
            "the main collector's read after the outage is the older generation"
        );

        let stamps: Vec<OffsetDateTime> = sqlx::query_scalar(
            "SELECT api_last_updated FROM election_snapshots ORDER BY fetched_at",
        )
        .fetch_all(&t.pool)
        .await
        .unwrap();
        assert_eq!(stamps, vec![first.at, lagging.at], "no revert is recorded");
    });
}
