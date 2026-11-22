//! The pollers against a local stand-in for Hypixel, which serves `/v2` and the unversioned API
//! with separate responses the way Cloudflare's separate copies can differ.
//!
//! The pollers wait on the real clock, so each test runs its poller for a set time and counts what
//! it fetched and spooled in that time.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Bytes;
use axum::http::Uri;
use axum::{Router, routing};
use serde_json::Value;
use skyblock_collector_core::archive::{self, Archive, Record};
use skyblock_collector_core::collect::{self, Ctx};
use skyblock_collector_core::feed;
use skyblock_collector_core::http::Client;
use skyblock_collector_core::spool::{Job, Priority, Spool};
use skyblock_collector_core::status::Status;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::time as tokio_time;
use tokio_util::sync::CancellationToken;

const ENDED: &[u8] = include_bytes!("fixtures/ended.json");
const AUCTIONS: &[u8] = include_bytes!("fixtures/auctions_page.json");
const ENDED_PATH: &str = "/skyblock/auctions_ended";

/// A job the poller spooled, with its body.
struct Spooled {
    job: Job,
    body: Vec<u8>,
}

/// What [`poll_ended`] spooled, and how often it asked each copy of the API.
struct Polled {
    bodies: Vec<Vec<u8>>,
    v2_hits: usize,
    unversioned_hits: usize,
}

/// Runs `poller` for `run` against `app`, served as the API base `/v2`, and returns the spooled
/// jobs in order.
async fn run<F, Fut>(app: Router, poller: F, run: Duration) -> Vec<Spooled>
where
    F: FnOnce(Arc<Ctx>) -> Fut,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the stand-in API should bind a local port");
    let base = format!(
        "http://{}/v2",
        listener
            .local_addr()
            .expect("the bound listener should have an address")
    );
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("the stand-in API should serve until the test ends");
    });

    let dir = tempfile::tempdir().expect("a temporary directory should be created");
    let spool =
        Arc::new(Spool::try_new(dir.path().join("spool"), 1 << 30).expect("the spool should open"));
    let shutdown = CancellationToken::new();
    let ctx = Arc::new(Ctx {
        client: Client::try_new(base.clone(), "test").expect("the client should build"),
        sweep_client: Client::try_new(base, "test").expect("the sweep client should build"),
        archive: Arc::new(
            Archive::try_new(dir.path().join("archive"), 3).expect("the archive should open"),
        ),
        spool: Some(spool.clone()),
        status: Status::new(),
        wake: Arc::new(Notify::new()),
        shutdown: shutdown.clone(),
        sweep_interval: Duration::from_secs(300),
    });
    let task = tokio::spawn(poller(ctx));
    tokio_time::sleep(run).await;
    shutdown.cancel();
    task.await
        .expect("the poller task should not panic")
        .expect("the poller should stop cleanly on shutdown");

    spool
        .pending()
        .expect("the spool should list its pending jobs")
        .iter()
        .map(|path| {
            let entry = Spool::read(path).expect("a spooled job should read back");
            Spooled {
                job: entry.meta.job,
                body: entry.body,
            }
        })
        .collect()
}

/// Polls ended auctions for `duration` from a server answering `/v2` with `v2` and the unversioned
/// API with `unversioned`, and returns the spooled bodies. The poll period is 10 s, so a seen
/// snapshot is retried after 1 s, and a `duration` of a few seconds covers a few polls.
async fn poll_ended(v2: &'static [u8], unversioned: &'static [u8], duration: Duration) -> Polled {
    let v2_hits = Arc::new(AtomicUsize::new(0));
    let unversioned_hits = Arc::new(AtomicUsize::new(0));
    let count = |hits: Arc<AtomicUsize>, body: &'static [u8]| {
        move || async move {
            hits.fetch_add(1, Ordering::SeqCst);
            body
        }
    };
    let app = Router::new()
        .route(
            &format!("/v2{ENDED_PATH}"),
            routing::get(count(v2_hits.clone(), v2)),
        )
        .route(
            ENDED_PATH,
            routing::get(count(unversioned_hits.clone(), unversioned)),
        );
    let jobs = run(
        app,
        |ctx| {
            collect::snapshots(
                ctx,
                feed::ENDED,
                ENDED_PATH,
                Duration::from_secs(10),
                Job::Ended,
                Priority::Essential,
            )
        },
        duration,
    )
    .await;
    let bodies: Vec<Vec<u8>> = jobs
        .into_iter()
        .map(|spooled| {
            assert_eq!(
                spooled.job,
                Job::Ended,
                "the ended-auctions poller spools only ended-auctions jobs"
            );
            spooled.body
        })
        .collect();
    Polled {
        bodies,
        v2_hits: v2_hits.load(Ordering::SeqCst),
        unversioned_hits: unversioned_hits.load(Ordering::SeqCst),
    }
}

#[test]
fn an_empty_snapshot_is_read_from_the_unversioned_api() {
    skyblock_collector_testdb::block_on(async {
        let polled = poll_ended(b"", ENDED, Duration::from_millis(500)).await;
        assert!(
            polled.bodies == [ENDED],
            "spooled {} bodies, not the fixture once",
            polled.bodies.len()
        );
        assert_eq!(polled.v2_hits, 1);
        assert_eq!(polled.unversioned_hits, 1);
    });
}

/// The first poll reads a new snapshot and the later ones the same snapshot again; neither falls
/// back.
#[test]
fn the_unversioned_api_is_not_asked_while_v2_answers() {
    skyblock_collector_testdb::block_on(async {
        let polled = poll_ended(ENDED, ENDED, Duration::from_millis(2_500)).await;
        assert!(
            polled.bodies == [ENDED],
            "spooled {} bodies, not the fixture once",
            polled.bodies.len()
        );
        assert!(
            polled.v2_hits >= 2,
            "v2 was polled {} times",
            polled.v2_hits
        );
        assert_eq!(polled.unversioned_hits, 0);
    });
}

/// Failed polls retry after 2 s and then 4 s more, so 4 s covers exactly two.
#[test]
fn nothing_is_spooled_when_both_copies_are_empty() {
    skyblock_collector_testdb::block_on(async {
        let polled = poll_ended(b"", b"", Duration::from_secs(4)).await;
        assert_eq!(polled.bodies, Vec::<Vec<u8>>::new(), "nothing is spooled");
        assert_eq!(polled.v2_hits, 2);
        assert_eq!(polled.unversioned_hits, 2);
    });
}

/// The second snapshot changed every listing on page 0 since the first, so the poller reads page 1
/// too, which comes back empty. Page 0 is read again 3 s after the first snapshot, which had no
/// `age` header, so a 4 s run reads both snapshots.
#[test]
fn an_empty_later_page_keeps_the_changes_on_page_0() {
    skyblock_collector_testdb::block_on(async {
        let mut page: Value = serde_json::from_slice(AUCTIONS).unwrap();
        let first_stamp = page["lastUpdated"].as_i64().unwrap();
        page["lastUpdated"] = (first_stamp + 60_000).into();
        for auction in page["auctions"].as_array_mut().unwrap() {
            auction["last_updated"] = (first_stamp + 1).into();
        }
        let second = Bytes::from(serde_json::to_vec(&page).unwrap());
        let page_zero_reads = Arc::new(AtomicUsize::new(0));
        let page_one_reads = Arc::new(AtomicUsize::new(0));
        let page_one_counter = page_one_reads.clone();
        let app = Router::new().route(
            "/v2/skyblock/auctions",
            routing::get(move |uri: Uri| async move {
                if uri.query().is_some_and(|q| q.contains("page=1")) {
                    page_one_counter.fetch_add(1, Ordering::SeqCst);
                    Bytes::new()
                } else if page_zero_reads.fetch_add(1, Ordering::SeqCst) == 0 {
                    Bytes::from_static(AUCTIONS)
                } else {
                    second.clone()
                }
            }),
        );
        let jobs = run(app, collect::auction_changes, Duration::from_secs(4)).await;
        assert_eq!(page_one_reads.load(Ordering::SeqCst), 1, "page 1 was read");
        assert_eq!(jobs.len(), 2, "both snapshots are spooled");
        for spooled in &jobs {
            assert_eq!(spooled.job, Job::AuctionsDelta);
            let pages: Vec<Value> = serde_json::from_slice(&spooled.body).unwrap();
            assert_eq!(pages.len(), 1, "only page 0 is kept");
        }
    });
}

#[test]
fn a_secondary_collector_only_archives() {
    skyblock_collector_testdb::block_on(async {
        let app = Router::new().route(
            &format!("/v2{ENDED_PATH}"),
            routing::get(|| async { Bytes::from_static(ENDED) }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/v2", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let archive = Arc::new(Archive::hourly(dir.path().join("archive"), 3).unwrap());
        let status = Status::without_fetch_log();
        let shutdown = CancellationToken::new();
        let ctx = Arc::new(Ctx {
            client: Client::try_new(base.clone(), "test").unwrap(),
            sweep_client: Client::try_new(base, "test").unwrap(),
            archive: archive.clone(),
            spool: None,
            status: status.clone(),
            wake: Arc::new(Notify::new()),
            shutdown: shutdown.clone(),
            sweep_interval: Duration::from_secs(300),
        });
        let task = tokio::spawn(collect::snapshots(
            ctx,
            feed::ENDED,
            ENDED_PATH,
            Duration::from_secs(10),
            Job::Ended,
            Priority::Essential,
        ));
        tokio_time::sleep(Duration::from_millis(500)).await;
        shutdown.cancel();
        task.await.unwrap().unwrap();
        archive.finish_all().unwrap();

        let files = archive::list_files(&dir.path().join("archive")).unwrap();
        assert_eq!(files.len(), 1);
        let name = files[0].file_name().unwrap().to_str().unwrap();
        assert!(name.contains('T'), "an hourly file: {name}");
        let lines: Vec<_> = archive::read_lines(&files[0])
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let record: Record<'_> = serde_json::from_slice(&lines[0]).unwrap();
        assert_eq!(record.body_bytes().unwrap(), ENDED);
        assert!(
            status.take_fetches().is_empty(),
            "no fetch_log rows are kept"
        );
    });
}
