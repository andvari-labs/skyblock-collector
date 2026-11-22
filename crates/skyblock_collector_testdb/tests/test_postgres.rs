//! Tests that the throwaway server starts, gives each test its own database and goes away.

use std::env;
use std::process::Command;

use skyblock_collector_testdb::TestPostgres;
use sqlx::PgConnection;

/// Connects to `url`, or fails the test.
async fn connect(url: &str) -> PgConnection {
    use sqlx::Connection as _;

    PgConnection::connect(url)
        .await
        .expect("a new database should accept a connection")
}

#[test]
fn each_database_is_new_and_apart_from_the_others() {
    skyblock_collector_testdb::block_on(async {
        let server = TestPostgres::start()
            .await
            .expect("the test server should start");
        let first = server.create_database().await.expect("the first database");
        let second = server.create_database().await.expect("the second database");
        assert_ne!(first, second, "each test should get a database of its own");

        let mut first = connect(&first).await;
        sqlx::query("CREATE TABLE marker (id integer)")
            .execute(&mut first)
            .await
            .expect("the first database should take a table");
        let mut second = connect(&second).await;
        let tables: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pg_tables WHERE schemaname = 'public'")
                .fetch_one(&mut second)
                .await
                .expect("the second database should answer a query");
        assert_eq!(
            tables, 0,
            "a table made in one database shouldn't show in another"
        );
    });
}

#[test]
fn dropping_the_server_removes_its_container() {
    skyblock_collector_testdb::block_on(async {
        let server = TestPostgres::start()
            .await
            .expect("the test server should start");
        let Some(name) = server.container().map(str::to_owned) else {
            assert!(
                env::var("TEST_DATABASE_URL").is_ok_and(|url| !url.is_empty()),
                "only a TEST_DATABASE_URL server comes without a container"
            );
            return;
        };
        drop(server);
        let listed = Command::new("docker")
            .args([
                "ps",
                "--all",
                "--quiet",
                "--filter",
                &format!("name=^{name}$"),
            ])
            .output()
            .expect("docker should list containers");
        assert!(
            listed.status.success(),
            "docker ps should succeed: {listed:?}"
        );
        assert_eq!(
            String::from_utf8_lossy(&listed.stdout),
            "",
            "the container {name} should be gone once the server is dropped"
        );
    });
}
