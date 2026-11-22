use std::fmt::{self, Debug, Formatter};
use std::process::{self, Command, Output};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, io, thread};

use sqlx::{AssertSqlSafe, Connection, PgConnection};
use tokio::time;

/// The Postgres image the tests run, the one the deployment's `skyblock-collector-db` runs.
const IMAGE: &str = "docker.io/library/postgres:18@sha256:b9f6b8ff03b62f3cfd9d9ba3fe56226a9991c79d3fb46fd46536263bf02be553";

/// How long a test container may live, in seconds. A test process that is killed, such as by
/// nextest's timeout or Ctrl-C, never removes its container, so the container ends itself.
const LIFETIME_SECS: &str = "1800";

/// How long the server may take to answer a query after the container starts. It stays under
/// nextest's 120-second limit, so a server that never comes up fails with the reason rather than
/// being killed silently.
const READY_WITHIN: Duration = Duration::from_secs(60);

/// How many times a container start is tried when the host port docker picked turns out taken.
const START_ATTEMPTS: u32 = 5;

/// The settings every test server runs with: durability off, since its data is thrown away.
const SETTINGS: [&str; 6] = [
    "-c",
    "fsync=off",
    "-c",
    "synchronous_commit=off",
    "-c",
    "full_page_writes=off",
];

/// Why a [`TestPostgres`] couldn't start or create a database.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum TestPostgresError {
    /// `docker` itself couldn't be run, such as when it isn't installed.
    #[error("could not run `docker {command}`; the tests need docker to start Postgres")]
    DockerUnavailable {
        /// The docker command, such as `run`.
        command: String,
        /// Why it couldn't run.
        source: io::Error,
    },
    /// A docker command ran and failed.
    #[error("`docker {command}` failed: {stderr}")]
    Docker {
        /// The docker command, such as `run`.
        command: String,
        /// What docker printed on standard error.
        stderr: String,
    },
    /// `docker port` didn't name a port on 127.0.0.1 for the container.
    #[error("`docker port` named no 127.0.0.1 port for the test container {container}: {printed}")]
    NoPort {
        /// The container's name.
        container: String,
        /// What `docker port` printed.
        printed: String,
    },
    /// The server didn't answer a query in time, or its container stopped first.
    #[error("the test Postgres server didn't accept connections: {reason}; container logs: {logs}")]
    NotReady {
        /// How long it waited, or that the container stopped.
        reason: String,
        /// The container's logs when it was still running, or why there are none.
        logs: String,
        /// The last connection error.
        source: sqlx::Error,
    },
    /// The server refused a statement, or the connection to run it.
    #[error("the test Postgres server refused `{statement}`")]
    Refused {
        /// The statement it refused.
        statement: String,
        /// Postgres's or the connection's error.
        source: sqlx::Error,
    },
}

/// A Postgres server for one test, where the test creates its own databases.
///
/// Each [`TestPostgres::start`] runs `docker` to start a container of its own, with its data on a
/// tmpfs and its port bound to a free port on 127.0.0.1, and dropping the value removes the
/// container. A process killed before then leaves the container to end itself within 30 minutes.
/// When `TEST_DATABASE_URL` is set and not empty, it names a server to use instead, whose user may
/// create databases. Then nothing is started, and the databases the tests create stay on that
/// server.
pub struct TestPostgres {
    admin_url: String,
    container: Option<String>,
}

impl Debug for TestPostgres {
    /// Leaves out the admin URL, whose password may be a real one from `TEST_DATABASE_URL`.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestPostgres")
            .field("container", &self.container)
            .finish_non_exhaustive()
    }
}

impl TestPostgres {
    /// Starts the server, or connects to the one `TEST_DATABASE_URL` names, and waits until it
    /// answers a query.
    ///
    /// # Errors
    ///
    /// Fails when `docker` can't start the container or name its port, or the server doesn't
    /// answer within a minute, or its container stops first.
    pub async fn start() -> Result<Self, TestPostgresError> {
        match configured_server(env::var("TEST_DATABASE_URL").ok()) {
            Some(admin_url) => {
                wait_until_ready(&admin_url, None).await?;
                Ok(Self {
                    admin_url,
                    container: None,
                })
            }
            None => Self::start_container(&SETTINGS).await,
        }
    }

    /// Starts a container whose Postgres runs with `settings`, and waits until it answers a query.
    async fn start_container(settings: &[&str]) -> Result<Self, TestPostgresError> {
        let name = run_container(settings)?;
        let mut server = Self {
            admin_url: String::new(),
            container: Some(name.clone()),
        };
        let port = docker(&["port", &name, "5432/tcp"])?;
        let printed = String::from_utf8_lossy(&port.stdout);
        let address = printed
            .lines()
            .find(|line| line.starts_with("127.0.0.1:"))
            .ok_or_else(|| TestPostgresError::NoPort {
                container: name.clone(),
                printed: printed.clone().into_owned(),
            })?;
        server.admin_url = format!("postgres://postgres:test@{address}/postgres");
        wait_until_ready(&server.admin_url, Some(&name)).await?;
        Ok(server)
    }

    /// The URL of the server's `postgres` database, as its superuser.
    #[must_use]
    pub fn admin_url(&self) -> &str {
        &self.admin_url
    }

    /// The name of the container this value started, or `None` for a `TEST_DATABASE_URL` server.
    #[must_use]
    pub fn container(&self) -> Option<&str> {
        self.container.as_deref()
    }

    /// Creates an empty database with a name no other test uses, and returns its URL.
    ///
    /// # Errors
    ///
    /// Fails when the server refuses the connection or the `CREATE DATABASE`.
    pub async fn create_database(&self) -> Result<String, TestPostgresError> {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let name = format!(
            "skc_test_{}_{}_{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let statement = format!("CREATE DATABASE {name}");
        let refused = |source| TestPostgresError::Refused {
            statement: statement.clone(),
            source,
        };
        let mut admin = PgConnection::connect(&self.admin_url)
            .await
            .map_err(refused)?;
        sqlx::query(AssertSqlSafe(statement.clone()))
            .execute(&mut admin)
            .await
            .map_err(refused)?;
        Ok(database_url(&self.admin_url, &name))
    }
}

impl Drop for TestPostgres {
    fn drop(&mut self) {
        if let Some(name) = &self.container {
            let removed = match docker(&["rm", "--force", "--volumes", name]) {
                Err(TestPostgresError::Docker { stderr, .. })
                    if stderr.contains("No such container")
                        || stderr.contains("already in progress") =>
                {
                    Ok(())
                }
                other => other.map(drop),
            };
            assert!(
                thread::panicking() || removed.is_ok(),
                "the test container {name} should be removed; remove it with `docker rm --force {name}`: {removed:?}"
            );
        }
    }
}

/// The server `TEST_DATABASE_URL` names, given its value: none when it is unset or empty.
fn configured_server(value: Option<String>) -> Option<String> {
    value.filter(|url| !url.is_empty())
}

/// `server_url` with its database replaced by `database`. Any query string stays, such as
/// `?host=/run/postgresql`, apart from a `dbname` parameter, which would override the path. A URL
/// without a database gets one.
fn database_url(server_url: &str, database: &str) -> String {
    let (base, query) = match server_url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (server_url, None),
    };
    let authority_start = base.find("://").map_or(0, |at| at + "://".len());
    let server = match base[authority_start..].find('/') {
        Some(slash) => &base[..authority_start + slash],
        None => base,
    };
    let kept: Vec<&str> = query
        .into_iter()
        .flat_map(|query| query.split('&'))
        .filter(|parameter| !parameter.is_empty() && !parameter.starts_with("dbname="))
        .collect();
    if kept.is_empty() {
        format!("{server}/{database}")
    } else {
        format!("{server}/{database}?{}", kept.join("&"))
    }
}

/// Runs a test container whose Postgres has `settings`, and returns its name. Rootless docker
/// picks the host port in its own namespace, where it can't see the ports the host already uses,
/// such as the ephemeral source ports of other tests' connections, so a start that fails to bind
/// is tried again under a new name.
fn run_container(settings: &[&str]) -> Result<String, TestPostgresError> {
    let mut attempt = 1;
    loop {
        let name = container_name();
        let mut arguments = vec![
            "run",
            "--detach",
            "--rm",
            "--init",
            "--name",
            &name,
            "--label",
            "skyblock-collector-test=postgres",
            "--publish",
            "127.0.0.1::5432",
            "--env",
            "POSTGRES_PASSWORD=test",
            "--mount",
            "type=tmpfs,destination=/var/lib/postgresql,tmpfs-size=1g",
            IMAGE,
            "timeout",
            "--signal=INT",
            "--kill-after=30s",
            LIFETIME_SECS,
            "docker-entrypoint.sh",
            "postgres",
        ];
        arguments.extend_from_slice(settings);
        match docker(&arguments) {
            Ok(_) => return Ok(name),
            Err(TestPostgresError::Docker { stderr, .. })
                if attempt < START_ATTEMPTS && stderr.contains("address already in use") =>
            {
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

/// A container name no other test process uses, so parallel test processes don't collide, even
/// processes in other PID namespaces that share the docker daemon.
fn container_name() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!(
        "skc-test-pg-{}-{}-{}",
        process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// Runs `docker` with `arguments`, and returns its output when it succeeds.
fn docker(arguments: &[&str]) -> Result<Output, TestPostgresError> {
    let command = arguments.first().copied().unwrap_or_default().to_owned();
    let output = Command::new("docker")
        .args(arguments)
        .output()
        .map_err(|source| TestPostgresError::DockerUnavailable {
            command: command.clone(),
            source,
        })?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(TestPostgresError::Docker {
            command,
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

/// Whether the container still runs. A container that stopped is gone, since it runs with `--rm`.
fn running(container: &str) -> bool {
    docker(&["inspect", "--format", "{{.State.Running}}", container])
        .is_ok_and(|output| output.stdout.starts_with(b"true"))
}

/// Connects to `url` and runs a query, within `limit`.
async fn answers(url: &str, limit: Duration) -> Result<(), sqlx::Error> {
    let attempt = async {
        let mut connection = PgConnection::connect(url).await?;
        sqlx::query("SELECT 1").execute(&mut connection).await?;
        Ok(())
    };
    time::timeout(limit, attempt).await.unwrap_or_else(|_| {
        Err(sqlx::Error::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "no answer before the readiness deadline",
        )))
    })
}

/// Waits until the server at `url` answers a query, for at most [`READY_WITHIN`] in all. The
/// image's entrypoint runs a temporary server with no TCP listener while it initializes, so the
/// first TCP connection reaches the final one.
async fn wait_until_ready(url: &str, container: Option<&str>) -> Result<(), TestPostgresError> {
    let deadline = Instant::now() + READY_WITHIN;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let source = match answers(url, left.max(Duration::from_millis(1))).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        if let Some(name) = container
            && !running(name)
        {
            return Err(TestPostgresError::NotReady {
                reason: "its container stopped".to_owned(),
                logs: "gone with the container".to_owned(),
                source,
            });
        }
        if Instant::now() >= deadline {
            let logs = match container {
                Some(name) => match docker(&["logs", "--tail", "40", name]) {
                    Ok(output) => format!(
                        "{}{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    ),
                    Err(error) => error.to_string(),
                },
                None => "none for a TEST_DATABASE_URL server".to_owned(),
            };
            return Err(TestPostgresError::NotReady {
                reason: format!("waited {READY_WITHIN:?}"),
                logs,
                source,
            });
        }
        time::sleep(Duration::from_millis(200)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{TestPostgres, TestPostgresError};

    #[test]
    fn database_url_replaces_the_database_and_keeps_the_rest() {
        assert_eq!(
            super::database_url("postgres://postgres:test@127.0.0.1:5432/postgres", "t1"),
            "postgres://postgres:test@127.0.0.1:5432/t1",
            "a plain URL"
        );
        assert_eq!(
            super::database_url("postgres://u:p@h/postgres?sslmode=disable", "t1"),
            "postgres://u:p@h/t1?sslmode=disable",
            "the query string stays"
        );
        assert_eq!(
            super::database_url("postgres:///postgres?host=/var/run/postgresql", "t1"),
            "postgres:///t1?host=/var/run/postgresql",
            "a socket directory in the query isn't taken for the path"
        );
        assert_eq!(
            super::database_url("postgres://u@h", "t1"),
            "postgres://u@h/t1",
            "a URL without a database gets one"
        );
        assert_eq!(
            super::database_url(
                "postgres://u@h/postgres?dbname=postgres&sslmode=disable",
                "t1"
            ),
            "postgres://u@h/t1?sslmode=disable",
            "a dbname parameter, which would override the path, goes"
        );
    }

    #[test]
    fn an_empty_test_database_url_counts_as_unset() {
        assert_eq!(super::configured_server(None), None, "unset");
        assert_eq!(super::configured_server(Some(String::new())), None, "empty");
        assert_eq!(
            super::configured_server(Some("postgres://u@h/postgres".to_owned())),
            Some("postgres://u@h/postgres".to_owned()),
            "a URL names a server"
        );
    }

    #[test]
    fn a_server_that_stops_while_starting_fails_with_the_reason() {
        crate::block_on(async {
            let started =
                TestPostgres::start_container(&["-c", "no_such_setting_for_this_test=1"]).await;
            match started {
                Err(TestPostgresError::NotReady { reason, .. }) => assert_eq!(
                    reason, "its container stopped",
                    "a Postgres that refuses its settings exits, and the start says so"
                ),
                other => panic!("a server that can't start should fail as NotReady: {other:?}"),
            }
        });
    }
}
