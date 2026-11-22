//! `skyblock-collector`: Records Hypixel's in-game market and election data from its public API into a raw archive and Postgres.

use std::env;
use std::future::Future;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::{Arg, ArgAction, ArgMatches, Command};
use futures::future::{self, Either};
use skyblock_collector_core::archive::{self, Archive};
use skyblock_collector_core::collect::{self, Ctx};
use skyblock_collector_core::config::Config;
use skyblock_collector_core::db::{self, Db};
use skyblock_collector_core::http::Client;
use skyblock_collector_core::ingest::{self, Ingest};
use skyblock_collector_core::spool::{Job, Priority, Spool};
use skyblock_collector_core::status::{self, Status};
use skyblock_collector_core::{feed, replay};
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::signal::unix::{self, SignalKind};
use tokio::signal::{self};
use tokio::sync::Notify;
use tokio::task::{self, JoinSet};
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

const RUN: &str = "run";
const SECONDARY: &str = "secondary";
const REPLAY: &str = "replay";
const BACKFILL: &str = "backfill";
const RECOVER_ARCHIVE: &str = "recover-archive";
const HEALTH: &str = "health";
const FILES: &str = "files";

/// What the command line asks for.
#[derive(Debug)]
enum Action {
    /// Collect continuously, the default.
    Run,
    /// Collect into an hourly archive only, with no database.
    Secondary,
    /// Write archived responses to the database, the whole archive when `files` is empty.
    Replay { files: Vec<PathBuf> },
    /// Write the snapshots in `files` that the database lacks.
    Backfill { files: Vec<PathBuf> },
    /// Finish archive files an unclean stop left open.
    RecoverArchive,
    /// Exit 0 when the running collector reports healthy.
    Health,
}

/// The archive files a `replay` or `backfill` names.
fn files_argument() -> Arg {
    Arg::new(FILES)
        .value_name("FILES")
        .num_args(0..)
        .action(ArgAction::Append)
        .value_parser(clap::value_parser!(PathBuf))
}

/// The command-line parser.
fn command_line() -> Command {
    Command::new("skyblock-collector")
        .version(env!("CARGO_PKG_VERSION"))
        .about("Collects Hypixel SkyBlock auction, sale, bazaar, and mayor data")
        .subcommand(Command::new(RUN).about("Collect continuously (the default)"))
        .subcommand(Command::new(SECONDARY).about(
            "Collect sales, bazaar, new listings, and the election into an hourly archive only, \
             with no database. Run on another network, its files fill the main collector's gaps \
             through `backfill`",
        ))
        .subcommand(
            Command::new(REPLAY)
                .about(
                    "Write archived responses to the database. With no files, replays the whole \
                     archive",
                )
                .arg(files_argument()),
        )
        .subcommand(
            Command::new(BACKFILL)
                .about(
                    "Write the sales, bazaar, new-listing, and election snapshots in these archive \
                     files that the database does not have yet, and nothing else",
                )
                .arg(files_argument()),
        )
        .subcommand(
            Command::new(RECOVER_ARCHIVE)
                .about("Finish archive files left open by an unclean stop, then exit"),
        )
        .subcommand(Command::new(HEALTH).about(
            "Exit 0 when the running collector reports healthy. Used by the container healthcheck",
        ))
}

fn files(matches: &ArgMatches) -> Vec<PathBuf> {
    matches
        .get_many::<PathBuf>(FILES)
        .map_or_default(|values| values.cloned().collect())
}

/// Reads the action from parsed arguments. No subcommand means `run`.
fn action(matches: &ArgMatches) -> Action {
    match matches.subcommand() {
        None | Some((RUN, _)) => Action::Run,
        Some((SECONDARY, _)) => Action::Secondary,
        Some((REPLAY, m)) => Action::Replay { files: files(m) },
        Some((BACKFILL, m)) => Action::Backfill { files: files(m) },
        Some((RECOVER_ARCHIVE, _)) => Action::RecoverArchive,
        Some((HEALTH, _)) => Action::Health,
        Some((other, _)) => unreachable!("clap accepts only the subcommands defined, not {other}"),
    }
}

fn main() -> Result<()> {
    let runtime = Builder::new_multi_thread().enable_all().build()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();
    let action = action(&command_line().get_matches());
    runtime.block_on(perform(action))
}

async fn perform(action: Action) -> Result<()> {
    match action {
        Action::Health => health().await,
        Action::Run => run(Config::from_env()?, Mode::Full).await,
        Action::Secondary => run(Config::from_env()?, Mode::Secondary).await,
        Action::Replay { files } => replay_files(Config::from_env()?, files).await,
        Action::Backfill { files } => backfill(Config::from_env()?, files).await,
        Action::RecoverArchive => recover_archive(Config::from_env()?).await,
    }
}

async fn recover_archive(config: Config) -> Result<()> {
    let archive = Archive::try_new(config.archive_dir, config.zstd_level)?;
    let leftovers = archive.leftover_partials()?;
    let recovered = task::spawn_blocking(move || archive.recover(&leftovers)).await??;
    tracing::info!(files = recovered, "recovered archive files");
    Ok(())
}

async fn health() -> Result<()> {
    let listen = env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let port = listen.rsplit_once(':').map_or("8080", |(_, p)| p);
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?
        .get(format!("http://127.0.0.1:{port}/health"))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    anyhow::ensure!(status.is_success(), "unhealthy: {}", body.trim());
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Full,
    Secondary,
}

/// Collects until SIGTERM or Ctrl-C, then stops the tasks, giving them 20 s, and finishes the
/// archive. Archive files an unclean stop left open are rewritten in the background, while new
/// records go to new files.
async fn run(config: Config, mode: Mode) -> Result<()> {
    use anyhow::Context as _;

    let secondary = mode == Mode::Secondary;
    tracing::info!(version = env!("CARGO_PKG_VERSION"), secondary, "starting");
    let database_url = if secondary {
        None
    } else {
        Some(config.database_url()?.to_owned())
    };
    let archive = Arc::new(if secondary {
        Archive::hourly(config.archive_dir.clone(), config.zstd_level)?
    } else {
        Archive::try_new(config.archive_dir.clone(), config.zstd_level)?
    });
    let leftovers = archive.leftover_partials()?;
    if !leftovers.is_empty() {
        tracing::warn!(
            files = leftovers.len(),
            "recovering archive files left open by an unclean stop"
        );
        let archive = archive.clone();
        task::spawn_blocking(move || match archive.recover(&leftovers) {
            Ok(n) => tracing::info!(files = n, "archive files recovered"),
            Err(e) => {
                tracing::error!(error = %e, "archive recovery failed; the .partial files are kept");
            }
        });
    }
    let spool = if database_url.is_some() {
        Some(Arc::new(Spool::try_new(
            config.spool_dir.clone(),
            config.spool_max_bytes,
        )?))
    } else {
        None
    };
    let status = if secondary {
        Status::without_fetch_log()
    } else {
        Status::new()
    };
    let shutdown = CancellationToken::new();
    let ctx = Arc::new(Ctx {
        client: Client::try_new(config.api_base.clone(), &config.user_agent)?,
        sweep_client: Client::try_new(config.api_base.clone(), &config.user_agent)?,
        archive: archive.clone(),
        spool,
        status: status.clone(),
        wake: Arc::new(Notify::new()),
        shutdown: shutdown.clone(),
        sweep_interval: config.sweep_interval,
    });

    let mut tasks = JoinSet::new();
    spawn_collectors(&mut tasks, &ctx, mode, database_url.clone());

    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    tracing::info!(listen = %config.listen, "status server up");
    let server_shutdown = shutdown.clone();
    tasks.spawn(async move {
        let tasks = if secondary {
            status::SECONDARY_TASKS
        } else {
            status::TASKS
        };
        let result = axum::serve(listener, status::router(status, tasks))
            .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
            .await;
        if let Err(e) = result {
            tracing::error!(error = %e, "status server stopped");
        }
    });

    wait_for_signal().await?;
    tracing::info!("stopping");
    shutdown.cancel();
    let drained = time::timeout(Duration::from_secs(20), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tracing::warn!("tasks did not stop within 20 s; closing anyway");
    }
    if let Some(url) = &database_url {
        ingest::write_last_fetches(url, &ctx.status).await;
    }
    task::spawn_blocking(move || archive.finish_all()).await??;
    tracing::info!("stopped");
    Ok(())
}

/// Starts the pollers under the supervisor, and with a database, the sweeps and the ingest
/// worker.
#[cfg_attr(
    dylint_lib = "trellis_lints",
    expect(
        shared_ownership,
        reason = "the supervisor restarts each poller with a clone of the context, so it needs the \
                  shared context itself rather than a borrow"
    )
)]
fn spawn_collectors(
    tasks: &mut JoinSet<()>,
    ctx: &Arc<Ctx>,
    mode: Mode,
    database_url: Option<String>,
) {
    let shutdown = &ctx.shutdown;
    supervise(tasks, "ended", shutdown.clone(), {
        let ctx = ctx.clone();
        move || {
            collect::snapshots(
                ctx.clone(),
                feed::ENDED,
                "skyblock/auctions_ended",
                Duration::from_secs(60),
                Job::Ended,
                Priority::Essential,
            )
        }
    });
    supervise(tasks, "auctions", shutdown.clone(), {
        let ctx = ctx.clone();
        move || collect::auction_changes(ctx.clone())
    });
    supervise(tasks, "bazaar", shutdown.clone(), {
        let ctx = ctx.clone();
        move || {
            collect::snapshots(
                ctx.clone(),
                feed::BAZAAR,
                "skyblock/bazaar",
                Duration::from_secs(20),
                Job::Bazaar,
                Priority::Optional,
            )
        }
    });
    let election_only = mode == Mode::Secondary;
    supervise(tasks, "slow", shutdown.clone(), {
        let ctx = ctx.clone();
        move || collect::slow(ctx.clone(), election_only)
    });
    if let (Some(spool), Some(url)) = (ctx.spool.clone(), database_url) {
        supervise(tasks, "sweep", shutdown.clone(), {
            let ctx = ctx.clone();
            move || collect::sweeps(ctx.clone())
        });
        supervise(tasks, "ingest", shutdown.clone(), {
            let (status, wake, shutdown) = (ctx.status.clone(), ctx.wake.clone(), shutdown.clone());
            move || {
                let ingest = Ingest {
                    spool: spool.clone(),
                    status: status.clone(),
                    wake: wake.clone(),
                    database_url: url.clone(),
                };
                let shutdown = shutdown.clone();
                async move { ingest.run(shutdown).await }
            }
        });
    }
}

/// Runs a task forever, restarting it after an error or panic, until shutdown.
fn supervise<F, Fut>(
    tasks: &mut JoinSet<()>,
    name: &'static str,
    shutdown: CancellationToken,
    make: F,
) where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    tasks.spawn(async move {
        let mut backoff = Duration::from_secs(1);
        loop {
            let outcome = tokio::spawn(make()).await;
            if shutdown.is_cancelled() {
                return;
            }
            match outcome {
                Ok(Ok(())) => tracing::warn!(task = name, "task returned; restarting"),
                Ok(Err(e)) => tracing::error!(task = name, error = %format!("{e:#}"), "task failed; restarting"),
                Err(e) => tracing::error!(task = name, error = %e, "task panicked; restarting"),
            }
            let cancelled = pin!(shutdown.cancelled());
            let slept = pin!(time::sleep(backoff));
            if let Either::Left(_) = future::select(cancelled, slept).await {
                return;
            }
            backoff = (backoff * 2).min(Duration::from_secs(60));
        }
    });
}

/// Waits for SIGTERM or Ctrl-C. A Ctrl-C handler that fails to install counts as a signal, so the
/// collector stops rather than running without one.
///
/// # Errors
///
/// Fails when the SIGTERM handler can't be installed.
async fn wait_for_signal() -> Result<()> {
    use anyhow::Context as _;

    let mut term =
        unix::signal(SignalKind::terminate()).context("installing the SIGTERM handler")?;
    let interrupted = pin!(signal::ctrl_c());
    let terminated = pin!(term.recv());
    future::select(interrupted, terminated).await;
    Ok(())
}

async fn replay_files(config: Config, files: Vec<PathBuf>) -> Result<()> {
    let pool = db::connect(config.database_url()?).await?;
    db::ensure_schema(&pool).await?;
    let files = if files.is_empty() {
        archive::list_files(&config.archive_dir)?
    } else {
        files
    };
    let summary = replay::replay_files(&pool, &Db::default(), &files).await?;
    tracing::info!(files = files.len(), ?summary, "replay finished");
    Ok(())
}

async fn backfill(config: Config, files: Vec<PathBuf>) -> Result<()> {
    let pool = db::connect(config.database_url()?).await?;
    db::ensure_schema(&pool).await?;
    let summary = replay::backfill_files(&pool, &Db::default(), &files).await?;
    tracing::info!(files = files.len(), ?summary, "backfill finished");
    Ok(())
}
