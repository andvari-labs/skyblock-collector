//! The settings the collector reads from its environment.

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};

/// The collector's settings, each read from an environment variable with a default.
#[derive(Debug, Clone)]
pub struct Config {
    /// Unset for a secondary collector, which has no database.
    database_url: Option<String>,
    /// Where the raw archive lives. Never pruned.
    pub archive_dir: PathBuf,
    /// Where snapshots wait for the database, from `SPOOL_DIR`.
    pub spool_dir: PathBuf,
    /// The most the spool holds before it refuses optional jobs, from `SPOOL_MAX_GB`.
    pub spool_max_bytes: u64,
    /// The address of the status server, from `LISTEN_ADDR`.
    pub listen: SocketAddr,
    /// The Hypixel API base URL without a trailing slash, from `HYPIXEL_API_BASE`.
    pub api_base: String,
    /// How long from the start of one full sweep to the next, from `SWEEP_INTERVAL_SECS`.
    pub sweep_interval: Duration,
    /// The zstd level of new archive files, from `ARCHIVE_ZSTD_LEVEL`.
    pub zstd_level: i32,
    /// The `User-Agent` header of every request, from `USER_AGENT`.
    pub user_agent: String,
}

impl Config {
    /// Reads the settings from the environment.
    ///
    /// # Errors
    ///
    /// Fails when a set variable doesn't parse: a number that isn't a whole number, a
    /// `LISTEN_ADDR` that isn't `host:port`, or a `SPOOL_MAX_GB` or `ARCHIVE_ZSTD_LEVEL` out of range.
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            database_url: env::var("DATABASE_URL").ok(),
            archive_dir: path("ARCHIVE_DIR", "/data/archive"),
            spool_dir: path("SPOOL_DIR", "/data/spool"),
            spool_max_bytes: gigabytes(number("SPOOL_MAX_GB", 50)?)
                .context("SPOOL_MAX_GB is out of range")?,
            listen: optional("LISTEN_ADDR", "0.0.0.0:8080")
                .parse()
                .context("LISTEN_ADDR must be host:port")?,
            api_base: optional("HYPIXEL_API_BASE", "https://api.hypixel.net/v2")
                .trim_end_matches('/')
                .to_owned(),
            sweep_interval: Duration::from_secs(number("SWEEP_INTERVAL_SECS", 300)?),
            zstd_level: i32::try_from(number("ARCHIVE_ZSTD_LEVEL", 9)?)
                .context("ARCHIVE_ZSTD_LEVEL is out of range")?,
            user_agent: optional(
                "USER_AGENT",
                concat!(
                    "skyblock-collector/",
                    env!("CARGO_PKG_VERSION"),
                    " (andvari-labs)"
                ),
            ),
        })
    }

    /// The Postgres connection URL.
    ///
    /// # Errors
    ///
    /// Fails when `DATABASE_URL` is not set.
    pub fn database_url(&self) -> Result<&str> {
        self.database_url
            .as_deref()
            .context("DATABASE_URL is not set")
    }
}

fn optional(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn path(name: &str, default: &str) -> PathBuf {
    PathBuf::from(optional(name, default))
}

fn number(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be a whole number, got {value:?}")),
        Err(_) => Ok(default),
    }
}

/// `count` gibibytes in bytes, or `None` when that doesn't fit in a `u64`.
fn gigabytes(count: u64) -> Option<u64> {
    count.checked_mul(1024 * 1024 * 1024)
}

#[cfg(test)]
mod tests {
    #[test]
    fn gigabytes_are_gibibytes_and_too_many_is_none_rather_than_a_wrap() {
        assert_eq!(
            super::gigabytes(50),
            Some(50 * 1024 * 1024 * 1024),
            "the default spool limit"
        );
        assert_eq!(super::gigabytes(0), Some(0), "a zero limit stays zero");
        assert_eq!(
            super::gigabytes(u64::MAX / 1024 / 1024 / 1024),
            Some((u64::MAX >> 30) << 30),
            "the largest count that fits"
        );
        assert_eq!(
            super::gigabytes((u64::MAX >> 30) + 1),
            None,
            "a count whose bytes overflow is refused, not wrapped or a panic"
        );
    }
}
