//! The HTTP client the pollers fetch the Hypixel API with.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use time::OffsetDateTime;

/// Response headers worth keeping: they say how fresh Cloudflare's copy was.
const KEPT_HEADERS: [&str; 6] = [
    "age",
    "cache-control",
    "cf-cache-status",
    "last-modified",
    "retry-after",
    "content-type",
];

/// A response, whatever its HTTP status, and when and how fast it arrived.
#[derive(Debug)]
pub struct Fetched {
    /// The URL requested.
    pub url: String,
    /// When the request was sent.
    pub started_at: OffsetDateTime,
    /// How long from sending the request to reading the whole body.
    pub duration_ms: u64,
    /// The HTTP status code.
    pub status: u16,
    /// The headers that say how fresh Cloudflare's copy was, such as `age`, that the response had.
    pub headers: HashMap<String, String>,
    /// The body, byte for byte.
    pub body: Bytes,
}

impl Fetched {
    /// The response is a 200.
    #[must_use]
    pub fn ok(&self) -> bool {
        self.status == 200
    }

    /// A header read as whole seconds, such as `age`, or None when it is missing or isn't one.
    #[must_use]
    pub fn header_secs(&self, name: &str) -> Option<u64> {
        self.headers.get(name).and_then(|v| v.trim().parse().ok())
    }
}

/// A request that failed before its whole response arrived: when it was sent, how long it took
/// to fail, and why.
#[derive(Debug)]
#[must_use]
pub struct FetchError {
    /// When the request was sent.
    pub started_at: OffsetDateTime,
    /// How long the request took to fail.
    pub duration_ms: u64,
    /// The transport failure.
    pub error: reqwest::Error,
}

/// A client for one API base URL.
#[derive(Debug, Clone)]
pub struct Client {
    inner: reqwest::Client,
    base: String,
}

impl Client {
    /// A client for the API at `base` that sends `user_agent`. Each request sets its own timeout,
    /// as [`Client::get`] says. Keepalive pings detect a dead HTTP/2 connection before a request
    /// waits out its whole timeout on it.
    ///
    /// # Errors
    ///
    /// Fails when the TLS backend can't be set up or `user_agent` isn't a valid header value.
    pub fn try_new(base: String, user_agent: &str) -> Result<Self> {
        let inner = reqwest::Client::builder()
            .user_agent(user_agent)
            .connect_timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(120))
            .http2_keep_alive_interval(Duration::from_secs(15))
            .http2_keep_alive_timeout(Duration::from_secs(5))
            .http2_keep_alive_while_idle(true)
            .build()?;
        Ok(Self { inner, base })
    }

    /// The URL of `path` on this client's API.
    #[must_use]
    pub fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base, path.trim_start_matches('/'))
    }

    /// The same resource on the unversioned API, which serves the same data. Cloudflare caches it
    /// apart from the `/v2` copy, so it can hold a different response for the same period.
    #[must_use]
    pub fn fallback_url(&self, path: &str) -> Option<String> {
        let base = self.base.strip_suffix("/v2")?;
        Some(format!("{base}/{}", path.trim_start_matches('/')))
    }

    /// Fetches a URL within `timeout`. Any HTTP status is a successful fetch; only transport
    /// failures are errors. Keep `timeout` under the feed's refresh period, so a hung request
    /// cannot outlast the snapshot it was meant to read.
    ///
    /// # Errors
    ///
    /// Fails only on a transport failure, such as a refused connection, a TLS failure, a
    /// response that took longer than `timeout`, or a body cut short. Any HTTP status is a
    /// response.
    pub async fn get(&self, url: &str, timeout: Duration) -> Result<Fetched, FetchError> {
        let started_at = OffsetDateTime::now_utc();
        let started = Instant::now();
        let elapsed = |s: Instant| u64::try_from(s.elapsed().as_millis()).unwrap_or(u64::MAX);
        let failed = |error| FetchError {
            started_at,
            duration_ms: elapsed(started),
            error,
        };
        let response = self
            .inner
            .get(url)
            .timeout(timeout)
            .send()
            .await
            .map_err(failed)?;
        let status = response.status().as_u16();
        let headers: HashMap<String, String> = KEPT_HEADERS
            .iter()
            .filter_map(|name| {
                response
                    .headers()
                    .get(*name)
                    .and_then(|v| v.to_str().ok())
                    .map(|v| ((*name).to_owned(), v.to_owned()))
            })
            .collect();
        let body = response.bytes().await.map_err(failed)?;
        Ok(Fetched {
            url: url.to_owned(),
            started_at,
            duration_ms: elapsed(started),
            status,
            headers,
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_fallback_is_the_same_path_without_v2() {
        let client = Client::try_new("https://api.hypixel.net/v2".into(), "test").unwrap();
        assert_eq!(
            client.fallback_url("skyblock/bazaar").as_deref(),
            Some("https://api.hypixel.net/skyblock/bazaar"),
            "the fallback drops /v2 and keeps the path"
        );
        let other = Client::try_new("https://api.hypixel.net".into(), "test").unwrap();
        assert_eq!(
            other.fallback_url("skyblock/bazaar"),
            None,
            "a base without /v2 has no fallback"
        );
    }
}
