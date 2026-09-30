#![cfg(any(feature = "http", feature = "tcp"))]
//! Send bounded metric summary batches to a remote collector over HTTP or TCP.
//!
//! [`RemoteSink`] implements [`Sink`] for use with
//! [`metrics_exporter_summary::Builder`](https://docs.rs/metrics-exporter-summary/latest/metrics_exporter_summary/struct.Builder.html).
//! Each write makes one transmission attempt; the recorder owns retry and queue
//! policy. Retries preserve the batch
//! identity, source, collection timestamp, statistical duration, and row values.
//!
//! # Features
//!
//! | Feature | Default | API |
//! | --- | --- | --- |
//! | `http` | Yes | `Endpoint::Http`, HTTP or HTTPS with certificate verification |
//! | `tcp` | No | `Endpoint::Tcp`, framed MessagePack with optional external TLS |
//!
//! Enable `tcp` with `features = ["tcp"]`; use `default-features = false` for a
//! TCP-only client. With neither feature enabled, this crate exports no API.
//! Both transports use the same MessagePack batch and acknowledgment format.
//!
//! # HTTP example
//!
//! The endpoint must include the collector's `/v1/batches` path. The sink creates
//! its HTTP client lazily on the recorder's writer thread; constructing it does
//! not connect to the collector. Use an `https://` endpoint to enable TLS;
//! `RemoteConfig::tls_ca_pem` adds trust roots for a private certificate authority.
//!
//! ```no_run
//! # #[cfg(feature = "http")]
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use metrics_exporter_summary::{metrics, Builder};
//! use metrics_summary_sink_remote::{AckPolicy, Endpoint, RemoteConfig, RemoteSink};
//! use std::time::Duration;
//!
//! let mut remote = RemoteConfig::new(Endpoint::Http(
//!     "http://collector.example.com/v1/batches".into(),
//! ));
//! remote.bearer_token = Some(std::env::var("METRICS_COLLECTOR_TOKEN")?);
//! remote.ack_policy = AckPolicy::ClickHouseConfirmed;
//! let sink = RemoteSink::new(remote)?;
//! let (recorder, control) = Builder::for_service("api", "api-01")?.build(sink)?;
//! recorder.install()?;
//!
//! metrics::counter!("requests.total").increment(1);
//! metrics::histogram!("request.latency_ns").record(12_000_000.0);
//! let report = control.shutdown(Duration::from_secs(30))?;
//! if !report.is_success() {
//!     return Err("metrics shutdown did not complete delivery".into());
//! }
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "http"))]
//! # fn main() {}
//! ```
//!
//! # TCP example
//!
//! TCP connects directly to the configured IPv4 or IPv6 address. It has no
//! built-in TLS; deployments can optionally route it through a TLS tunnel.
//!
//! ```no_run
//! # #[cfg(feature = "tcp")]
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use metrics_summary_sink_remote::{AckPolicy, Endpoint, RemoteConfig, RemoteSink};
//!
//! let mut config = RemoteConfig::new(Endpoint::Tcp("10.0.0.8:9092".parse()?));
//! config.bearer_token = Some(std::env::var("METRICS_COLLECTOR_TOKEN")?);
//! config.ack_policy = AckPolicy::ClickHouseConfirmed;
//! let sink = RemoteSink::new(config)?;
//! // Pass `sink` to metrics_exporter_summary::Builder::build.
//! # let _ = sink;
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "tcp"))]
//! # fn main() {}
//! ```
//!
//! # Delivery and resource limits
//!
//! [`AckPolicy::Enqueued`] is the default: success means the collector accepted
//! ownership in memory, not that ClickHouse stored the batch. A collector crash
//! can lose such batches. [`AckPolicy::ClickHouseConfirmed`] waits for storage
//! confirmation. A timeout after transmission has an unknown outcome; neither
//! policy provides exactly-once storage. Collector deduplication is bounded and
//! process-local, so retries after an uncertain database write, cache eviction,
//! or restart can create duplicate observations.
//!
//! Align [`RemoteConfig::limits`] with the recorder and collector, including
//! label count and length limits, retained batch bytes, and encoded body size. Requests
//! are uncompressed. The sink has no independent queue; [`Sink::flush`] only
//! checks its deadline and does not upgrade an Enqueued acknowledgment to
//! database confirmation.
//! Label keys require no allowlist and are preserved across both transports.
//! Missing `host` and `instance` inherit source values for identity and storage;
//! no other labels are added. ClickHouse storage requires matching string columns
//! for the labels supplied by each row.
//!
//! HTTP and HTTPS endpoints require a host and reject URL credentials, query
//! strings, and fragments; redirects and environment HTTP proxies are disabled.
//! TLS is a deployment choice: HTTP and TCP may connect to remote addresses
//! directly, while HTTPS verifies the server's certificate and hostname.
//! See the [collector deployment guide](https://github.com/SF-Zhou/metrics-exporter-summary/tree/main/deploy/collector)
//! for TLS proxy configuration and coordinated timeout settings.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

use metrics_summary_core::{Batch, CommitOutcome, CompletionBoundary, ErrorKind, Sink, WriteError};
use metrics_summary_protocol::{decode_ack, encode_request, validate_ack, Status, MAX_ACK_BYTES};
pub use metrics_summary_protocol::{AckPolicy, ProtocolLimits};
#[cfg(any(feature = "http", feature = "tcp"))]
use std::io::Read;
#[cfg(feature = "tcp")]
use std::{
    io::Write,
    net::{SocketAddr, TcpStream},
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
/// Transport and destination for a remote collector.
pub enum Endpoint {
    /// Full HTTP or HTTPS ingestion URL, such as `http://collector.example.com/v1/batches`.
    #[cfg_attr(docsrs, doc(cfg(feature = "http")))]
    #[cfg(feature = "http")]
    Http(
        /// Full ingestion URL with no credentials, query string, or fragment.
        String,
    ),
    /// Direct TCP collector endpoint or endpoint of an optional TLS tunnel.
    #[cfg_attr(docsrs, doc(cfg(feature = "tcp")))]
    #[cfg(feature = "tcp")]
    Tcp(
        /// IPv4 or IPv6 socket address.
        SocketAddr,
    ),
}
#[derive(Clone)]
/// Remote transport configuration, validated by [`RemoteSink::new`].
///
/// Use [`RemoteConfig::new`] for defaults. `Debug` redacts the endpoint and token.
pub struct RemoteConfig {
    /// Transport destination; HTTP requires the full ingestion path.
    pub endpoint: Endpoint,
    /// Required completion boundary. Defaults to [`AckPolicy::Enqueued`].
    pub ack_policy: AckPolicy,
    /// Optional shared token, containing 1..=4096 visible ASCII bytes when set.
    /// Production collectors require a token; the library also permits unauthenticated tests.
    pub bearer_token: Option<String>,
    /// Additional PEM trust roots for a private HTTPS collector; hostname verification remains enabled.
    /// A configured bundle must contain 1..=1,048,576 bytes and 1..=32 certificates.
    #[cfg_attr(docsrs, doc(cfg(feature = "http")))]
    #[cfg(feature = "http")]
    pub tls_ca_pem: Option<Vec<u8>>,
    /// Connection establishment timeout. Defaults to 3 seconds; must be positive and at most one hour.
    pub connect_timeout: Duration,
    /// Maximum duration of one write attempt, further capped by the caller's deadline.
    /// Defaults to 10 seconds; must be positive and at most one hour.
    pub request_timeout: Duration,
    /// Encoded message and model validation budgets; uses [`ProtocolLimits::default`].
    /// The encoded byte limit must be positive and fit in `u32`.
    pub limits: ProtocolLimits,
}
impl std::fmt::Debug for RemoteConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteConfig")
            .field("endpoint", &"[configured]")
            .field("ack_policy", &self.ack_policy)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}
impl RemoteConfig {
    /// Configure an endpoint with Enqueued acknowledgment, default protocol
    /// limits, 3-second connect and 10-second request timeouts, and no token.
    /// The endpoint selects HTTP, HTTPS, or TCP. Validation happens in [`RemoteSink::new`].
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            ack_policy: AckPolicy::Enqueued,
            bearer_token: None,
            #[cfg(feature = "http")]
            tls_ca_pem: None,
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(10),
            limits: ProtocolLimits::default(),
        }
    }
}
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
/// Invalid endpoint, credentials, TLS roots, timeout, or resource limits.
pub struct ConfigError(
    /// Configuration failure description.
    pub String,
);
fn error(kind: ErrorKind, outcome: CommitOutcome, message: impl Into<String>) -> WriteError {
    WriteError::new(kind, outcome, message)
}
fn permanent(message: impl Into<String>) -> WriteError {
    error(ErrorKind::Permanent, CommitOutcome::NotCommitted, message)
}
fn remaining(deadline: Instant) -> Result<Duration, WriteError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| {
            error(
                ErrorKind::Timeout,
                CommitOutcome::Unknown,
                "remote deadline exceeded",
            )
        })
}

/// Synchronous sink that sends one immutable batch per transmission attempt.
///
/// Move the sink into the recorder to keep network I/O and blocking HTTP client
/// destruction on its dedicated writer thread. Direct callers must likewise
/// use and drop it outside an asynchronous runtime worker.
pub struct RemoteSink {
    config: RemoteConfig,
    #[cfg(feature = "http")]
    client: Option<reqwest::blocking::Client>,
    #[cfg(feature = "tcp")]
    stream: Option<TcpStream>,
}
impl RemoteSink {
    /// Validate configuration without opening a connection or creating a runtime.
    ///
    /// Returns [`ConfigError`] for invalid limits, timeouts, authentication,
    /// TLS roots, or a malformed or unsupported HTTP endpoint.
    /// Use and drop the sink on the recorder's synchronous writer thread.
    pub fn new(config: RemoteConfig) -> Result<Self, ConfigError> {
        config
            .limits
            .validation
            .validate()
            .map_err(|error| ConfigError(error.to_string()))?;
        if config.connect_timeout.is_zero()
            || config.request_timeout.is_zero()
            || config.connect_timeout > Duration::from_secs(3600)
            || config.request_timeout > Duration::from_secs(3600)
            || config.limits.max_encoded_bytes == 0
            || config.limits.max_encoded_bytes > u32::MAX as usize
        {
            return Err(ConfigError(
                "timeouts must be positive and at most one hour; encoded limits must be positive and fit u32".into(),
            ));
        }
        if config.bearer_token.as_ref().is_some_and(|v| {
            v.is_empty() || v.len() > 4096 || !v.bytes().all(|b| b.is_ascii_graphic())
        }) {
            return Err(ConfigError("invalid bearer token".into()));
        }
        #[cfg(feature = "http")]
        if let Some(pem) = &config.tls_ca_pem {
            if pem.is_empty() || pem.len() > 1024 * 1024 {
                return Err(ConfigError(
                    "TLS CA bundle must contain 1..=1048576 bytes".into(),
                ));
            }
            let certificates = reqwest::Certificate::from_pem_bundle(pem)
                .map_err(|_| ConfigError("invalid TLS CA bundle".into()))?;
            if certificates.is_empty() || certificates.len() > 32 {
                return Err(ConfigError(
                    "TLS CA bundle must contain 1..=32 certificates".into(),
                ));
            }
        }
        match &config.endpoint {
            #[cfg(feature = "http")]
            Endpoint::Http(endpoint) => {
                let url = url::Url::parse(endpoint).map_err(|e| ConfigError(e.to_string()))?;
                if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
                    return Err(ConfigError(
                        "HTTP or HTTPS endpoint with a host is required".into(),
                    ));
                }
                if !url.username().is_empty()
                    || url.password().is_some()
                    || url.query().is_some()
                    || url.fragment().is_some()
                {
                    return Err(ConfigError(
                        "endpoint must not contain credentials, query, or fragment".into(),
                    ));
                }
            }
            #[cfg(feature = "tcp")]
            Endpoint::Tcp(_) => {}
        }
        Ok(Self {
            config,
            #[cfg(feature = "http")]
            client: None,
            #[cfg(feature = "tcp")]
            stream: None,
        })
    }
    #[cfg(feature = "http")]
    fn http(
        &mut self,
        endpoint: &str,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<Vec<u8>, WriteError> {
        if self.client.is_none() {
            let mut builder = reqwest::blocking::Client::builder()
                .tls_backend_rustls()
                .connect_timeout(self.config.connect_timeout)
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .pool_max_idle_per_host(1);
            if let Some(pem) = &self.config.tls_ca_pem {
                builder = builder.tls_certs_merge(
                    reqwest::Certificate::from_pem_bundle(pem)
                        .map_err(|_| permanent("invalid TLS CA bundle"))?,
                );
            }
            self.client = Some(
                builder
                    .build()
                    .map_err(|_| permanent("cannot initialize HTTP client"))?,
            );
        }
        let mut req = self
            .client
            .as_ref()
            .unwrap()
            .post(endpoint)
            .header("content-type", metrics_summary_protocol::CONTENT_TYPE)
            .timeout(remaining(deadline)?)
            .body(body);
        if let Some(token) = &self.config.bearer_token {
            req = req.bearer_auth(token);
        }
        let response = req.send().map_err(|e| {
            error(
                if e.is_timeout() {
                    ErrorKind::Timeout
                } else {
                    ErrorKind::Retryable
                },
                CommitOutcome::Unknown,
                if e.is_timeout() {
                    "remote HTTP request timed out"
                } else {
                    "remote HTTP request failed"
                },
            )
        })?;
        let status = response.status();
        if status.as_u16() == 408 {
            return Err(error(
                ErrorKind::Timeout,
                CommitOutcome::Unknown,
                "collector HTTP request timeout",
            ));
        }
        if status.as_u16() == 429 || status.as_u16() == 503 {
            return Err(error(
                ErrorKind::Retryable,
                CommitOutcome::Unknown,
                format!("collector HTTP {}", status.as_u16()),
            ));
        }
        if !status.is_success() {
            return Err(error(
                if status.is_server_error() {
                    ErrorKind::Retryable
                } else {
                    ErrorKind::Permanent
                },
                if status.is_server_error() {
                    CommitOutcome::Unknown
                } else {
                    CommitOutcome::NotCommitted
                },
                format!("collector HTTP {}", status.as_u16()),
            ));
        }
        if !matches!(status.as_u16(), 200 | 202)
            || response
                .headers()
                .get("content-type")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.split(';').next())
                != Some(metrics_summary_protocol::CONTENT_TYPE)
        {
            return Err(error(
                ErrorKind::Permanent,
                CommitOutcome::Unknown,
                "invalid HTTP ACK status or content type",
            ));
        }
        let mut bytes = Vec::new();
        response
            .take(MAX_ACK_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| error(ErrorKind::Retryable, CommitOutcome::Unknown, e.to_string()))?;
        if bytes.len() > MAX_ACK_BYTES {
            return Err(error(
                ErrorKind::Permanent,
                CommitOutcome::Unknown,
                "oversized ACK",
            ));
        }
        Ok(bytes)
    }
    #[cfg(feature = "tcp")]
    fn tcp(
        &mut self,
        addr: SocketAddr,
        body: &[u8],
        deadline: Instant,
    ) -> Result<Vec<u8>, WriteError> {
        let attempt = (|| -> Result<Vec<u8>, WriteError> {
            let io_error = |e: std::io::Error| {
                error(
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                    ) {
                        ErrorKind::Timeout
                    } else {
                        ErrorKind::Retryable
                    },
                    CommitOutcome::Unknown,
                    e.to_string(),
                )
            };
            if self.stream.is_none() {
                let mut stream = TcpStream::connect_timeout(
                    &addr,
                    self.config.connect_timeout.min(remaining(deadline)?),
                )
                .map_err(io_error)?;
                stream.set_nodelay(true).map_err(io_error)?;
                stream
                    .set_read_timeout(Some(remaining(deadline)?))
                    .map_err(io_error)?;
                stream
                    .set_write_timeout(Some(remaining(deadline)?))
                    .map_err(io_error)?;
                write_tcp(&mut stream, metrics_summary_protocol::TCP_MAGIC, deadline)?;
                let token = self.config.bearer_token.as_deref().unwrap_or("").as_bytes();
                write_tcp(&mut stream, &(token.len() as u16).to_be_bytes(), deadline)?;
                write_tcp(&mut stream, token, deadline)?;
                let mut hello = [0; 4];
                read_tcp(&mut stream, &mut hello, deadline)?;
                if hello != *metrics_summary_protocol::TCP_MAGIC {
                    return Err(error(
                        ErrorKind::Permanent,
                        CommitOutcome::NotCommitted,
                        "TCP authentication or version rejected",
                    ));
                }
                self.stream = Some(stream);
            }
            let stream = self.stream.as_mut().unwrap();
            stream
                .set_write_timeout(Some(remaining(deadline)?))
                .map_err(io_error)?;
            write_tcp(stream, &(body.len() as u32).to_be_bytes(), deadline)?;
            stream
                .set_write_timeout(Some(remaining(deadline)?))
                .map_err(io_error)?;
            write_tcp(stream, body, deadline)?;
            stream
                .set_read_timeout(Some(remaining(deadline)?))
                .map_err(io_error)?;
            let mut length = [0; 4];
            read_tcp(stream, &mut length, deadline)?;
            let length = u32::from_be_bytes(length) as usize;
            if length > MAX_ACK_BYTES {
                return Err(error(
                    ErrorKind::Permanent,
                    CommitOutcome::Unknown,
                    "oversized ACK frame",
                ));
            }
            stream
                .set_read_timeout(Some(remaining(deadline)?))
                .map_err(io_error)?;
            let mut bytes = vec![0; length];
            read_tcp(stream, &mut bytes, deadline)?;
            Ok(bytes)
        })();
        if attempt.is_err() {
            self.stream = None;
        }
        attempt
    }
}
impl Sink for RemoteSink {
    fn write(&mut self, batch: Arc<Batch>, deadline: Instant) -> Result<(), WriteError> {
        if Instant::now() >= deadline {
            return Err(error(
                ErrorKind::Timeout,
                CommitOutcome::NotCommitted,
                "deadline elapsed before sending",
            ));
        }
        let deadline = deadline.min(Instant::now() + self.config.request_timeout);
        let body = encode_request(&batch, self.config.ack_policy, &self.config.limits)
            .map_err(|e| permanent(e.to_string()))?;
        let response = match self.config.endpoint.clone() {
            #[cfg(feature = "http")]
            Endpoint::Http(endpoint) => self.http(&endpoint, body, deadline)?,
            #[cfg(feature = "tcp")]
            Endpoint::Tcp(addr) => self.tcp(addr, &body, deadline)?,
        };
        let ack = decode_ack(&response)
            .map_err(|e| error(ErrorKind::Permanent, CommitOutcome::Unknown, e.to_string()))?;
        if let Some(id) = ack.id.clone() {
            let id: metrics_summary_core::BatchId = id.try_into().map_err(|_| {
                error(
                    ErrorKind::Permanent,
                    CommitOutcome::Unknown,
                    "invalid ACK identity",
                )
            })?;
            if id != batch.id {
                return Err(error(
                    ErrorKind::Permanent,
                    CommitOutcome::Unknown,
                    "ACK batch ID mismatch",
                ));
            }
        }
        let status = Status::try_from(ack.status).unwrap();
        if status != Status::Ok {
            return Err(error(
                match status {
                    Status::Overloaded | Status::Unavailable | Status::Unknown => {
                        ErrorKind::Retryable
                    }
                    _ => ErrorKind::Permanent,
                },
                if status == Status::Unknown {
                    CommitOutcome::Unknown
                } else {
                    CommitOutcome::NotCommitted
                },
                ack.message,
            ));
        }
        validate_ack(&ack, batch.id, self.config.ack_policy)
            .map_err(|e| error(ErrorKind::Permanent, CommitOutcome::Unknown, e.to_string()))
    }
    fn flush(&mut self, deadline: Instant) -> Result<(), WriteError> {
        if Instant::now() >= deadline {
            Err(error(
                ErrorKind::Timeout,
                CommitOutcome::NotCommitted,
                "flush deadline elapsed",
            ))
        } else {
            Ok(())
        }
    }
    fn completion_boundary(&self) -> CompletionBoundary {
        match self.config.ack_policy {
            AckPolicy::Enqueued => CompletionBoundary::RemoteAccepted,
            AckPolicy::ClickHouseConfirmed => CompletionBoundary::StorageConfirmed,
        }
    }
}

#[cfg(feature = "tcp")]
fn read_tcp(
    stream: &mut TcpStream,
    mut buffer: &mut [u8],
    deadline: Instant,
) -> Result<(), WriteError> {
    while !buffer.is_empty() {
        stream
            .set_read_timeout(Some(remaining(deadline)?))
            .map_err(tcp_error)?;
        match stream.read(buffer) {
            Ok(0) => {
                return Err(error(
                    ErrorKind::Retryable,
                    CommitOutcome::Unknown,
                    "TCP closed during frame",
                ))
            }
            Ok(n) => {
                buffer = &mut buffer[n..];
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(tcp_error(e)),
        }
    }
    Ok(())
}
#[cfg(feature = "tcp")]
fn write_tcp(
    stream: &mut TcpStream,
    mut buffer: &[u8],
    deadline: Instant,
) -> Result<(), WriteError> {
    while !buffer.is_empty() {
        stream
            .set_write_timeout(Some(remaining(deadline)?))
            .map_err(tcp_error)?;
        match stream.write(buffer) {
            Ok(0) => {
                return Err(error(
                    ErrorKind::Retryable,
                    CommitOutcome::Unknown,
                    "TCP closed during send",
                ))
            }
            Ok(n) => {
                buffer = &buffer[n..];
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(tcp_error(e)),
        }
    }
    Ok(())
}
#[cfg(feature = "tcp")]
fn tcp_error(e: std::io::Error) -> WriteError {
    error(
        if matches!(
            e.kind(),
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
        ) {
            ErrorKind::Timeout
        } else {
            ErrorKind::Retryable
        },
        CommitOutcome::Unknown,
        e.to_string(),
    )
}

#[cfg(test)]
mod tests;
