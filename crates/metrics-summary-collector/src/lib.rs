//! Bounded metric summary ingestion with grouped ClickHouse writes.
//!
//! [`Collector`] validates and admits immutable source batches, combines them
//! into bounded storage groups, and writes those groups on dedicated operating
//! system threads. Network tasks run on Tokio; database I/O does not block Tokio
//! workers. Accepted batches retain their count and byte reservations while
//! queued, in flight, and retrying, until confirmation or terminal failure.
//!
//! # Features and deployment
//!
//! | Feature | Default | Listener |
//! | --- | --- | --- |
//! | `http` | Yes | `Collector::serve_http`, MessagePack `POST /v1/batches` |
//! | `tcp` | No | `Collector::serve_tcp`, authenticated framed MessagePack |
//!
//! Enable `features = ["tcp"]` for both transports, or additionally set
//! `default-features = false` for TCP only. Without either feature, the library
//! still supports direct [`Collector::submit`] calls and custom [`GroupWriter`]s.
//! The included binary requires at least one explicitly configured listener.
//!
//! Both listeners accept the configured bind address and serve plaintext.
//! TLS is a deployment option: use an HTTPS proxy or authenticated TLS tunnel
//! when encryption is needed. The library accepts an optional shared bearer
//! token; the deployment binary requires one. Use separate collector instances for separate trust
//! domains. See the [deployment guide](https://github.com/SF-Zhou/metrics-exporter-summary/tree/main/deploy/collector)
//! for TOML, service, container, and TLS proxy examples.
//!
//! # Embed an HTTP collector
//!
//! Apply the [ClickHouse schema](https://github.com/SF-Zhou/metrics-exporter-summary/tree/main/deploy/clickhouse)
//! before ingestion. Keep the collector and writer validation limits identical;
//! each writer's group budgets must cover the collector's group budgets.
//!
//! ```no_run
//! # #[cfg(feature = "http")]
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use metrics_summary_collector::{Collector, CollectorConfig};
//! use metrics_summary_sink_clickhouse::{ClickHouseBatchWriter, ClickHouseConfig};
//! use std::time::{Duration, Instant};
//! use tokio::net::TcpListener;
//!
//! let config = CollectorConfig::default();
//! let database = ClickHouseConfig {
//!     endpoint: "http://127.0.0.1:8123".into(),
//!     database: "metrics".into(),
//!     validation_limits: config.validation.clone(),
//!     ..ClickHouseConfig::default()
//! };
//! let writer = ClickHouseBatchWriter::new(database)?;
//! let token = std::env::var("METRICS_COLLECTOR_TOKEN")?;
//! let collector = Collector::new(config, Some(token), vec![Box::new(writer)])?;
//! let listener = TcpListener::bind("0.0.0.0:9091").await?;
//! collector.serve_http(listener, async {
//!     tokio::signal::ctrl_c().await.expect("install Ctrl-C handler");
//! }).await?;
//! let report = collector.shutdown(Instant::now() + Duration::from_secs(30)).await;
//! if !report.drained || report.dropped_after_acceptance != 0 {
//!     return Err("collector shut down with unconfirmed or dropped batches".into());
//! }
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "http"))]
//! # fn main() {}
//! ```
//!
//! # Acknowledgment and failure semantics
//!
//! [`AckPolicy::Enqueued`] confirms in-memory ownership; there is no write-ahead
//! log and a process crash can lose acknowledged work.
//! [`AckPolicy::ClickHouseConfirmed`] waits for the writer to report storage
//! success. A confirmation timeout does not cancel accepted work. The sender
//! must treat that timeout as an unknown outcome and preserve the batch identity
//! and content if retrying.
//!
//! Deduplication is bounded, process-local, and keyed by batch identity and
//! content. It does not provide exactly-once database insertion: retries after
//! an unknown write outcome, cache eviction, or restart can duplicate rows.
//! Inspect [`Collector::diagnostics`] for drops, retries, and pending age;
//! admission and readiness are not storage confirmation.
//!
//! Dropping the final [`Collector`] handle stops admission and wakes workers,
//! but does not wait for them. Stop listeners and explicitly call
//! [`Collector::shutdown`] while the Tokio runtime remains available to observe
//! the drain result. Custom writers must honor deadlines and avoid secrets in
//! errors. Shutdown does not forcibly terminate a writer that ignores its
//! deadline.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

use metrics_summary_core::{Batch, BatchId, CommitOutcome, ErrorKind, WriteError};
use metrics_summary_protocol::{AckPolicy, ProtocolLimits, Status};
use metrics_summary_sink_clickhouse::ClickHouseBatchWriter;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use tokio::sync::{watch, Semaphore};

#[cfg(feature = "http")]
mod http;
#[cfg(feature = "tcp")]
mod tcp;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// Admission, grouping, retry, and listener resource limits.
///
/// Missing Serde fields take their [`Default`] values and unknown fields are
/// rejected. All count and byte budgets must be positive; all `_ms` values are
/// milliseconds in `1..=86_400_000`. Limits apply per collector, shared across
/// listeners and writer threads. Byte limits are estimates or encoded sizes,
/// not a bound on total process RSS; decoding and storage encoding also need
/// temporary memory.
pub struct CollectorConfig {
    /// Maximum concurrent HTTP/TCP connections across listeners. Default: 256.
    pub max_connections: usize,
    /// Maximum concurrent receiving requests and, separately, waiting submitters.
    /// Default: 128 permits in each budget.
    pub max_requests: usize,
    /// Maximum encoded MessagePack request bytes, also bounded by `u32::MAX`.
    /// Default: 8 MiB.
    pub max_encoded_bytes: usize,
    /// Maximum admitted batches across queue, writes, and retries. Default: 1,024.
    pub max_pending_batches: usize,
    /// Maximum retained model byte estimate across all admitted batches. Default: 128 MiB.
    pub max_pending_bytes: usize,
    /// Maximum source batches in one storage group. Default: 64.
    pub group_max_batches: usize,
    /// Maximum total metric rows in one storage group. Default: 100,000.
    pub group_max_rows: usize,
    /// Maximum retained model byte estimate in one storage group. Default: 32 MiB.
    pub group_max_bytes: usize,
    /// Maximum JSON-encoded storage bytes across both table inserts in one group.
    /// Default: 64 MiB; distinct from the MessagePack request budget.
    pub group_max_encoded_bytes: usize,
    /// Maximum grouping delay from the oldest queued batch's acceptance, in milliseconds.
    /// Default: 100; full groups and shutdown trigger an earlier flush.
    /// This bounds batching waits, not queue delay while all writers are busy.
    pub group_max_delay_ms: u64,
    /// Listener request/confirmation timeout in milliseconds. Default: 30,000.
    /// TCP handshake and ACK transmission also use this timeout.
    pub request_timeout_ms: u64,
    /// Maximum duration of each database write attempt, in milliseconds. Default: 10,000.
    pub db_write_timeout_ms: u64,
    /// Retry time budget from the start of processing a group, in milliseconds.
    /// Default: 60,000; shutdown can shorten it.
    pub retry_deadline_ms: u64,
    /// Maximum write attempts for a group, including its first attempt. Default: 5.
    pub retry_max_attempts: usize,
    /// Base exponential retry backoff in milliseconds. Default: 100.
    /// Delay is this value times `2^min(attempt, 10)`, with attempts starting at 1.
    pub retry_backoff_ms: u64,
    /// Maximum pending and terminal deduplication entries. Default: 4,096.
    /// Must cover [`Self::max_pending_batches`]; old terminal entries may be evicted.
    pub dedup_capacity: usize,
    /// Retention of terminal deduplication entries in milliseconds. Default: 300,000.
    /// Capacity pressure can evict terminal entries earlier; pending entries do not expire.
    pub dedup_ttl_ms: u64,
    /// Accept in-memory ownership acknowledgment requests. Default: `true`.
    pub allow_enqueued: bool,
    /// Accept storage confirmation acknowledgment requests. Default: `true`.
    pub allow_confirmed: bool,
    /// If set, reject batches whose source application differs. Default: `None`.
    /// This is an additional admission check, not a separate authentication identity.
    pub allowed_application: Option<String>,
    /// Model validation, including label count and length limits; uses the core defaults.
    /// These limits must match every [`ClickHouseBatchWriter`] supplied to the collector.
    pub validation: metrics_summary_core::ValidationLimits,
}
impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            max_connections: 256,
            max_requests: 128,
            max_encoded_bytes: 8 * 1024 * 1024,
            max_pending_batches: 1024,
            max_pending_bytes: 128 * 1024 * 1024,
            group_max_batches: 64,
            group_max_rows: 100_000,
            group_max_bytes: 32 * 1024 * 1024,
            group_max_encoded_bytes: 64 * 1024 * 1024,
            group_max_delay_ms: 100,
            request_timeout_ms: 30_000,
            db_write_timeout_ms: 10_000,
            retry_deadline_ms: 60_000,
            retry_max_attempts: 5,
            retry_backoff_ms: 100,
            dedup_capacity: 4096,
            dedup_ttl_ms: 300_000,
            allow_enqueued: true,
            allow_confirmed: true,
            allowed_application: None,
            validation: Default::default(),
        }
    }
}
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
/// Invalid collector limits, writer compatibility, credentials, or worker configuration.
pub struct ConfigError(
    /// Configuration failure description.
    pub String,
);
impl CollectorConfig {
    /// Check nonzero budgets, timeout bounds, dedup capacity, semaphore limits,
    /// and that at least one acknowledgment policy is enabled.
    ///
    /// This does not bind listeners or connect to storage. Writer compatibility
    /// is checked separately by [`Collector::new`].
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validation
            .validate()
            .map_err(|error| ConfigError(error.to_string()))?;
        if [
            self.max_connections,
            self.max_requests,
            self.max_encoded_bytes,
            self.max_pending_batches,
            self.max_pending_bytes,
            self.group_max_batches,
            self.group_max_rows,
            self.group_max_bytes,
            self.group_max_encoded_bytes,
            self.retry_max_attempts,
            self.dedup_capacity,
        ]
        .contains(&0)
            || [
                self.group_max_delay_ms,
                self.request_timeout_ms,
                self.db_write_timeout_ms,
                self.retry_deadline_ms,
                self.retry_backoff_ms,
                self.dedup_ttl_ms,
            ]
            .contains(&0)
        {
            return Err(ConfigError(
                "resource limits and timeouts must be positive".into(),
            ));
        }
        if self.dedup_capacity < self.max_pending_batches
            || self.max_encoded_bytes > u32::MAX as usize
        {
            return Err(ConfigError(
                "dedup capacity must cover pending batches; frame limits must fit u32".into(),
            ));
        }
        if self.max_connections > Semaphore::MAX_PERMITS
            || self.max_requests > Semaphore::MAX_PERMITS
            || [
                self.group_max_delay_ms,
                self.request_timeout_ms,
                self.db_write_timeout_ms,
                self.retry_deadline_ms,
                self.retry_backoff_ms,
                self.dedup_ttl_ms,
            ]
            .iter()
            .any(|n| *n > 86_400_000)
        {
            return Err(ConfigError(
                "semaphore capacity exceeded or timeout exceeds 24 hours".into(),
            ));
        }
        if !self.allow_enqueued && !self.allow_confirmed {
            return Err(ConfigError(
                "at least one ACK policy must be enabled".into(),
            ));
        }
        Ok(())
    }
    /// Copy the MessagePack byte budget and model validation limits for request decoding.
    pub fn protocol_limits(&self) -> ProtocolLimits {
        ProtocolLimits {
            max_encoded_bytes: self.max_encoded_bytes,
            validation: self.validation.clone(),
        }
    }
}
/// Synchronous backend executed on one dedicated writer thread per instance.
///
/// [`ClickHouseBatchWriter`] implements this trait. Custom implementations can
/// supply another backend or fault-test collector behavior without a database.
/// Returning success is treated as storage confirmation for the whole group.
pub trait GroupWriter: Send + 'static {
    /// Reject incompatible limits before the collector can acknowledge any batch.
    fn validate_config(&self, _config: &CollectorConfig) -> Result<(), ConfigError> {
        Ok(())
    }
    /// Write all batches before the monotonic deadline and report their commit outcome.
    ///
    /// The collector may retry the same immutable group on retryable failures.
    /// Honor the deadline and use [`WriteError`] to distinguish failures known
    /// not to have committed from unknown outcomes. Error messages must not
    /// contain credentials or sensitive backend response bodies.
    fn write_group(&mut self, batches: &[Arc<Batch>], deadline: Instant) -> Result<(), WriteError>;
}
impl GroupWriter for ClickHouseBatchWriter {
    fn validate_config(&self, config: &CollectorConfig) -> Result<(), ConfigError> {
        let database = self.config();
        if database.max_group_batches < config.group_max_batches
            || database.max_group_rows < config.group_max_rows
            || database.max_group_bytes < config.group_max_bytes
            || database.max_encoded_bytes < config.group_max_encoded_bytes
            || database.validation_limits != config.validation
        {
            return Err(ConfigError(
                "ClickHouse limits must cover collector groups and validation limits must match"
                    .into(),
            ));
        }
        Ok(())
    }
    fn write_group(&mut self, batches: &[Arc<Batch>], deadline: Instant) -> Result<(), WriteError> {
        ClickHouseBatchWriter::write_group(self, batches, deadline)
    }
}
#[derive(Clone, Debug)]
enum Completion {
    Pending,
    Written,
    Dropped { unknown: bool, message: String },
}
struct Entry {
    fingerprint: [u8; 32],
    completion: watch::Sender<Completion>,
    terminal_at: Option<Instant>,
}
struct Accepted {
    batch: Arc<Batch>,
    bytes: usize,
    encoded_bytes: usize,
    accepted_at: Instant,
}
struct State {
    queue: VecDeque<Accepted>,
    entries: HashMap<BatchId, Entry>,
    pending_batches: usize,
    pending_bytes: usize,
    closing: bool,
    shutdown_deadline: Option<Instant>,
    active_workers: usize,
    last_write_error: Option<LastWriteError>,
    last_confirmed_unix_ms: Option<u64>,
    oldest_pending: HashMap<BatchId, Instant>,
}
#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    written: AtomicU64,
    dropped: AtomicU64,
    retries: AtomicU64,
    rejected: AtomicU64,
    duplicates: AtomicU64,
    groups: AtomicU64,
    rows: AtomicU64,
    retrying_batches: AtomicU64,
    retrying_bytes: AtomicU64,
}
struct Shared {
    config: CollectorConfig,
    state: Mutex<State>,
    wake: Condvar,
    counters: Counters,
    waiters: Arc<Semaphore>,
    #[cfg(any(feature = "http", feature = "tcp"))]
    requests: Arc<Semaphore>,
    #[cfg(any(feature = "http", feature = "tcp"))]
    connections: Arc<Semaphore>,
    token: Option<String>,
}
struct Owner {
    shared: Arc<Shared>,
    workers: Mutex<Vec<thread::JoinHandle<()>>>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        state.closing = true;
        self.shared.wake.notify_all();
    }
}
#[derive(Clone)]
/// Shared admission queue, deduplication state, diagnostics, and writer ownership.
///
/// Clones address the same collector. Call [`Self::shutdown`] explicitly to
/// observe draining; dropping the last handle does not wait for writer threads.
pub struct Collector {
    owner: Arc<Owner>,
}
#[derive(Clone, Debug, Serialize)]
/// Most recently observed storage write failure, retained after later recovery.
pub struct LastWriteError {
    /// Wall-clock Unix milliseconds when the collector observed the failure.
    pub unix_time_ms: u64,
    /// Retry classification returned by the backend.
    pub kind: ErrorKind,
    /// Whether the failed write is known not to have committed or is uncertain.
    pub outcome: CommitOutcome,
    /// Backend reason, bounded to 1024 UTF-8 bytes with the bearer token redacted.
    /// Custom writers must omit credentials and sensitive backend response bodies.
    pub message: String,
}
#[derive(Clone, Debug, Serialize)]
/// Point-in-time queue state and cumulative counters for this collector process.
///
/// Independent counters can advance while this snapshot is read. Counts do
/// not establish an exactly-once storage guarantee.
pub struct Diagnostics {
    /// Last backend failure; not cleared by a subsequent successful write.
    pub last_write_error: Option<LastWriteError>,
    /// Wall-clock Unix milliseconds of the most recent successful storage group.
    pub last_confirmed_unix_ms: Option<u64>,
    /// Total batches newly admitted, excluding deduplicated submissions.
    pub accepted_batches: u64,
    /// Total admitted batches whose group was confirmed by its writer.
    pub clickhouse_confirmed_batches: u64,
    /// Total admitted batches reaching terminal failure without confirmation.
    pub dropped_after_acceptance: u64,
    /// Total group retries after the initial write attempt.
    pub retries: u64,
    /// Total protocol or admission requests rejected through the collector's rejection path.
    pub rejected_requests: u64,
    /// Total submissions matched to an existing pending or confirmed batch identity.
    pub duplicate_requests: u64,
    /// Total successfully confirmed storage groups.
    pub inserted_groups: u64,
    /// Total rows in successfully confirmed groups; retries with unknown outcomes can store more.
    pub inserted_rows: u64,
    /// Current admitted batches awaiting terminal confirmation or failure.
    pub pending_batches: usize,
    /// Current retained model byte estimate for all pending batches.
    pub pending_bytes: usize,
    /// Current admitted batches in groups that have entered retry processing.
    pub retrying_batches: u64,
    /// Current retained model byte estimate of groups in retry processing.
    pub retrying_bytes: u64,
    /// Monotonic age of the oldest pending batch, in milliseconds; zero when empty.
    pub oldest_pending_age_ms: u64,
    /// Current pending and terminal deduplication entries.
    pub dedup_entries: usize,
    /// Whether new batch admission has stopped.
    pub closing: bool,
}
#[derive(Debug, Serialize)]
/// Result of a bounded shutdown attempt.
///
/// Successful delivery requires both [`Self::drained`] and zero
/// [`Self::dropped_after_acceptance`]. A drained queue alone can include batches
/// that were dropped after a terminal failure.
pub struct ShutdownReport {
    /// Whether no accepted batches remain pending at the time of the report.
    pub drained: bool,
    /// Accepted batches still pending when shutdown returns.
    pub unconfirmed_batches: usize,
    /// Retained model byte estimate of the remaining pending batches.
    pub unconfirmed_bytes: usize,
    /// Cumulative accepted batches dropped during the collector's lifetime.
    pub dropped_after_acceptance: u64,
}
impl Collector {
    /// Validate configuration and start one dedicated operating system thread per writer.
    ///
    /// Supply 1..=64 writers. Once transferred to their worker threads, database
    /// I/O and writer destruction happen there. Construction failure can drop
    /// untransferred writers on the calling thread; when building from an async
    /// context, supply fresh [`ClickHouseBatchWriter`]s whose HTTP runtimes have
    /// not been initialized. Listeners are started separately. A token,
    /// when supplied, must contain 1..=4096 visible ASCII bytes; `None` disables
    /// transport authentication and is intended for explicitly trusted local use.
    ///
    /// Returns [`ConfigError`] if configuration or writer limits are incompatible,
    /// credentials are invalid, or a worker thread cannot be started.
    pub fn new(
        config: CollectorConfig,
        bearer_token: Option<String>,
        writers: Vec<Box<dyn GroupWriter>>,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        for writer in &writers {
            writer.validate_config(&config)?;
        }
        if writers.is_empty() || writers.len() > 64 {
            return Err(ConfigError(
                "writer concurrency must be between 1 and 64".into(),
            ));
        }
        if bearer_token.as_ref().is_some_and(|t| {
            t.is_empty() || t.len() > 4096 || !t.bytes().all(|b| b.is_ascii_graphic())
        }) {
            return Err(ConfigError("invalid bearer token".into()));
        }
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                entries: HashMap::new(),
                pending_batches: 0,
                pending_bytes: 0,
                closing: false,
                shutdown_deadline: None,
                active_workers: writers.len(),
                last_write_error: None,
                last_confirmed_unix_ms: None,
                oldest_pending: HashMap::new(),
            }),
            wake: Condvar::new(),
            counters: Counters::default(),
            waiters: Arc::new(Semaphore::new(config.max_requests)),
            #[cfg(any(feature = "http", feature = "tcp"))]
            requests: Arc::new(Semaphore::new(config.max_requests)),
            #[cfg(any(feature = "http", feature = "tcp"))]
            connections: Arc::new(Semaphore::new(config.max_connections)),
            token: bearer_token,
            config,
        });
        let mut handles = Vec::new();
        for mut writer in writers {
            let worker_shared = shared.clone();
            match thread::Builder::new()
                .name("summary-collector-writer".into())
                .spawn(move || {
                    worker(&worker_shared, &mut *writer);
                    // Runtime-owning clients must be destroyed on this same writer thread.
                    drop(writer);
                    worker_shared.state.lock().unwrap().active_workers -= 1;
                    worker_shared.wake.notify_all();
                }) {
                Ok(handle) => handles.push(handle),
                Err(e) => {
                    shared.state.lock().unwrap().closing = true;
                    shared.wake.notify_all();
                    return Err(ConfigError(e.to_string()));
                }
            }
        }
        Ok(Self {
            owner: Arc::new(Owner {
                shared,
                workers: Mutex::new(handles),
            }),
        })
    }
    fn shared(&self) -> &Arc<Shared> {
        &self.owner.shared
    }
    /// The validated, immutable configuration shared by this collector's handles.
    pub fn config(&self) -> &CollectorConfig {
        &self.shared().config
    }
    /// Whether shutdown has stopped new admission.
    pub fn is_closing(&self) -> bool {
        self.shared().state.lock().unwrap().closing
    }
    /// Snapshot pending work, cumulative delivery counters, and the last write failure.
    pub fn diagnostics(&self) -> Diagnostics {
        let s = self.shared();
        let state = s.state.lock().unwrap();
        let c = &s.counters;
        Diagnostics {
            last_write_error: state.last_write_error.clone(),
            last_confirmed_unix_ms: state.last_confirmed_unix_ms,
            accepted_batches: c.accepted.load(Ordering::Relaxed),
            clickhouse_confirmed_batches: c.written.load(Ordering::Relaxed),
            dropped_after_acceptance: c.dropped.load(Ordering::Relaxed),
            retries: c.retries.load(Ordering::Relaxed),
            rejected_requests: c.rejected.load(Ordering::Relaxed),
            duplicate_requests: c.duplicates.load(Ordering::Relaxed),
            inserted_groups: c.groups.load(Ordering::Relaxed),
            inserted_rows: c.rows.load(Ordering::Relaxed),
            pending_batches: state.pending_batches,
            pending_bytes: state.pending_bytes,
            retrying_batches: c.retrying_batches.load(Ordering::Relaxed),
            retrying_bytes: c.retrying_bytes.load(Ordering::Relaxed),
            oldest_pending_age_ms: state
                .oldest_pending
                .values()
                .min()
                .map_or(0, |i| i.elapsed().as_millis().min(u64::MAX as u128) as u64),
            dedup_entries: state.entries.len(),
            closing: state.closing,
        }
    }
    #[cfg(any(feature = "http", feature = "tcp"))]
    fn authenticate(&self, token: Option<&str>) -> bool {
        use subtle::ConstantTimeEq;
        match (&self.shared().token, token) {
            (Some(expected), Some(actual)) => {
                bool::from(expected.as_bytes().ct_eq(actual.as_bytes()))
            }
            (None, _) => true,
            _ => false,
        }
    }
    fn reject(
        &self,
        id: Option<BatchId>,
        policy: AckPolicy,
        status: Status,
        message: &str,
    ) -> metrics_summary_protocol::wire::Ack {
        self.shared()
            .counters
            .rejected
            .fetch_add(1, Ordering::Relaxed);
        metrics_summary_protocol::ack(id, policy, status, message)
    }
    /// Validate and admit one batch, then acknowledge at the requested boundary.
    ///
    /// Validation, capacity reservation, and ownership transfer precede an
    /// Enqueued acknowledgment. Confirmed requests wait until the writer succeeds
    /// or the monotonic deadline expires. Cancellation and confirmation timeouts
    /// never remove accepted work. Retrying must preserve the batch identity and
    /// content; conflicting content under the same identity is rejected.
    ///
    /// Direct calls are trusted and do not perform bearer-token authentication
    /// or enforce an encoded MessagePack body limit; network handlers perform those
    /// checks before calling this method. Model limits, source application,
    /// storage encoding size, acknowledgment policy, and queue budgets still
    /// apply. Rejections are returned as protocol acknowledgment statuses.
    pub async fn submit(
        &self,
        batch: Batch,
        policy: AckPolicy,
        deadline: Instant,
    ) -> metrics_summary_protocol::wire::Ack {
        let id = batch.id;
        let permit = match self.shared().waiters.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                return self.reject(Some(id), policy, Status::Overloaded, "too many ACK waiters")
            }
        };
        if deadline <= Instant::now() {
            return self.reject(
                Some(id),
                policy,
                Status::Unavailable,
                "request deadline elapsed before admission",
            );
        }
        let config = self.config();
        if (!config.allow_enqueued && policy == AckPolicy::Enqueued)
            || (!config.allow_confirmed && policy == AckPolicy::ClickHouseConfirmed)
        {
            return self.reject(Some(id), policy, Status::Invalid, "ACK policy disabled");
        }
        if config
            .allowed_application
            .as_ref()
            .is_some_and(|a| a != &batch.source.application)
        {
            return self.reject(
                Some(id),
                policy,
                Status::Unauthorized,
                "source application forbidden",
            );
        }
        if let Err(e) = batch.validate(&config.validation) {
            return self.reject(Some(id), policy, Status::Invalid, &e.to_string());
        }
        let bytes = batch.estimated_bytes();
        let encoded_bytes = match metrics_summary_sink_clickhouse::encoded_batch_bytes_with_limits(
            &batch,
            &config.validation,
            config.group_max_encoded_bytes,
            deadline,
        ) {
            Ok(n) => n,
            Err(e) => {
                return self.reject(
                    Some(id),
                    policy,
                    if e.kind == ErrorKind::Timeout {
                        Status::Unavailable
                    } else {
                        Status::Invalid
                    },
                    &e.to_string(),
                )
            }
        };
        if batch.rows.len() > config.group_max_rows
            || bytes > config.group_max_bytes
            || bytes > config.max_pending_bytes
        {
            return self.reject(
                Some(id),
                policy,
                Status::Invalid,
                "batch cannot fit one insert group",
            );
        }
        // Canonical model encoding excludes requested ACK level; retries may ask for stronger confirmation.
        let canonical = match metrics_summary_protocol::encode_wire_request(
            &metrics_summary_protocol::request(&batch, AckPolicy::Enqueued),
        ) {
            Ok(bytes) => bytes,
            Err(error) => {
                return self.reject(Some(id), policy, Status::Invalid, &error.to_string());
            }
        };
        let fingerprint: [u8; 32] = Sha256::digest(canonical).into();
        let admission = {
            let mut state = self.shared().state.lock().unwrap();
            if Instant::now() >= deadline {
                return self.reject(
                    Some(id),
                    policy,
                    Status::Unavailable,
                    "deadline elapsed before admission",
                );
            }
            if state.closing {
                return self.reject(Some(id), policy, Status::Unavailable, "collector closing");
            }
            let now = Instant::now();
            let ttl = Duration::from_millis(config.dedup_ttl_ms);
            state
                .entries
                .retain(|_, e| e.terminal_at.is_none_or(|t| now.duration_since(t) < ttl));
            let existing = if let Some(entry) = state.entries.get(&id) {
                if entry.fingerprint != fingerprint {
                    return self.reject(
                        Some(id),
                        policy,
                        Status::Invalid,
                        "same batch ID has different content",
                    );
                }
                if !matches!(*entry.completion.borrow(), Completion::Dropped { .. }) {
                    self.shared()
                        .counters
                        .duplicates
                        .fetch_add(1, Ordering::Relaxed);
                    Some(entry.completion.subscribe())
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(receiver) = existing {
                Ok(receiver)
            } else {
                if state.entries.len() >= config.dedup_capacity {
                    let oldest = state
                        .entries
                        .iter()
                        .filter_map(|(id, e)| e.terminal_at.map(|t| (*id, t)))
                        .min_by_key(|(_, t)| *t)
                        .map(|(id, _)| id);
                    if let Some(oldest) = oldest {
                        state.entries.remove(&oldest);
                    }
                }
                if state.pending_batches >= config.max_pending_batches
                    || bytes > config.max_pending_bytes.saturating_sub(state.pending_bytes)
                    || state.entries.len() >= config.dedup_capacity
                {
                    Err("collector pending or dedup capacity exhausted")
                } else {
                    let (tx, rx) = watch::channel(Completion::Pending);
                    state.entries.insert(
                        id,
                        Entry {
                            fingerprint,
                            completion: tx,
                            terminal_at: None,
                        },
                    );
                    state.pending_batches += 1;
                    state.pending_bytes += bytes;
                    state.oldest_pending.insert(id, now);
                    state.queue.push_back(Accepted {
                        batch: Arc::new(batch),
                        bytes,
                        encoded_bytes,
                        accepted_at: now,
                    });
                    self.shared()
                        .counters
                        .accepted
                        .fetch_add(1, Ordering::Relaxed);
                    self.shared().wake.notify_one();
                    Ok(rx)
                }
            }
        };
        let mut completion = match admission {
            Ok(receiver) => receiver,
            Err(message) => return self.reject(Some(id), policy, Status::Overloaded, message),
        };
        if policy == AckPolicy::Enqueued {
            return metrics_summary_protocol::ack(Some(id), policy, Status::Ok, "");
        }
        loop {
            let current = completion.borrow().clone();
            match current {
                Completion::Written => {
                    return metrics_summary_protocol::ack(
                        Some(id),
                        AckPolicy::ClickHouseConfirmed,
                        Status::Ok,
                        "",
                    )
                }
                Completion::Dropped { unknown, message } => {
                    return metrics_summary_protocol::ack(
                        Some(id),
                        policy,
                        if unknown {
                            Status::Unknown
                        } else {
                            Status::Unavailable
                        },
                        message,
                    )
                }
                Completion::Pending => {}
            }
            if tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                completion.changed(),
            )
            .await
            .is_err()
            {
                drop(permit);
                return metrics_summary_protocol::ack(
                    Some(id),
                    policy,
                    Status::Unknown,
                    "confirmation deadline elapsed; accepted batch continues processing",
                );
            }
        }
    }
    /// Stop admission, immediately flush groups, and wait up to the supplied monotonic deadline.
    ///
    /// Signal and finish listeners separately; this method owns the ingestion
    /// state and storage workers, not listener tasks. Repeated calls can shorten,
    /// but never extend, the worker deadline. The returned report distinguishes
    /// pending work from batches already dropped after acceptance. A custom writer
    /// that ignores its deadline can outlive this call; it is not forcibly stopped.
    pub async fn shutdown(&self, deadline: Instant) -> ShutdownReport {
        {
            let mut state = self.shared().state.lock().unwrap();
            state.closing = true;
            state.shutdown_deadline = Some(
                state
                    .shutdown_deadline
                    .map_or(deadline, |old| old.min(deadline)),
            );
            self.shared().wake.notify_all();
        }
        loop {
            let done = self.shared().state.lock().unwrap().active_workers == 0;
            if done || Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(
                Duration::from_millis(5).min(deadline.saturating_duration_since(Instant::now())),
            )
            .await;
        }
        let d = self.diagnostics();
        // Discard only completed handles; a faulty writer ignoring its deadline cannot block shutdown.
        self.owner
            .workers
            .lock()
            .unwrap()
            .retain(|handle| !handle.is_finished());
        ShutdownReport {
            drained: d.pending_batches == 0,
            unconfirmed_batches: d.pending_batches,
            unconfirmed_bytes: d.pending_bytes,
            dropped_after_acceptance: d.dropped_after_acceptance,
        }
    }
}
fn worker(shared: &Arc<Shared>, writer: &mut dyn GroupWriter) {
    loop {
        let group = {
            let mut state = shared.state.lock().unwrap();
            while state.queue.is_empty() && !state.closing {
                state = shared.wake.wait(state).unwrap();
            }
            if state.queue.is_empty() && state.closing {
                return;
            }
            let oldest = state.queue.front().unwrap().accepted_at;
            let flush_at = oldest + Duration::from_millis(shared.config.group_max_delay_ms);
            // Bounded wait lets small traffic flush; arrivals wake this wait when the group fills.
            loop {
                let mut rows = 0usize;
                let mut bytes = 0usize;
                let mut encoded = 0usize;
                let mut count = 0usize;
                for item in &state.queue {
                    rows = rows.saturating_add(item.batch.rows.len());
                    bytes = bytes.saturating_add(item.bytes);
                    encoded = encoded.saturating_add(item.encoded_bytes);
                    count += 1;
                    if rows >= shared.config.group_max_rows
                        || bytes >= shared.config.group_max_bytes
                        || encoded >= shared.config.group_max_encoded_bytes
                        || count >= shared.config.group_max_batches
                    {
                        break;
                    }
                }
                if state.closing
                    || Instant::now() >= flush_at
                    || rows >= shared.config.group_max_rows
                    || bytes >= shared.config.group_max_bytes
                    || encoded >= shared.config.group_max_encoded_bytes
                    || count >= shared.config.group_max_batches
                {
                    break;
                }
                state = shared
                    .wake
                    .wait_timeout(state, flush_at.saturating_duration_since(Instant::now()))
                    .unwrap()
                    .0;
            }
            let mut group = Vec::new();
            let mut rows = 0usize;
            let mut bytes = 0usize;
            let mut encoded = 0usize;
            while let Some(item) = state.queue.front() {
                if group.len() >= shared.config.group_max_batches
                    || rows.saturating_add(item.batch.rows.len()) > shared.config.group_max_rows
                    || bytes.saturating_add(item.bytes) > shared.config.group_max_bytes
                    || encoded.saturating_add(item.encoded_bytes)
                        > shared.config.group_max_encoded_bytes
                {
                    break;
                }
                let item = state.queue.pop_front().unwrap();
                rows += item.batch.rows.len();
                bytes += item.bytes;
                encoded += item.encoded_bytes;
                group.push(item);
            }
            group
        };
        if group.is_empty() {
            continue;
        }
        let batches: Vec<_> = group.iter().map(|a| a.batch.clone()).collect();
        let retry_until = Instant::now() + Duration::from_millis(shared.config.retry_deadline_ms);
        let mut attempt = 0;
        let mut unknown = false;
        let mut retrying = false;
        let completion = loop {
            let deadline = shared
                .state
                .lock()
                .unwrap()
                .shutdown_deadline
                .map_or(retry_until, |d| d.min(retry_until));
            if Instant::now() >= deadline {
                let message = record_write_error(
                    shared,
                    &WriteError::new(
                        ErrorKind::Timeout,
                        if unknown {
                            CommitOutcome::Unknown
                        } else {
                            CommitOutcome::NotCommitted
                        },
                        "collector write budget exhausted",
                    ),
                );
                break Completion::Dropped { unknown, message };
            }
            attempt += 1;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                writer.write_group(
                    &batches,
                    deadline.min(
                        Instant::now() + Duration::from_millis(shared.config.db_write_timeout_ms),
                    ),
                )
            }));
            let result = match result {
                Ok(result) => result,
                Err(_) => {
                    unknown = true;
                    let message = record_write_error(
                        shared,
                        &WriteError::new(
                            ErrorKind::Permanent,
                            CommitOutcome::Unknown,
                            "group writer panicked",
                        ),
                    );
                    break Completion::Dropped { unknown, message };
                }
            };
            match result {
                Ok(()) => break Completion::Written,
                Err(e) => {
                    let reason = record_write_error(shared, &e);
                    unknown |= e.outcome == CommitOutcome::Unknown;
                    if !e.is_retryable() || attempt >= shared.config.retry_max_attempts {
                        break Completion::Dropped {
                            unknown,
                            message: reason,
                        };
                    }
                    shared.counters.retries.fetch_add(1, Ordering::Relaxed);
                    if !retrying {
                        retrying = true;
                        shared
                            .counters
                            .retrying_batches
                            .fetch_add(group.len() as u64, Ordering::Relaxed);
                        shared.counters.retrying_bytes.fetch_add(
                            group.iter().map(|a| a.bytes as u64).sum(),
                            Ordering::Relaxed,
                        );
                    }
                    let delay = Duration::from_millis(
                        shared
                            .config
                            .retry_backoff_ms
                            .saturating_mul(1u64 << attempt.min(10)),
                    );
                    let retry_at = Instant::now() + delay;
                    let mut state = shared.state.lock().unwrap();
                    loop {
                        let actual_deadline = state
                            .shutdown_deadline
                            .map_or(deadline, |d| d.min(deadline));
                        let wake_at = retry_at.min(actual_deadline);
                        if Instant::now() >= wake_at {
                            break;
                        }
                        state = shared
                            .wake
                            .wait_timeout(state, wake_at.saturating_duration_since(Instant::now()))
                            .unwrap()
                            .0;
                    }
                }
            }
        };
        if retrying {
            shared
                .counters
                .retrying_batches
                .fetch_sub(group.len() as u64, Ordering::Relaxed);
            shared.counters.retrying_bytes.fetch_sub(
                group.iter().map(|a| a.bytes as u64).sum(),
                Ordering::Relaxed,
            );
        }
        let success = matches!(completion, Completion::Written);
        if success {
            shared.counters.groups.fetch_add(1, Ordering::Relaxed);
            shared.counters.rows.fetch_add(
                batches.iter().map(|b| b.rows.len() as u64).sum(),
                Ordering::Relaxed,
            );
        }
        let mut state = shared.state.lock().unwrap();
        if success {
            state.last_confirmed_unix_ms = Some(unix_ms());
        }
        for accepted in group {
            state.pending_batches -= 1;
            state.pending_bytes -= accepted.bytes;
            state.oldest_pending.remove(&accepted.batch.id);
            if let Some(entry) = state.entries.get_mut(&accepted.batch.id) {
                entry.completion.send_replace(completion.clone());
                entry.terminal_at = Some(Instant::now());
            }
            if success {
                shared.counters.written.fetch_add(1, Ordering::Relaxed);
            } else {
                shared.counters.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        shared.wake.notify_all();
    }
}

#[cfg(test)]
mod tests;

fn bounded_message(mut message: String) -> String {
    let mut end = message.len().min(1024);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message.truncate(end);
    message
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
fn record_write_error(shared: &Shared, error: &WriteError) -> String {
    let mut end = error
        .message
        .len()
        .min(1024 + shared.token.as_ref().map_or(0, |t| t.len()));
    while !error.message.is_char_boundary(end) {
        end -= 1;
    }
    let message = match &shared.token {
        Some(token) => error.message[..end].replace(token, "[REDACTED]"),
        None => error.message[..end].to_owned(),
    };
    let message = bounded_message(message);
    shared.state.lock().unwrap().last_write_error = Some(LastWriteError {
        unix_time_ms: unix_ms(),
        kind: error.kind,
        outcome: error.outcome,
        message: message.clone(),
    });
    message
}
