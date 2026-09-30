//! Transport-independent immutable metric summaries and synchronous sink contracts.
//!
//! Histograms and counters describe incremental collection windows; gauges are
//! point samples. Quantiles cannot be merged across exported batches.
//! Sink implementations must respect their supplied monotonic deadline and retain
//! the original batch identity on retries.
//!
//! # Model and validation
//!
//! [`Source`] identifies a service instance. [`BatchId`] adds a session UUID and
//! increasing sequence; a [`Batch`] contains one collection round of [`Row`]s.
//! Each row holds a [`MetricValue`]: an interval Counter delta, current integer
//! Gauge value, or Histogram summary. Empty Histogram windows have no row; empty
//! batches remain valid. The histogram mean is `sum / count`.
//!
//! Public model fields and Serde deserialization do not automatically validate
//! data. Call [`Batch::validate`] before admitting a manually constructed or
//! deserialized batch; [`ValidationLimits`] bounds text, cardinality, and retained
//! memory. The bundled protocol and sinks perform these checks at their boundaries.
//!
//! ```
//! use metrics_summary_core::{
//!     Batch, BatchId, MetricValue, Row, Source, ValidationLimits, MODEL_VERSION,
//! };
//! use uuid::Uuid;
//!
//! let batch = Batch {
//!     model_version: MODEL_VERSION,
//!     id: BatchId { source_session_id: Uuid::new_v4(), sequence: 1 },
//!     source: Source {
//!         application: "api".into(),
//!         instance: "worker-1".into(),
//!         hostname: "node-1".into(),
//!         attributes: Default::default(),
//!     },
//!     timestamp: 1_700_000_000_000_000_000,
//!     duration_ns: 10_000_000_000,
//!     rows: vec![Row {
//!         metric_id: 1,
//!         name: "requests".into(),
//!         labels: Default::default(),
//!         unit: None,
//!         value: MetricValue::CounterDelta { delta_value: 42 },
//!     }],
//! };
//! batch.validate(&ValidationLimits::default())?;
//! # Ok::<(), metrics_summary_core::ValidationError>(())
//! ```
//!
//! Label keys are unrestricted except for text and resource limits. Use
//! [`effective_labels`] to fill missing `host` from [`Source::hostname`] and
//! `instance` from [`Source::instance`], without mutating the row. Explicit values
//! take precedence; other absent labels remain absent. Counter and Gauge share a
//! storage family; their effective name/label identities must be distinct.
//!
//! # Timing and delivery
//!
//! [`Batch::timestamp`] is collection-completion Unix nanoseconds, while
//! [`Batch::duration_ns`] is its monotonic statistical span. Wall-clock rollback
//! can reverse timestamp order; [`BatchId`] defines transport identity and its
//! sequence establishes order within one source session.
//! Neither metadata field guarantees exactly synchronized shard boundaries.
//!
//! [`Sink`] is synchronous and writer-owned. Its [`CompletionBoundary`] defines
//! what success means. [`WriteError`] independently describes retryability through
//! [`ErrorKind`] and commit certainty through [`CommitOutcome`]. A timeout does not
//! prove cancellation, and retrying an unknown outcome can duplicate stored data.
//! Deadlines bound interruptible waits and are checked before new I/O/publication;
//! non-interruptible cleanup can delay return after a successful commit.
//!
//! Applications normally start with the [recorder] and a [memory], [ClickHouse],
//! or [remote] sink rather than assembling batches themselves.
//!
//! [recorder]: https://docs.rs/metrics-exporter-summary
//! [memory]: https://docs.rs/metrics-summary-sink-memory
//! [ClickHouse]: https://docs.rs/metrics-summary-sink-clickhouse
//! [remote]: https://docs.rs/metrics-summary-sink-remote

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, mem::size_of, sync::Arc, time::Instant};
use uuid::Uuid;

/// Version of the in-memory data model, independent of transport and table versions.
pub const MODEL_VERSION: u32 = 1;

/// Immutable source metadata. `hostname` is required even if `instance` is a pod ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Source {
    /// Application or service name.
    pub application: String,
    /// User-selected identity of the running instance.
    pub instance: String,
    /// Actual host name, supplied explicitly or discovered by [`Source::new`].
    pub hostname: String,
    /// Bounded, user-defined source attributes such as cluster and environment.
    pub attributes: BTreeMap<String, String>,
}

impl Source {
    /// Creates source metadata using the operating system host name.
    ///
    /// Fails on a missing, empty, invalid UTF-8 or oversized name; it never silently
    /// replaces a failed host lookup with an invented hostname. Other source fields
    /// are checked against [`ValidationLimits::default`].
    pub fn new(
        application: impl Into<String>,
        instance: impl Into<String>,
    ) -> Result<Self, ValidationError> {
        let hostname = hostname::get()
            .map_err(|error| {
                ValidationError::new(
                    "source.hostname",
                    format!("hostname lookup failed: {error}"),
                )
            })?
            .into_string()
            .map_err(|_| ValidationError::new("source.hostname", "hostname is not valid UTF-8"))?;
        let source = Self {
            application: application.into(),
            instance: instance.into(),
            hostname,
            attributes: BTreeMap::new(),
        };
        source.validate(&ValidationLimits::default())?;
        Ok(source)
    }

    /// Conservative estimate of owned memory, including string capacity and map nodes.
    pub fn estimated_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(string_heap_bytes(&self.application))
            .saturating_add(string_heap_bytes(&self.instance))
            .saturating_add(string_heap_bytes(&self.hostname))
            .saturating_add(map_heap_bytes(&self.attributes))
    }

    /// Validates all source fields and configured size limits.
    pub fn validate(&self, limits: &ValidationLimits) -> Result<(), ValidationError> {
        limits.validate()?;
        checked_text(
            "source.application",
            &self.application,
            limits.max_source_field_bytes,
            true,
        )?;
        checked_text(
            "source.instance",
            &self.instance,
            limits.max_source_field_bytes,
            true,
        )?;
        checked_text(
            "source.hostname",
            &self.hostname,
            limits.max_source_field_bytes,
            true,
        )?;
        checked_map(
            "source.attributes",
            &self.attributes,
            limits.max_source_attributes,
            limits.max_attribute_key_bytes,
            limits.max_attribute_value_bytes,
        )
    }
}

/// Stable source batch identity, preserved across every retry and collector grouping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BatchId {
    /// Unique recorder-session identity, including restarts and same-process recorders.
    pub source_session_id: Uuid,
    /// Nonzero, strictly increasing collection sequence within the session.
    pub sequence: u64,
}

/// A complete collection round, including empty rounds.
///
/// The completion timestamp may move backwards with the wall clock. The statistical
/// span is measured independently with a monotonic clock. Use [`BatchId::sequence`]
/// for ordering; neither time field determines identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Batch {
    /// Data-model version; currently [`MODEL_VERSION`].
    pub model_version: u32,
    /// Original source identity.
    pub id: BatchId,
    /// Source metadata, including a mandatory host name.
    pub source: Source,
    /// Collection completion wall time, in signed Unix nanoseconds.
    pub timestamp: i64,
    /// Monotonic nanoseconds between successive collection completions; the first
    /// round starts at recorder creation. May be zero. This is the statistical span,
    /// not the time spent scanning or merging. Thread-shard boundaries may differ.
    pub duration_ns: u64,
    /// Complete metric rows. Histogram rows are absent for empty windows.
    pub rows: Vec<Row>,
}

impl Batch {
    /// Conservative retained-memory estimate, including unused vector and string capacity.
    ///
    /// This estimate includes allocator headroom, map nodes and an `Arc` header;
    /// it is a budget unit, not a promise about a particular allocator's resident size.
    pub fn estimated_bytes(&self) -> usize {
        self.rows.iter().fold(
            size_of::<Self>()
                .saturating_add(12 * size_of::<usize>())
                .saturating_add(
                    self.source
                        .estimated_bytes()
                        .saturating_sub(size_of::<Source>()),
                )
                .saturating_add(self.rows.capacity().saturating_mul(size_of::<Row>())),
            |total, row| {
                total.saturating_add(row.estimated_bytes().saturating_sub(size_of::<Row>()))
            },
        )
    }

    /// Number of histogram samples in this batch, saturating on overflow.
    pub fn histogram_samples(&self) -> u64 {
        self.rows.iter().fold(0_u64, |total, row| match row.value {
            MetricValue::HistogramSummary { count, .. } => total.saturating_add(count),
            _ => total,
        })
    }

    /// Validates the entire batch atomically before it is accepted by a sink.
    ///
    /// Validation checks retained memory as well as the length of every variable
    /// field. Every signed timestamp and unsigned duration is valid, including a
    /// zero duration; wall-clock ordering is intentionally unrestricted.
    pub fn validate(&self, limits: &ValidationLimits) -> Result<(), ValidationError> {
        if self.model_version != MODEL_VERSION {
            return Err(ValidationError::new(
                "model_version",
                "unsupported model version",
            ));
        }
        if self.id.source_session_id.is_nil() {
            return Err(ValidationError::new(
                "id.source_session_id",
                "session UUID must not be nil",
            ));
        }
        if self.id.sequence == 0 {
            return Err(ValidationError::new(
                "id.sequence",
                "sequence must be nonzero",
            ));
        }
        if self.rows.len() > limits.max_rows {
            return Err(ValidationError::new("rows", "row count exceeds limit"));
        }
        if self.estimated_bytes() > limits.max_batch_bytes {
            return Err(ValidationError::new(
                "batch",
                "estimated memory exceeds limit",
            ));
        }
        self.source.validate(limits)?;
        let mut ids = std::collections::HashSet::with_capacity(self.rows.len());
        let mut identities = std::collections::HashSet::with_capacity(self.rows.len());
        for row in &self.rows {
            row.validate(limits)?;
            if !ids.insert(row.metric_id) {
                return Err(ValidationError::new(
                    "rows.metric_id",
                    "duplicate metric ID",
                ));
            }
            let labels = effective_label_refs(&self.source, &row.labels, limits)?;
            // Counter and Gauge share the scalar storage table. They must not
            // acquire indistinguishable names/labels merely through different IDs.
            let distribution = row.value.kind() == MetricKind::Histogram;
            if !identities.insert((distribution, row.name.as_str(), labels)) {
                return Err(ValidationError::new(
                    "rows.labels",
                    "duplicate scalar or distribution name and effective labels",
                ));
            }
        }
        Ok(())
    }
}

/// One metric series' complete summary or scalar snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Row {
    /// Stable, nonzero recorder-local series ID, never reused during the session.
    pub metric_id: u64,
    /// Original metric name.
    pub name: String,
    /// Explicit labels. Missing host/instance inherit source values; other absent
    /// labels remain absent. See [`effective_labels`].
    pub labels: BTreeMap<String, String>,
    /// Optional declared unit, without implicit conversion.
    pub unit: Option<String>,
    /// Instrument-specific sample or summary.
    pub value: MetricValue,
}

impl Row {
    /// Conservative estimate of owned row memory, including allocation capacities.
    pub fn estimated_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(string_heap_bytes(&self.name))
            .saturating_add(map_heap_bytes(&self.labels))
            .saturating_add(self.unit.as_ref().map_or(0, string_heap_bytes))
    }

    /// Validates metric identity, metadata, and the instrument-specific value.
    pub fn validate(&self, limits: &ValidationLimits) -> Result<(), ValidationError> {
        limits.validate()?;
        if self.metric_id == 0 {
            return Err(ValidationError::new(
                "row.metric_id",
                "metric ID must be nonzero",
            ));
        }
        checked_text("row.name", &self.name, limits.max_name_bytes, true)?;
        checked_map(
            "row.labels",
            &self.labels,
            limits.max_labels,
            limits.max_label_key_bytes,
            limits.max_label_value_bytes,
        )?;
        checked_label_keys(&self.labels)?;
        if let Some(unit) = &self.unit {
            checked_text("row.unit", unit, limits.max_unit_bytes, true)?;
        }
        self.value.validate()
    }
}

/// Type of instrument. Counter and Gauge share a storage family and cannot use
/// the same name and effective labels in one batch. Histograms use a separate family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum MetricKind {
    /// Counter increments accumulated within one collection window.
    Counter,
    /// Current scalar gauge value.
    Gauge,
    /// Incremental histogram summary.
    Histogram,
}

/// Instrument-specific metric data. Every floating-point value must be finite.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetricValue {
    /// Incremental histogram distribution over one collection window.
    HistogramSummary {
        /// Nonzero number of valid weighted samples, at most 2^53 so storage can
        /// represent the count exactly as a floating-point number.
        count: u64,
        /// Sum of the original weighted samples.
        sum: f64,
        /// Exact minimum original sample, subject only to sample representation.
        min: f64,
        /// Estimated 50th percentile.
        p50: f64,
        /// Estimated 90th percentile.
        p90: f64,
        /// Estimated 95th percentile.
        p95: f64,
        /// Estimated 99th percentile.
        p99: f64,
        /// Exact maximum original sample, subject only to sample representation.
        max: f64,
    },
    /// Counter increments accumulated within one collection window. Exported
    /// deltas can be added across windows; they are not lifetime totals.
    /// A missing batch cannot be recovered from a later delta.
    CounterDelta {
        /// Total increment in this collection window, without earlier windows.
        /// Zero is valid; the maximum is `i64::MAX`, matching scalar storage.
        delta_value: u64,
    },
    /// Initialized gauge sampled at collection time.
    GaugeSnapshot {
        /// Current signed 64-bit integer gauge value, preserved exactly.
        current_value: i64,
    },
}

impl MetricValue {
    /// Returns the instrument kind associated with this value.
    pub fn kind(&self) -> MetricKind {
        match self {
            Self::HistogramSummary { .. } => MetricKind::Histogram,
            Self::CounterDelta { .. } => MetricKind::Counter,
            Self::GaugeSnapshot { .. } => MetricKind::Gauge,
        }
    }

    /// Checks the counter's Int64 limit and histogram finite/count/quantile invariants.
    pub fn validate(&self) -> Result<(), ValidationError> {
        match *self {
            Self::HistogramSummary {
                count,
                sum,
                min,
                p50,
                p90,
                p95,
                p99,
                max,
            } => {
                if count == 0 {
                    return Err(ValidationError::new(
                        "histogram.count",
                        "empty histograms must not produce rows",
                    ));
                }
                if count > (1_u64 << 53) {
                    return Err(ValidationError::new(
                        "histogram.count",
                        "sample count exceeds exact floating-point representation limit",
                    ));
                }
                if [sum, min, p50, p90, p95, p99, max]
                    .iter()
                    .any(|value| !value.is_finite())
                {
                    return Err(ValidationError::new(
                        "histogram",
                        "all statistics must be finite",
                    ));
                }
                if !(min <= p50 && p50 <= p90 && p90 <= p95 && p95 <= p99 && p99 <= max) {
                    return Err(ValidationError::new(
                        "histogram",
                        "quantiles must be ordered between min and max",
                    ));
                }
            }
            Self::CounterDelta { delta_value } if delta_value > i64::MAX as u64 => {
                return Err(ValidationError::new(
                    "counter.delta_value",
                    "counter delta must fit Int64",
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

/// Independent limits for validating untrusted or manually constructed batches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ValidationLimits {
    /// Maximum conservative retained-memory estimate for one batch.
    pub max_batch_bytes: usize,
    /// Maximum rows in one batch.
    pub max_rows: usize,
    /// Maximum labels per metric row, including inherited host and instance.
    pub max_labels: usize,
    /// Maximum metric name UTF-8 byte length.
    pub max_name_bytes: usize,
    /// Maximum label key UTF-8 byte length.
    pub max_label_key_bytes: usize,
    /// Maximum label value UTF-8 byte length.
    pub max_label_value_bytes: usize,
    /// Maximum unit UTF-8 byte length.
    pub max_unit_bytes: usize,
    /// Maximum source attribute count.
    pub max_source_attributes: usize,
    /// Maximum application, instance and hostname UTF-8 byte length.
    pub max_source_field_bytes: usize,
    /// Maximum source attribute key UTF-8 byte length.
    pub max_attribute_key_bytes: usize,
    /// Maximum source attribute value UTF-8 byte length.
    pub max_attribute_value_bytes: usize,
}

impl Default for ValidationLimits {
    fn default() -> Self {
        Self {
            max_batch_bytes: 16 * 1024 * 1024,
            max_rows: 10_000,
            max_labels: 32,
            max_name_bytes: 256,
            max_label_key_bytes: 128,
            max_label_value_bytes: 1024,
            max_unit_bytes: 64,
            max_source_attributes: 32,
            max_source_field_bytes: 1024,
            max_attribute_key_bytes: 128,
            max_attribute_value_bytes: 1024,
        }
    }
}

impl ValidationLimits {
    /// Validates the retained-memory budget. Other limits are checked against
    /// actual content, so zero row or label limits can still admit empty batches.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.max_batch_bytes == 0 {
            return Err(ValidationError::new(
                "max_batch_bytes",
                "retained-memory budget must be positive",
            ));
        }
        Ok(())
    }
}

fn checked_label_keys(labels: &BTreeMap<String, String>) -> Result<(), ValidationError> {
    if labels.keys().any(|name| name.chars().any(char::is_control)) {
        return Err(ValidationError::new(
            "row.labels",
            "label keys must not contain control characters",
        ));
    }
    Ok(())
}

fn effective_label_refs<'a>(
    source: &'a Source,
    labels: &'a BTreeMap<String, String>,
    limits: &ValidationLimits,
) -> Result<BTreeMap<&'a str, &'a str>, ValidationError> {
    let count = labels
        .len()
        .saturating_add(usize::from(!labels.contains_key("host")))
        .saturating_add(usize::from(!labels.contains_key("instance")));
    if count > limits.max_labels {
        return Err(ValidationError::new(
            "row.labels",
            "entry count including default labels exceeds limit",
        ));
    }
    let mut effective: BTreeMap<_, _> = labels
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    effective.entry("host").or_insert(source.hostname.as_str());
    effective
        .entry("instance")
        .or_insert(source.instance.as_str());
    for (name, value) in &effective {
        checked_text("row.labels.key", name, limits.max_label_key_bytes, true)?;
        checked_text(
            "row.labels.value",
            value,
            limits.max_label_value_bytes,
            *name == "host",
        )?;
    }
    Ok(effective)
}

/// Preserves all explicit labels and fills missing `host` from [`Source::hostname`]
/// and `instance` from [`Source::instance`]. No other labels are added. Explicit
/// values take precedence; the final `host` must be nonempty.
///
/// Rejects invalid source metadata, empty or control-character label keys, and
/// labels exceeding their count or text limits after defaults are applied.
/// It does not alter the original row.
pub fn effective_labels(
    source: &Source,
    labels: &BTreeMap<String, String>,
    limits: &ValidationLimits,
) -> Result<BTreeMap<String, String>, ValidationError> {
    source.validate(limits)?;
    checked_map(
        "row.labels",
        labels,
        limits.max_labels,
        limits.max_label_key_bytes,
        limits.max_label_value_bytes,
    )?;
    checked_label_keys(labels)?;
    Ok(effective_label_refs(source, labels, limits)?
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect())
}

/// A rejected field and its validation failure, without copying potentially large input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    /// Logical model field that failed validation.
    pub field: String,
    /// Human-readable reason.
    pub message: String,
}

impl ValidationError {
    /// Creates a validation error for a field.
    pub fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}
impl std::error::Error for ValidationError {}

/// The minimum completion guarantee of every successful write on a sink instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompletionBoundary {
    /// The whole batch has been atomically published to a local reader.
    LocalPublished,
    /// A collector owns the whole batch in its bounded memory queue.
    RemoteAccepted,
    /// All required storage insertions have been acknowledged.
    StorageConfirmed,
    /// Explicitly named third-party completion semantics.
    Custom(String),
}

/// Whether another write attempt could resolve a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorKind {
    /// Temporary failure, eligible for a bounded retry.
    Retryable,
    /// Invalid data/configuration or another nonrecoverable failure.
    Permanent,
    /// The attempt deadline expired; retry only within the enclosing budget.
    Timeout,
}

/// Commit knowledge is independent of retryability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitOutcome {
    /// The sink knows that this attempt did not commit the batch.
    NotCommitted,
    /// The batch may already have been accepted or committed.
    Unknown,
}

/// Classified sink error. Unknown outcomes require retries with the original identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteError {
    /// Retry and deadline classification.
    pub kind: ErrorKind,
    /// Bounded diagnostic message; implementations should omit credentials.
    pub message: String,
    /// Whether this attempt is known not to have committed.
    pub outcome: CommitOutcome,
}

impl WriteError {
    /// Creates a classified write failure.
    pub fn new(kind: ErrorKind, outcome: CommitOutcome, message: impl Into<String>) -> Self {
        Self {
            kind,
            outcome,
            message: message.into(),
        }
    }
    /// Whether retrying with the same immutable batch can be useful.
    pub fn is_retryable(&self) -> bool {
        matches!(self.kind, ErrorKind::Retryable | ErrorKind::Timeout)
    }
}
impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} ({:?}): {}", self.kind, self.outcome, self.message)
    }
}
impl std::error::Error for WriteError {}

/// Synchronous, writer-owned destination for complete immutable batches.
///
/// Implementations perform at most one network attempt in `write`; caller-owned
/// retries preserve the same `Arc<Batch>`. `deadline` is monotonic: implementations
/// check it before starting I/O or publishing state, and bound interruptible waits
/// by it. Local validation, allocation and destruction cannot be preempted, so
/// cleanup may delay the return beyond the deadline. Expiry must not start a new
/// visible commit, nor turn an already completed commit into `NotCommitted`.
/// Successful writes have reached the instance's fixed [`Sink::completion_boundary`].
pub trait Sink: Send + 'static {
    /// Fixed minimum success guarantee for this sink instance.
    fn completion_boundary(&self) -> CompletionBoundary;
    /// Commits a complete batch within the deadline, or reports a classified error.
    fn write(&mut self, batch: Arc<Batch>, deadline: Instant) -> Result<(), WriteError>;
    /// Completes sink-owned work up to the same boundary; never upgrades its guarantee.
    fn flush(&mut self, deadline: Instant) -> Result<(), WriteError>;
}

// A sparsely occupied BTreeMap can require an allocation with 11 key/value slots,
// parent links and 12 child edges for each surviving entry. Charge a full node per
// entry plus allocator headroom, avoiding reliance on occupancy/layout guarantees.
fn map_heap_bytes(map: &BTreeMap<String, String>) -> usize {
    const NODE_BYTES: usize = 11 * size_of::<(String, String)>() + 16 * size_of::<usize>() + 64;
    map.iter().fold(0, |total: usize, (key, value)| {
        total
            .saturating_add(NODE_BYTES)
            .saturating_add(string_heap_bytes(key))
            .saturating_add(string_heap_bytes(value))
    })
}

fn string_heap_bytes(value: &String) -> usize {
    if value.capacity() == 0 {
        0
    } else {
        // Include per-allocation bookkeeping and rounding headroom, not just
        // initialized UTF-8 bytes. The estimate is deliberately allocator-neutral.
        value.capacity().saturating_add(32)
    }
}

fn checked_text(
    field: &str,
    value: &str,
    maximum: usize,
    required: bool,
) -> Result<(), ValidationError> {
    if required && value.trim().is_empty() {
        return Err(ValidationError::new(
            field,
            "value must not be empty or whitespace",
        ));
    }
    if value.len() > maximum {
        return Err(ValidationError::new(
            field,
            "UTF-8 byte length exceeds limit",
        ));
    }
    if value.contains('\0') {
        return Err(ValidationError::new(
            field,
            "NUL characters are not allowed",
        ));
    }
    Ok(())
}

fn checked_map(
    field: &str,
    map: &BTreeMap<String, String>,
    maximum: usize,
    max_key: usize,
    max_value: usize,
) -> Result<(), ValidationError> {
    if map.len() > maximum {
        return Err(ValidationError::new(field, "entry count exceeds limit"));
    }
    for (key, value) in map {
        checked_text(field, key, max_key, true)?;
        checked_text(field, value, max_value, false)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
