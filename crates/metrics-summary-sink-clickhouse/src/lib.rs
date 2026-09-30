//! Bounded ClickHouse HTTP storage for metric collection windows.
//!
//! Use [`ClickHouseSink`] with the recorder's `Builder`, or use
//! [`ClickHouseBatchWriter`] to insert a group of immutable [`Batch`] values.
//! The transport uses HTTP (normally port **8123**) or HTTPS; native protocol
//! port 9000 is not an HTTP endpoint.
//!
//! # Quick start
//!
//! Add `metrics-exporter-summary` and `metrics-summary-sink-clickhouse` to your
//! dependencies. Provision both tables using the repository's
//! [table DDL](https://github.com/SF-Zhou/metrics-exporter-summary/blob/main/deploy/clickhouse/schema.sql),
//! adjusting its database name to match your configuration. The writer reads
//! `system.columns` to check required column types before the first nonempty
//! write; it never creates or alters tables. The database user needs INSERT
//! access and permission to inspect the required metadata.
//!
//! ```no_run
//! use metrics_exporter_summary::{metrics, Builder};
//! use metrics_summary_sink_clickhouse::{ClickHouseConfig, ClickHouseSink};
//! use std::{error::Error, time::Duration};
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let sink = ClickHouseSink::new(ClickHouseConfig {
//!         endpoint: "http://127.0.0.1:8123".into(),
//!         database: "metrics".into(), // Must match your provisioned tables.
//!         username: std::env::var("METRICS_CLICKHOUSE_USER").ok(),
//!         password: std::env::var("METRICS_CLICKHOUSE_PASSWORD").ok(),
//!         ..ClickHouseConfig::default()
//!     })?;
//!     // The default recorder collects every 10 seconds on a background thread.
//!     // `host` is discovered from the OS; `instance` identifies this process.
//!     let (recorder, control) =
//!         Builder::for_service("api", format!("pid-{}", std::process::id()))?
//!             .build(sink)?;
//!     metrics::with_local_recorder(&recorder, || {
//!         metrics::counter!("api.requests", "tag" => "read").increment(1);
//!         metrics::gauge!("api.inflight").set(1.0); // Integer-valued gauge.
//!         metrics::histogram!("api.latency", "tag" => "read").record(750_000.0);
//!     });
//!     // Stop producing metrics, then collect and wait for the final write.
//!     let report = control.shutdown(Duration::from_secs(35))?;
//!     if !report.is_success() {
//!         return Err(format!("metrics delivery incomplete: {report:?}").into());
//!     }
//!     Ok(())
//! }
//! ```
//!
//! `with_local_recorder` scopes instrumentation to the calling thread. A server
//! can instead install the recorder globally with `recorder.install()?` before
//! starting its workers. Retain the control handle and inspect its shutdown
//! report to observe delivery failures. See the
//! [complete direct example](https://github.com/SF-Zhou/metrics-exporter-summary/blob/main/crates/metrics-summary-sink-clickhouse/examples/direct.rs)
//! for environment-based endpoint and private-CA configuration.
//!
//! # Stored values and labels
//!
//! Counter deltas and signed integer gauge snapshots share the `counters` table
//! with `val Int64`. Counter deltas must fit `i64`; the recorder rejects fractional
//! and out-of-range gauge updates. Gauges keep their current value across
//! collections. Counter increments and histogram samples are drained per window.
//! Metric names must distinguish counters from gauges because no instrument-kind
//! field is written.
//!
//! Histogram windows use `distributions`, with `count`, `mean`, `min`, `max`,
//! `p50`, `p90`, `p95`, and `p99` as `Float64`. Counts must be in `1..=2^53`.
//! Record latency in **nanoseconds**, as in the example; this sink performs no
//! unit conversion. Quantiles from different windows cannot be merged into an
//! overall percentile.
//!
//! Both tables store the metric name in `metricName` and the collection-end time
//! in `TIMESTAMP DateTime`. Subsecond fractions are truncated; the seconds must
//! fit `u32`. Source metadata, batch duration and identity, row unit, and internal
//! metric ID are not automatically added as stored columns.
//!
//! Labels are an end-to-end contract between instrumentation and the deployed
//! tables. Every supplied label is preserved as a flat column; there is no label
//! whitelist. Missing `host` and `instance` use the source hostname and instance,
//! and explicit instrument labels override either default. The effective host
//! must be nonempty. Other absent labels are omitted and use database defaults.
//!
//! Each label needs a `String` or `LowCardinality(String)` column on the table
//! receiving that row. Labels cannot overwrite that table's timestamp, metric
//! name, or numeric value columns. Column types are checked before either INSERT;
//! a label absent from the metadata cache triggers a fresh schema read. No label
//! configuration is needed when adding a column. Additional deployed columns are
//! allowed; table engines, sort keys, and TTL are left to the operator. See the
//! [deployment guide](https://github.com/SF-Zhou/metrics-exporter-summary/blob/main/deploy/clickhouse/README.md)
//! for the complete schema and label contract.
//!
//! # Completion, retries, and thread ownership
//!
//! Each call performs one attempt. A successful nonempty write confirms
//! ClickHouse acceptance under the server's durability settings. Async insertion,
//! when enabled, always waits with `wait_for_async_insert=1`. There is no database
//! retry identity or deduplication guarantee. Writes to the two tables are not
//! transactional: one table may commit before the other fails. Lost replies or
//! partial writes can return [`CommitOutcome::Unknown`]; retrying the original
//! immutable batches can insert duplicates. The caller owns retry policy; the
//! recorder supplies its own bounded retry policy when using [`ClickHouseSink`].
//!
//! Constructors perform local validation without initializing the HTTP runtime.
//! After the first schema check or nonempty write initializes the blocking HTTP
//! client, perform I/O and drop the writer on a blocking worker thread, outside
//! an async runtime's task context. Passing a newly constructed sink to the
//! recorder's `Builder` handles this ownership through its dedicated writer
//! thread. For manual async integration, keep the writer and its destruction
//! inside a dedicated thread or a blocking task.

#![warn(missing_docs)]

use metrics_summary_core::{
    effective_labels, Batch, CommitOutcome, CompletionBoundary, ErrorKind, MetricValue, Row, Sink,
    ValidationLimits, WriteError,
};
use reqwest::blocking::Client;
use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    io::{self, Read, Write},
    sync::Arc,
    time::{Duration, Instant},
};
use url::Url;
use uuid::Uuid;

/// The server release used by the real integration suite and deployment example.
pub const TESTED_SERVER_VERSION: &str = "26.8.6.5";
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// Distinct table identifiers within [`ClickHouseConfig::database`].
///
/// Each name must start with an ASCII letter or underscore, contain only ASCII
/// letters, digits, or underscores, and be at most 128 bytes long.
#[derive(Clone, Debug)]
pub struct TableNames {
    /// Table for counter deltas and gauge snapshots. Defaults to `"counters"`.
    pub counters: String,
    /// Table for histogram summaries. Defaults to `"distributions"`.
    pub distributions: String,
}

impl Default for TableNames {
    fn default() -> Self {
        Self {
            counters: "counters".into(),
            distributions: "distributions".into(),
        }
    }
}

/// Endpoint, credentials, schema contract, and resource bounds for HTTP insertion.
///
/// Source batches and encoded JSON have separate budgets. These are logical
/// payload limits, not an exact process-memory bound: encoding vector capacity
/// and temporary label maps require additional memory. All group and encoding
/// limits must be positive. HTTPS uses the standard trusted roots supplied by
/// rustls; redirects are disabled. Debug output redacts credentials and endpoint.
#[derive(Clone)]
pub struct ClickHouseConfig {
    /// Absolute HTTP(S) URL, defaulting to `"http://127.0.0.1:8123"`.
    /// URL credentials, query parameters, and fragments are forbidden.
    pub endpoint: String,
    /// Provisioned database name. Defaults to the database in the supplied
    /// [table DDL](https://github.com/SF-Zhou/metrics-exporter-summary/blob/main/deploy/clickhouse/schema.sql).
    /// The same identifier rules as [`TableNames`] apply.
    pub database: String,
    /// HTTP Basic authentication username. Defaults to `None` (no auth header).
    /// If provided, it must be nonempty and contain no colon, CR, or LF.
    pub username: Option<String>,
    /// HTTP Basic authentication password. Defaults to `None`.
    /// Requires a username and must not contain CR or LF.
    pub password: Option<String>,
    /// Additional PEM roots for private PKI, at most 1 MiB and 32 certificates.
    /// Defaults to `None`; a supplied bundle must be nonempty. Server name and
    /// certificate verification always remain enabled. See [`read_ca_bundle`].
    pub tls_ca_pem: Option<Vec<u8>>,
    /// Destination tables. Defaults to [`TableNames::default`].
    pub tables: TableNames,
    /// Connection timeout, defaulting to 2 seconds. Must be in `(0, 1 hour]`.
    pub connect_timeout: Duration,
    /// Per-request timeout, defaulting to 10 seconds. Must be in `(0, 1 hour]`.
    /// Each request also uses the remaining caller-supplied operation deadline,
    /// taking whichever timeout is shorter.
    pub request_timeout: Duration,
    /// Enables server-side asynchronous insertion. Defaults to `false`.
    /// Always paired with `wait_for_async_insert=1`; success still waits for
    /// server confirmation.
    pub async_insert: bool,
    /// Maximum batches in one write group. Defaults to 256.
    pub max_group_batches: usize,
    /// Maximum sum of [`Batch::estimated_bytes`] in one group, in bytes.
    /// Defaults to 32 MiB; HTTP encoding is charged separately.
    pub max_group_bytes: usize,
    /// Maximum total rows across all batches in a group. Defaults to 100,000.
    pub max_group_rows: usize,
    /// Maximum total JSONEachRow bytes across both INSERT bodies, including
    /// newlines. Defaults to 64 MiB; vector allocation may reserve extra capacity.
    pub max_encoded_bytes: usize,
    /// Per-batch model and label size bounds.
    /// Defaults to [`ValidationLimits::default`]. Provision matching string
    /// columns for every label used by rows sent to each table.
    pub validation_limits: ValidationLimits,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:8123".into(),
            database: "metrics_summary".into(),
            username: None,
            password: None,
            tls_ca_pem: None,
            tables: TableNames::default(),
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(10),
            async_insert: false,
            max_group_batches: 256,
            max_group_bytes: 32 * 1024 * 1024,
            max_group_rows: 100_000,
            max_encoded_bytes: 64 * 1024 * 1024,
            validation_limits: ValidationLimits::default(),
        }
    }
}

impl fmt::Debug for ClickHouseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseConfig")
            .field("endpoint", &"[configured]")
            .field("database", &self.database)
            .field("credentials", &"[redacted]")
            .field("custom_tls_roots", &self.tls_ca_pem.is_some())
            .field("tables", &self.tables)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("async_insert", &self.async_insert)
            .field("max_group_batches", &self.max_group_batches)
            .field("max_group_bytes", &self.max_group_bytes)
            .field("max_group_rows", &self.max_group_rows)
            .field("max_encoded_bytes", &self.max_encoded_bytes)
            .field("validation_limits", &self.validation_limits)
            .finish()
    }
}

/// Invalid local endpoint, credentials, schema names, TLS roots, or limits.
#[derive(Debug, thiserror::Error)]
#[error("invalid ClickHouse configuration: {0}")]
pub struct ConfigError(
    /// Configuration failure description, without credential values.
    pub &'static str,
);

impl ClickHouseConfig {
    /// Checks local configuration without connecting to ClickHouse.
    ///
    /// This validates the endpoint, identifiers, credentials, CA bundle, positive
    /// bounds, and timeouts. It does not check server reachability or deployed
    /// columns; use [`ClickHouseBatchWriter::verify_schema`] for that check.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validation_limits
            .validate()
            .map_err(|_| ConfigError("invalid validation limits"))?;
        if let Some(pem) = &self.tls_ca_pem {
            parse_ca_bundle(pem)?;
        }
        let endpoint = Url::parse(&self.endpoint)
            .map_err(|_| ConfigError("endpoint must be an absolute HTTP(S) URL"))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(ConfigError(
                "endpoint must use HTTP(S), without URL credentials, query or fragment",
            ));
        }
        for name in [
            &self.database,
            &self.tables.counters,
            &self.tables.distributions,
        ] {
            if !valid_identifier(name) {
                return Err(ConfigError(
                    "database and table names must be SQL identifiers of at most 128 ASCII bytes",
                ));
            }
        }
        if self.tables.counters == self.tables.distributions {
            return Err(ConfigError("instrument tables must be distinct"));
        }
        if self.password.is_some() && self.username.is_none() {
            return Err(ConfigError("password requires username"));
        }
        if self
            .username
            .as_ref()
            .is_some_and(|s| s.is_empty() || s.contains(':') || s.contains(['\r', '\n']))
            || self
                .password
                .as_ref()
                .is_some_and(|s| s.contains(['\r', '\n']))
        {
            return Err(ConfigError("invalid HTTP credentials"));
        }
        if self.connect_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.connect_timeout > Duration::from_secs(3600)
            || self.request_timeout > Duration::from_secs(3600)
        {
            return Err(ConfigError(
                "timeouts must be positive and at most one hour",
            ));
        }
        if self.max_group_batches == 0
            || self.max_group_bytes == 0
            || self.max_group_rows == 0
            || self.max_encoded_bytes == 0
        {
            return Err(ConfigError("group and encoding limits must be positive"));
        }
        Ok(())
    }
}

fn parse_ca_bundle(pem: &[u8]) -> Result<Vec<reqwest::Certificate>, ConfigError> {
    if pem.is_empty() || pem.len() > 1024 * 1024 {
        return Err(ConfigError(
            "TLS CA PEM bundle must contain between 1 byte and 1 MiB",
        ));
    }
    let certificates = reqwest::Certificate::from_pem_bundle(pem)
        .map_err(|_| ConfigError("invalid TLS CA PEM bundle"))?;
    if certificates.is_empty() || certificates.len() > 32 {
        return Err(ConfigError(
            "TLS CA bundle must contain between 1 and 32 certificates",
        ));
    }
    Ok(certificates)
}

/// Reads and validates a private-PKI CA bundle without unbounded file allocation.
///
/// Reads at most 1 MiB plus one byte to detect oversized input. Returns a PEM
/// bundle containing 1 to 32 certificates, suitable for
/// [`ClickHouseConfig::tls_ca_pem`]. File errors are returned unchanged; empty,
/// oversized, or invalid bundles return [`io::ErrorKind::InvalidData`].
pub fn read_ca_bundle(path: impl AsRef<std::path::Path>) -> io::Result<Vec<u8>> {
    let mut pem = Vec::new();
    std::fs::File::open(path)?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut pem)?;
    parse_ca_bundle(&pem).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(pem)
}

fn valid_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.as_bytes()
            .first()
            .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
        && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Synchronous, bounded writer for groups of immutable metric batches.
///
/// Both table bodies are validated and encoded before any INSERT. Each nonempty
/// table receives one HTTP INSERT per call; no internal retries or deduplication
/// occur. See the [crate documentation](crate) for partial-commit semantics and
/// blocking-thread ownership requirements.
pub struct ClickHouseBatchWriter {
    config: ClickHouseConfig,
    endpoint: Url,
    client: Option<Client>,
    schema_checked: bool,
    schema_columns: BTreeMap<(String, String), String>,
}

impl fmt::Debug for ClickHouseBatchWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseBatchWriter")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ClickHouseBatchWriter {
    /// Validates configuration and creates an uninitialized writer.
    ///
    /// Performs no network I/O and does not create the blocking HTTP client.
    /// Construction is safe before moving the writer onto its owning worker.
    pub fn new(config: ClickHouseConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        let endpoint = Url::parse(&config.endpoint).map_err(|_| ConfigError("invalid endpoint"))?;
        Ok(Self {
            config,
            endpoint,
            client: None,
            schema_checked: false,
            schema_columns: BTreeMap::new(),
        })
    }

    /// Returns the immutable configuration used for all validation and requests.
    pub fn config(&self) -> &ClickHouseConfig {
        &self.config
    }

    /// Prevalidates and encodes the entire group before its first INSERT. A failed
    /// call may have committed either table. Stored rows have no retry identity;
    /// retrying an uncertain result can insert duplicates.
    ///
    /// Uses the supplied absolute monotonic deadline for encoding, schema checks,
    /// and HTTP requests. Empty groups or groups containing no rows need no I/O.
    /// Returns [`CommitOutcome::NotCommitted`] for local pre-insertion rejection;
    /// failures after a possible or confirmed partial commit carry
    /// [`CommitOutcome::Unknown`]. Identical repeated batches in the group are
    /// written repeatedly; conflicting content for the same batch ID is rejected.
    pub fn write_group(
        &mut self,
        batches: &[Arc<Batch>],
        deadline: Instant,
    ) -> Result<(), WriteError> {
        check_deadline(deadline, CommitOutcome::NotCommitted)?;
        if batches.len() > self.config.max_group_batches {
            return Err(invalid("too many batches in ClickHouse group"));
        }
        let mut bytes = 0usize;
        let mut rows = 0usize;
        let mut identities = BTreeMap::new();
        for batch in batches {
            check_deadline(deadline, CommitOutcome::NotCommitted)?;
            if let Some(previous) = identities.insert(batch.id, batch.as_ref()) {
                if previous != batch.as_ref() {
                    return Err(invalid(
                        "conflicting content for the same ClickHouse source BatchId",
                    ));
                }
            }
            batch
                .validate(&self.config.validation_limits)
                .map_err(|_| invalid("batch validation failed before ClickHouse insertion"))?;
            bytes = bytes
                .checked_add(batch.estimated_bytes())
                .ok_or_else(|| invalid("group byte count overflow"))?;
            rows = rows
                .checked_add(batch.rows.len())
                .ok_or_else(|| invalid("group row count overflow"))?;
            if bytes > self.config.max_group_bytes || rows > self.config.max_group_rows {
                return Err(invalid("ClickHouse group exceeds configured limits"));
            }
            timestamp(batch.timestamp)?;
        }
        let mut bodies: [Vec<u8>; 2] = std::array::from_fn(|_| Vec::new());
        let mut required_labels: [BTreeSet<&str>; 2] = std::array::from_fn(|_| BTreeSet::new());
        let mut remaining = self.config.max_encoded_bytes;
        for batch in batches {
            let timestamp = timestamp(batch.timestamp)?;
            for row in &batch.rows {
                check_deadline(deadline, CommitOutcome::NotCommitted)?;
                let index = match row.value {
                    MetricValue::CounterDelta { .. } | MetricValue::GaugeSnapshot { .. } => 0,
                    MetricValue::HistogramSummary { .. } => 1,
                };
                required_labels[index].extend(["host", "instance"]);
                required_labels[index].extend(row.labels.keys().map(String::as_str));
                let mut output = BoundedOutput {
                    bytes: &mut bodies[index],
                    remaining: &mut remaining,
                };
                serde_json::to_writer(
                    &mut output,
                    &EncodedRow::new(batch, row, timestamp, &self.config.validation_limits)?,
                )
                .map_err(|_| {
                    invalid("ClickHouse encoded group exceeds limit or is not serializable")
                })?;
                output
                    .write_all(b"\n")
                    .map_err(|_| invalid("ClickHouse encoded group exceeds limit"))?;
            }
        }
        if rows == 0 {
            return check_deadline(deadline, CommitOutcome::NotCommitted).map(|_| ());
        }
        self.verify_label_columns(&required_labels, deadline)?;
        let names = [
            &self.config.tables.counters,
            &self.config.tables.distributions,
        ];
        let mut any_committed = false;
        for (name, body) in names.into_iter().zip(bodies) {
            if body.is_empty() {
                continue;
            }
            if let Err(mut error) = self.insert(name, body, deadline) {
                if any_committed {
                    error.outcome = CommitOutcome::Unknown;
                }
                return Err(error);
            }
            any_committed = true;
        }
        Ok(())
    }

    /// Checks the deployed schema before publishing anything. Automatically called
    /// before the first nonempty write and when a label is absent from cached
    /// metadata; call again after other intentional DDL changes.
    ///
    /// Reads `system.columns` for both configured tables, checking metric name,
    /// `host`/`instance` string types, `TIMESTAMP DateTime`, scalar `Int64`, and
    /// distribution `Float64` columns. Extra columns and operator-controlled table settings are
    /// allowed. This read-only operation uses the supplied absolute monotonic
    /// deadline; its failures always have [`CommitOutcome::NotCommitted`].
    pub fn verify_schema(&mut self, deadline: Instant) -> Result<(), WriteError> {
        self.schema_checked = false;
        check_deadline(deadline, CommitOutcome::NotCommitted)?;
        if self.client.is_none() {
            let mut builder = Client::builder()
                .tls_backend_rustls()
                .connect_timeout(self.config.connect_timeout)
                .timeout(self.config.request_timeout)
                .redirect(reqwest::redirect::Policy::none())
                .pool_max_idle_per_host(1);
            if let Some(pem) = &self.config.tls_ca_pem {
                builder = builder.tls_certs_merge(
                    parse_ca_bundle(pem).map_err(|_| invalid("invalid TLS CA bundle"))?,
                );
            }
            self.client = Some(
                builder
                    .build()
                    .map_err(|_| invalid("cannot initialize ClickHouse HTTP client"))?,
            );
        }
        let names = [
            &self.config.tables.counters,
            &self.config.tables.distributions,
        ];
        let query = format!("SELECT table, groupArray((name, type)) AS columns FROM system.columns WHERE database = '{}' AND table IN ('{}', '{}') GROUP BY table FORMAT JSONEachRow", self.config.database, names[0], names[1]);
        let body = self
            .execute(&query, Vec::new(), deadline, false)
            .map_err(|mut e| {
                e.outcome = CommitOutcome::NotCommitted;
                e
            })?;
        let mut columns: BTreeMap<(String, String), String> = BTreeMap::new();
        for line in body.split(|b| *b == b'\n').filter(|line| !line.is_empty()) {
            let table: SchemaTable = serde_json::from_slice(line)
                .map_err(|_| invalid("invalid ClickHouse metadata response"))?;
            for (name, column_type) in table.columns {
                if columns
                    .insert((table.table.clone(), name), column_type)
                    .is_some()
                {
                    return Err(invalid("duplicate ClickHouse metadata column"));
                }
            }
        }
        for (index, table) in names.into_iter().enumerate() {
            let time_type = columns
                .get(&(table.clone(), "TIMESTAMP".into()))
                .map(String::as_str);
            if !time_type.is_some_and(|t| {
                t == "DateTime" || (t.starts_with("DateTime('") && t.ends_with("')"))
            }) {
                return Err(invalid("ClickHouse TIMESTAMP column must be DateTime"));
            }
            for name in ["metricName", "host", "instance"] {
                if !matches!(
                    columns
                        .get(&(table.clone(), name.to_string()))
                        .map(String::as_str),
                    Some("String" | "LowCardinality(String)")
                ) {
                    return Err(invalid(
                        "ClickHouse metricName or label column is missing or has the wrong type",
                    ));
                }
            }
            for &(name, column_type) in VALUE_COLUMNS[index] {
                if columns
                    .get(&(table.clone(), name.to_string()))
                    .map(String::as_str)
                    != Some(column_type)
                {
                    return Err(invalid(
                        "ClickHouse value column is missing or has the wrong type",
                    ));
                }
            }
        }
        // Additional deployed columns, engines, sorting and partition keys belong
        // to the operator. They are deliberately not constrained or modified.
        self.schema_columns = columns;
        self.schema_checked = true;
        Ok(())
    }

    fn verify_label_columns(
        &mut self,
        required: &[BTreeSet<&str>; 2],
        deadline: Instant,
    ) -> Result<(), WriteError> {
        let names = [
            &self.config.tables.counters,
            &self.config.tables.distributions,
        ];
        let missing_cached_column = names.iter().zip(required).any(|(table, labels)| {
            labels.iter().any(|label| {
                !self
                    .schema_columns
                    .contains_key(&((**table).clone(), (*label).to_string()))
            })
        });
        if !self.schema_checked || missing_cached_column {
            self.verify_schema(deadline)?;
        }
        let names = [
            &self.config.tables.counters,
            &self.config.tables.distributions,
        ];
        for (table, labels) in names.into_iter().zip(required) {
            for label in labels {
                check_deadline(deadline, CommitOutcome::NotCommitted)?;
                if !matches!(
                    self.schema_columns
                        .get(&(table.clone(), (*label).to_string()))
                        .map(String::as_str),
                    Some("String" | "LowCardinality(String)")
                ) {
                    return Err(invalid(
                        "ClickHouse label column is missing or has the wrong type",
                    ));
                }
            }
        }
        Ok(())
    }

    fn insert(&self, table: &str, body: Vec<u8>, deadline: Instant) -> Result<(), WriteError> {
        let query = format!(
            "INSERT INTO `{}`.`{}` FORMAT JSONEachRow",
            self.config.database, table
        );
        self.execute(&query, body, deadline, true).map(|_| ())
    }

    fn execute(
        &self,
        query: &str,
        body: Vec<u8>,
        deadline: Instant,
        expect_empty: bool,
    ) -> Result<Vec<u8>, WriteError> {
        let remaining =
            check_deadline(deadline, CommitOutcome::NotCommitted)?.min(self.config.request_timeout);
        let query_id = Uuid::new_v4().to_string();
        let mut request = self
            .client
            .as_ref()
            .expect("client initialized before insertion")
            .post(self.endpoint.clone())
            .query(&[
                ("query", query),
                ("query_id", query_id.as_str()),
                ("wait_end_of_query", "1"),
                ("send_progress_in_http_headers", "0"),
                (
                    "async_insert",
                    if self.config.async_insert { "1" } else { "0" },
                ),
                ("wait_for_async_insert", "1"),
                ("input_format_skip_unknown_fields", "0"),
                ("input_format_defaults_for_omitted_fields", "1"),
                ("input_format_allow_errors_num", "0"),
                ("input_format_allow_errors_ratio", "0"),
                ("date_time_input_format", "basic"),
            ])
            .timeout(remaining)
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(body);
        if let Some(username) = &self.config.username {
            request = request.basic_auth(username, self.config.password.as_ref());
        }
        let response = request.send().map_err(|e| {
            WriteError::new(
                if e.is_timeout() {
                    ErrorKind::Timeout
                } else {
                    ErrorKind::Retryable
                },
                if e.is_connect() {
                    CommitOutcome::NotCommitted
                } else {
                    CommitOutcome::Unknown
                },
                "ClickHouse HTTP transport failed",
            )
        })?;
        let status = response.status();
        let matching_id = response
            .headers()
            .get("x-clickhouse-query-id")
            .and_then(|v| v.to_str().ok())
            == Some(query_id.as_str());
        let exception = response
            .headers()
            .get("x-clickhouse-exception-code")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok());
        let mut response_body = Vec::new();
        response
            .take(MAX_RESPONSE_BYTES as u64 + 1)
            .read_to_end(&mut response_body)
            .map_err(|error| {
                WriteError::new(
                    if error.kind() == io::ErrorKind::TimedOut
                        || error
                            .get_ref()
                            .and_then(|inner| inner.downcast_ref::<reqwest::Error>())
                            .is_some_and(reqwest::Error::is_timeout)
                    {
                        ErrorKind::Timeout
                    } else {
                        ErrorKind::Retryable
                    },
                    CommitOutcome::Unknown,
                    "ClickHouse response body was interrupted",
                )
            })?;
        if response_body.len() > MAX_RESPONSE_BYTES {
            return Err(WriteError::new(
                ErrorKind::Retryable,
                CommitOutcome::Unknown,
                "ClickHouse response exceeds bounded size",
            ));
        }
        if status == reqwest::StatusCode::OK
            && exception.is_none_or(|n| n == 0)
            && matching_id
            && (!expect_empty || response_body.iter().all(u8::is_ascii_whitespace))
        {
            return Ok(response_body);
        }
        let exception = exception.or_else(|| parse_exception_code(&response_body));
        let (kind, outcome) = classify_failure(status.as_u16(), exception);
        // Never include server error bodies or request URLs: they may echo credentials or data.
        Err(WriteError::new(
            kind,
            outcome,
            format!(
                "ClickHouse insertion not confirmed (HTTP {}, exception {:?})",
                status.as_u16(),
                exception
            ),
        ))
    }
}

const VALUE_COLUMNS: [&[(&str, &str)]; 2] = [
    &[("val", "Int64")],
    &[
        ("count", "Float64"),
        ("mean", "Float64"),
        ("min", "Float64"),
        ("max", "Float64"),
        ("p50", "Float64"),
        ("p90", "Float64"),
        ("p95", "Float64"),
        ("p99", "Float64"),
    ],
];
#[derive(Deserialize)]
struct SchemaTable {
    table: String,
    columns: Vec<(String, String)>,
}

fn parse_exception_code(body: &[u8]) -> Option<u32> {
    let s = std::str::from_utf8(body).ok()?.trim_start();
    s.strip_prefix("Code: ")?.split('.').next()?.parse().ok()
}

fn classify_failure(status: u16, code: Option<u32>) -> (ErrorKind, CommitOutcome) {
    // Syntax, unknown objects/settings, type/column mismatch, and authorization
    // errors are permanent. Other database errors may have executed a prefix.
    if matches!(
        code,
        Some(16 | 20 | 27 | 32 | 36 | 41 | 47 | 53 | 60 | 62 | 70 | 81 | 117 | 194 | 497 | 516)
    ) || matches!(status, 400 | 401 | 403 | 404 | 405 | 413 | 415 | 422)
    {
        (ErrorKind::Permanent, CommitOutcome::Unknown)
    } else if status == 408 || status == 504 || code == Some(159) {
        (ErrorKind::Timeout, CommitOutcome::Unknown)
    } else if (300..400).contains(&status) {
        (ErrorKind::Permanent, CommitOutcome::Unknown)
    } else {
        (ErrorKind::Retryable, CommitOutcome::Unknown)
    }
}

fn invalid(message: &'static str) -> WriteError {
    WriteError::new(ErrorKind::Permanent, CommitOutcome::NotCommitted, message)
}

/// Exact size of the JSONEachRow data for one batch, without allocating its
/// encoded body. Collectors can charge this additional bound before acknowledging
/// ownership, and sum it when assembling groups. Call [`Batch::validate`] first.
/// Rejects timestamps outside DateTime, scalar values not representable as Int64,
/// and labels that collide with the target table's metric columns. Includes
/// supplied labels, host/instance defaults, JSON escaping, and one newline per row.
/// This counts payload bytes rather than allocation capacity and performs no I/O;
/// actual database label columns are checked only by the writer.
pub fn encoded_batch_bytes(batch: &Batch, limits: &ValidationLimits) -> Result<usize, WriteError> {
    count_encoded_batch_bytes(batch, limits, usize::MAX, None)
}

/// Exact encoding size, stopping before exceeding the byte budget or deadline.
///
/// `max_bytes` bounds total JSONEachRow payload bytes including newlines;
/// `deadline` is an absolute monotonic deadline. No storage I/O occurs. Call
/// [`Batch::validate`] first. A byte-budget failure is permanent, and an expired
/// deadline is a timeout; both report [`CommitOutcome::NotCommitted`].
pub fn encoded_batch_bytes_with_limits(
    batch: &Batch,
    limits: &ValidationLimits,
    max_bytes: usize,
    deadline: Instant,
) -> Result<usize, WriteError> {
    count_encoded_batch_bytes(batch, limits, max_bytes, Some(deadline))
}

fn count_encoded_batch_bytes(
    batch: &Batch,
    limits: &ValidationLimits,
    max_bytes: usize,
    deadline: Option<Instant>,
) -> Result<usize, WriteError> {
    let check_time = || {
        deadline.map_or(Ok(()), |d| {
            check_deadline(d, CommitOutcome::NotCommitted).map(|_| ())
        })
    };
    check_time()?;
    limits
        .validate()
        .map_err(|_| invalid("invalid label configuration"))?;
    timestamp(batch.timestamp)?;
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|n| *n <= self.limit)
                .ok_or_else(|| io::Error::other("encoding budget exhausted"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    if batch.rows.is_empty() {
        return Ok(0);
    }
    let timestamp = timestamp(batch.timestamp)?;
    let mut counter = Counter {
        bytes: 0,
        limit: max_bytes,
    };
    for row in &batch.rows {
        check_time()?;
        let encoded = EncodedRow::new(batch, row, timestamp, limits)?;
        serde_json::to_writer(&mut counter, &encoded)
            .map_err(|_| invalid("ClickHouse encoded batch exceeds limit"))?;
        counter
            .write_all(b"\n")
            .map_err(|_| invalid("ClickHouse encoded batch exceeds limit"))?;
    }
    check_time()?;
    Ok(counter.bytes)
}
fn check_deadline(deadline: Instant, outcome: CommitOutcome) -> Result<Duration, WriteError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| {
            WriteError::new(
                ErrorKind::Timeout,
                outcome,
                "ClickHouse write deadline elapsed",
            )
        })
}

struct BoundedOutput<'a> {
    bytes: &'a mut Vec<u8>,
    remaining: &'a mut usize,
}
impl Write for BoundedOutput<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() > *self.remaining {
            return Err(io::Error::other("encoding budget exhausted"));
        }
        self.bytes.extend_from_slice(buf);
        *self.remaining -= buf.len();
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn timestamp(nanos: i64) -> Result<u32, WriteError> {
    u32::try_from(nanos.div_euclid(1_000_000_000))
        .map_err(|_| invalid("timestamp is outside ClickHouse DateTime range"))
}

fn scalar_value(value: &MetricValue) -> Result<Option<i64>, WriteError> {
    match *value {
        MetricValue::CounterDelta { delta_value } => i64::try_from(delta_value)
            .map(Some)
            .map_err(|_| invalid("counter value is outside Int64 range")),
        MetricValue::GaugeSnapshot { current_value } => Ok(Some(current_value)),
        MetricValue::HistogramSummary { .. } => Ok(None),
    }
}

struct EncodedRow<'a> {
    row: &'a Row,
    timestamp: u32,
    labels: BTreeMap<String, String>,
    scalar: Option<i64>,
}

impl<'a> EncodedRow<'a> {
    fn new(
        batch: &Batch,
        row: &'a Row,
        timestamp: u32,
        limits: &ValidationLimits,
    ) -> Result<Self, WriteError> {
        let labels = effective_labels(&batch.source, &row.labels, limits)
            .map_err(|_| invalid("invalid ClickHouse row labels"))?;
        let scalar = scalar_value(&row.value)?;
        let value_columns = VALUE_COLUMNS[usize::from(scalar.is_none())];
        if labels.keys().any(|label| {
            matches!(label.as_str(), "TIMESTAMP" | "metricName")
                || value_columns.iter().any(|(name, _)| label == name)
        }) {
            return Err(invalid(
                "ClickHouse label conflicts with a stored metric column",
            ));
        }
        if let MetricValue::HistogramSummary { count, sum, .. } = row.value {
            if count == 0 || count > (1u64 << 53) || !(sum / count as f64).is_finite() {
                return Err(invalid(
                    "histogram count or mean cannot be represented as Float64",
                ));
            }
        }
        Ok(Self {
            row,
            timestamp,
            labels,
            scalar,
        })
    }
}

impl Serialize for EncodedRow<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let value_fields = if self.scalar.is_some() { 1 } else { 8 };
        let mut map = serializer.serialize_map(Some(2 + self.labels.len() + value_fields))?;
        map.serialize_entry("TIMESTAMP", &self.timestamp)?;
        map.serialize_entry("metricName", &self.row.name)?;
        for (name, value) in &self.labels {
            map.serialize_entry(name, value)?;
        }
        if let Some(value) = self.scalar {
            map.serialize_entry("val", &value)?;
        } else if let MetricValue::HistogramSummary {
            count,
            sum,
            min,
            max,
            p50,
            p90,
            p95,
            p99,
        } = self.row.value
        {
            map.serialize_entry("count", &(count as f64))?;
            map.serialize_entry("mean", &(sum / count as f64))?;
            map.serialize_entry("min", &min)?;
            map.serialize_entry("max", &max)?;
            map.serialize_entry("p50", &p50)?;
            map.serialize_entry("p90", &p90)?;
            map.serialize_entry("p95", &p95)?;
            map.serialize_entry("p99", &p99)?;
        }
        map.end()
    }
}

/// Direct recorder sink that writes one source batch through a group writer.
///
/// Reports [`CompletionBoundary::StorageConfirmed`]. Its [`Sink::write`] performs
/// one attempt; the recorder owns retries. [`Sink::flush`] checks the deadline
/// without sending an additional request because writes already wait for server
/// confirmation. See the [crate example](crate) for recorder setup.
#[derive(Debug)]
pub struct ClickHouseSink {
    writer: ClickHouseBatchWriter,
}
impl ClickHouseSink {
    /// Validates local configuration and creates a sink without network I/O.
    /// The HTTP client and schema check are deferred until the first nonempty write.
    pub fn new(config: ClickHouseConfig) -> Result<Self, ConfigError> {
        Ok(Self {
            writer: ClickHouseBatchWriter::new(config)?,
        })
    }
    /// Takes ownership of an existing writer, preserving its checked schema and
    /// HTTP client. If initialized, keep its use and destruction on a blocking
    /// worker thread as described in the [crate documentation](crate).
    pub fn from_writer(writer: ClickHouseBatchWriter) -> Self {
        Self { writer }
    }
}
impl Sink for ClickHouseSink {
    fn completion_boundary(&self) -> CompletionBoundary {
        CompletionBoundary::StorageConfirmed
    }
    fn write(&mut self, batch: Arc<Batch>, deadline: Instant) -> Result<(), WriteError> {
        self.writer.write_group(&[batch], deadline)
    }
    fn flush(&mut self, deadline: Instant) -> Result<(), WriteError> {
        check_deadline(deadline, CommitOutcome::NotCommitted).map(|_| ())
    }
}

#[cfg(test)]
mod tests;
