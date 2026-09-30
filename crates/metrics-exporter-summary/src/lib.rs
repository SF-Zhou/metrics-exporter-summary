//! Bounded, thread-sharded `metrics` recorder with pluggable summary sinks.
//!
//! Start with [`Builder`], choose a [`Sink`], and keep the returned [`Control`] to
//! inspect delivery results and shut down. The [`metrics`] re-export is the facade
//! version implemented by this recorder; cache its metric handles after installing
//! the recorder with [`SummaryRecorder::install`].
//!
//! # Quick start
//!
//! Add the recorder and a sink to your application's dependencies:
//!
//! ```toml
//! [dependencies]
//! metrics-exporter-summary = "0.1.0-alpha"
//! metrics-summary-sink-memory = "0.1.0-alpha"
//! ```
//!
//! This example uses a local recorder and deterministic manual collection, so it
//! requires no process-global installation or external service:
//!
//! ```
//! use metrics_exporter_summary::{metrics, Builder, Config, Source};
//! use metrics_summary_sink_memory::{MemorySink, Retention};
//! use std::time::Duration;
//!
//! let (sink, reader) = MemorySink::new(Retention::default())?;
//! let (recorder, control) = Builder::new(Source {
//!     application: "api".into(),
//!     instance: "worker-1".into(),
//!     hostname: "node-1".into(),
//!     attributes: Default::default(),
//! })
//! .config(Config { collect_interval: None, ..Config::default() })
//! .build(sink)?;
//!
//! let (requests, latency, active) = metrics::with_local_recorder(&recorder, || {
//!     (
//!         metrics::counter!("requests", "tag" => "read"),
//!         metrics::histogram!("request.latency_ns", "tag" => "read"),
//!         metrics::gauge!("requests.active"),
//!     )
//! });
//! active.increment(1.0);
//! requests.increment(1);
//! latency.record(12_000_000.0); // Record nanoseconds directly.
//! active.decrement(1.0);
//!
//! let report = control.flush(Duration::from_secs(5))?;
//! assert!(report.is_success());
//! let snapshot = reader.get(report.target)?;
//! assert_eq!(snapshot.rows.len(), 3);
//!
//! // Stop all producers first, then inspect the final delivery report.
//! assert!(control.shutdown_default()?.is_success());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! For a service, [`Builder::for_service`] discovers the OS hostname and
//! [`SummaryRecorder::install`] installs the recorder globally. Keep [`Control`]
//! separately. The [HTTP server example] demonstrates cached handles, request
//! cancellation, periodic collection, and graceful shutdown.
//!
//! # Choose an output
//!
//! | Sink | Successful write means |
//! | --- | --- |
//! | [MemorySink] | A complete snapshot is visible to readers |
//! | [ClickHouseSink] | Both required table inserts were confirmed |
//! | [RemoteSink] | The collector accepted ownership or confirmed database writes, according to its ACK policy |
//!
//! Implement [`Sink`] for another destination. The recorder owns retries; every
//! attempt receives the same immutable batch and identity. Sinks run on a dedicated
//! blocking writer thread, independently of recording and collection.
//!
//! # Collection and labels
//!
//! Automatic collection defaults to ten seconds; [`Config::collect_interval`]
//! disables it when set to `None`. Explicit flushes close a new window and reset
//! the automatic timer. Histogram observations and Counter increments are drained
//! each collection, including when subsequent delivery fails. Counter `absolute`
//! contributes only increases over its retained total. Gauges retain their current
//! value; once initialized they are sampled every round.
//!
//! Counter window deltas and Gauge values must fit signed 64-bit storage. Gauge
//! inputs must be finite integers; invalid inputs and overflowing updates are
//! rejected and counted. Histograms retain exact count/min/max and approximate
//! p50/p90/p95/p99. Exported quantiles cannot be merged into a global percentile.
//! Latency examples use nanoseconds; describing a unit does not convert samples.
//!
//! Labels accept arbitrary nonempty keys without control characters, subject to
//! [`ValidationLimits`]. Source hostname and instance supply missing `host` and
//! `instance` values; explicit values override them. Other absent labels remain
//! absent. All effective labels participate in series identity, and Counter and
//! Gauge need distinct names for the same labels. Storage sinks may impose their
//! own column mapping requirements.
//!
//! [`Batch::timestamp`] is collection-finish Unix nanoseconds and
//! [`Batch::duration_ns`] is the monotonic span since the previous completion.
//! Shards close one at a time, so sample boundaries are approximate. Retries
//! preserve both fields.
//!
//! # Capacity and shutdown
//!
//! [`Config`] bounds registrations, histogram shards, descriptions, queued batches,
//! and retries. Its series count is a ceiling: the retained-batch byte budget also
//! limits registration. Rejected handles are no-ops; inspect
//! [`Control::diagnostics`] for rejected registrations and lost observations.
//! A full queue drops the newly collected complete batch. There is no durable log.
//!
//! Stop and join producers before [`Control::shutdown`]. Both flush and shutdown
//! block the caller; use a blocking task when calling them from an async runtime.
//! Always inspect [`FlushReport::is_success`]: `Ok(report)` can contain known loss.
//! A caller timeout does not cancel accepted writes. Dropping the last [`Control`]
//! starts nonblocking cleanup and cannot report final delivery. Database retries after
//! uncertain outcomes can create duplicates; no exactly-once storage guarantee is
//! made.
//!
//! [HTTP server example]: https://github.com/SF-Zhou/metrics-exporter-summary/blob/main/crates/metrics-exporter-summary/examples/server.rs
//! [MemorySink]: https://docs.rs/metrics-summary-sink-memory/latest/metrics_summary_sink_memory/struct.MemorySink.html
//! [ClickHouseSink]: https://docs.rs/metrics-summary-sink-clickhouse/latest/metrics_summary_sink_clickhouse/struct.ClickHouseSink.html
//! [RemoteSink]: https://docs.rs/metrics-summary-sink-remote/latest/metrics_summary_sink_remote/struct.RemoteSink.html

#![warn(missing_docs)]

mod clock;
mod config;
mod control;
mod diagnostics;
mod histogram;
mod registry;
mod sampler;
mod scalar;
mod writer;

pub use clock::{Clock, SystemClock};
pub use config::{BuildError, Config};
pub use control::{Control, ControlError, FlushReport, Lifecycle};
pub use diagnostics::{DiagnosticsSnapshot, LastWriteError};
pub use metrics_summary_core::{
    Batch, BatchId, CompletionBoundary, MetricKind, MetricValue, Row, Sink, Source,
    ValidationLimits,
};
pub use registry::{Builder, SummaryRecorder};

/// The metrics facade version implemented by this recorder.
pub use metrics;

pub(crate) fn unix_nanos() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_nanos().min(i64::MAX as u128) as i64,
        Err(e) => -(e.duration().as_nanos().min(i64::MAX as u128) as i64),
    }
}
