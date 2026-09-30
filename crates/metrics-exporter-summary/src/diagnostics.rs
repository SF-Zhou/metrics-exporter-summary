use std::sync::atomic::{AtomicU64, Ordering};

/// Last observed sink failure, retained independently of metrics and logs.
/// Messages are bounded to 1024 Unicode scalar values; built-in sinks redact
/// credentials and do not include server response bodies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastWriteError {
    /// Wall-clock time of the failure in signed Unix nanoseconds.
    pub timestamp_unix_ns: i64,
    /// Classified sink error, retained even if a later retry succeeds.
    pub error: metrics_summary_core::WriteError,
}

/// Cumulative counters, current resource use, and latest operation timings.
///
/// Rejected observations were never accepted; dropped observations were accepted
/// and subsequently lost locally. Fields are read independently, so concurrent
/// updates can occur between field reads.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DiagnosticsSnapshot {
    /// Histogram observations admitted to shard state over the recorder lifetime.
    pub accepted_histogram_samples: u64,
    /// Nonfinite histogram inputs or nonfinite/fractional Gauge inputs rejected.
    pub invalid_samples: u64,
    /// Updates or histogram windows rejected after numeric range/validity checks.
    pub arithmetic_overflows: u64,
    /// Metric registrations rejected by schema or resource limits.
    pub registrations_rejected: u64,
    /// Description updates rejected by text validation or metadata limits.
    pub descriptions_rejected: u64,
    /// Conflicting units ignored while retaining the first valid unit for a name/kind.
    pub unit_conflicts: u64,
    /// Histogram record attempts rejected because no shard capacity was available.
    pub shards_rejected: u64,
    /// Registration, description, or observation attempts after admission closed.
    pub closing_rejections: u64,
    /// Histogram observations rejected when thread-local shard state was unavailable.
    pub tls_rejections: u64,
    /// Completed collection rounds, including empty rounds and later-dropped batches.
    pub collected_batches: u64,
    /// Batches successfully written to the configured sink completion boundary.
    pub written_batches: u64,
    /// Collected batches discarded locally, including terminal delivery failures.
    pub dropped_batches: u64,
    /// Metric rows lost during collection or discarded with complete batches.
    pub dropped_rows: u64,
    /// Accepted histogram observations lost during collection or delivery.
    pub dropped_histogram_samples: u64,
    /// Failed sink write attempts, including failures later recovered by retry.
    pub write_failures: u64,
    /// Scheduled retries after a failure; expiry can prevent the next sink attempt.
    pub retries: u64,
    /// Caught sampler, writer, or sink panics.
    pub worker_panics: u64,
    /// Collections whose execution exceeded the configured automatic interval.
    pub collection_overruns: u64,
    /// Current registered series, retained until recorder shutdown.
    pub registered_series: u64,
    /// Current allocated histogram shards, including exited producers awaiting drain.
    pub active_shards: u64,
    /// Batches queued or currently being written/retried.
    pub queued_batches: u64,
    /// Estimated batch bytes queued or currently being written/retried.
    pub queued_bytes: u64,
    /// Execution time of the most recent collection, in nanoseconds.
    pub last_collection_ns: u64,
    /// Execution time of the most recent batch delivery, including retry work, in nanoseconds.
    pub last_write_ns: u64,
    /// Unix nanoseconds, or zero before the first successful write.
    pub last_success_unix_ns: u64,
}

macro_rules! diagnostics {
    ($($field:ident),* $(,)?) => {
        #[derive(Default)]
        pub(crate) struct Diagnostics { $(pub $field: AtomicU64,)* }
        impl Diagnostics {
            pub fn snapshot(&self) -> DiagnosticsSnapshot {
                DiagnosticsSnapshot { $($field: self.$field.load(Ordering::Relaxed),)* }
            }
        }
    };
}
diagnostics!(
    accepted_histogram_samples,
    invalid_samples,
    arithmetic_overflows,
    registrations_rejected,
    descriptions_rejected,
    unit_conflicts,
    shards_rejected,
    closing_rejections,
    tls_rejections,
    collected_batches,
    written_batches,
    dropped_batches,
    dropped_rows,
    dropped_histogram_samples,
    write_failures,
    retries,
    worker_panics,
    collection_overruns,
    registered_series,
    active_shards,
    queued_batches,
    queued_bytes,
    last_collection_ns,
    last_write_ns,
    last_success_unix_ns,
);

pub(crate) fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
        Some(n.saturating_add(value))
    });
}

pub(crate) fn duration_ns(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}
