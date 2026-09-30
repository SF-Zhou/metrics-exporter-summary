//! Bounded, atomically published history of complete metric collection rounds.
//!
//! Reader-held `Arc`s can outlive eviction; their memory is owned by the reader and
//! is outside the ring's budget. Dropping the writer preserves the reader's history.
//! Publication and retention changes share one deadline check and atomic commit.
//! Evicted batches are reclaimed afterwards, outside the history lock. Their Rust
//! destructors cannot be interrupted, so reclamation may delay the return past the
//! deadline without changing an already successful publication into a failed write.
//!
//! # Use with the recorder
//!
//! [`MemorySink::new`] returns a writer and a cloneable [`SnapshotReader`]. Pass
//! the writer to the recorder's [`Builder::build`][recorder-build], then use its
//! flush report's batch ID with [`SnapshotReader::get`] to read that exact
//! publication. See the [recorder quick start] for a runnable integration example.
//!
//! [`SnapshotReader::latest`] only reads; it never triggers collection.
//! [`SnapshotReader::after`] returns a bounded page and reports a retention gap
//! when older data may have been evicted. Neither an unavailable snapshot nor a
//! gap should be treated as a zero-valued observation. See [`ReadError`] and
//! [`MissingReason`] for unavailable IDs, and [`MemoryDiagnostics`] for retention
//! and duplicate-write counters.
//!
//! Each sink binds to its first accepted source session and metadata. Repeating
//! a retained batch with identical content succeeds without another publication;
//! conflicting content or an unverifiable evicted ID is rejected. Retention never
//! combines separate windows: Counter/Histogram data stays per batch, and Gauge
//! rows remain individual samples. Caller-held snapshots can outlive the ring.
//!
//! Use [`MemorySink::with_validation_limits`] when customizing label resource
//! limits or the batch budget. Arbitrary label keys and source defaults follow the [core model].
//! Keep validation settings aligned with the recorder.
//!
//! # Publish a manually constructed batch
//!
//! ```
//! use metrics_summary_core::{Batch, BatchId, Sink, Source, MODEL_VERSION};
//! use metrics_summary_sink_memory::{MemorySink, Retention};
//! use std::{sync::Arc, time::{Duration, Instant}};
//! use uuid::Uuid;
//!
//! let (mut sink, reader) = MemorySink::new(Retention::default())?;
//! let batch = Arc::new(Batch {
//!     model_version: MODEL_VERSION,
//!     id: BatchId { source_session_id: Uuid::new_v4(), sequence: 1 },
//!     source: Source::new("worker", "worker-1")?,
//!     timestamp: 2,
//!     duration_ns: 1,
//!     rows: Vec::new(), // An empty collection is still a complete snapshot.
//! });
//! sink.write(batch.clone(), Instant::now() + Duration::from_secs(1))?;
//! assert_eq!(reader.get(batch.id)?.id, batch.id);
//! drop(sink);
//! assert!(reader.diagnostics().writer_closed);
//! assert_eq!(reader.after(None, 10)?.snapshots.len(), 1);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! [recorder-build]: https://docs.rs/metrics-exporter-summary/latest/metrics_exporter_summary/struct.Builder.html#method.build
//! [recorder quick start]: https://docs.rs/metrics-exporter-summary
//! [core model]: metrics_summary_core

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use metrics_summary_core::{
    Batch, BatchId, CommitOutcome, CompletionBoundary, ErrorKind, Sink, Source, ValidationLimits,
    WriteError,
};
use parking_lot::Mutex;
use std::{collections::VecDeque, fmt, sync::Arc, time::Instant};

/// Simultaneous limits for complete snapshots held by the internal ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    /// Maximum number of retained complete snapshots; must be nonzero.
    pub max_snapshots: usize,
    /// Maximum sum of [`Batch::estimated_bytes`] for retained snapshots.
    pub max_retained_bytes: usize,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            max_snapshots: 60,
            max_retained_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Invalid memory-sink configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    /// Human-readable invalid configuration description.
    pub message: String,
}
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for ConfigError {}

/// Atomic memory publisher. One sink binds to the first successfully published session.
pub struct MemorySink {
    shared: Arc<Shared>,
    validation: ValidationLimits,
}

/// Cloneable read-only access to successful snapshots, including after writer shutdown.
#[derive(Clone)]
pub struct SnapshotReader {
    shared: Arc<Shared>,
}

struct Shared {
    retention: Retention,
    state: Mutex<State>,
}

#[derive(Clone)]
struct RetainedSnapshot {
    batch: Arc<Batch>,
    bytes: usize,
}

#[derive(Default)]
struct State {
    snapshots: VecDeque<RetainedSnapshot>,
    source: Option<(BatchId, Source)>,
    retained_bytes: usize,
    max_evicted_sequence: u64,
    published_snapshots: u64,
    duplicate_writes: u64,
    evicted_snapshots: u64,
    evicted_bytes: u64,
    retention_gap_queries: u64,
    writer_closed: bool,
}

/// A replacement prepared without mutating visible history. Keeping the old ring
/// intact until the final deadline check makes failed planning atomic as well.
struct Publication {
    snapshots: VecDeque<RetainedSnapshot>,
    id: BatchId,
    first_source: Option<Source>,
    retained_bytes: usize,
    max_evicted_sequence: u64,
    evicted_snapshots: u64,
    evicted_bytes: u64,
}

impl State {
    fn prepare_publication(
        &self,
        batch: &Arc<Batch>,
        bytes: usize,
        retention: Retention,
        deadline: Instant,
    ) -> Result<Publication, WriteError> {
        let mut retained_bytes = self.retained_bytes;
        let mut evicted_count = 0;
        let mut evicted_bytes = self.evicted_bytes;
        let mut max_evicted_sequence = self.max_evicted_sequence;
        // The caller already checked that this batch fits by itself. Use the
        // cached sizes instead of scanning every row of every evicted batch.
        for snapshot in &self.snapshots {
            check_deadline(deadline)?;
            if self.snapshots.len() - evicted_count < retention.max_snapshots
                && retained_bytes <= retention.max_retained_bytes - bytes
            {
                break;
            }
            retained_bytes -= snapshot.bytes;
            evicted_count += 1;
            evicted_bytes =
                evicted_bytes.saturating_add(u64::try_from(snapshot.bytes).unwrap_or(u64::MAX));
            max_evicted_sequence = max_evicted_sequence.max(snapshot.batch.id.sequence);
        }
        let mut snapshots = VecDeque::with_capacity(self.snapshots.len() - evicted_count + 1);
        for snapshot in self.snapshots.iter().skip(evicted_count) {
            check_deadline(deadline)?;
            snapshots.push_back(snapshot.clone());
        }
        snapshots.push_back(RetainedSnapshot {
            batch: batch.clone(),
            bytes,
        });
        let first_source = self.source.is_none().then(|| batch.source.clone());
        check_deadline(deadline)?;
        Ok(Publication {
            snapshots,
            id: batch.id,
            first_source,
            retained_bytes: retained_bytes + bytes,
            max_evicted_sequence,
            evicted_snapshots: self
                .evicted_snapshots
                .saturating_add(u64::try_from(evicted_count).unwrap_or(u64::MAX)),
            evicted_bytes,
        })
    }

    /// The caller must release the history lock before dropping the returned ring.
    fn publish(
        &mut self,
        publication: Publication,
        deadline: Instant,
    ) -> Result<VecDeque<RetainedSnapshot>, WriteError> {
        check_deadline(deadline)?;
        // No allocation, row traversal or last-Arc destruction follows this check
        // until the complete replacement and its metadata have become visible.
        let retired = std::mem::replace(&mut self.snapshots, publication.snapshots);
        if let Some(source) = publication.first_source {
            self.source = Some((publication.id, source));
        } else if let Some((latest_id, _)) = &mut self.source {
            *latest_id = publication.id;
        }
        self.retained_bytes = publication.retained_bytes;
        self.max_evicted_sequence = publication.max_evicted_sequence;
        self.evicted_snapshots = publication.evicted_snapshots;
        self.evicted_bytes = publication.evicted_bytes;
        self.published_snapshots = self.published_snapshots.saturating_add(1);
        Ok(retired)
    }
}

/// Why a historical ID is unavailable. The bounded ring does not keep tombstones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingReason {
    /// It may have been evicted or may never have reached this sink.
    Unknown,
}

/// A snapshot lookup or cursor could not be satisfied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadError {
    /// The ID belongs to a different recorder session than this reader.
    WrongSource,
    /// The requested sequence is above the highest successful publication, or no batch exists.
    NotVisible,
    /// This sequence is at or below the publication watermark but is not retained.
    NotRetained {
        /// The history is bounded and cannot always distinguish absent from evicted IDs.
        reason: MissingReason,
    },
    /// History queries must request at least one snapshot.
    InvalidLimit,
}
impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::WrongSource => "snapshot belongs to another source session",
            Self::NotVisible => "snapshot has not become visible",
            Self::NotRetained { .. } => {
                "snapshot is not retained; publication or eviction history is unknown"
            }
            Self::InvalidLimit => "history query limit must be nonzero",
        })
    }
}
impl std::error::Error for ReadError {}

/// One consistent, bounded page of complete snapshots and its publication metadata.
#[derive(Debug, Clone)]
pub struct HistoryPage {
    /// Snapshots strictly after the cursor, in ascending sequence order.
    pub snapshots: Vec<Arc<Batch>>,
    /// Earliest snapshot retained when the page was read.
    pub oldest_retained: Option<BatchId>,
    /// Highest successfully published snapshot when the page was read.
    pub latest_visible: Option<BatchId>,
    /// Largest sequence known to have been successfully published and then evicted.
    pub max_evicted_sequence: u64,
    /// A successfully published snapshot after the cursor has been evicted.
    pub retention_gap: bool,
}

/// Bounded memory history statistics; evictions are not failed writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryDiagnostics {
    /// Current retained snapshot count.
    pub retained_snapshots: usize,
    /// Current retained estimated bytes.
    pub retained_bytes: usize,
    /// Lifetime successful publications, excluding idempotent duplicates.
    pub published_snapshots: u64,
    /// Lifetime successful retained duplicate writes.
    pub duplicate_writes: u64,
    /// Lifetime snapshots evicted normally by retention.
    pub evicted_snapshots: u64,
    /// Lifetime estimated bytes evicted, saturating on overflow.
    pub evicted_bytes: u64,
    /// History queries that detected an actual retention gap.
    pub retention_gap_queries: u64,
    /// Highest sequence known to have been evicted.
    pub max_evicted_sequence: u64,
    /// The sink has been dropped; retained history remains readable.
    pub writer_closed: bool,
}

impl MemorySink {
    /// Constructs a writer and reader with default model validation limits.
    pub fn new(retention: Retention) -> Result<(Self, SnapshotReader), ConfigError> {
        Self::with_validation_limits(retention, ValidationLimits::default())
    }

    /// Constructs a writer and reader with explicitly selected batch validation limits.
    pub fn with_validation_limits(
        retention: Retention,
        validation: ValidationLimits,
    ) -> Result<(Self, SnapshotReader), ConfigError> {
        validation.validate().map_err(|error| ConfigError {
            message: error.to_string(),
        })?;
        if retention.max_snapshots == 0
            || retention.max_retained_bytes == 0
            || validation.max_batch_bytes == 0
        {
            return Err(ConfigError {
                message: "snapshot count, retained byte and validation byte limits must be nonzero"
                    .into(),
            });
        }
        let shared = Arc::new(Shared {
            retention,
            state: Mutex::new(State::default()),
        });
        Ok((
            Self {
                shared: shared.clone(),
                validation,
            },
            SnapshotReader { shared },
        ))
    }
}

impl Sink for MemorySink {
    fn completion_boundary(&self) -> CompletionBoundary {
        CompletionBoundary::LocalPublished
    }

    fn write(&mut self, batch: Arc<Batch>, deadline: Instant) -> Result<(), WriteError> {
        check_deadline(deadline)?;
        batch
            .validate(&self.validation)
            .map_err(|error| permanent(error.to_string()))?;
        let bytes = batch.estimated_bytes();
        if bytes > self.shared.retention.max_retained_bytes {
            return Err(permanent(
                "snapshot cannot fit in the retention byte budget",
            ));
        }
        let Some(mut state) = self.shared.state.try_lock_until(deadline) else {
            return Err(timeout());
        };
        check_deadline(deadline)?;
        if let Some((latest_id, source)) = &state.source {
            if latest_id.source_session_id != batch.id.source_session_id {
                return Err(permanent(
                    "memory sink is bound to a different source session",
                ));
            }
            if source != &batch.source {
                return Err(permanent(
                    "source metadata changed within a recorder session",
                ));
            }
            if batch.id.sequence <= latest_id.sequence {
                if let Some(previous) = state
                    .snapshots
                    .iter()
                    .find(|previous| previous.batch.id == batch.id)
                {
                    if previous.batch.as_ref() != batch.as_ref() {
                        return Err(permanent("batch ID was reused with conflicting content"));
                    }
                    check_deadline(deadline)?;
                    state.duplicate_writes = state.duplicate_writes.saturating_add(1);
                    return Ok(());
                }
                return Err(permanent("batch is older than the publication watermark and cannot be verified after eviction"));
            }
        }
        let publication =
            state.prepare_publication(&batch, bytes, self.shared.retention, deadline)?;
        let retired = state.publish(publication, deadline)?;
        drop(state);
        // Once publication succeeds, non-interruptible deallocation may exceed
        // the deadline. Returning NotCommitted here would misreport visible data.
        drop(retired);
        Ok(())
    }

    fn flush(&mut self, deadline: Instant) -> Result<(), WriteError> {
        check_deadline(deadline)
    }
}

impl Drop for MemorySink {
    fn drop(&mut self) {
        self.shared.state.lock().writer_closed = true;
    }
}

impl SnapshotReader {
    /// Returns the most recently published complete snapshot, without triggering collection.
    pub fn latest(&self) -> Option<Arc<Batch>> {
        self.shared
            .state
            .lock()
            .snapshots
            .back()
            .map(|snapshot| snapshot.batch.clone())
    }

    /// Looks up an exact source batch ID, never substitutes another sequence.
    pub fn get(&self, id: BatchId) -> Result<Arc<Batch>, ReadError> {
        let state = self.shared.state.lock();
        let Some((latest_id, _)) = &state.source else {
            return Err(ReadError::NotVisible);
        };
        if latest_id.source_session_id != id.source_session_id {
            return Err(ReadError::WrongSource);
        }
        if let Some(snapshot) = state
            .snapshots
            .iter()
            .find(|snapshot| snapshot.batch.id == id)
        {
            return Ok(snapshot.batch.clone());
        }
        if id.sequence > latest_id.sequence {
            Err(ReadError::NotVisible)
        } else {
            Err(ReadError::NotRetained {
                reason: MissingReason::Unknown,
            })
        }
    }

    /// Reads a consistent page strictly after `cursor`; `None` starts before sequence one.
    ///
    /// Results are capped at both `limit` and configured `max_snapshots`. `limit=0`
    /// is invalid. A cursor's missing sequence alone does not indicate eviction:
    /// `retention_gap` is true only when a known eviction lies after that cursor.
    pub fn after(&self, cursor: Option<BatchId>, limit: usize) -> Result<HistoryPage, ReadError> {
        if limit == 0 {
            return Err(ReadError::InvalidLimit);
        }
        let mut state = self.shared.state.lock();
        if let (Some(cursor), Some((latest_id, _))) = (cursor, &state.source) {
            if cursor.source_session_id != latest_id.source_session_id {
                return Err(ReadError::WrongSource);
            }
        }
        let sequence = cursor.map_or(0, |id| id.sequence);
        let retention_gap = state.max_evicted_sequence > sequence;
        if retention_gap {
            state.retention_gap_queries = state.retention_gap_queries.saturating_add(1);
        }
        Ok(HistoryPage {
            snapshots: state
                .snapshots
                .iter()
                .filter(|snapshot| snapshot.batch.id.sequence > sequence)
                .take(limit.min(self.shared.retention.max_snapshots))
                .map(|snapshot| snapshot.batch.clone())
                .collect(),
            oldest_retained: state.snapshots.front().map(|snapshot| snapshot.batch.id),
            latest_visible: state.source.as_ref().map(|(id, _)| *id),
            max_evicted_sequence: state.max_evicted_sequence,
            retention_gap,
        })
    }

    /// Returns a consistent snapshot of retention and publication diagnostics.
    pub fn diagnostics(&self) -> MemoryDiagnostics {
        let state = self.shared.state.lock();
        MemoryDiagnostics {
            retained_snapshots: state.snapshots.len(),
            retained_bytes: state.retained_bytes,
            published_snapshots: state.published_snapshots,
            duplicate_writes: state.duplicate_writes,
            evicted_snapshots: state.evicted_snapshots,
            evicted_bytes: state.evicted_bytes,
            retention_gap_queries: state.retention_gap_queries,
            max_evicted_sequence: state.max_evicted_sequence,
            writer_closed: state.writer_closed,
        }
    }
}

fn check_deadline(deadline: Instant) -> Result<(), WriteError> {
    if Instant::now() >= deadline {
        Err(timeout())
    } else {
        Ok(())
    }
}
fn timeout() -> WriteError {
    WriteError::new(
        ErrorKind::Timeout,
        CommitOutcome::NotCommitted,
        "memory publication deadline expired",
    )
}
fn permanent(message: impl Into<String>) -> WriteError {
    WriteError::new(ErrorKind::Permanent, CommitOutcome::NotCommitted, message)
}

#[cfg(test)]
mod tests;
