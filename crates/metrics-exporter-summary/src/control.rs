use crate::{
    config::BuildError,
    diagnostics::{add, DiagnosticsSnapshot},
    registry::{Shared, CLOSED, CLOSING, RUNNING},
    writer::Queue,
};
use crossbeam_channel::{bounded, Sender, TrySendError};
use metrics_summary_core::{BatchId, CompletionBoundary, Sink, WriteError};
use parking_lot::{Condvar, Mutex};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::{Duration, Instant},
};

/// Recorder admission and background cleanup state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    /// New registrations, observations, and collection requests are admitted.
    Running,
    /// Admission has stopped; final collection and writer cleanup are in progress.
    Closing,
    /// Terminal recorder state; successful delivery is not implied.
    Closed,
}

/// Cumulative results through `target`, never hiding previously known local loss.
/// A remote acceptance boundary does not include losses after a collector ACK.
#[derive(Debug, Clone)]
pub struct FlushReport {
    /// Last collection included in this completion barrier.
    pub target: BatchId,
    /// The sink's fixed success boundary, not necessarily durable storage.
    pub completion_boundary: CompletionBoundary,
    /// Complete collection rounds through the target, including empty rounds.
    pub collected_batches: u64,
    /// Batches successfully delivered to the configured boundary through the target.
    pub written_batches: u64,
    /// Collected batches discarded locally through the target.
    pub dropped_batches: u64,
    /// Rows lost during collection or discarded with batches through the target.
    pub dropped_rows: u64,
    /// Previously accepted histogram observations lost through the target.
    pub dropped_histogram_samples: u64,
    /// Batches that exhausted writer delivery, including uncertain outcomes.
    pub failed_batches: u64,
    /// Failed batches whose sink may have committed before reporting an error.
    pub unknown_batches: u64,
    /// Failure to complete the sink flush at this barrier, including timeout or panic.
    pub sink_flush_error: Option<WriteError>,
}
impl FlushReport {
    /// True only when every batch and accepted sample through the barrier reached
    /// the configured completion boundary, and the final sink flush succeeded.
    pub fn is_success(&self) -> bool {
        self.dropped_batches == 0
            && self.dropped_rows == 0
            && self.dropped_histogram_samples == 0
            && self.failed_batches == 0
            && self.sink_flush_error.is_none()
    }
}

/// A control request was not admitted, the caller's wait expired, or a worker failed.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ControlError {
    /// A flush was not collected before admission closed.
    #[error("recorder is closing or closed")]
    Closing,
    /// The configured number of outstanding flush requests has been reached.
    #[error("outstanding control request limit reached")]
    Busy,
    /// The timeout cannot be added to the current monotonic clock.
    #[error("timeout is not representable")]
    InvalidTimeout,
    /// The caller stopped waiting; admitted background work may still complete.
    #[error(
        "control wait timed out (target {target:?}, {unconfirmed_batches} unconfirmed batches)"
    )]
    Timeout {
        /// Assigned target, or `None` if the sampler has not assigned one yet.
        target: Option<BatchId>,
        /// Collected batches not yet recorded as written or dropped at timeout.
        unconfirmed_batches: u64,
    },
    /// A sampler or writer failure prevented a normal completion report.
    #[error("background worker failed")]
    WorkerFailed,
}

/// Cloneable control handle. Last-handle drop starts nonblocking cleanup;
/// explicit shutdown is required to observe final delivery results.
#[derive(Clone)]
pub struct Control {
    inner: Arc<ControlInner>,
}
struct ControlInner {
    runtime: Arc<Runtime>,
    commands: Sender<Command>,
    gate: Mutex<()>,
}
pub(crate) enum Command {
    Flush(Arc<Ticket>),
    Wake,
}

pub(crate) struct Runtime {
    pub shared: Arc<Shared>,
    pub queue: Queue,
    pub boundary: CompletionBoundary,
    pub shutdown: Mutex<Option<Arc<Ticket>>>,
    slots: Arc<AtomicUsize>,
    tickets: Mutex<Vec<Weak<Ticket>>>,
}
pub(crate) struct Ticket {
    pub deadline: Instant,
    state: Mutex<TicketState>,
    changed: Condvar,
}
#[derive(Default)]
struct TicketState {
    target: Option<BatchId>,
    result: Option<Result<FlushReport, ControlError>>,
    slots: Option<Arc<AtomicUsize>>,
}
impl Ticket {
    fn new(deadline: Instant, slots: Option<Arc<AtomicUsize>>) -> Self {
        Self {
            deadline,
            state: Mutex::new(TicketState {
                slots,
                ..TicketState::default()
            }),
            changed: Condvar::new(),
        }
    }
    pub fn set_target(&self, target: BatchId) {
        self.state.lock().target = Some(target);
    }
    pub fn complete(&self, result: Result<FlushReport, ControlError>) {
        let mut state = self.state.lock();
        if state.result.is_none() {
            // Release admission before publishing completion. The caller may
            // immediately flush again while a worker still owns this ticket.
            if let Some(slots) = state.slots.take() {
                slots.fetch_sub(1, Ordering::AcqRel);
            }
            state.result = Some(result);
            self.changed.notify_all();
        }
    }
    fn wait(&self, deadline: Instant, runtime: &Runtime) -> Result<FlushReport, ControlError> {
        let mut state = self.state.lock();
        loop {
            if let Some(result) = &state.result {
                return result.clone();
            }
            let Some(remaining) = deadline
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
            else {
                let d = runtime.shared.diagnostics.snapshot();
                return Err(ControlError::Timeout {
                    target: state.target,
                    unconfirmed_batches: d
                        .collected_batches
                        .saturating_sub(d.written_batches.saturating_add(d.dropped_batches)),
                });
            };
            self.changed.wait_for(&mut state, remaining);
        }
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        // Failed admission or abandoned work may never publish a completion.
        if let Some(slots) = self.state.get_mut().slots.take() {
            slots.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl Runtime {
    fn track(&self, ticket: &Arc<Ticket>) {
        let mut tickets = self.tickets.lock();
        tickets.retain(|ticket| ticket.strong_count() > 0);
        tickets.push(Arc::downgrade(ticket));
    }
    pub fn ensure_shutdown(&self) -> Arc<Ticket> {
        let mut shutdown = self.shutdown.lock();
        shutdown
            .get_or_insert_with(|| {
                // Shutdown caller timeout never cancels accepted writes. The background
                // operation has its own configured cleanup budget and batch deadlines.
                let ticket = Arc::new(Ticket::new(
                    Instant::now() + self.shared.config.shutdown_timeout,
                    None,
                ));
                self.track(&ticket);
                ticket
            })
            .clone()
    }
    pub fn fail(&self) {
        add(&self.shared.diagnostics.worker_panics, 1);
        self.shared.close_admission();
        self.queue.abort(&self.shared);
        self.shared.finish();
        for ticket in self.tickets.lock().iter().filter_map(Weak::upgrade) {
            ticket.complete(Err(ControlError::WorkerFailed));
        }
    }
}
impl Control {
    pub(crate) fn start(shared: Arc<Shared>, sink: Box<dyn Sink>) -> Result<Self, BuildError> {
        let (commands, receiver) = bounded(shared.config.max_control_requests + 1);
        let runtime = Arc::new(Runtime {
            boundary: sink.completion_boundary(),
            shared,
            queue: Queue::default(),
            shutdown: Mutex::new(None),
            slots: Arc::new(AtomicUsize::new(0)),
            tickets: Mutex::new(Vec::new()),
        });
        let writer_runtime = runtime.clone();
        std::thread::Builder::new()
            .name("summary-writer".into())
            .spawn(move || {
                crate::registry::suppress();
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    crate::writer::run(writer_runtime.clone(), sink)
                }))
                .is_err()
                {
                    writer_runtime.fail();
                }
            })
            .map_err(|e| BuildError(format!("cannot start writer: {e}")))?;
        let sampler_runtime = runtime.clone();
        if let Err(error) = std::thread::Builder::new()
            .name("summary-sampler".into())
            .spawn(move || {
                crate::registry::suppress();
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    crate::sampler::run(sampler_runtime.clone(), receiver)
                }))
                .is_err()
                {
                    sampler_runtime.fail();
                }
            })
        {
            runtime.fail();
            return Err(BuildError(format!("cannot start sampler: {error}")));
        }
        Ok(Self {
            inner: Arc::new(ControlInner {
                runtime,
                commands,
                gate: Mutex::new(()),
            }),
        })
    }
    /// Collects one new window and waits through its sink completion barrier.
    /// Timeout ends only this caller's wait; already accepted work remains bounded
    /// and continues. Outstanding timed-out controls retain their admission slot
    /// until their background work completes.
    pub fn flush(&self, timeout: Duration) -> Result<FlushReport, ControlError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(ControlError::InvalidTimeout)?;
        let ticket = {
            let _gate = self.inner.gate.lock();
            let runtime = &self.inner.runtime;
            if !runtime.shared.running() {
                return Err(ControlError::Closing);
            }
            runtime
                .slots
                .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    (n < runtime.shared.config.max_control_requests).then_some(n + 1)
                })
                .map_err(|_| ControlError::Busy)?;
            let ticket = Arc::new(Ticket::new(deadline, Some(runtime.slots.clone())));
            runtime.track(&ticket);
            match self.inner.commands.try_send(Command::Flush(ticket.clone())) {
                Ok(()) => ticket,
                Err(TrySendError::Full(_)) => return Err(ControlError::Busy),
                Err(TrySendError::Disconnected(_)) => return Err(ControlError::WorkerFailed),
            }
        };
        ticket.wait(deadline, &self.inner.runtime)
    }
    /// Closes admission once, performs final collection, and drains the writer.
    /// Repeated calls wait for the same operation, including after a timeout.
    pub fn shutdown(&self, timeout: Duration) -> Result<FlushReport, ControlError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(ControlError::InvalidTimeout)?;
        let ticket = {
            let _gate = self.inner.gate.lock();
            let runtime = &self.inner.runtime;
            if runtime.shared.lifecycle.load(Ordering::Acquire) == CLOSED
                && runtime.shutdown.lock().is_none()
            {
                return Err(ControlError::WorkerFailed);
            }
            let ticket = runtime.ensure_shutdown();
            runtime.shared.close_admission();
            let _ = self.inner.commands.try_send(Command::Wake);
            ticket
        };
        ticket.wait(deadline, &self.inner.runtime)
    }
    /// Shuts down using the configured default caller budget.
    pub fn shutdown_default(&self) -> Result<FlushReport, ControlError> {
        self.shutdown(self.inner.runtime.shared.config.shutdown_timeout)
    }
    /// Reads independent diagnostic counters without recording metrics recursively.
    pub fn diagnostics(&self) -> DiagnosticsSnapshot {
        self.inner.runtime.shared.diagnostics.snapshot()
    }
    /// Last sink error (including recovered retries), with a bounded message and
    /// timestamp. It remains available after shutdown for operational diagnosis.
    pub fn last_write_error(&self) -> Option<crate::LastWriteError> {
        self.inner.runtime.shared.last_error.lock().clone()
    }
    /// Current lifecycle state.
    pub fn lifecycle(&self) -> Lifecycle {
        match self.inner.runtime.shared.lifecycle.load(Ordering::Acquire) {
            RUNNING => Lifecycle::Running,
            CLOSING => Lifecycle::Closing,
            _ => Lifecycle::Closed,
        }
    }
}
impl Drop for ControlInner {
    fn drop(&mut self) {
        if self.runtime.shared.running() {
            self.runtime.ensure_shutdown();
            self.runtime.shared.close_admission();
            let _ = self.commands.try_send(Command::Wake);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_ticket_releases_admission_while_worker_still_holds_it() {
        let slots = Arc::new(AtomicUsize::new(1));
        let caller = Arc::new(Ticket::new(Instant::now(), Some(slots.clone())));
        let worker = caller.clone();

        worker.complete(Err(ControlError::WorkerFailed));
        assert_eq!(
            slots.load(Ordering::Acquire),
            0,
            "completion must free admission before the worker drops its ticket"
        );
        drop(caller);

        // Admit another request while the completed worker still owns its Arc.
        slots.fetch_add(1, Ordering::AcqRel);
        let next = Ticket::new(Instant::now(), Some(slots.clone()));
        worker.complete(Err(ControlError::Closing));
        assert!(matches!(
            worker.state.lock().result,
            Some(Err(ControlError::WorkerFailed))
        ));
        drop(worker);
        assert_eq!(
            slots.load(Ordering::Acquire),
            1,
            "repeated completion and late drop must not release another request's slot"
        );
        drop(next);
        assert_eq!(slots.load(Ordering::Acquire), 0);
    }

    #[test]
    fn unfinished_ticket_retains_admission_until_its_last_owner_drops() {
        let slots = Arc::new(AtomicUsize::new(1));
        let caller = Arc::new(Ticket::new(Instant::now(), Some(slots.clone())));
        let worker = caller.clone();

        // A caller can stop waiting while accepted background work is unfinished.
        drop(caller);
        assert_eq!(slots.load(Ordering::Acquire), 1);
        drop(worker);
        assert_eq!(slots.load(Ordering::Acquire), 0);
    }
}
