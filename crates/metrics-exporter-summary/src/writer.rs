use crate::{
    control::{FlushReport, Runtime, Ticket},
    diagnostics::{add, duration_ns},
    registry::Shared,
};
use metrics_summary_core::{Batch, BatchId, CommitOutcome, ErrorKind, Sink, WriteError};
use parking_lot::{Condvar, Mutex};
use std::{
    collections::VecDeque,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

#[derive(Default, Clone, Copy)]
pub(crate) struct Losses {
    pub batches: u64,
    pub rows: u64,
    pub samples: u64,
}
impl Losses {
    pub fn drop_batch(&mut self, batch: &Batch) {
        self.batches = self.batches.saturating_add(1);
        self.rows = self.rows.saturating_add(batch.rows.len() as u64);
        self.samples = self.samples.saturating_add(batch.histogram_samples());
    }
}
pub(crate) enum Work {
    Data {
        batch: Arc<Batch>,
        born: Instant,
    },
    Barrier {
        ticket: Arc<Ticket>,
        target: BatchId,
        losses: Losses,
        shutdown: bool,
    },
}
#[derive(Default)]
pub(crate) struct Queue {
    state: Mutex<QueueState>,
    ready: Condvar,
}
#[derive(Default)]
struct QueueState {
    items: VecDeque<Work>,
    batches: usize,
    bytes: usize,
    aborted: bool,
}
impl Queue {
    pub fn push_batch(&self, shared: &Shared, batch: Arc<Batch>, born: Instant) -> bool {
        let bytes = batch.estimated_bytes();
        let mut state = self.state.lock();
        if state.aborted
            || state.batches >= shared.config.queue_max_batches
            || state.bytes.saturating_add(bytes) > shared.config.queue_max_bytes
        {
            return false;
        }
        state.batches += 1;
        state.bytes += bytes;
        state.items.push_back(Work::Data { batch, born });
        shared
            .diagnostics
            .queued_batches
            .store(state.batches as u64, Ordering::Relaxed);
        shared
            .diagnostics
            .queued_bytes
            .store(state.bytes as u64, Ordering::Relaxed);
        self.ready.notify_one();
        true
    }
    pub fn barrier(&self, work: Work) {
        let mut state = self.state.lock();
        if state.aborted {
            if let Work::Barrier { ticket, .. } = work {
                ticket.complete(Err(crate::ControlError::WorkerFailed));
            }
        } else {
            state.items.push_back(work);
            self.ready.notify_one();
        }
    }
    fn pop(&self) -> Option<Work> {
        let mut state = self.state.lock();
        loop {
            if let Some(work) = state.items.pop_front() {
                return Some(work);
            }
            if state.aborted {
                return None;
            }
            self.ready.wait(&mut state);
        }
    }
    fn release(&self, shared: &Shared, batch: &Batch) {
        let mut state = self.state.lock();
        state.batches = state.batches.saturating_sub(1);
        state.bytes = state.bytes.saturating_sub(batch.estimated_bytes());
        shared
            .diagnostics
            .queued_batches
            .store(state.batches as u64, Ordering::Relaxed);
        shared
            .diagnostics
            .queued_bytes
            .store(state.bytes as u64, Ordering::Relaxed);
    }
    pub fn abort(&self, shared: &Shared) {
        let mut state = self.state.lock();
        state.aborted = true;
        for work in state.items.drain(..) {
            match work {
                Work::Data { batch, .. } => record_drop(shared, &batch),
                Work::Barrier { ticket, .. } => {
                    ticket.complete(Err(crate::ControlError::WorkerFailed))
                }
            }
        }
        state.batches = 0;
        state.bytes = 0;
        shared
            .diagnostics
            .queued_batches
            .store(0, Ordering::Relaxed);
        shared.diagnostics.queued_bytes.store(0, Ordering::Relaxed);
        self.ready.notify_all();
    }
}
pub(crate) fn record_drop(shared: &Shared, batch: &Batch) {
    add(&shared.diagnostics.dropped_batches, 1);
    add(&shared.diagnostics.dropped_rows, batch.rows.len() as u64);
    add(
        &shared.diagnostics.dropped_histogram_samples,
        batch.histogram_samples(),
    );
}

pub(crate) fn run(runtime: Arc<Runtime>, mut sink: Box<dyn Sink>) {
    let shared = &runtime.shared;
    let mut losses = Losses::default();
    let mut written = 0u64;
    let mut failed = 0u64;
    let mut unknown = 0u64;
    let mut poisoned = false;
    while let Some(work) = runtime.queue.pop() {
        match work {
            Work::Data { batch, born } => {
                let started = Instant::now();
                let result = if poisoned {
                    Err(panic_error())
                } else {
                    write_with_retry(shared, &mut *sink, &batch, born, &mut poisoned)
                };
                shared
                    .diagnostics
                    .last_write_ns
                    .store(duration_ns(started.elapsed()), Ordering::Relaxed);
                match result {
                    Ok(()) => {
                        written = written.saturating_add(1);
                        add(&shared.diagnostics.written_batches, 1);
                        shared
                            .diagnostics
                            .last_success_unix_ns
                            .store(shared.clock.unix_nanos().max(0) as u64, Ordering::Relaxed);
                    }
                    Err(error) => {
                        shared.note_error(&error);
                        failed = failed.saturating_add(1);
                        if error.outcome == CommitOutcome::Unknown {
                            unknown = unknown.saturating_add(1);
                        }
                        losses.drop_batch(&batch);
                        record_drop(shared, &batch);
                    }
                }
                runtime.queue.release(shared, &batch);
            }
            Work::Barrier {
                ticket,
                target,
                losses: collection,
                shutdown,
            } => {
                let flush_error = if poisoned {
                    Some(panic_error())
                } else if Instant::now() >= ticket.deadline {
                    Some(WriteError::new(
                        ErrorKind::Timeout,
                        CommitOutcome::Unknown,
                        "sink flush deadline elapsed",
                    ))
                } else {
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        sink.flush(ticket.deadline)
                    })) {
                        Ok(result) => result.err(),
                        Err(_) => {
                            poisoned = true;
                            add(&shared.diagnostics.worker_panics, 1);
                            Some(panic_error())
                        }
                    }
                };
                if let Some(error) = &flush_error {
                    shared.note_error(error);
                }
                let report = FlushReport {
                    target,
                    completion_boundary: runtime.boundary.clone(),
                    collected_batches: target.sequence,
                    written_batches: written,
                    dropped_batches: losses.batches.saturating_add(collection.batches),
                    dropped_rows: losses.rows.saturating_add(collection.rows),
                    dropped_histogram_samples: losses.samples.saturating_add(collection.samples),
                    failed_batches: failed,
                    unknown_batches: unknown,
                    sink_flush_error: flush_error,
                };
                if shutdown {
                    // Drivers are initialized and destroyed on this same thread,
                    // before Closed is published or the shutdown waiter is released.
                    drop(sink);
                    shared.finish();
                    ticket.complete(Ok(report));
                    return;
                }
                ticket.complete(Ok(report));
            }
        }
    }
}

fn panic_error() -> WriteError {
    WriteError::new(
        ErrorKind::Permanent,
        CommitOutcome::Unknown,
        "sink panicked; further sink calls disabled",
    )
}
fn write_with_retry(
    shared: &Shared,
    sink: &mut dyn Sink,
    batch: &Arc<Batch>,
    born: Instant,
    poisoned: &mut bool,
) -> Result<(), WriteError> {
    let total_deadline = born + shared.config.retry_deadline;
    let mut last = WriteError::new(
        ErrorKind::Timeout,
        CommitOutcome::NotCommitted,
        "batch expired before write",
    );
    let mut outcome_unknown = false;
    for attempt in 0..shared.config.max_write_attempts {
        if Instant::now() >= total_deadline {
            break;
        }
        let deadline = total_deadline.min(Instant::now() + shared.config.write_timeout);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            sink.write(batch.clone(), deadline)
        }));
        match result {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(mut error)) => {
                add(&shared.diagnostics.write_failures, 1);
                outcome_unknown |= error.outcome == CommitOutcome::Unknown;
                if outcome_unknown {
                    error.outcome = CommitOutcome::Unknown;
                }
                shared.note_error(&error);
                let retry = error.is_retryable();
                last = error;
                if !retry {
                    break;
                }
            }
            Err(_) => {
                add(&shared.diagnostics.worker_panics, 1);
                *poisoned = true;
                return Err(panic_error());
            }
        }
        if attempt + 1 == shared.config.max_write_attempts {
            break;
        }
        let base = shared
            .config
            .retry_initial_backoff
            .saturating_mul(1u32 << attempt.min(30))
            .min(shared.config.retry_max_backoff);
        let mix = batch
            .id
            .sequence
            .wrapping_mul(0x9e3779b97f4a7c15)
            .wrapping_add(attempt as u64);
        let jitter = Duration::from_nanos((base.as_nanos() / 4).min(u64::MAX as u128) as u64)
            .mul_f64((mix % 1000) as f64 / 1000.0);
        let wait = base
            .saturating_add(jitter)
            .min(shared.config.retry_max_backoff);
        let remaining = total_deadline.saturating_duration_since(Instant::now());
        if wait >= remaining {
            break;
        }
        add(&shared.diagnostics.retries, 1);
        std::thread::sleep(wait);
    }
    Err(last)
}
