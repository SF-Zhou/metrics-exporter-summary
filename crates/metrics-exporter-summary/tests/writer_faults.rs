use metrics_exporter_summary::{metrics, Builder, Config, ControlError, Lifecycle};
use metrics_summary_core::{
    Batch, CommitOutcome, CompletionBoundary, ErrorKind, MetricValue, Sink, WriteError,
};
use metrics_summary_sink_memory::{MemorySink, Retention};
use std::{
    collections::VecDeque,
    sync::{mpsc, Arc, Condvar, Mutex},
    thread,
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(10);

fn config() -> Config {
    Config {
        collect_interval: None,
        retry_initial_backoff: Duration::from_millis(1),
        retry_max_backoff: Duration::from_millis(2),
        ..Default::default()
    }
}

struct ScriptedSink {
    attempts: Arc<Mutex<Vec<Arc<Batch>>>>,
    errors: VecDeque<WriteError>,
    memory: MemorySink,
    panic: bool,
}

impl Sink for ScriptedSink {
    fn completion_boundary(&self) -> CompletionBoundary {
        CompletionBoundary::LocalPublished
    }
    fn write(&mut self, batch: Arc<Batch>, deadline: Instant) -> Result<(), WriteError> {
        self.attempts.lock().unwrap().push(batch.clone());
        assert!(!self.panic, "injected sink panic");
        if let Some(error) = self.errors.pop_front() {
            Err(error)
        } else {
            self.memory.write(batch, deadline)
        }
    }
    fn flush(&mut self, deadline: Instant) -> Result<(), WriteError> {
        self.memory.flush(deadline)
    }
}

#[test]
fn bounded_retries_keep_original_arc_id_window_hostname_and_content() {
    let (memory, reader) = MemorySink::new(Retention::default()).unwrap();
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let sink = ScriptedSink {
        attempts: attempts.clone(),
        memory,
        panic: false,
        errors: [
            WriteError::new(ErrorKind::Timeout, CommitOutcome::Unknown, "ACK lost"),
            WriteError::new(
                ErrorKind::Retryable,
                CommitOutcome::NotCommitted,
                "temporarily unavailable",
            ),
        ]
        .into(),
    };
    let (recorder, control) = Builder::for_service("faults", "retry")
        .unwrap()
        .config(config())
        .build(sink)
        .unwrap();
    metrics::with_local_recorder(&recorder, || {
        metrics::histogram!("duration").record_many(2.0, 3);
        metrics::counter!("requests").increment(7);
    });
    let report = control.flush(WAIT).unwrap();
    assert!(report.is_success(), "{report:?}");
    assert_eq!(
        report.unknown_batches, 0,
        "successful retry resolves the uncertain failure"
    );
    let attempts = attempts.lock().unwrap();
    assert_eq!(attempts.len(), 3);
    assert!(attempts
        .iter()
        .all(|batch| Arc::ptr_eq(batch, &attempts[0])));
    assert_eq!(reader.get(report.target).unwrap().histogram_samples(), 3);
    assert!(attempts.iter().all(|batch| batch
        .rows
        .iter()
        .any(|row| { row.value == MetricValue::CounterDelta { delta_value: 7 } })));
    assert_eq!(control.diagnostics().write_failures, 2);
    assert_eq!(control.diagnostics().retries, 2);
    let last_error = control
        .last_write_error()
        .expect("recovered failures remain diagnosable");
    assert_eq!(last_error.error.message, "temporarily unavailable");
    assert_eq!(last_error.error.outcome, CommitOutcome::Unknown);
    drop(attempts);
    let final_report = control.shutdown(WAIT).unwrap();
    assert!(reader
        .get(final_report.target)
        .unwrap()
        .rows
        .iter()
        .any(|row| { row.value == MetricValue::CounterDelta { delta_value: 0 } }));
}

#[test]
fn unknown_outcome_survives_a_later_definite_failure() {
    let (memory, _) = MemorySink::new(Retention::default()).unwrap();
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let sink = ScriptedSink {
        attempts: attempts.clone(),
        memory,
        panic: false,
        errors: [
            WriteError::new(
                ErrorKind::Retryable,
                CommitOutcome::Unknown,
                "may have committed",
            ),
            WriteError::new(
                ErrorKind::Permanent,
                CommitOutcome::NotCommitted,
                "retry rejected",
            ),
        ]
        .into(),
    };
    let (recorder, control) = Builder::for_service("faults", "unknown")
        .unwrap()
        .config(config())
        .build(sink)
        .unwrap();
    metrics::with_local_recorder(&recorder, || metrics::histogram!("duration").record(1.0));
    let report = control.flush(WAIT).unwrap();
    assert!(!report.is_success());
    assert_eq!(report.failed_batches, 1);
    assert_eq!(report.unknown_batches, 1);
    assert_eq!(report.dropped_histogram_samples, 1);
    assert_eq!(attempts.lock().unwrap().len(), 2);
    assert_eq!(
        control.last_write_error().unwrap().error.outcome,
        CommitOutcome::Unknown
    );
    assert!(
        !control.shutdown(WAIT).unwrap().is_success(),
        "later barriers retain known historical loss"
    );
}

#[test]
fn operational_error_history_is_bounded_and_survives_shutdown() {
    let (memory, _) = MemorySink::new(Retention::default()).unwrap();
    let sink = ScriptedSink {
        attempts: Arc::new(Mutex::new(Vec::new())),
        memory,
        panic: false,
        errors: [WriteError::new(
            ErrorKind::Permanent,
            CommitOutcome::NotCommitted,
            "故障".repeat(5000),
        )]
        .into(),
    };
    let (recorder, control) = Builder::for_service("faults", "bounded-error")
        .unwrap()
        .config(config())
        .build(sink)
        .unwrap();
    metrics::with_local_recorder(&recorder, || metrics::counter!("requests").increment(1));
    assert!(!control.flush(WAIT).unwrap().is_success());
    let last = control.last_write_error().unwrap();
    assert_eq!(last.error.message.chars().count(), 1024);
    control.shutdown(WAIT).unwrap();
    assert_eq!(control.last_write_error(), Some(last));
}

#[test]
fn extreme_control_configuration_fails_without_allocating_or_spawning() {
    for max_control_requests in [0, 65_537, usize::MAX - 1, usize::MAX] {
        let (sink, _) = MemorySink::new(Retention::default()).unwrap();
        assert!(Builder::for_service("faults", "invalid-config")
            .unwrap()
            .config(Config {
                max_control_requests,
                ..config()
            })
            .build(sink)
            .is_err());
    }
}

#[test]
fn retry_budget_and_permanent_errors_terminate_without_hiding_loss() {
    for (kind, expected_attempts) in [(ErrorKind::Retryable, 3), (ErrorKind::Permanent, 1)] {
        let (memory, _) = MemorySink::new(Retention::default()).unwrap();
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let sink = ScriptedSink {
            attempts: attempts.clone(),
            memory,
            panic: false,
            errors: (0..10)
                .map(|_| WriteError::new(kind, CommitOutcome::NotCommitted, "injected failure"))
                .collect(),
        };
        let (recorder, control) = Builder::for_service("faults", "exhaustion")
            .unwrap()
            .config(Config {
                max_write_attempts: 3,
                ..config()
            })
            .build(sink)
            .unwrap();
        metrics::with_local_recorder(&recorder, || {
            metrics::histogram!("duration").record_many(1.0, 4)
        });
        let report = control.flush(WAIT).unwrap();
        assert!(!report.is_success());
        assert_eq!(report.failed_batches, 1);
        assert_eq!(report.unknown_batches, 0);
        assert_eq!(report.dropped_histogram_samples, 4);
        assert_eq!(attempts.lock().unwrap().len(), expected_attempts);
        control.shutdown(WAIT).unwrap();
    }
}

#[test]
fn a_panicking_sink_is_disabled_and_shutdown_still_finishes() {
    let (memory, _) = MemorySink::new(Retention::default()).unwrap();
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let sink = ScriptedSink {
        attempts: attempts.clone(),
        memory,
        panic: true,
        errors: VecDeque::new(),
    };
    let (recorder, control) = Builder::for_service("faults", "panic")
        .unwrap()
        .config(config())
        .build(sink)
        .unwrap();
    metrics::with_local_recorder(&recorder, || metrics::histogram!("duration").record(1.0));
    let report = control.flush(WAIT).unwrap();
    assert!(!report.is_success());
    assert_eq!(report.unknown_batches, 1);
    assert_eq!(control.diagnostics().worker_panics, 1);
    assert_eq!(attempts.lock().unwrap().len(), 1);
    let shutdown = control.shutdown(WAIT).unwrap();
    assert!(!shutdown.is_success());
    assert_eq!(
        attempts.lock().unwrap().len(),
        1,
        "never call a poisoned sink again"
    );
    assert_eq!(control.lifecycle(), Lifecycle::Closed);
}

#[derive(Default)]
struct Gate {
    released: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

struct GatedSink {
    gate: Arc<Gate>,
    entered: Option<mpsc::Sender<()>>,
    memory: MemorySink,
}
impl Sink for GatedSink {
    fn completion_boundary(&self) -> CompletionBoundary {
        CompletionBoundary::LocalPublished
    }
    fn write(&mut self, batch: Arc<Batch>, deadline: Instant) -> Result<(), WriteError> {
        if let Some(entered) = self.entered.take() {
            entered.send(()).unwrap();
        }
        let mut released = self.gate.released.lock().unwrap();
        while !*released {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return Err(WriteError::new(
                    ErrorKind::Timeout,
                    CommitOutcome::NotCommitted,
                    "test gate deadline",
                ));
            };
            released = self
                .gate
                .changed
                .wait_timeout(released, remaining)
                .unwrap()
                .0;
        }
        self.memory.write(batch, deadline)
    }
    fn flush(&mut self, deadline: Instant) -> Result<(), WriteError> {
        self.memory.flush(deadline)
    }
}

#[test]
fn full_queue_counts_inflight_and_accounts_for_every_accepted_sample() {
    let (memory, reader) = MemorySink::new(Retention::default()).unwrap();
    let gate = Arc::new(Gate::default());
    let (entered, receiver) = mpsc::channel();
    let sink = GatedSink {
        gate: gate.clone(),
        entered: Some(entered),
        memory,
    };
    let (recorder, control) = Builder::for_service("faults", "full")
        .unwrap()
        .config(Config {
            queue_max_batches: 1,
            ..config()
        })
        .build(sink)
        .unwrap();
    let (histogram, counter) = metrics::with_local_recorder(&recorder, || {
        (
            metrics::histogram!("duration"),
            metrics::counter!("requests"),
        )
    });
    histogram.record_many(1.0, 3);
    counter.absolute(3);
    let first_control = control.clone();
    let first = thread::spawn(move || first_control.flush(WAIT));
    receiver.recv_timeout(WAIT).unwrap();
    assert_eq!(
        control.diagnostics().queued_batches,
        1,
        "inflight data still consumes queue budget"
    );
    histogram.record_many(2.0, 7);
    counter.absolute(10);
    let second_control = control.clone();
    let before_dropped_collection = Instant::now();
    let second = thread::spawn(move || second_control.flush(WAIT));
    let deadline = Instant::now() + WAIT;
    while control.diagnostics().collected_batches < 2 {
        assert!(
            Instant::now() < deadline,
            "sampler failed to collect the second window"
        );
        thread::yield_now();
    }
    // Collection counters are published immediately before queue admission.
    while control.diagnostics().dropped_histogram_samples != 7 {
        assert!(
            Instant::now() < deadline,
            "full-queue rejection did not complete"
        );
        thread::yield_now();
    }
    let after_dropped_collection = Instant::now();
    assert_eq!(control.diagnostics().dropped_histogram_samples, 7);
    gate.release();
    assert!(first.join().unwrap().unwrap().is_success());
    assert!(!second.join().unwrap().unwrap().is_success());
    // Only five NEW increments follow the dropped window; do not re-export its seven.
    counter.absolute(15);
    let before_final = Instant::now();
    let report = control.shutdown(WAIT).unwrap();
    let after_final = Instant::now();
    assert!(!report.is_success());
    assert_eq!(report.dropped_histogram_samples, 7);
    let final_batch = reader.get(report.target).unwrap();
    assert_eq!(final_batch.id.sequence, 3);
    assert!(final_batch
        .rows
        .iter()
        .any(|row| { row.value == MetricValue::CounterDelta { delta_value: 5 } }));
    assert_eq!(report.dropped_rows, 2);
    // Dropped windows advance the boundary too. Recovery must not claim the
    // statistical span of the samples that were already discarded.
    assert!(
        final_batch.duration_ns as u128
            >= before_final
                .duration_since(after_dropped_collection)
                .as_nanos()
    );
    assert!(
        final_batch.duration_ns as u128
            <= after_final
                .duration_since(before_dropped_collection)
                .as_nanos()
    );
    let published: u64 = reader
        .after(None, 100)
        .unwrap()
        .snapshots
        .iter()
        .map(|batch| batch.histogram_samples())
        .sum();
    assert_eq!(
        published + report.dropped_histogram_samples,
        control.diagnostics().accepted_histogram_samples
    );
    assert_eq!(control.diagnostics().queued_batches, 0);
    assert_eq!(control.diagnostics().queued_bytes, 0);
}

#[test]
fn control_admission_is_bounded_while_writer_is_blocked() {
    let (memory, _) = MemorySink::new(Retention::default()).unwrap();
    let gate = Arc::new(Gate::default());
    let (entered, receiver) = mpsc::channel();
    let sink = GatedSink {
        gate: gate.clone(),
        entered: Some(entered),
        memory,
    };
    let (_, control) = Builder::for_service("faults", "controls")
        .unwrap()
        .config(Config {
            max_control_requests: 1,
            ..config()
        })
        .build(sink)
        .unwrap();
    let first_control = control.clone();
    let first = thread::spawn(move || first_control.flush(WAIT));
    receiver.recv_timeout(WAIT).unwrap();
    assert!(matches!(control.flush(WAIT), Err(ControlError::Busy)));
    gate.release();
    assert!(first.join().unwrap().unwrap().is_success());
    assert!(control.flush(WAIT).unwrap().is_success());
    control.shutdown(WAIT).unwrap();
}

#[test]
fn caller_timeout_does_not_cancel_shutdown_or_its_final_batch() {
    let (memory, reader) = MemorySink::new(Retention::default()).unwrap();
    let gate = Arc::new(Gate::default());
    let (entered, receiver) = mpsc::channel();
    let sink = GatedSink {
        gate: gate.clone(),
        entered: Some(entered),
        memory,
    };
    let (recorder, control) = Builder::for_service("faults", "shutdown")
        .unwrap()
        .config(config())
        .build(sink)
        .unwrap();
    let histogram = metrics::with_local_recorder(&recorder, || metrics::histogram!("duration"));
    histogram.record_many(3.0, 5);
    let first_control = control.clone();
    let first = thread::spawn(move || first_control.shutdown(Duration::from_millis(20)));
    receiver.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        first.join().unwrap(),
        Err(ControlError::Timeout { .. })
    ));
    assert_eq!(control.lifecycle(), Lifecycle::Closing);
    histogram.record(999.0);
    gate.release();
    let report = control.shutdown(WAIT).unwrap();
    assert!(report.is_success(), "{report:?}");
    assert_eq!(reader.get(report.target).unwrap().histogram_samples(), 5);
    assert_eq!(control.diagnostics().closing_rejections, 1);
    assert_eq!(control.lifecycle(), Lifecycle::Closed);
    assert_eq!(control.shutdown(WAIT).unwrap().target, report.target);
}
