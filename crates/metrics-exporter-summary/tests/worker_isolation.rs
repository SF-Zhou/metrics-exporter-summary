use metrics_exporter_summary::{metrics, Builder, Config, Lifecycle, SummaryRecorder};
use metrics_summary_core::{Batch, CompletionBoundary, MetricValue, Sink, WriteError};
use metrics_summary_sink_memory::{MemorySink, Retention};
use std::{
    sync::{mpsc, Arc, Mutex, OnceLock},
    thread,
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(10);

#[test]
fn repeated_thread_churn_reclaims_all_shards_and_conserves_every_round() {
    const ROUNDS: usize = 32;
    const THREADS: usize = 4;
    const SERIES: usize = 4;
    const SAMPLES_PER_PHASE: usize = 17;
    let (sink, reader) = MemorySink::new(Retention {
        max_snapshots: ROUNDS * 2 + 1,
        max_retained_bytes: 8 * 1024 * 1024,
    })
    .unwrap();
    let (recorder, control) = Builder::for_service("churn", "worker")
        .unwrap()
        .config(Config {
            collect_interval: None,
            max_shards: THREADS * SERIES,
            buffer_capacity: 8,
            ..Default::default()
        })
        .build(sink)
        .unwrap();
    let handles = metrics::with_local_recorder(&recorder, || {
        (0..SERIES)
            .map(|series| metrics::histogram!("churn", "uid" => series.to_string()))
            .collect::<Vec<_>>()
    });
    let mut expected_sum = 0.0;
    for round in 0..ROUNDS {
        let (ready_tx, ready_rx) = mpsc::channel();
        let mut producers = Vec::new();
        let mut releases = Vec::new();
        let phase_sum = (0..THREADS)
            .flat_map(|lane| {
                (0..SERIES).map(move |series| {
                    ((round * 100 + lane * 10 + series + 1) * SAMPLES_PER_PHASE) as f64
                })
            })
            .sum::<f64>();
        for lane in 0..THREADS {
            let (release_tx, release_rx) = mpsc::channel();
            releases.push(release_tx);
            let ready_tx = ready_tx.clone();
            let handles = handles.clone();
            producers.push(thread::spawn(move || {
                for phase in 0..2 {
                    for (series, handle) in handles.iter().enumerate() {
                        handle.record_many(
                            (round * 100 + lane * 10 + series + 1) as f64,
                            SAMPLES_PER_PHASE,
                        );
                    }
                    if phase == 0 {
                        ready_tx.send(()).unwrap();
                        release_rx.recv_timeout(WAIT).unwrap();
                    }
                }
            }));
        }
        drop(ready_tx);
        for _ in 0..THREADS {
            ready_rx.recv_timeout(WAIT).unwrap();
        }

        // All producers stay alive through this collection. Their empty shards
        // must remain reusable by phase two on those same OS threads.
        let first = control.flush(WAIT).unwrap();
        let live_shards = control.diagnostics().active_shards;
        for release in releases {
            release.send(()).unwrap();
        }
        for producer in producers {
            producer.join().unwrap();
        }
        assert!(first.is_success(), "round {round}: {first:?}");
        assert_eq!(live_shards, (THREADS * SERIES) as u64);

        // Joining establishes that the TLS destructors have finished. This
        // collection must drain their tails and return the entire shard quota.
        let last = control.flush(WAIT).unwrap();
        assert!(last.is_success(), "round {round}: {last:?}");
        for report in [first, last] {
            let batch = reader.get(report.target).unwrap();
            assert_eq!(batch.rows.len(), SERIES);
            assert_eq!(
                batch.histogram_samples(),
                (THREADS * SERIES * SAMPLES_PER_PHASE) as u64
            );
            let actual_sum: f64 = batch
                .rows
                .iter()
                .map(|row| match row.value {
                    MetricValue::HistogramSummary { sum, .. } => sum,
                    _ => panic!("unexpected scalar"),
                })
                .sum();
            assert_eq!(actual_sum, phase_sum);
        }
        expected_sum += phase_sum * 2.0;
        let diagnostics = control.diagnostics();
        assert_eq!(diagnostics.active_shards, 0, "round {round}");
        assert_eq!(diagnostics.registered_series, SERIES as u64);
        assert_eq!(diagnostics.shards_rejected, 0, "round {round}");
        assert_eq!(diagnostics.tls_rejections, 0, "round {round}");
        assert_eq!(diagnostics.dropped_histogram_samples, 0);
        assert_eq!(
            diagnostics.accepted_histogram_samples,
            ((round + 1) * THREADS * SERIES * SAMPLES_PER_PHASE * 2) as u64
        );
    }
    assert!(control.shutdown(WAIT).unwrap().is_success());
    let history = reader.after(None, usize::MAX).unwrap();
    assert!(!history.retention_gap);
    assert_eq!(history.snapshots.len(), ROUNDS * 2 + 1);
    let mut count = 0;
    let mut sum = 0.0;
    for batch in &history.snapshots {
        for row in &batch.rows {
            if let MetricValue::HistogramSummary {
                count: row_count,
                sum: row_sum,
                ..
            } = row.value
            {
                count += row_count;
                sum += row_sum;
            }
        }
    }
    assert_eq!(
        count,
        (ROUNDS * THREADS * SERIES * SAMPLES_PER_PHASE * 2) as u64
    );
    assert_eq!(sum, expected_sum);
    assert_eq!(control.lifecycle(), Lifecycle::Closed);
}

struct Instrumentation {
    recorder: SummaryRecorder,
    histogram: metrics::Histogram,
    counter: metrics::Counter,
    gauge: metrics::Gauge,
}

struct InstrumentedSink {
    memory: MemorySink,
    instrumentation: Arc<OnceLock<Instrumentation>>,
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl InstrumentedSink {
    fn instrument(&self, phase: &'static str) {
        let inputs = self.instrumentation.get().unwrap();
        // Cached handles bypass registration entirely. Suppression must apply
        // at the record/update entry point as well as at new registration.
        inputs.histogram.record(1000.0);
        inputs.histogram.record_many(2000.0, 3);
        inputs.counter.increment(1000);
        inputs.counter.absolute(2000);
        inputs.gauge.set(1000.0);
        inputs.gauge.increment(1000.0);
        inputs.gauge.decrement(1.0);
        metrics::with_local_recorder(&inputs.recorder, || {
            metrics::histogram!("sink.histogram", "tag" => phase).record(3000.0);
            metrics::counter!("sink.counter", "tag" => phase).increment(3000);
            metrics::gauge!("sink.gauge", "tag" => phase).set(3000.0);
            metrics::describe_histogram!("caller.histogram", metrics::Unit::Seconds, "nested");
        });
        self.calls.lock().unwrap().push(phase);
    }
}

impl Sink for InstrumentedSink {
    fn completion_boundary(&self) -> CompletionBoundary {
        CompletionBoundary::LocalPublished
    }
    fn write(&mut self, batch: Arc<Batch>, deadline: Instant) -> Result<(), WriteError> {
        self.instrument("write");
        self.memory.write(batch, deadline)
    }
    fn flush(&mut self, deadline: Instant) -> Result<(), WriteError> {
        self.instrument("flush");
        self.memory.flush(deadline)
    }
}

#[test]
fn sink_instrumentation_cannot_feed_back_through_cached_or_new_handles() {
    let (memory, reader) = MemorySink::new(Retention::default()).unwrap();
    let instrumentation = Arc::new(OnceLock::new());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (recorder, control) = Builder::for_service("reentry", "worker")
        .unwrap()
        .config(Config {
            collect_interval: None,
            ..Default::default()
        })
        .build(InstrumentedSink {
            memory,
            instrumentation: instrumentation.clone(),
            calls: calls.clone(),
        })
        .unwrap();
    let (histogram, counter, gauge) = metrics::with_local_recorder(&recorder, || {
        (
            metrics::histogram!("caller.histogram"),
            metrics::counter!("caller.counter"),
            metrics::gauge!("caller.gauge"),
        )
    });
    assert!(instrumentation
        .set(Instrumentation {
            recorder,
            histogram: histogram.clone(),
            counter: counter.clone(),
            gauge: gauge.clone(),
        })
        .is_ok());
    histogram.record(1.0);
    counter.increment(3);
    gauge.set(7.0);
    for round in 0..3 {
        let report = control.flush(WAIT).unwrap();
        assert!(report.is_success(), "{report:?}");
        let batch = reader.get(report.target).unwrap();
        assert_eq!(batch.rows.len(), if round == 0 { 3 } else { 2 });
        for row in &batch.rows {
            assert!(
                row.unit.is_none(),
                "nested describe must also be suppressed"
            );
            match row.value {
                MetricValue::HistogramSummary { count, sum, .. } => {
                    assert_eq!((count, sum), (1, 1.0));
                }
                MetricValue::CounterDelta { delta_value } => {
                    assert_eq!(delta_value, if round == 0 { 3 } else { 0 });
                }
                MetricValue::GaugeSnapshot { current_value } => assert_eq!(current_value, 7),
            }
        }
        let diagnostics = control.diagnostics();
        assert_eq!(diagnostics.registered_series, 3);
        assert_eq!(diagnostics.active_shards, 1);
        assert_eq!(diagnostics.accepted_histogram_samples, 1);
        assert_eq!(diagnostics.registrations_rejected, 0);
        assert_eq!(diagnostics.descriptions_rejected, 0);
        assert_eq!(diagnostics.worker_panics, 0);
    }

    // The writer's suppression must not disable the calling business thread.
    histogram.record(2.0);
    counter.increment(4);
    gauge.set(8.0);
    let report = control.shutdown(WAIT).unwrap();
    assert!(report.is_success(), "{report:?}");
    let batch = reader.get(report.target).unwrap();
    assert_eq!(batch.rows.len(), 3);
    assert_eq!(batch.histogram_samples(), 1);
    for row in &batch.rows {
        assert!(row.unit.is_none());
        match row.value {
            MetricValue::HistogramSummary { count, sum, .. } => assert_eq!((count, sum), (1, 2.0)),
            MetricValue::CounterDelta { delta_value } => assert_eq!(delta_value, 4),
            MetricValue::GaugeSnapshot { current_value } => assert_eq!(current_value, 8),
        }
    }
    assert_eq!(control.diagnostics().accepted_histogram_samples, 2);
    assert_eq!(control.lifecycle(), Lifecycle::Closed);
    assert_eq!(
        *calls.lock().unwrap(),
        ["write", "flush", "write", "flush", "write", "flush", "write", "flush"]
    );
}
