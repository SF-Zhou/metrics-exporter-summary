use metrics_exporter_summary::{
    metrics, Builder, Config, Control, Lifecycle, MetricValue, SummaryRecorder,
};
use metrics_summary_sink_memory::{MemorySink, Retention, SnapshotReader};
use std::{
    sync::{mpsc, Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(10);

fn setup(config: Config) -> (SummaryRecorder, Control, SnapshotReader) {
    let (sink, reader) = MemorySink::new(Retention {
        max_snapshots: 256,
        max_retained_bytes: 64 * 1024 * 1024,
    })
    .unwrap();
    let (recorder, control) = Builder::for_service("contract-tests", "worker")
        .unwrap()
        .config(Config {
            collect_interval: None,
            ..config
        })
        .build(sink)
        .unwrap();
    (recorder, control, reader)
}

fn histogram_totals(reader: &SnapshotReader) -> (u64, f64) {
    reader
        .after(None, usize::MAX)
        .unwrap()
        .snapshots
        .iter()
        .flat_map(|batch| &batch.rows)
        .fold((0, 0.0), |(count, sum), row| {
            if let MetricValue::HistogramSummary {
                count: n, sum: s, ..
            } = row.value
            {
                (count + n, sum + s)
            } else {
                (count, sum)
            }
        })
}

#[test]
fn canonical_registration_kind_separation_and_hostname() {
    let (recorder, control, reader) = setup(Config::default());
    metrics::with_local_recorder(&recorder, || {
        metrics::histogram!("requests", "tag" => "POST", "pod" => "/").record(2.0);
        metrics::histogram!("requests", "pod" => "/", "tag" => "POST").record(4.0);
        metrics::counter!("requests", "tag" => "POST", "pod" => "/").increment(9);
        metrics::gauge!("active_requests", "pod" => "/", "tag" => "POST").set(7.0);
        metrics::gauge!("requests", "pod" => "/", "tag" => "POST").set(99.0);
        metrics::histogram!("invalid", "tag" => "a", "tag" => "b").record(99.0);
    });
    let report = control.flush(WAIT).unwrap();
    assert!(report.is_success());
    let batch = reader.get(report.target).unwrap();
    assert_eq!(batch.rows.len(), 3);
    assert!(!batch.source.hostname.trim().is_empty());
    assert_eq!(histogram_totals(&reader), (2, 6.0));
    assert_eq!(control.diagnostics().registered_series, 3);
    assert_eq!(control.diagnostics().registrations_rejected, 2);
    assert!(control.shutdown(WAIT).unwrap().is_success());
}

#[test]
fn shared_handles_concurrent_collect_and_thread_exit_conserve_samples() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 4096;
    let (recorder, control, reader) = setup(Config {
        buffer_capacity: 16,
        ..Default::default()
    });
    let (histogram, counter) = metrics::with_local_recorder(&recorder, || {
        (
            metrics::histogram!("latency"),
            metrics::counter!("requests"),
        )
    });
    let start = Arc::new(Barrier::new(THREADS + 1));
    thread::scope(|scope| {
        for producer in 0..THREADS {
            let histogram = histogram.clone();
            let counter = counter.clone();
            let start = start.clone();
            scope.spawn(move || {
                start.wait();
                for _ in 0..PER_THREAD {
                    histogram.record((producer + 1) as f64);
                    counter.increment(1);
                }
            });
        }
        start.wait();
        for _ in 0..16 {
            assert!(control.flush(WAIT).unwrap().is_success());
        }
    });
    let report = control.shutdown(WAIT).unwrap();
    assert!(report.is_success(), "{report:?}");
    let counter_total: u64 = reader
        .after(None, usize::MAX)
        .unwrap()
        .snapshots
        .iter()
        .flat_map(|batch| &batch.rows)
        .filter_map(|row| match row.value {
            MetricValue::CounterDelta { delta_value } => Some(delta_value),
            _ => None,
        })
        .sum();
    assert_eq!(counter_total, (THREADS * PER_THREAD) as u64);
    assert_eq!(
        histogram_totals(&reader),
        (
            (THREADS * PER_THREAD) as u64,
            (PER_THREAD * THREADS * (THREADS + 1) / 2) as f64
        )
    );
    assert_eq!(
        control.diagnostics().accepted_histogram_samples,
        (THREADS * PER_THREAD) as u64
    );
    assert_eq!(control.diagnostics().active_shards, 0);
    assert_eq!(control.lifecycle(), Lifecycle::Closed);
}

#[test]
fn arbitrary_labels_are_preserved_and_source_defaults_normalize_identity() {
    use std::collections::BTreeMap;
    let source = metrics_summary_core::Source {
        application: "labels".into(),
        instance: "default-instance".into(),
        hostname: "default-host".into(),
        attributes: BTreeMap::new(),
    };
    let (sink, reader) = MemorySink::new(Retention::default()).unwrap();
    let (recorder, control) = Builder::new(source)
        .config(Config {
            collect_interval: None,
            ..Default::default()
        })
        .build(sink)
        .unwrap();
    metrics::with_local_recorder(&recorder, || {
        metrics::counter!("requests").increment(1);
        metrics::counter!("requests", "host" => "default-host", "instance" => "default-instance")
            .increment(2);
        metrics::counter!("requests", "host" => "other-host", "instance" => "other-instance", "区域" => "东区", "route/path" => "/", "metricName" => "value").increment(5);
        metrics::counter!("requests", "route" => "/").increment(7);
        metrics::counter!("requests", "Tag" => "read").increment(11);
        metrics::counter!("requests", "tag" => "read").increment(13);
        metrics::counter!("requests", "tag" => "").increment(17);
        metrics::counter!("requests", "host" => "").increment(99);
    });
    let report = control.flush(WAIT).unwrap();
    assert!(report.is_success());
    let batch = reader.get(report.target).unwrap();
    assert_eq!(batch.rows.len(), 6);
    let defaults = batch.rows.iter().find(|r| r.labels.len() == 2).unwrap();
    assert_eq!(defaults.labels["host"], "default-host");
    assert_eq!(defaults.labels["instance"], "default-instance");
    assert_eq!(defaults.value, MetricValue::CounterDelta { delta_value: 3 });
    let overridden = batch
        .rows
        .iter()
        .find(|r| r.labels["host"] == "other-host")
        .unwrap();
    assert_eq!(overridden.labels["instance"], "other-instance");
    assert_eq!(overridden.labels["区域"], "东区");
    assert_eq!(overridden.labels["route/path"], "/");
    assert_eq!(overridden.labels["metricName"], "value");
    assert_eq!(
        overridden.value,
        MetricValue::CounterDelta { delta_value: 5 }
    );
    for (key, value, delta) in [
        ("route", "/", 7),
        ("Tag", "read", 11),
        ("tag", "read", 13),
        ("tag", "", 17),
    ] {
        let row = batch
            .rows
            .iter()
            .find(|row| row.labels.get(key).map(String::as_str) == Some(value))
            .unwrap();
        assert_eq!(row.value, MetricValue::CounterDelta { delta_value: delta });
    }
    assert_eq!(control.diagnostics().registered_series, 6);
    assert_eq!(control.diagnostics().registrations_rejected, 1);
    control.shutdown(WAIT).unwrap();
}

#[test]
fn arbitrary_label_keys_require_no_configuration_but_invalid_keys_are_rejected() {
    let (recorder, control, reader) = setup(Config::default());
    metrics::with_local_recorder(&recorder, || {
        for label in [
            "timestamp",
            "metricName",
            "count",
            "HOST",
            "a-b",
            "a.b",
            "a`b",
            "区域",
            "1name",
        ] {
            metrics::histogram!("latency", label => "same-value").record(2.0);
            metrics::histogram!("latency", label => "same-value").record(4.0);
        }
        for label in ["", " ", "bad\nkey", "bad\tkey", "bad\0key", "bad\u{7f}key"] {
            metrics::histogram!("latency", label => "invalid").record(999.0);
        }
        metrics::histogram!("latency", "route" => "/", "route" => "/").record(999.0);
    });
    let report = control.shutdown(WAIT).unwrap();
    assert!(report.is_success());
    let batch = reader.get(report.target).unwrap();
    assert_eq!(batch.rows.len(), 9);
    assert!(batch.rows.iter().all(|row| row.labels.len() == 3));
    assert_eq!(batch.histogram_samples(), 18);
    assert_eq!(control.diagnostics().registrations_rejected, 7);
}

#[test]
fn inherited_labels_count_toward_registration_limits() {
    let (recorder, control, reader) = setup(Config {
        validation: metrics_summary_core::ValidationLimits {
            max_labels: 2,
            ..Default::default()
        },
        ..Default::default()
    });
    metrics::with_local_recorder(&recorder, || {
        metrics::counter!("requests").increment(1);
        metrics::counter!("requests", "route" => "/").increment(99);
    });
    let report = control.shutdown(WAIT).unwrap();
    assert!(report.is_success());
    assert_eq!(reader.get(report.target).unwrap().rows.len(), 1);
    assert_eq!(control.diagnostics().registrations_rejected, 1);
}

#[test]
fn idle_threads_are_collected_and_exited_threads_release_shard_capacity() {
    let (recorder, control, reader) = setup(Config {
        max_shards: 1,
        ..Default::default()
    });
    let histogram = metrics::with_local_recorder(&recorder, || metrics::histogram!("idle"));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (exit_tx, exit_rx) = mpsc::channel();
    let cloned = histogram.clone();
    let producer = thread::spawn(move || {
        cloned.record(5.0);
        ready_tx.send(()).unwrap();
        exit_rx.recv().unwrap();
    });
    ready_rx.recv_timeout(WAIT).unwrap();
    assert!(control.flush(WAIT).unwrap().is_success());
    assert_eq!(histogram_totals(&reader), (1, 5.0));
    assert_eq!(control.diagnostics().active_shards, 1);
    histogram.record(99.0); // Current thread cannot create a second live shard.
    assert_eq!(control.diagnostics().shards_rejected, 1);
    exit_tx.send(()).unwrap();
    producer.join().unwrap();
    control.flush(WAIT).unwrap();
    assert_eq!(control.diagnostics().active_shards, 0);
    histogram.record(7.0);
    assert!(control.shutdown(WAIT).unwrap().is_success());
    assert_eq!(histogram_totals(&reader), (2, 12.0));
}

#[test]
fn tls_eviction_returns_to_the_original_shard_and_instances_do_not_mix() {
    const SERIES: usize = 1025;
    let (first, first_control, first_reader) = setup(Config {
        max_shards: SERIES,
        ..Default::default()
    });
    let (second, second_control, second_reader) = setup(Config::default());
    let handles = metrics::with_local_recorder(&first, || {
        (0..SERIES)
            .map(|id| metrics::histogram!("many", "uid" => id.to_string()))
            .collect::<Vec<_>>()
    });
    let other = metrics::with_local_recorder(&second, || metrics::histogram!("many", "uid" => "0"));
    for handle in &handles {
        handle.record(1.0);
    }
    other.record(99.0);
    for handle in &handles {
        handle.record(2.0);
    }
    assert_eq!(first_control.diagnostics().active_shards, SERIES as u64);
    assert_eq!(first_control.diagnostics().shards_rejected, 0);
    first_control.shutdown(WAIT).unwrap();
    second_control.shutdown(WAIT).unwrap();
    assert_eq!(
        histogram_totals(&first_reader),
        ((SERIES * 2) as u64, (SERIES * 3) as f64)
    );
    assert_eq!(histogram_totals(&second_reader), (1, 99.0));
    assert_ne!(
        first_reader.latest().unwrap().id.source_session_id,
        second_reader.latest().unwrap().id.source_session_id
    );
}

#[test]
fn record_many_invalid_values_and_arithmetic_overflow_preserve_valid_state() {
    let (recorder, control, reader) = setup(Config::default());
    metrics::with_local_recorder(&recorder, || {
        let repeated = metrics::histogram!("repeated");
        repeated.record_many(7.0, 0);
        repeated.record_many(3.0, 1000);
        repeated.record_many(-1.0, 1);
        for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            repeated.record(invalid);
        }
        let overflow = metrics::histogram!("overflow");
        overflow.record(f64::MAX);
        overflow.record(f64::MAX);
    });
    let report = control.flush(WAIT).unwrap();
    assert!(report.is_success(), "{report:?}");
    let batch = reader.get(report.target).unwrap();
    let repeated = batch
        .rows
        .iter()
        .find(|row| row.name == "repeated")
        .unwrap();
    assert!(matches!(
        repeated.value,
        MetricValue::HistogramSummary {
            count: 1001,
            sum: 2999.0,
            max: 3.0,
            ..
        }
    ));
    let overflow = batch
        .rows
        .iter()
        .find(|row| row.name == "overflow")
        .unwrap();
    assert!(
        matches!(overflow.value, MetricValue::HistogramSummary { count: 1, sum, max, .. } if sum == f64::MAX && max == f64::MAX)
    );
    assert_eq!(control.diagnostics().invalid_samples, 3);
    assert_eq!(control.diagnostics().arithmetic_overflows, 1);
    control.shutdown(WAIT).unwrap();
}

#[test]
fn counter_absolute_and_gauge_updates_are_linearized_and_fit_int64() {
    let (recorder, control, reader) = setup(Config::default());
    let (counter, gauge, invalid, overflow, counter_overflow) =
        metrics::with_local_recorder(&recorder, || {
            (
                metrics::counter!("counter"),
                metrics::gauge!("gauge"),
                metrics::gauge!("uninitialized"),
                metrics::gauge!("overflow"),
                metrics::counter!("counter_overflow"),
            )
        });
    counter.absolute(100);
    counter.absolute(3);
    counter.increment(7);
    gauge.set(10.0);
    thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                for _ in 0..1000 {
                    gauge.increment(3.0);
                    gauge.decrement(2.0);
                    counter.increment(1);
                }
            });
        }
    });
    invalid.set(f64::NAN);
    invalid.set(0.5);
    gauge.set(f64::INFINITY);
    // Arithmetic remains exact even above f64's integer precision range.
    overflow.set((i64::MAX - 1023) as f64);
    overflow.increment(1023.0);
    overflow.increment(1.0);
    counter_overflow.absolute(i64::MAX as u64);
    counter_overflow.increment(1);
    let report = control.flush(WAIT).unwrap();
    assert!(report.is_success());
    let batch = reader.get(report.target).unwrap();
    let find = |name| {
        &batch
            .rows
            .iter()
            .find(|row| row.name == name)
            .unwrap()
            .value
    };
    assert_eq!(
        find("counter"),
        &MetricValue::CounterDelta { delta_value: 8107 }
    );
    assert_eq!(
        find("gauge"),
        &MetricValue::GaugeSnapshot {
            current_value: 8010
        }
    );
    assert_eq!(
        find("overflow"),
        &MetricValue::GaugeSnapshot {
            current_value: i64::MAX
        }
    );
    assert_eq!(
        find("counter_overflow"),
        &MetricValue::CounterDelta {
            delta_value: i64::MAX as u64
        }
    );
    assert!(!batch.rows.iter().any(|row| row.name == "uninitialized"));
    assert_eq!(control.diagnostics().invalid_samples, 3);
    assert_eq!(control.diagnostics().arithmetic_overflows, 2);
    let second = control.flush(WAIT).unwrap();
    let mut expected = batch.rows.clone();
    for row in &mut expected {
        if let MetricValue::CounterDelta { delta_value } = &mut row.value {
            *delta_value = 0;
        }
    }
    assert_eq!(reader.get(second.target).unwrap().rows, expected);

    // Each window is Int64-bounded, while absolute's private baseline can exceed
    // Int64 after many windows. Reject a whole update that would overflow pending.
    counter_overflow.increment(1);
    counter_overflow.absolute(u64::MAX); // would overflow this interval
    counter_overflow.absolute(u64::MAX - 1);
    counter_overflow.increment(1); // pending is already Int64::MAX
    counter.increment(2);
    counter.absolute(i64::MAX as u64);
    let third = reader.get(control.flush(WAIT).unwrap().target).unwrap();
    assert_eq!(
        third
            .rows
            .iter()
            .find(|r| r.name == "counter_overflow")
            .unwrap()
            .value,
        MetricValue::CounterDelta {
            delta_value: i64::MAX as u64
        }
    );
    assert_eq!(
        third
            .rows
            .iter()
            .find(|r| r.name == "counter")
            .unwrap()
            .value,
        MetricValue::CounterDelta {
            delta_value: i64::MAX as u64 - 8107
        }
    );
    assert_eq!(control.diagnostics().arithmetic_overflows, 4);

    counter_overflow.increment(1); // total reaches UInt64::MAX, window contains one
    counter_overflow.increment(1); // logical total must not wrap
    counter_overflow.absolute(u64::MAX);
    overflow.set(i64::MAX as f64); // this f64 is 2^63, outside Int64
    overflow.set(i64::MIN as f64);
    overflow.decrement(1.0); // preserve MIN on overflow
    let final_report = control.shutdown(WAIT).unwrap();
    assert!(final_report.is_success());
    let final_batch = reader.get(final_report.target).unwrap();
    assert_eq!(
        final_batch
            .rows
            .iter()
            .find(|r| r.name == "counter_overflow")
            .unwrap()
            .value,
        MetricValue::CounterDelta { delta_value: 1 }
    );
    assert_eq!(
        final_batch
            .rows
            .iter()
            .find(|r| r.name == "overflow")
            .unwrap()
            .value,
        MetricValue::GaugeSnapshot {
            current_value: i64::MIN
        }
    );
    assert_eq!(control.diagnostics().arithmetic_overflows, 7);
}

#[test]
fn counter_and_histogram_windows_clear_but_absolute_baselines_and_gauges_persist() {
    let (recorder, control, reader) = setup(Config::default());
    let (counter, histogram, gauge) = metrics::with_local_recorder(&recorder, || {
        (
            metrics::counter!("requests"),
            metrics::histogram!("latency"),
            metrics::gauge!("active"),
        )
    });
    let check = |id, delta, samples, active| {
        let batch = reader.get(id).unwrap();
        assert_eq!(batch.histogram_samples(), samples);
        assert_eq!(
            batch
                .rows
                .iter()
                .find(|row| row.name == "requests")
                .unwrap()
                .value,
            MetricValue::CounterDelta { delta_value: delta }
        );
        assert_eq!(
            batch
                .rows
                .iter()
                .find(|row| row.name == "active")
                .unwrap()
                .value,
            MetricValue::GaugeSnapshot {
                current_value: active
            }
        );
    };
    counter.increment(5);
    histogram.record(10.0);
    gauge.increment(1.0);
    let first = control.flush(WAIT).unwrap();
    check(first.target, 5, 1, 1);

    counter.increment(3);
    histogram.record(20.0);
    check(control.flush(WAIT).unwrap().target, 3, 1, 1);
    check(control.flush(WAIT).unwrap().target, 0, 0, 1);

    // The logical total is eight. absolute() adds only the new twelve events,
    // even across empty windows or reordered absolute observations.
    counter.absolute(20);
    counter.absolute(15);
    counter.absolute(20);
    counter.increment(2);
    check(control.flush(WAIT).unwrap().target, 14, 0, 1);
    counter.absolute(20);
    counter.absolute(25);
    gauge.decrement(1.0); // A request may finish several collection windows later.
    let final_report = control.shutdown(WAIT).unwrap();
    assert!(final_report.is_success());
    check(final_report.target, 3, 0, 0);
    check(first.target, 5, 1, 1); // Published batches remain immutable.
}

#[test]
fn late_description_keeps_first_unit_and_description_only_names_are_bounded() {
    let (recorder, control, reader) = setup(Config {
        max_descriptions: 2,
        ..Default::default()
    });
    metrics::with_local_recorder(&recorder, || {
        metrics::histogram!("duration").record(1.0);
        metrics::describe_histogram!("duration", metrics::Unit::Seconds, "request duration");
        metrics::describe_histogram!(
            "duration",
            metrics::Unit::Milliseconds,
            "conflicting duration"
        );
        metrics::describe_counter!("reserved", "description without registration");
        metrics::describe_counter!("rejected", "third descriptor cannot bypass limits");
        metrics::counter!("third").increment(1);
        metrics::counter!("reserved").increment(2);
    });
    let report = control.flush(WAIT).unwrap();
    assert!(report.is_success());
    let batch = reader.get(report.target).unwrap();
    assert_eq!(batch.rows.len(), 2);
    assert_eq!(
        batch
            .rows
            .iter()
            .find(|row| row.name == "duration")
            .unwrap()
            .unit
            .as_deref(),
        Some("seconds")
    );
    assert_eq!(control.diagnostics().unit_conflicts, 1);
    assert_eq!(control.diagnostics().descriptions_rejected, 1);
    assert_eq!(control.diagnostics().registrations_rejected, 1);
    control.shutdown(WAIT).unwrap();
}

#[test]
fn registration_and_description_budgets_leave_existing_series_usable() {
    let config = Config {
        max_series: 100,
        max_description_length: 4,
        validation: metrics_exporter_summary::ValidationLimits {
            max_rows: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let (recorder, control, reader) = setup(config);
    metrics::with_local_recorder(&recorder, || {
        metrics::counter!("accepted1").increment(1);
        metrics::counter!("accepted2").increment(2);
        metrics::counter!("rejected").increment(999);
        metrics::counter!("accepted1").increment(3);
        metrics::describe_counter!("accepted1", "too long");
    });
    let report = control.flush(WAIT).unwrap();
    assert!(report.is_success());
    assert_eq!(reader.get(report.target).unwrap().rows.len(), 2);
    assert_eq!(control.diagnostics().registrations_rejected, 1);
    assert_eq!(control.diagnostics().descriptions_rejected, 1);
    control.shutdown(WAIT).unwrap();
}

#[test]
fn shutdown_barrier_rejects_late_handles_and_remains_idempotent() {
    let (recorder, control, reader) = setup(Config::default());
    let (histogram, counter, gauge) = metrics::with_local_recorder(&recorder, || {
        (
            metrics::histogram!("duration"),
            metrics::counter!("count"),
            metrics::gauge!("load"),
        )
    });
    histogram.record(4.0);
    counter.increment(5);
    gauge.set(6.0);
    let report = control.shutdown(WAIT).unwrap();
    assert!(report.is_success());
    histogram.record(99.0);
    counter.increment(99);
    gauge.increment(99.0);
    metrics::with_local_recorder(&recorder, || {
        metrics::histogram!("new_after_close").record(1.0)
    });
    let repeated = control.shutdown(WAIT).unwrap();
    assert_eq!(repeated.target, report.target);
    assert_eq!(reader.latest().unwrap().id, report.target);
    assert_eq!(histogram_totals(&reader), (1, 4.0));
    assert_eq!(control.diagnostics().closing_rejections, 4);
    assert_eq!(control.lifecycle(), Lifecycle::Closed);
}

#[test]
fn registration_reserves_complete_rows_including_late_units() {
    use metrics_summary_core::{Batch, BatchId, Row, Source, MODEL_VERSION};
    use std::collections::BTreeMap;
    let source = Source {
        application: "a".into(),
        instance: "i".into(),
        hostname: "h".into(),
        attributes: BTreeMap::new(),
    };
    let empty = Batch {
        model_version: MODEL_VERSION,
        id: BatchId {
            source_session_id: uuid::Uuid::new_v4(),
            sequence: 1,
        },
        source: source.clone(),
        timestamp: 0,
        duration_ns: 0,
        rows: vec![],
    };
    let labels: Vec<_> = [
        ("host", "h"),
        ("pod", ""),
        ("instance", "i"),
        ("tag", ""),
        ("thread", ""),
        ("uid", ""),
        ("statusCode", ""),
        ("mount_name", ""),
        ("io", ""),
    ]
    .into_iter()
    .map(|(key, value)| metrics::Label::new(key, value))
    .collect();
    let reserved_row = Row {
        metric_id: 1,
        name: "g0".into(),
        labels: metrics_summary_core::effective_labels(
            &source,
            &labels
                .iter()
                .map(|label| (label.key().to_owned(), label.value().to_owned()))
                .collect(),
            &Default::default(),
        )
        .unwrap(),
        unit: Some("u".repeat(64)),
        value: MetricValue::GaugeSnapshot { current_value: 1 },
    };
    let limit = empty.estimated_bytes() + reserved_row.estimated_bytes();
    let (sink, reader) = MemorySink::new(Retention::default()).unwrap();
    let (recorder, control) = Builder::new(source)
        .config(Config {
            collect_interval: None,
            validation: metrics_exporter_summary::ValidationLimits {
                max_batch_bytes: limit,
                ..Default::default()
            },
            ..Default::default()
        })
        .build(sink)
        .unwrap();
    metrics::with_local_recorder(&recorder, || {
        metrics::gauge!("g0", labels.clone()).set(1.0);
        metrics::gauge!("g1", labels.clone()).set(2.0);
        metrics::describe_gauge!("g0", metrics::Unit::Seconds, "late unit");
    });
    assert_eq!(control.diagnostics().registered_series, 1);
    assert_eq!(control.diagnostics().registrations_rejected, 1);
    for _ in 0..3 {
        let report = control.flush(WAIT).unwrap();
        assert!(
            report.is_success(),
            "accepted permanent gauge must always fit: {report:?}"
        );
        let batch = reader.get(report.target).unwrap();
        assert!(batch.estimated_bytes() <= limit);
        assert_eq!(batch.rows[0].unit.as_deref(), Some("seconds"));
    }
    control.shutdown(WAIT).unwrap();
}

#[test]
fn aggregate_overflow_discards_only_the_invalid_summary_and_accounts_samples() {
    let (recorder, control, reader) = setup(Config::default());
    let histogram = metrics::with_local_recorder(&recorder, || {
        metrics::counter!("healthy").increment(1);
        metrics::histogram!("overflow")
    });
    thread::scope(|scope| {
        for _ in 0..2 {
            scope.spawn(|| histogram.record(f64::MAX));
        }
    });
    let report = control.flush(WAIT).unwrap();
    assert!(!report.is_success());
    assert_eq!(report.dropped_rows, 1);
    assert_eq!(report.dropped_histogram_samples, 2);
    assert_eq!(report.dropped_batches, 0);
    assert_eq!(control.diagnostics().accepted_histogram_samples, 2);
    let batch = reader.get(report.target).unwrap();
    assert_eq!(batch.rows.len(), 1);
    assert_eq!(batch.rows[0].name, "healthy");
    control.shutdown(WAIT).unwrap();
}

#[test]
fn wall_clock_rollback_changes_timestamps_without_reordering_or_invalidating_batches() {
    use std::sync::atomic::{AtomicI64, Ordering};
    struct TestClock(AtomicI64);
    impl metrics_exporter_summary::Clock for TestClock {
        fn unix_nanos(&self) -> i64 {
            self.0.load(Ordering::Relaxed)
        }
    }
    let clock = Arc::new(TestClock(AtomicI64::new(100)));
    let (sink, reader) = MemorySink::new(Retention::default()).unwrap();
    let before_build = Instant::now();
    let (recorder, control) = Builder::for_service("clock", "test")
        .unwrap()
        .clock(clock.clone())
        .config(Config {
            collect_interval: None,
            ..Default::default()
        })
        .build(sink)
        .unwrap();
    let after_build = Instant::now();
    let histogram = metrics::with_local_recorder(&recorder, || metrics::histogram!("duration"));
    histogram.record(1.0);
    clock.0.store(50, Ordering::Relaxed);
    let before_first = Instant::now();
    let first = control.flush(WAIT).unwrap();
    let after_first = Instant::now();
    let first_batch = reader.get(first.target).unwrap();
    assert!(first.is_success());
    assert_eq!(first_batch.timestamp, 50);
    assert!(first_batch.duration_ns as u128 >= before_first.duration_since(after_build).as_nanos());
    assert!(first_batch.duration_ns as u128 <= after_first.duration_since(before_build).as_nanos());
    histogram.record(2.0);
    clock.0.store(-200, Ordering::Relaxed);
    let before_second = Instant::now();
    let second = control.flush(WAIT).unwrap();
    let after_second = Instant::now();
    let second_batch = reader.get(second.target).unwrap();
    assert_eq!(second_batch.timestamp, -200);
    assert!(
        second_batch.duration_ns as u128 >= before_second.duration_since(after_first).as_nanos()
    );
    assert!(
        second_batch.duration_ns as u128 <= after_second.duration_since(before_first).as_nanos()
    );
    let total_span = first_batch.duration_ns as u128 + second_batch.duration_ns as u128;
    assert!(total_span >= before_second.duration_since(after_build).as_nanos());
    assert!(total_span <= after_second.duration_since(before_build).as_nanos());
    assert!(second.target.sequence > first.target.sequence);
    assert_eq!(histogram_totals(&reader), (2, 3.0));
    assert_eq!(reader.latest().unwrap().id, second.target);
    // Even an empty round advances the statistical boundary. A stationary wall
    // clock must not turn its duration into zero or include the previous span.
    let before_empty = Instant::now();
    let empty = control.flush(WAIT).unwrap();
    let after_empty = Instant::now();
    let empty_batch = reader.get(empty.target).unwrap();
    assert!(empty.is_success());
    assert!(empty_batch.rows.is_empty());
    assert_eq!(empty_batch.timestamp, -200);
    assert!(
        empty_batch.duration_ns as u128 >= before_empty.duration_since(after_second).as_nanos()
    );
    assert!(
        empty_batch.duration_ns as u128 <= after_empty.duration_since(before_second).as_nanos()
    );
    let total_span = total_span + empty_batch.duration_ns as u128;
    assert!(total_span >= before_empty.duration_since(after_build).as_nanos());
    assert!(total_span <= after_empty.duration_since(before_build).as_nanos());
    control.shutdown(WAIT).unwrap();
}

#[test]
fn shutdown_racing_producers_conserves_every_accepted_observation() {
    let (recorder, control, reader) = setup(Config::default());
    let histogram = metrics::with_local_recorder(&recorder, || metrics::histogram!("racing"));
    let start = Arc::new(Barrier::new(9));
    thread::scope(|scope| {
        for _ in 0..8 {
            let start = start.clone();
            let histogram = histogram.clone();
            scope.spawn(move || {
                histogram.record(1.0); // Each producer has accepted data before closing.
                start.wait();
                for _ in 0..10_000 {
                    histogram.record(1.0);
                }
            });
        }
        start.wait();
        assert!(control.shutdown(WAIT).unwrap().is_success());
    });
    let diagnostics = control.diagnostics();
    assert_eq!(
        histogram_totals(&reader).0,
        diagnostics.accepted_histogram_samples
    );
    assert_eq!(
        diagnostics.accepted_histogram_samples + diagnostics.closing_rejections,
        8 * 10_001
    );
    assert_eq!(diagnostics.active_shards, 0);
}

#[test]
fn describe_only_byte_budget_cannot_grow_with_dynamic_names() {
    let (recorder, control, reader) = setup(Config {
        max_description_bytes: 270,
        ..Default::default()
    });
    metrics::with_local_recorder(&recorder, || {
        metrics::describe_counter!("a", "first");
        for index in 0..1000 {
            metrics::describe_counter!(format!("dynamic.{index}"), "text");
        }
        metrics::counter!("a").increment(3);
    });
    assert_eq!(control.diagnostics().descriptions_rejected, 1000);
    let report = control.shutdown(WAIT).unwrap();
    assert!(report.is_success());
    let batch = reader.get(report.target).unwrap();
    assert_eq!(batch.rows.len(), 1);
    assert_eq!(
        batch.rows[0].value,
        MetricValue::CounterDelta { delta_value: 3 }
    );
}

#[test]
fn invalid_limits_fail_before_starting_workers_or_allocating_control_queues() {
    for config in [
        Config {
            max_control_requests: usize::MAX,
            ..Default::default()
        },
        Config {
            max_control_requests: 65_537,
            ..Default::default()
        },
        Config {
            max_control_requests: 0,
            ..Default::default()
        },
        Config {
            max_shards: usize::MAX,
            ..Default::default()
        },
        Config {
            queue_max_bytes: 1,
            ..Default::default()
        },
        Config {
            collect_interval: Some(Duration::ZERO),
            ..Default::default()
        },
        Config {
            write_timeout: Duration::ZERO,
            ..Default::default()
        },
        Config {
            retry_deadline: Duration::MAX,
            ..Default::default()
        },
    ] {
        let (sink, _) = MemorySink::new(Retention::default()).unwrap();
        assert!(Builder::for_service("invalid", "config")
            .unwrap()
            .config(config)
            .build(sink)
            .is_err());
    }
}
