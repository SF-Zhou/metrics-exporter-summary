use super::*;
use metrics_summary_core::{MetricValue, Row, MODEL_VERSION};
use std::{collections::BTreeMap, thread, time::Duration};
use uuid::Uuid;

fn batch(session: Uuid, sequence: u64) -> Arc<Batch> {
    Arc::new(Batch {
        model_version: MODEL_VERSION,
        id: BatchId {
            source_session_id: session,
            sequence,
        },
        source: Source {
            application: "test".into(),
            instance: "instance".into(),
            hostname: "host".into(),
            attributes: BTreeMap::new(),
        },
        timestamp: 2,
        duration_ns: 1,
        rows: vec![Row {
            metric_id: 1,
            name: "count".into(),
            labels: BTreeMap::new(),
            unit: None,
            value: MetricValue::CounterDelta {
                delta_value: sequence,
            },
        }],
    })
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

#[test]
fn count_retention_paging_and_explicit_missing_results() {
    let session = Uuid::new_v4();
    let (mut sink, reader) = MemorySink::new(Retention {
        max_snapshots: 2,
        max_retained_bytes: 1_000_000,
    })
    .unwrap();
    let first = batch(session, 1);
    assert_eq!(reader.get(first.id).unwrap_err(), ReadError::NotVisible);
    sink.write(first.clone(), deadline()).unwrap();
    sink.write(batch(session, 3), deadline()).unwrap();
    let page = reader.after(Some(first.id), 100).unwrap();
    assert_eq!(page.snapshots.len(), 1);
    assert_eq!(page.snapshots[0].id.sequence, 3);
    assert!(
        !page.retention_gap,
        "missing sequence 2 was never published, not evicted"
    );
    assert!(matches!(
        reader.get(BatchId {
            source_session_id: session,
            sequence: 2
        }),
        Err(ReadError::NotRetained {
            reason: MissingReason::Unknown
        })
    ));
    sink.write(batch(session, 4), deadline()).unwrap();
    assert!(matches!(
        reader.get(first.id),
        Err(ReadError::NotRetained { .. })
    ));
    let page = reader.after(None, usize::MAX).unwrap();
    assert!(page.retention_gap);
    assert_eq!(page.oldest_retained.unwrap().sequence, 3);
    assert_eq!(page.latest_visible.unwrap().sequence, 4);
    assert_eq!(page.max_evicted_sequence, 1);
    assert_eq!(
        page.snapshots
            .iter()
            .map(|b| b.id.sequence)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    assert!(
        !reader.after(Some(first.id), 1).unwrap().retention_gap,
        "an eviction at the cursor is not after it"
    );
    assert_eq!(
        reader
            .get(BatchId {
                source_session_id: session,
                sequence: 5
            })
            .unwrap_err(),
        ReadError::NotVisible
    );
    assert_eq!(
        reader
            .get(BatchId {
                source_session_id: Uuid::new_v4(),
                sequence: 4
            })
            .unwrap_err(),
        ReadError::WrongSource
    );
    assert!(matches!(
        reader.after(
            Some(BatchId {
                source_session_id: Uuid::new_v4(),
                sequence: 4
            }),
            1
        ),
        Err(ReadError::WrongSource)
    ));
    assert!(matches!(
        reader.after(None, 0),
        Err(ReadError::InvalidLimit)
    ));
    assert_eq!(reader.diagnostics().evicted_snapshots, 1);
    assert_eq!(reader.diagnostics().retention_gap_queries, 1);
}

#[test]
fn bytes_evict_whole_snapshots_and_oversize_rejects_atomically() {
    let session = Uuid::new_v4();
    let first = batch(session, 1);
    let bytes = first.estimated_bytes();
    let (mut sink, reader) = MemorySink::new(Retention {
        max_snapshots: 100,
        max_retained_bytes: bytes * 2 - 1,
    })
    .unwrap();
    sink.write(first.clone(), deadline()).unwrap();
    sink.write(batch(session, 2), deadline()).unwrap();
    let before = reader.diagnostics();
    assert_eq!(before.retained_snapshots, 1);
    assert_eq!(before.retained_bytes, bytes);
    assert_eq!(before.evicted_bytes, bytes as u64);
    let mut large = (*batch(session, 3)).clone();
    large.rows[0].name.reserve(10_000);
    let error = sink.write(Arc::new(large), deadline()).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Permanent);
    assert_eq!(error.outcome, CommitOutcome::NotCommitted);
    assert_eq!(reader.diagnostics(), before);
    assert_eq!(reader.latest().unwrap().id.sequence, 2);
    assert_eq!(first.id.sequence, 1, "reader-held Arc survives eviction");
}

#[test]
fn retries_are_idempotent_but_conflicts_and_unverifiable_old_ids_are_rejected() {
    let session = Uuid::new_v4();
    let first = batch(session, 1);
    let (mut sink, reader) = MemorySink::new(Retention {
        max_snapshots: 1,
        ..Default::default()
    })
    .unwrap();
    sink.write(first.clone(), deadline()).unwrap();
    sink.write(Arc::new((*first).clone()), deadline()).unwrap();
    assert_eq!(reader.diagnostics().published_snapshots, 1);
    assert_eq!(reader.diagnostics().duplicate_writes, 1);
    let mut changed = (*first).clone();
    changed.rows.clear();
    assert_eq!(
        sink.write(Arc::new(changed), deadline()).unwrap_err().kind,
        ErrorKind::Permanent
    );
    assert!(sink.write(batch(Uuid::new_v4(), 2), deadline()).is_err());
    let mut changed_source = (*batch(session, 2)).clone();
    changed_source.source.hostname = "other-host".into();
    assert!(sink.write(Arc::new(changed_source), deadline()).is_err());
    sink.write(batch(session, 2), deadline()).unwrap();
    assert!(sink.write(first, deadline()).is_err());
    assert_eq!(reader.latest().unwrap().id.sequence, 2);
}

#[test]
fn time_metadata_is_preserved_and_ordering_uses_sequence_despite_clock_rollback() {
    let session = Uuid::new_v4();
    let (mut sink, reader) = MemorySink::new(Retention::default()).unwrap();
    for (index, (timestamp, duration_ns)) in [(i64::MAX, 0), (-1, u64::MAX), (i64::MIN, 1)]
        .into_iter()
        .enumerate()
    {
        let mut value = (*batch(session, index as u64 + 1)).clone();
        value.timestamp = timestamp;
        value.duration_ns = duration_ns;
        let published = Arc::new(value);
        sink.write(published.clone(), deadline()).unwrap();
        assert_eq!(reader.latest().unwrap(), published);
        // A retained ID is only idempotent if the time metadata also matches.
        for change_timestamp in [true, false] {
            let mut conflict = (*published).clone();
            if change_timestamp {
                conflict.timestamp = timestamp.wrapping_add(1);
            } else {
                conflict.duration_ns = duration_ns.wrapping_add(1);
            }
            let error = sink.write(Arc::new(conflict), deadline()).unwrap_err();
            assert_eq!(error.kind, ErrorKind::Permanent);
            assert_eq!(error.outcome, CommitOutcome::NotCommitted);
            assert_eq!(reader.latest().unwrap(), published);
        }
    }
    let page = reader.after(None, 3).unwrap();
    assert_eq!(
        page.snapshots
            .iter()
            .map(|value| (value.id.sequence, value.timestamp, value.duration_ns))
            .collect::<Vec<_>>(),
        [(1, i64::MAX, 0), (2, -1, u64::MAX), (3, i64::MIN, 1)]
    );
}

#[test]
fn empty_rounds_are_retained_and_reader_survives_writer() {
    let (mut sink, reader) = MemorySink::new(Retention::default()).unwrap();
    assert_eq!(
        sink.completion_boundary(),
        CompletionBoundary::LocalPublished
    );
    let mut empty = (*batch(Uuid::new_v4(), 1)).clone();
    empty.rows.clear();
    sink.write(Arc::new(empty), deadline()).unwrap();
    sink.flush(deadline()).unwrap();
    let clone = reader.clone();
    drop(sink);
    drop(reader);
    assert!(clone.diagnostics().writer_closed);
    assert!(clone.latest().unwrap().rows.is_empty());
    assert_eq!(clone.diagnostics().retained_snapshots, 1);
}

#[test]
fn invalid_first_publication_does_not_bind_source() {
    let (mut sink, reader) = MemorySink::new(Retention::default()).unwrap();
    let mut invalid = (*batch(Uuid::new_v4(), 1)).clone();
    invalid.source.hostname.clear();
    assert!(sink.write(Arc::new(invalid), deadline()).is_err());
    let valid = batch(Uuid::new_v4(), 1);
    sink.write(valid.clone(), deadline()).unwrap();
    assert_eq!(reader.latest().unwrap().id, valid.id);
}

#[test]
fn deadlines_include_lock_waits_and_prevent_publication() {
    let (mut sink, reader) = MemorySink::new(Retention::default()).unwrap();
    let value = batch(Uuid::new_v4(), 1);
    assert_eq!(
        sink.write(value.clone(), Instant::now()).unwrap_err().kind,
        ErrorKind::Timeout
    );
    assert_eq!(
        sink.flush(Instant::now()).unwrap_err().kind,
        ErrorKind::Timeout
    );
    let state = reader.shared.state.lock();
    thread::scope(|scope| {
        let attempt = scope.spawn(|| sink.write(value, Instant::now() + Duration::from_millis(10)));
        assert_eq!(
            attempt.join().unwrap().unwrap_err().kind,
            ErrorKind::Timeout
        );
    });
    drop(state);
    assert!(reader.latest().is_none());
}

#[test]
fn expiration_after_retention_planning_preserves_all_history_and_diagnostics() {
    let session = Uuid::new_v4();
    let (mut sink, reader) = MemorySink::new(Retention {
        max_snapshots: 2,
        ..Default::default()
    })
    .unwrap();
    sink.write(batch(session, 1), deadline()).unwrap();
    sink.write(batch(session, 2), deadline()).unwrap();
    let before = reader.diagnostics();
    let history = reader.after(None, 10).unwrap().snapshots;
    let incoming = batch(session, 3);

    let mut state = reader.shared.state.lock();
    let publication = state
        .prepare_publication(
            &incoming,
            incoming.estimated_bytes(),
            reader.shared.retention,
            deadline(),
        )
        .unwrap();
    assert_eq!(publication.evicted_snapshots, 1);
    assert_eq!(publication.snapshots.front().unwrap().batch.id.sequence, 2);
    // Deterministically expire between planning and commit, without relying on
    // a short sleep, scheduler timing, or allocator/deallocation speed.
    let error = state.publish(publication, Instant::now()).err().unwrap();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert_eq!(error.outcome, CommitOutcome::NotCommitted);
    drop(state);

    assert_eq!(reader.diagnostics(), before);
    assert_eq!(reader.after(None, 10).unwrap().snapshots, history);
    sink.write(incoming, deadline()).unwrap();
    assert_eq!(reader.latest().unwrap().id.sequence, 3);
    assert_eq!(reader.diagnostics().evicted_snapshots, 1);
}

#[test]
fn publication_releases_the_history_lock_before_retired_batches_are_reclaimed() {
    let session = Uuid::new_v4();
    let (mut sink, reader) = MemorySink::new(Retention {
        max_snapshots: 1,
        ..Default::default()
    })
    .unwrap();
    let first = batch(session, 1);
    let first_weak = Arc::downgrade(&first);
    sink.write(first, deadline()).unwrap();
    let incoming = batch(session, 2);

    let mut state = reader.shared.state.lock();
    let publication = state
        .prepare_publication(
            &incoming,
            incoming.estimated_bytes(),
            reader.shared.retention,
            deadline(),
        )
        .unwrap();
    let retired = state.publish(publication, deadline()).unwrap();
    drop(state);

    // The new complete snapshot and eviction counters are already readable,
    // while the evicted batch's final strong reference has not been released.
    assert_eq!(reader.latest().unwrap().id, incoming.id);
    assert_eq!(reader.diagnostics().evicted_snapshots, 1);
    assert!(first_weak.upgrade().is_some());
    drop(retired);
    assert!(first_weak.upgrade().is_none());
}

#[test]
fn concurrent_readers_only_observe_whole_ordered_batches() {
    let session = Uuid::new_v4();
    let (mut sink, reader) = MemorySink::new(Retention {
        max_snapshots: 8,
        ..Default::default()
    })
    .unwrap();
    thread::scope(|scope| {
        scope.spawn(|| {
            for sequence in 1..=1000 {
                let mut value = (*batch(session, sequence)).clone();
                value.rows = (1..=16)
                    .map(|metric_id| Row {
                        metric_id,
                        name: format!("metric.{metric_id}"),
                        labels: BTreeMap::new(),
                        unit: None,
                        value: MetricValue::CounterDelta {
                            delta_value: sequence,
                        },
                    })
                    .collect();
                sink.write(Arc::new(value), deadline()).unwrap();
            }
        });
        for _ in 0..1000 {
            let page = reader.after(None, 8).unwrap();
            assert!(page
                .snapshots
                .windows(2)
                .all(|pair| pair[0].id.sequence < pair[1].id.sequence));
            for snapshot in page.snapshots {
                assert_eq!(snapshot.rows.len(), 16);
                assert!(snapshot.rows.iter().all(|row| row.value
                    == MetricValue::CounterDelta {
                        delta_value: snapshot.id.sequence
                    }));
            }
        }
    });
    assert_eq!(reader.latest().unwrap().id.sequence, 1000);
    assert_eq!(reader.diagnostics().published_snapshots, 1000);
    assert_eq!(reader.diagnostics().evicted_snapshots, 992);
}

#[test]
fn invalid_resource_limits_fail_at_construction() {
    assert!(MemorySink::new(Retention {
        max_snapshots: 0,
        max_retained_bytes: 1024
    })
    .is_err());
    assert!(MemorySink::new(Retention {
        max_snapshots: 1,
        max_retained_bytes: 0
    })
    .is_err());
    assert!(MemorySink::with_validation_limits(
        Retention::default(),
        ValidationLimits {
            max_batch_bytes: 0,
            ..Default::default()
        }
    )
    .is_err());
}

#[test]
fn arbitrary_labels_are_preserved_without_schema_configuration() {
    let mut value = (*batch(Uuid::new_v4(), 1)).clone();
    for (key, label) in [
        ("region", "east"),
        ("区域", "东区"),
        ("route/path", "/"),
        ("metricName", "label-value"),
    ] {
        value.rows[0].labels.insert(key.into(), label.into());
    }
    let value = Arc::new(value);
    let (mut sink, reader) = MemorySink::new(Retention::default()).unwrap();
    sink.write(value.clone(), deadline()).unwrap();
    assert_eq!(reader.latest().unwrap(), value);
    let mut invalid = (*value).clone();
    invalid.id.sequence += 1;
    invalid.rows[0]
        .labels
        .insert("bad\nkey".into(), "value".into());
    assert_eq!(
        sink.write(Arc::new(invalid), deadline())
            .unwrap_err()
            .outcome,
        CommitOutcome::NotCommitted
    );
    assert_eq!(reader.latest().unwrap(), value);
}

#[test]
fn counter_intervals_remain_independent_in_history_including_zero_and_retries() {
    let session = Uuid::new_v4();
    let (mut sink, reader) = MemorySink::new(Retention::default()).unwrap();
    for (index, delta_value) in [5, 0, 3].into_iter().enumerate() {
        let mut value = (*batch(session, index as u64 + 1)).clone();
        value.rows[0].value = MetricValue::CounterDelta { delta_value };
        let value = Arc::new(value);
        sink.write(value.clone(), deadline()).unwrap();
        sink.write(value, deadline()).unwrap();
        assert_eq!(
            reader.latest().unwrap().rows[0].value,
            MetricValue::CounterDelta { delta_value }
        );
    }
    let history = reader.after(None, 3).unwrap();
    assert_eq!(history.snapshots.len(), 3);
    let deltas: Vec<_> = history
        .snapshots
        .iter()
        .map(|batch| {
            let MetricValue::CounterDelta { delta_value } = batch.rows[0].value else {
                panic!("expected counter delta")
            };
            delta_value
        })
        .collect();
    assert_eq!(deltas, [5, 0, 3]);
    assert_eq!(deltas.iter().sum::<u64>(), 8);
    assert_eq!(reader.diagnostics().duplicate_writes, 3);
}

#[test]
fn complete_distribution_and_signed_gauge_snapshots_survive_publication() {
    let session = Uuid::new_v4();
    let (mut sink, reader) = MemorySink::new(Retention::default()).unwrap();
    for (index, current_value) in [i64::MIN, 9_007_199_254_740_993, i64::MAX]
        .into_iter()
        .enumerate()
    {
        let mut value = (*batch(session, index as u64 + 1)).clone();
        value.rows[0].value = MetricValue::GaugeSnapshot { current_value };
        let mut distribution = value.rows[0].clone();
        distribution.metric_id = 2;
        distribution.value = MetricValue::HistogramSummary {
            count: 2,
            sum: 3.0,
            min: 1.0,
            p50: 1.0,
            p90: 2.0,
            p95: 2.0,
            p99: 2.0,
            max: 2.0,
        };
        value.rows.push(distribution);
        let value = Arc::new(value);
        sink.write(value.clone(), deadline()).unwrap();
        assert_eq!(reader.latest().unwrap(), value);
    }
}
