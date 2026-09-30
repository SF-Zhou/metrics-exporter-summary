use crate::{
    control::{Command, ControlError, Runtime},
    diagnostics::{add, duration_ns},
    histogram::Accumulator,
    registry::{Scalar, Series, CLOSED},
    writer::{record_drop, Losses, Work},
};
use crossbeam_channel::{Receiver, RecvTimeoutError};
use metrics_summary_core::{Batch, BatchId, MetricValue, Row, MODEL_VERSION};
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
    time::Instant,
};
use tdigest::TDigest;

pub(crate) fn run(runtime: Arc<Runtime>, commands: Receiver<Command>) {
    let interval = runtime.shared.config.collect_interval;
    let mut next = interval.map(|duration| Instant::now() + duration);
    let mut previous_finished = runtime.shared.started_at;
    let mut losses = Losses::default();
    loop {
        if runtime.shared.lifecycle.load(Ordering::Acquire) == CLOSED {
            return;
        }
        if !runtime.shared.running() {
            for command in commands.try_iter() {
                if let Command::Flush(ticket) = command {
                    ticket.complete(Err(ControlError::Closing));
                }
            }
            let ticket = runtime.ensure_shutdown();
            let target = collect(&runtime, &mut previous_finished, &mut losses);
            ticket.set_target(target);
            runtime.queue.barrier(Work::Barrier {
                ticket,
                target,
                losses,
                shutdown: true,
            });
            return;
        }
        let command = match next {
            Some(next) => {
                match commands.recv_timeout(next.saturating_duration_since(Instant::now())) {
                    Ok(command) => Some(command),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => {
                        runtime.shared.close_admission();
                        continue;
                    }
                }
            }
            None => match commands.recv() {
                Ok(command) => Some(command),
                Err(_) => {
                    runtime.shared.close_admission();
                    continue;
                }
            },
        };
        match command {
            Some(Command::Wake) => continue,
            Some(Command::Flush(ticket)) => {
                if !runtime.shared.running() {
                    ticket.complete(Err(ControlError::Closing));
                    continue;
                }
                if Instant::now() >= ticket.deadline {
                    ticket.complete(Err(ControlError::Timeout {
                        target: None,
                        unconfirmed_batches: runtime
                            .shared
                            .diagnostics
                            .queued_batches
                            .load(Ordering::Relaxed),
                    }));
                    continue;
                }
                let target = collect(&runtime, &mut previous_finished, &mut losses);
                ticket.set_target(target);
                runtime.queue.barrier(Work::Barrier {
                    ticket,
                    target,
                    losses,
                    shutdown: false,
                });
            }
            None => {
                collect(&runtime, &mut previous_finished, &mut losses);
            }
        }
        // Manual collection resets the periodic delay. Missed wall-clock periods
        // are never replayed as invented empty windows.
        next = interval.map(|duration| Instant::now() + duration);
    }
}

struct Aggregate {
    series: Arc<Series>,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    digests: Vec<TDigest>,
    invalid: bool,
}

fn collect(runtime: &Runtime, previous_finished: &mut Instant, losses: &mut Losses) -> BatchId {
    let shared = &runtime.shared;
    let born = Instant::now();
    let (epoch, mut shards, mut series) = {
        let mut registry = shared.registry.lock();
        let epoch = registry.epoch;
        registry.epoch = epoch.checked_add(1).expect("collection sequence exhausted");
        (
            epoch,
            registry.shards.values().cloned().collect::<Vec<_>>(),
            registry.series.values().cloned().collect::<Vec<_>>(),
        )
    };
    shards.sort_unstable_by_key(|shard| (shard.series.id, shard.producer.id));
    series.sort_unstable_by_key(|series| series.id);
    let mut swapped = Vec::with_capacity(shards.len());
    let mut retired = Vec::new();
    for shard in &shards {
        let replacement = Accumulator::new(epoch + 1, shared.config.digest_compression);
        let mut state = shard.state.lock();
        // Observe end BEFORE the swap, while holding the producer's write lock.
        // Reading end after the swap could discard a final record in the new state.
        let ended = shard.producer.ended.load(Ordering::Acquire);
        debug_assert_eq!(state.epoch, epoch);
        let old = std::mem::replace(&mut *state, replacement);
        drop(state);
        if ended {
            retired.push((shard.producer.id, shard.series.id));
        }
        if old.count > 0 {
            swapped.push((shard.series.clone(), old));
        }
    }
    let mut rows: Vec<Row> = Vec::with_capacity(series.len());
    for series in &series {
        let value = match &mut *series.scalar.lock() {
            // Close the window under the same lock as producer updates. Even if
            // delivery later fails, these increments never enter another window.
            Scalar::Counter { pending, .. } => Some(MetricValue::CounterDelta {
                delta_value: std::mem::take(pending),
            }),
            Scalar::Gauge(Some(current_value)) => Some(MetricValue::GaugeSnapshot {
                current_value: *current_value,
            }),
            _ => None,
        };
        if let Some(value) = value {
            rows.push(series.row(value));
        }
    }
    if !retired.is_empty() {
        let mut registry = shared.registry.lock();
        for key in retired {
            registry.shards.remove(&key);
        }
        shared
            .diagnostics
            .active_shards
            .store(registry.shards.len() as u64, Ordering::Relaxed);
    }
    drop(shards);
    let mut aggregates: BTreeMap<u64, Aggregate> = BTreeMap::new();
    for (series, mut state) in swapped {
        state.compress();
        let aggregate = aggregates.entry(series.id).or_insert_with(|| Aggregate {
            series,
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            digests: Vec::new(),
            invalid: false,
        });
        aggregate.count = match aggregate.count.checked_add(state.count) {
            Some(count) => count,
            None => {
                aggregate.invalid = true;
                u64::MAX
            }
        };
        aggregate.sum += state.sum;
        aggregate.invalid |= !aggregate.sum.is_finite() || aggregate.count > (1 << 53);
        aggregate.min = aggregate.min.min(state.min.unwrap_or(f64::INFINITY));
        aggregate.max = aggregate.max.max(state.max.unwrap_or(f64::NEG_INFINITY));
        aggregate.digests.push(state.digest);
    }
    for (_, aggregate) in aggregates {
        let value = if aggregate.invalid {
            None
        } else {
            let mut digest = TDigest::merge_digests(aggregate.digests);
            digest.flush();
            let quantiles = (
                digest.estimate_quantile(0.5),
                digest.estimate_quantile(0.9),
                digest.estimate_quantile(0.95),
                digest.estimate_quantile(0.99),
            );
            match quantiles {
                (Some(p50), Some(p90), Some(p95), Some(p99)) => {
                    let value = MetricValue::HistogramSummary {
                        count: aggregate.count,
                        sum: aggregate.sum,
                        min: aggregate.min,
                        p50,
                        p90,
                        p95,
                        p99,
                        max: aggregate.max,
                    };
                    value.validate().ok().map(|()| value)
                }
                _ => None,
            }
        };
        if let Some(value) = value {
            rows.push(aggregate.series.row(value));
        } else {
            losses.rows = losses.rows.saturating_add(1);
            losses.samples = losses.samples.saturating_add(aggregate.count);
            add(&shared.diagnostics.dropped_rows, 1);
            add(
                &shared.diagnostics.dropped_histogram_samples,
                aggregate.count,
            );
            add(&shared.diagnostics.arithmetic_overflows, 1);
        }
    }
    rows.sort_unstable_by_key(|row| row.metric_id);
    let id = BatchId {
        source_session_id: shared.session,
        sequence: epoch + 1,
    };
    // Finish this logical observation window after scanning and summarizing.
    // Use a monotonic clock for its span; wall-clock adjustments only affect
    // the timestamp. Advance even when publication later drops this batch.
    let timestamp = shared.clock.unix_nanos();
    let finished = Instant::now();
    let span_ns = duration_ns(finished.saturating_duration_since(*previous_finished));
    *previous_finished = finished;
    let batch = Arc::new(Batch {
        model_version: MODEL_VERSION,
        id,
        source: shared.source.clone(),
        timestamp,
        duration_ns: span_ns,
        rows,
    });
    add(&shared.diagnostics.collected_batches, 1);
    if batch.validate(&shared.config.validation).is_err()
        || !runtime.queue.push_batch(shared, batch.clone(), born)
    {
        losses.drop_batch(&batch);
        record_drop(shared, &batch);
    }
    let elapsed = born.elapsed();
    shared
        .diagnostics
        .last_collection_ns
        .store(duration_ns(elapsed), Ordering::Relaxed);
    if shared
        .config
        .collect_interval
        .is_some_and(|interval| elapsed > interval)
    {
        add(&shared.diagnostics.collection_overruns, 1);
    }
    id
}
