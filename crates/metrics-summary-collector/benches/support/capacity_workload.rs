//! Reproducible capacity acceptance workload. Run in release mode with
//! `cargo bench -p metrics-exporter-summary --bench capacity`.

use metrics_exporter_summary::{metrics, Builder, Config};
use metrics_summary_core::{Batch, CompletionBoundary, Sink, WriteError};
use serde_json::json;
use std::{
    fs,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier, Mutex, OnceLock,
    },
    thread,
    time::{Duration, Instant},
};

const THREADS: usize = 200;
const SERIES: usize = 1000;
const RATE: usize = 100_000;
const SECONDS: usize = 30;

fn quantiles(values: &mut [u64]) -> serde_json::Value {
    values.sort_unstable();
    let at = |q: f64| values[((values.len() - 1) as f64 * q).ceil() as usize];
    json!({"count": values.len(), "p50_ns": at(0.5), "p99_ns": at(0.99), "max_ns": values[values.len() - 1]})
}

fn proc_kib(field: &str) -> u64 {
    fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|line| {
            let rest = line.strip_prefix(field)?;
            rest.split_whitespace().next()?.parse().ok()
        })
        .unwrap_or(0)
}

fn cpu_ticks() -> u64 {
    let stat = fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let values: Vec<_> = stat
        .rsplit_once(')')
        .map(|(_, suffix)| suffix.split_whitespace().collect())
        .unwrap_or_default();
    [11, 12]
        .iter()
        .map(|index| {
            values
                .get(*index)
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0)
        })
        .sum()
}

fn sleep_until(deadline: Instant) {
    if let Some(delay) = deadline.checked_duration_since(Instant::now()) {
        thread::sleep(delay);
    }
}

fn observation(lane: usize, position: usize) -> f64 {
    // SplitMix64 gives reproducible variation across both threads and rounds.
    // Values follow a 5 ms exponential tail with about 0.1% one-second outliers.
    let mut bits = (lane as u64 * 1_000_003 + position as u64).wrapping_add(0x9e3779b97f4a7c15);
    bits = (bits ^ (bits >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    bits = (bits ^ (bits >> 27)).wrapping_mul(0x94d049bb133111eb);
    bits ^= bits >> 31;
    let uniform = ((bits >> 11) + 1) as f64 / (1_u64 << 53) as f64;
    -uniform.ln() * 5_000_000.0
        + if bits & 1023 == 0 {
            1_000_000_000.0
        } else {
            0.0
        }
}

#[derive(Default)]
struct SinkMeasurements {
    samples: u64,
    rows: u64,
    bytes: u64,
    hostname: String,
    write_ns: Vec<u64>,
    batches: Vec<serde_json::Value>,
}

struct ObservedSink<S> {
    inner: S,
    measurements: Arc<Mutex<SinkMeasurements>>,
}
impl<S: Sink> Sink for ObservedSink<S> {
    fn completion_boundary(&self) -> CompletionBoundary {
        self.inner.completion_boundary()
    }
    fn write(&mut self, batch: Arc<Batch>, deadline: Instant) -> Result<(), WriteError> {
        let started = Instant::now();
        let result = self.inner.write(batch.clone(), deadline);
        let elapsed = started.elapsed().as_nanos() as u64;
        let mut measurements = self.measurements.lock().unwrap();
        measurements.write_ns.push(elapsed);
        measurements.batches.push(json!({"sequence": batch.id.sequence, "rows": batch.rows.len(), "samples": batch.histogram_samples(), "write_ns": elapsed, "success": result.is_ok()}));
        if result.is_ok() {
            measurements.samples += batch.histogram_samples();
            measurements.rows += batch.rows.len() as u64;
            measurements.bytes += batch.estimated_bytes() as u64;
            if measurements.hostname.is_empty() {
                measurements.hostname = batch.source.hostname.clone();
            }
        }
        result
    }
    fn flush(&mut self, deadline: Instant) -> Result<(), WriteError> {
        self.inner.flush(deadline)
    }
}

/// Runs the identical workload against any writer-owned sink, inspecting only its
/// promised completion boundary. External storage must be verified separately.
pub fn run<S: Sink>(sink: S, application: &str) -> serde_json::Value {
    let metric_labels = std::env::var("SUMMARY_BENCH_LABELS")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("SUMMARY_BENCH_LABELS must be 0 or 2")
        })
        .unwrap_or(0);
    assert!(
        matches!(metric_labels, 0 | 2),
        "SUMMARY_BENCH_LABELS must be 0 or 2"
    );
    let measurements = Arc::new(Mutex::new(SinkMeasurements::default()));
    let sink = ObservedSink {
        inner: sink,
        measurements: measurements.clone(),
    };
    let config = Config {
        max_shards: THREADS * SERIES,
        buffer_capacity: 32,
        collect_interval: Some(Duration::from_secs(10)),
        ..Default::default()
    };
    let (recorder, control) = Builder::for_service(application, "200x1000")
        .unwrap()
        .config(config)
        .build(sink)
        .unwrap();
    let mut registration_ns = Vec::with_capacity(SERIES);
    let registration_start = Instant::now();
    let handles: Arc<Vec<_>> = Arc::new(metrics::with_local_recorder(&recorder, || {
        (0..SERIES)
            .map(|series| {
                let started = Instant::now();
                let handle = if metric_labels == 2 {
                    metrics::histogram!(format!("capacity.histogram.{series}"), "uid" => series.to_string(), "tag" => "read")
                } else {
                    metrics::histogram!(format!("capacity.histogram.{series}"))
                };
                registration_ns.push(started.elapsed().as_nanos() as u64);
                handle
            })
            .collect()
    }));
    let registration_elapsed = registration_start.elapsed();
    let before_threads_rss_kib = proc_kib("VmRSS:");
    let warmup_barrier = Arc::new(Barrier::new(THREADS + 1));
    let start_barrier = Arc::new(Barrier::new(THREADS + 1));
    let start_time = Arc::new(OnceLock::<Instant>::new());
    let warmup_start = Instant::now();
    let mut producers = Vec::with_capacity(THREADS);
    for lane in 0..THREADS {
        let handles = handles.clone();
        let warmup_barrier = warmup_barrier.clone();
        let start_barrier = start_barrier.clone();
        let start_time = start_time.clone();
        producers.push(
            thread::Builder::new()
                .name(format!("business-{lane}"))
                .spawn(move || {
                    let per_thread = RATE / THREADS * SECONDS;
                    let mut shard_creation_ns = Vec::with_capacity(SERIES);
                    let mut record_ns = Vec::with_capacity(per_thread);
                    let mut max_schedule_lag_ns = 0_u64;
                    for (series, handle) in handles.iter().enumerate() {
                        let value = observation(lane, series);
                        let started = Instant::now();
                        handle.record(value);
                        shard_creation_ns.push(started.elapsed().as_nanos() as u64);
                    }
                    warmup_barrier.wait();
                    start_barrier.wait();
                    let start = *start_time.get().unwrap();
                    let period_ns = 1_000_000_000_u64 / (RATE / THREADS) as u64;
                    // Stagger lanes over one period, avoiding an artificial 200-thread
                    // wakeup burst while preserving the aggregate 100,000/s schedule.
                    let offset_ns = period_ns * lane as u64 / THREADS as u64;
                    for sample in 0..per_thread {
                        let due =
                            start + Duration::from_nanos(period_ns * sample as u64 + offset_ns);
                        sleep_until(due);
                        max_schedule_lag_ns = max_schedule_lag_ns
                            .max(Instant::now().saturating_duration_since(due).as_nanos() as u64);
                        let series = sample % SERIES;
                        let value = observation(lane, sample + SERIES);
                        let started = Instant::now();
                        handles[series].record(value);
                        record_ns.push(started.elapsed().as_nanos() as u64);
                    }
                    (
                        shard_creation_ns,
                        record_ns,
                        max_schedule_lag_ns,
                        Instant::now().saturating_duration_since(start),
                    )
                })
                .unwrap(),
        );
    }
    warmup_barrier.wait();
    let warmup_elapsed = warmup_start.elapsed();
    let warmup_flush_started = Instant::now();
    let warmup_report = control.flush(Duration::from_secs(60)).unwrap();
    let warmup_flush_ns = warmup_flush_started.elapsed().as_nanos() as u64;
    assert!(warmup_report.is_success(), "warmup loss: {warmup_report:?}");
    assert_eq!(
        control.diagnostics().active_shards,
        (THREADS * SERIES) as u64
    );
    let after_warmup_rss_kib = proc_kib("VmRSS:");
    let running = Arc::new(AtomicBool::new(true));
    let monitor_running = running.clone();
    let monitor_control = control.clone();
    let monitor = thread::spawn(move || {
        let mut collections = Vec::new();
        let mut previous = 0;
        let mut peak_rss = 0;
        let mut max_active_shards = 0;
        let mut max_queue_batches = 0;
        while monitor_running.load(Ordering::Relaxed) {
            let diagnostics = monitor_control.diagnostics();
            if diagnostics.collected_batches != previous {
                previous = diagnostics.collected_batches;
                collections.push(diagnostics.last_collection_ns);
            }
            max_active_shards = max_active_shards.max(diagnostics.active_shards);
            max_queue_batches = max_queue_batches.max(diagnostics.queued_batches);
            peak_rss = peak_rss.max(proc_kib("VmRSS:"));
            thread::sleep(Duration::from_millis(10));
        }
        (collections, peak_rss, max_active_shards, max_queue_batches)
    });
    let ticks_before = cpu_ticks();
    start_time
        .set(Instant::now() + Duration::from_millis(100))
        .unwrap();
    start_barrier.wait();
    let mut shard_creation_ns = Vec::with_capacity(THREADS * SERIES);
    let mut record_ns = Vec::with_capacity(RATE * SECONDS);
    let mut max_schedule_lag_ns = 0;
    let mut elapsed = Duration::ZERO;
    for producer in producers {
        let (mut creation, mut records, lag, duration) = producer.join().unwrap();
        shard_creation_ns.append(&mut creation);
        record_ns.append(&mut records);
        max_schedule_lag_ns = max_schedule_lag_ns.max(lag);
        elapsed = elapsed.max(duration);
    }
    let ticks_after = cpu_ticks();
    let shutdown_started = Instant::now();
    let report = control.shutdown(Duration::from_secs(60)).unwrap();
    let shutdown_elapsed = shutdown_started.elapsed();
    running.store(false, Ordering::Relaxed);
    let (mut collection_ns, sampled_peak_rss_kib, max_active_shards, max_queue_batches) =
        monitor.join().unwrap();
    collection_ns.push(control.diagnostics().last_collection_ns);
    let mut measurements = measurements.lock().unwrap();
    let published_samples = measurements.samples;
    let expected_samples = (THREADS * SERIES + RATE * SECONDS) as u64;
    let diagnostics = control.diagnostics();
    assert_eq!(
        published_samples, expected_samples,
        "accepted samples must be conserved"
    );
    assert_eq!(diagnostics.accepted_histogram_samples, expected_samples);
    assert!(report.is_success(), "delivery loss: {report:?}");
    assert_eq!(diagnostics.shards_rejected, 0);
    assert_eq!(diagnostics.registrations_rejected, 0);
    assert_eq!(diagnostics.write_failures, 0);
    assert_eq!(diagnostics.retries, 0);
    json!({
        "source_session_id": recorder.source_session_id().to_string(),
        "source_completion_boundary": report.completion_boundary,
        "sink_write_calls": quantiles(&mut measurements.write_ns),
        "source_flush_ns": [warmup_flush_ns, shutdown_elapsed.as_nanos() as u64],
        "output_rows": measurements.rows, "output_estimated_bytes": measurements.bytes, "write_batches": measurements.batches,
        "workload": {"threads": THREADS, "series": SERIES, "thread_series_combinations": THREADS * SERIES, "target_qps": RATE, "scheduled_seconds": SECONDS, "sustained_samples": RATE * SECONDS, "warmup_samples": THREADS * SERIES, "buffer_capacity": 32, "digest_compression": 100, "collect_interval_seconds": 10, "distribution": "nanosecond values: deterministic 5ms exponential tail plus 0.1% one-second outliers; varies by thread and round", "explicit_metric_labels_per_series": metric_labels, "default_label_keys": ["host", "instance"]},
        "registration": {"elapsed_ns": registration_elapsed.as_nanos() as u64, "calls": quantiles(&mut registration_ns)},
        "cold_shards": {"elapsed_ns": warmup_elapsed.as_nanos() as u64, "calls": quantiles(&mut shard_creation_ns)},
        "record_calls": quantiles(&mut record_ns),
        "actual_seconds": elapsed.as_secs_f64(), "achieved_qps": (RATE * SECONDS) as f64 / elapsed.as_secs_f64(), "max_schedule_lag_ns": max_schedule_lag_ns,
        "cpu_ticks_during_sustained_phase": ticks_after.saturating_sub(ticks_before),
        "memory": {"before_threads_rss_kib": before_threads_rss_kib, "after_warmup_rss_kib": after_warmup_rss_kib, "sampled_peak_rss_kib": sampled_peak_rss_kib, "process_peak_rss_kib": proc_kib("VmHWM:"), "final_rss_kib": proc_kib("VmRSS:"), "latency_measurement_bytes": (THREADS * SERIES + RATE * SECONDS) * std::mem::size_of::<u64>()},
        "collection_calls": quantiles(&mut collection_ns), "shutdown_ns": shutdown_elapsed.as_nanos() as u64,
        "conservation": {"expected_samples": expected_samples, "accepted_samples": diagnostics.accepted_histogram_samples, "published_samples": published_samples, "dropped_samples": report.dropped_histogram_samples, "dropped_batches": report.dropped_batches, "published_batches": report.written_batches},
        "observed_max_active_shards": max_active_shards, "observed_max_queued_batches": max_queue_batches,
        "environment": {"available_parallelism": thread::available_parallelism().map(|n| n.get()).unwrap_or(0), "cpu_max": fs::read_to_string("/sys/fs/cgroup/cpu.max").unwrap_or_default().trim(), "memory_max": fs::read_to_string("/sys/fs/cgroup/memory.max").unwrap_or_default().trim(), "hostname": measurements.hostname}
    })
}
