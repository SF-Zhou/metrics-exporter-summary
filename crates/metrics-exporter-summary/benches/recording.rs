use metrics_exporter_summary::{Builder, Config};
use metrics_summary_sink_memory::{MemorySink, Retention};
use std::time::{Duration, Instant};

fn main() {
    let iterations = std::env::var("SUMMARY_BENCH_SAMPLES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100_000);
    let (sink, _) = MemorySink::new(Retention {
        max_snapshots: 4,
        max_retained_bytes: 64 * 1024 * 1024,
    })
    .unwrap();
    let (recorder, control) = Builder::for_service("benchmark", "local")
        .unwrap()
        .config(Config {
            collect_interval: None,
            ..Config::default()
        })
        .build(sink)
        .unwrap();
    let histogram = metrics::with_local_recorder(&recorder, || metrics::histogram!("latency"));
    let mut latencies = Vec::with_capacity(iterations);
    let started = Instant::now();
    for i in 0..iterations {
        let before = Instant::now();
        histogram.record(((i * 7919) % 10000) as f64 / 1e6);
        latencies.push(before.elapsed().as_nanos());
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    if !latencies.is_empty() {
        println!(
            "samples={iterations} throughput={:.0}/s record_ns p50={} p99={} max={}",
            iterations as f64 / elapsed.as_secs_f64(),
            latencies[iterations / 2],
            latencies[iterations * 99 / 100],
            latencies[iterations - 1]
        );
    }
    let report = control.shutdown(Duration::from_secs(10)).unwrap();
    println!("report={report:?} diagnostics={:?}", control.diagnostics());
}
