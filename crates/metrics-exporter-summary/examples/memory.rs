//! Run with `cargo run -p metrics-exporter-summary --example memory`.
use metrics_exporter_summary::{Builder, Config};
use metrics_summary_sink_memory::{MemorySink, Retention};
use std::{error::Error, time::Duration};

fn main() -> Result<(), Box<dyn Error>> {
    let (sink, reader) = MemorySink::new(Retention {
        max_snapshots: 10,
        max_retained_bytes: 16 * 1024 * 1024,
    })?;
    let (recorder, control) = Builder::for_service("example", "local")?
        .config(Config {
            collect_interval: None,
            ..Config::default()
        })
        .build(sink)?;
    let (latency, requests, active) = metrics::with_local_recorder(&recorder, || {
        (
            metrics::histogram!("rpc.duration", "tag" => "read"),
            metrics::counter!("rpc.requests"),
            metrics::gauge!("rpc.active"),
        )
    });
    active.set(0.0);
    for round in 0..3 {
        for sample in 1..=1000 {
            latency.record(sample as f64);
            requests.increment(1);
        }
        let report = control.flush(Duration::from_secs(5))?;
        if !report.is_success() {
            return Err(format!("incomplete flush: {report:?}").into());
        }
        let batch = reader.get(report.target)?;
        println!(
            "round {round}: {}",
            serde_json::to_string_pretty(batch.as_ref())?
        );
    }
    let report = control.shutdown_default()?;
    if !report.is_success() {
        return Err(format!("incomplete shutdown: {report:?}").into());
    }
    Ok(())
}
