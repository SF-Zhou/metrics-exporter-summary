//! Reproducible capacity acceptance workload with complete snapshot verification.
#[path = "support/capacity_workload.rs"]
mod workload;
use metrics_summary_sink_memory::{MemorySink, Retention};
fn main() {
    let (sink, reader) = MemorySink::new(Retention {
        max_snapshots: 32,
        max_retained_bytes: 64 * 1024 * 1024,
    })
    .unwrap();
    let mut result = workload::run(sink, "capacity-benchmark");
    let snapshots = reader.after(None, usize::MAX).unwrap();
    assert!(!snapshots.retention_gap);
    let samples: u64 = snapshots
        .snapshots
        .iter()
        .map(|batch| batch.histogram_samples())
        .sum();
    assert_eq!(
        Some(samples),
        result["conservation"]["expected_samples"].as_u64()
    );
    result["memory"]["retained_batch_bytes"] = reader.diagnostics().retained_bytes.into();
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
}
