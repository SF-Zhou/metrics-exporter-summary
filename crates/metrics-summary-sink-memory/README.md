# metrics-summary-sink-memory

Bounded complete-snapshot history with independent read-only queries and retention diagnostics.

Part of the metrics-exporter-summary workspace. Every batch includes a nonempty
hostname, application, instance and a stable source session identity.

Snapshots preserve collection completion `timestamp` and monotonic statistical
`duration_ns` exactly, including zero durations and backwards wall-clock changes.
History is ordered by source batch sequence. Reusing a retained ID with different
time metadata is rejected as a content conflict.

Retention planning caches each snapshot's byte estimate and prepares a replacement
ring of `Arc` references; it never copies the retained batch data. Temporary ring
metadata is bounded by `max_snapshots` entries. The deadline is checked throughout
planning and immediately before publishing the entire replacement. An expired
attempt leaves both history and retention diagnostics unchanged. Evicted batches
are reclaimed after publication, outside the history lock. Rust deallocation
cannot be interrupted and may delay the function's return past the deadline; an
already successful publication still returns success.

Each snapshot's `CounterDelta.delta_value` and histogram summary cover only its
own interval. The history ring retains earlier snapshots independently; it never
adds their observations to the latest snapshot. Counter deltas range from zero
through `i64::MAX`. Gauges retain an exact `i64` current value. Histogram snapshots
include exact min/max and estimated p50/p90/p95/p99, with count in `1..=2^53`.
Missing intervals cannot be reconstructed from later counter deltas.

Label names need no schema configuration. The sink preserves supplied labels and
applies resource and content validation. For identity checks, missing `host` and
`instance` use `Source.hostname` and `Source.instance`; no other labels are added.
Use `MemorySink::with_validation_limits` to customize resource budgets.
The sink rejects duplicate storage-family/name/effective-label identities before
publishing a snapshot.
Counters and gauges share the scalar family, so they need distinct names for the
same labels; histograms use a separate distribution family.

See the crate API documentation for configuration and examples. The repository
README, DESIGN.md and deploy directory describe delivery guarantees, resource
limits, ClickHouse schema, TLS termination and operations.

[API documentation](https://docs.rs/metrics-summary-sink-memory).

Rust 1.95 or newer. Licensed under either MIT or Apache-2.0 at your option.
