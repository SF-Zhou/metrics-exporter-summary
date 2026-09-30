# metrics-summary-core

Transport-independent metric batches, bounded validation and synchronous sink contracts.

Part of the metrics-exporter-summary workspace. Every batch includes a nonempty
hostname, application, instance and a stable source session identity.

`CounterDelta { delta_value }` contains only increments from the current collection
interval, in the range `0..=i64::MAX`. `GaugeSnapshot.current_value` is an exact
`i64` current-value snapshot. Histogram summaries contain only that interval's
observations: count and sum, exact min/max, and estimated p50/p90/p95/p99. Count
must be in `1..=2^53`, so its Float64 storage representation stays exact. Sum
received counter deltas for an aggregate count; a missing batch cannot be
recovered from later deltas.

`Batch.timestamp` is collection completion wall time in signed Unix nanoseconds.
`Batch.duration_ns` is the unsigned monotonic span between collection completions,
or from recorder creation for the first round. Zero is valid; this span is not
scan/merge execution time. Wall-clock rollback does not change batch ordering or
the duration; use the source session and sequence for identity and ordering.

Label keys are application-defined, case-sensitive strings. There is no allowlist
or extra-label configuration. Validation bounds counts and UTF-8 lengths and
rejects invalid keys and values; SQL column constraints belong to the storage sink.

`effective_labels` preserves all supplied labels and fills only missing `host`
from `Source.hostname` and `instance` from `Source.instance`. Explicit row values
take precedence. The final `host` must be nonempty. `hostname` is not an alias.
The effective label count, including these defaults, must fit `max_labels`.

Batch validation rejects duplicate storage-family/name/effective-label identities
even when their internal metric IDs differ. Counter and gauge share the scalar
family; give them distinct metric names for the same labels, including across
processes. Histograms use a separate distribution family. Source identity and
duration remain transport metadata; the ClickHouse schema does not store them.

See the crate API documentation for configuration and examples. The repository
README, DESIGN.md and deploy directory describe delivery guarantees, resource
limits, ClickHouse schema, TLS termination and operations.

[API documentation](https://docs.rs/metrics-summary-core).

Rust 1.95 or newer. Licensed under either MIT or Apache-2.0 at your option.
