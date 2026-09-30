# metrics-exporter-summary

Thread-sharded metrics recorder with bounded collection, writer retries and explicit lifecycle controls.

Part of the metrics-exporter-summary workspace. Every batch includes a nonempty
hostname, application, instance and a stable source session identity.
Its time fields are the collection-finish `timestamp` and the monotonic
statistical span `duration_ns`; scan/merge execution time stays in diagnostics.
Histogram samples and counter increments are drained on every collection.
Counter rows contain an Int64-bounded `delta_value` for that interval (zero when
idle), while initialized gauges retain and publish their current Int64 value each
round. Gauge fractional inputs and out-of-range updates are rejected. Counter
`absolute` calls retain a monotonic baseline and contribute only new increases.
Histograms include exact min/max and p50/p90/p95/p99 estimates. Label keys have no
allowlist: applications agree their names with storage and queries. Missing `host`
and `instance` default to Source.hostname and Source.instance; other labels are
preserved as supplied. Latency examples record nanoseconds directly.

Run the HTTP server example with:

```sh
cargo run -p metrics-exporter-summary --example server
curl http://127.0.0.1:3000/work
# After the first two-second collection:
curl http://127.0.0.1:3000/metrics
curl http://127.0.0.1:3000/diagnostics
```

Run the curl commands in another terminal. `examples/server.rs` demonstrates
global installation, cached request metrics, explicit resource limits, periodic
MemorySink publication, and shutdown after draining HTTP requests on Ctrl-C
or SIGTERM (Unix).
Its snapshot endpoint returns JSON; it reads already collected summaries.
Optional environment variables: `SERVER_ADDR`, `METRICS_INSTANCE`, `POD_NAME`.
Axum and Tokio are dev-dependencies used by this example.

See the crate API documentation for configuration and examples. The repository
README, DESIGN.md and deploy directory describe delivery guarantees, resource
limits, ClickHouse schema, TLS termination and operations.

[API documentation](https://docs.rs/metrics-exporter-summary).

Rust 1.95 or newer. Licensed under either MIT or Apache-2.0 at your option.
