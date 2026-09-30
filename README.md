# metrics-exporter-summary

A Rust `metrics` recorder that keeps thread-local distribution sketches and exports
compact statistical summaries to memory, ClickHouse or a remote collector.
Counters export interval deltas;
gauges are sampled current values. Every batch includes **hostname**, application,
instance and a unique recorder session.

The workspace contains seven crates:

| Crate | Purpose | API documentation |
| --- | --- | --- |
| `metrics-summary-core` | Source/batch model, validation, sink and error contracts | [docs.rs](https://docs.rs/metrics-summary-core) |
| `metrics-exporter-summary` | Recorder, thread shards, sampler, writer and controls | [docs.rs](https://docs.rs/metrics-exporter-summary) |
| `metrics-summary-sink-memory` | Bounded snapshot history and read-only reader | [docs.rs](https://docs.rs/metrics-summary-sink-memory) |
| `metrics-summary-sink-clickhouse` | Shared writer for counters/distributions tables | [docs.rs](https://docs.rs/metrics-summary-sink-clickhouse) |
| `metrics-summary-protocol` | Versioned MessagePack requests/ACKs and bounded decoding | [docs.rs](https://docs.rs/metrics-summary-protocol) |
| `metrics-summary-sink-remote` | HTTP and TCP clients with configurable ACK boundary | [docs.rs](https://docs.rs/metrics-summary-sink-remote) |
| `metrics-summary-collector` | Authenticated bounded ingestion, grouping and database writes | [docs.rs](https://docs.rs/metrics-summary-collector) |

[DESIGN.md](DESIGN.md) describes the behavioral contract and deployment limits.
Rust 1.95 or newer is required. `Cargo.lock` is generated locally and is not
tracked. Generate it before using `--locked`; subsequent commands then reuse
that resolution. Retain the generated lockfile with build or benchmark artifacts
when you need to reproduce their dependency versions.

## In-process snapshots

Add the recorder, memory sink and `metrics` to your application. In this checkout,
run `cargo run -p metrics-exporter-summary --example memory` for a complete example.

```rust
use metrics_exporter_summary::{Builder, Config};
use metrics_summary_sink_memory::{MemorySink, Retention};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (sink, reader) = MemorySink::new(Retention {
        max_snapshots: 10,
        max_retained_bytes: 16 * 1024 * 1024,
    })?;
    let (recorder, control) = Builder::for_service("storage", "worker-1")?
        .config(Config { collect_interval: None, ..Config::default() })
        .build(sink)?;

    // For services: recorder.install()?; then cache handles after installation.
    // For tests and embedded use, avoid process-global recorder state:
    let latency = metrics::with_local_recorder(&recorder, || {
        metrics::histogram!("rpc.duration", "tag" => "read")
    });
    latency.record(1_200_000.0); // latency in nanoseconds

    let report = control.flush(Duration::from_secs(5))?;
    assert!(report.is_success());
    let snapshot = reader.get(report.target)?;
    println!("hostname={} rows={:?}", snapshot.source.hostname, snapshot.rows);

    // First stop producers, then explicitly close and inspect the report.
    let report = control.shutdown(Duration::from_secs(35))?;
    assert!(report.is_success());
    Ok(())
}
```

`Builder::for_service` discovers the operating system hostname and reports lookup
failure. To override hostname (for example, to report the physical host instead of
a container hostname), construct a `Source` explicitly and use `Builder::new`.
Hostname must be nonempty. Source hostname and instance supply defaults for the
`host` and `instance` metric labels; an explicit metric label overrides that default.

Label keys are an end-to-end application and storage convention; the summary
crates have no key allowlist. `host` inherits `Source.hostname` and `instance`
inherits `Source.instance` when omitted. Explicit labels override these defaults;
all other labels are preserved as supplied. These defaults and canonical key
ordering are applied before computing series identity. Duplicate or invalid keys,
invalid values, exceeded resource limits and an empty effective host reject
registration and increment `registrations_rejected`. Counter and Gauge must use
distinct names for the same labels, because both are stored in `counters` without
an instrument discriminator.

Custom labels require no recorder, protocol or collector schema configuration.
For ClickHouse, provision a matching `String` or `LowCardinality(String)` column
in each table that receives the label, and include the dimension in your queries.
The writer checks actual columns and rejects collisions with stored metric fields;
it never creates or alters columns. The supplied DDL and dashboard use `host`,
`pod`, `instance`, `tag`, `thread`, `uid`, `statusCode`, `mount_name`, and `io`.
Columns omitted from a row, including `method` or `type` unless supplied explicitly,
use database defaults. `ValidationLimits` controls resource limits only.

The default database is `metrics_summary`. ClickHouse stores two row formats:

- `counters`: `TIMESTAMP`, `metricName`, `val Int64`, and the supplied labels.
  Counter interval deltas and Gauge current values share this table.
- `distributions`: `TIMESTAMP`, `metricName`, `count`, `mean`, `min`, `max`,
  `p50`, `p90`, `p95`, `p99`, and the supplied labels. All statistics are Float64.

`TIMESTAMP` is a seconds-resolution `DateTime`; subsecond time is truncated.
There are no stored duration, session, sequence, unit, source-attribute or internal
metric-ID columns. Batch IDs and precise timing remain in the memory/transport
model for lifecycle control and collector admission. The supplied DDL creates
MergeTree tables, including the optional `method`/`type` label columns.
Schema checks require the written columns and types, permit additional columns,
and do not constrain or change the deployed engine, sorting key, partition or TTL.

The default automatic collection period is 10 seconds. Set `collect_interval` to
`None` for deterministic manual snapshots. An explicit flush closes a window and
resets the automatic timer. Reading memory history never creates a new window.

Each in-process/transport batch carries `timestamp` (signed Unix nanoseconds when scanning and
summarization finish) and `duration_ns` (unsigned nanoseconds since the previous
collection finished, or recorder creation for the first batch). The duration is
the statistical span, measured with a monotonic clock; it includes time between
collections and is independent of wall-clock adjustments. Manual and empty
collections also advance this boundary. Collection execution time remains in
`Control::diagnostics().last_collection_ns`. Retries keep both fields unchanged.

Histogram observations and pending counter increments are drained at collection,
so each batch contains only that window's data. Idle counters report zero; empty
histogram windows have no row. A counter's `absolute(v)` still accepts a monotonic
external total, but only its newly observed increase is added to the current
window. Repeating the same absolute value never counts it again. Gauges retain
their current value across collections, so an in-flight request may increment in
one window and decrement in a later one. Once initialized, gauges are sampled
each round even without updates. Pending counter deltas must fit Int64. Gauge
arguments must be finite integers in the Int64 range; checked integer arithmetic
preserves exact updates even above 2^53. Fractional or out-of-range inputs and
overflowing updates are rejected and diagnosed without changing prior state.
Latency instrumentation records nanoseconds directly, with no implicit unit
conversion based on `describe`.

Use [the benchmark guide](docs/BENCHMARKS.md) to reproduce capacity, recording
latency, memory and quantile checks for your workload and deployment environment.

## HTTP server example

[examples/server.rs](crates/metrics-exporter-summary/examples/server.rs) runs an
Axum service with an installed global recorder, cached Counter/Gauge/Histogram
handles, bounded configuration, and automatic collection every two seconds.
It uses MemorySink so it runs without a collector or database:

```sh
cargo run -p metrics-exporter-summary --example server

# In another terminal, send concurrent requests and inspect the next snapshot.
seq 1 20 | xargs -P 4 -I '{}' curl -fsS http://127.0.0.1:3000/work
sleep 2
curl -fsS http://127.0.0.1:3000/metrics
curl -fsS http://127.0.0.1:3000/diagnostics
```

`/metrics` returns the latest summary batch as JSON and returns 503 before the
first collection. Reading it never triggers a flush. Each `/work` request updates
the interval request count, in-flight gauge and handler-duration histogram in
nanoseconds; the request guard also finishes instrumentation if a handler is cancelled.
Inspection endpoints do not record their own requests.

Optional environment variables are `SERVER_ADDR` (default `127.0.0.1:3000`),
`METRICS_INSTANCE` (default `server-<pid>`) and `POD_NAME` (default empty). Hostname
is discovered automatically. Metrics use `tag=work` and `pod`, plus the automatic
`host` and `instance` defaults.

Press Ctrl-C, or send SIGTERM on Unix, to stop admission and drain active HTTP
requests. The example then calls `Control::shutdown_default` on a blocking task,
checks the delivery report and prints the final snapshot. Stop and join any
additional background producers before this shutdown call in your own service.
For collector delivery, use the same registration and lifecycle with a configured
RemoteSink from the [remote example](crates/metrics-summary-sink-remote/examples/remote.rs);
the snapshot inspection routes are specific to MemorySink. Axum and Tokio are
example-only dependencies of the recorder crate.

## Database and collector deployment

Provision the `metrics_summary` database and tables using the supplied DDL in the
[ClickHouse deployment instructions](deploy/clickhouse/README.md).
HTTP(S) insertion remains supported; configure the server's HTTP port, not its
Native TCP port. Grafana queries the same `counters` and `distributions` tables.
The [collector deployment guide](deploy/collector/README.md) covers TOML configuration,
Bearer authentication, direct HTTP/TCP, optional TLS termination and graceful shutdown.
The [Grafana guide](deploy/grafana/README.md) includes a provisioned dashboard,
read-only database grants and reproducible query checks.

```sh
cargo build --release -p metrics-summary-collector --all-features
target/release/metrics-summary-collector --config deploy/collector/collector.toml --check-config
```

Never put tokens or database passwords in URLs or version-controlled configuration.
Supply secrets using `METRICS_COLLECTOR_TOKEN` and `CLICKHOUSE_PASSWORD`. The
collector can listen on any configured address; the template serves direct HTTP
on `0.0.0.0:9091`. Applications may connect directly over HTTP or TCP, or use an
optional HTTPS proxy or TCP TLS tunnel. Bearer authentication is required in
either layout. The collector has no built-in server TLS; HTTPS clients verify
certificates and hostnames. `--check-config` validates syntax and limits without
opening a listener or writing database rows.

## Delivery and query semantics

- The recorder uses bounded memory, without a WAL. Process/collector crashes and
  exhausted retry budgets can lose data. Queue saturation drops newly collected
  complete batches and records diagnostic counts.
- `Enqueued` ACK means the collector owns the admitted batch in memory.
  `ClickHouseConfirmed` waits for all required table inserts. A flush report names
  its boundary and retains known local losses; `Ok(report)` alone does not mean
  lossless success. Check `report.is_success()`.
- Timeouts do not prove a write was cancelled. Retries preserve original IDs and
  contents. The existing MergeTree tables contain no source-row identity, so retries
  after an unknown outcome or partial write can create duplicate rows. Collector
  deduplication is bounded and in-memory; it does not guarantee database exactly-once
  delivery across cache expiry or restart. There are no deduplicated FINAL views.
- No cross-table transaction is promised. Part of a failed batch may be visible.
- `sum(mean * count) / sum(count)` is the stored distributions' combined mean. `avg(p99)` and `max(p99)` describe
  local-window p99 values; neither reconstructs an overall p99.
- A scan closes shards one at a time. `timestamp` and `duration_ns` describe a
  logical round with approximate sample boundaries. Session/sequence establishes
  order inside the transport even when the wall clock moves backwards. The database
  stores neither duration nor sequence. Counter panels therefore show received
  increments per fixed time bucket, divided by the bucket width, rather than an
  exact per-collection rate. Missing windows remain missing; Gauge ordering within
  the same second is ambiguous. Counter and Gauge query semantics must be selected
  by metric name. Retries resend the immutable collected
  batch; they neither drain live state again nor add its values to another window.
- Memory-reader `Arc` references can outlive retention. Caller-held snapshots are
  outside the ring's byte budget. Completed flush targets can later be evicted.

Registry entries remain until shutdown. Registering too many unique label
combinations is rejected; metric handles from rejected registrations are no-ops.
`max_series` is a count ceiling, not a capacity guarantee: the retained-batch byte
budget also limits registration. The default 10,000-series ceiling can therefore
admit fewer series, depending on names, labels and the 16 MiB batch budget. Inspect
`registered_series` and `registrations_rejected` when sizing a deployment.
Histogram shard admission is retried on later records after exited shards are
drained. Registration reserves enough output space for the core batch model.
Transport and storage encodings have separate limits. Coordinate these with
series count and label sizes; collector admission checks the encoded storage
budget before taking ownership. ClickHouse writes supplied labels as flat columns
and does not copy unrelated source attributes into each row.
Metadata descriptions have separate entry and byte quotas.

The application-to-collector protocol uses MessagePack for both requests and
ACKs. HTTP uses `POST /v1/batches` with `Content-Type: application/msgpack`; TCP
uses the `MXS1` handshake and length-prefixed frames. Finite histogram values
retain their exact `f64` representation, and integer timestamps, counts and
identities retain their full signed or unsigned integer precision. Wire version 1
rejects compression and accepts only the defined MessagePack layout. Senders and
collectors must use matching protocol definitions.

Protocol decoding is bounded by encoded byte, row, label, string and retained
model limits; malformed or oversized messages reject the whole batch. These
limits are not a process RSS guarantee: include encoded bodies, decoding work,
pending batches and request concurrency when sizing the collector.

Invalid floating inputs and overflowing scalar updates are rejected, preserving
the previous state. Histogram statistics that become invalid only during shard
merging are dropped as one complete metric window, with explicit sample loss.
`record_many` is an O(n) compatibility implementation. Compression runs under the
shard lock; record calls do not have a constant-latency guarantee.

Read `Control::diagnostics()` out of band. Alert on dropped samples/batches,
registration or shard rejections, write failures, oldest collector backlog and
`dropped_after_acceptance`. Enqueued successes do not count as database confirmations.
`Control::last_write_error()` retains the latest bounded sink error and timestamp,
including errors from retries that subsequently recovered.

## Verification

```sh
cargo +1.95.0 generate-lockfile
cargo fmt --all -- --check
cargo test --workspace --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +1.95.0 check --workspace --all-targets --all-features --locked
cargo bench -p metrics-exporter-summary --bench recording --locked
```

The real database suite runs only when explicitly requested against a disposable
instance; follow the ClickHouse guide. Unit and controlled fault tests use loopback
services and synchronization barriers. The recorder and memory sink's normal
dependencies do not include an async runtime or network client.

See [the release guide](docs/RELEASING.md) for workspace archive verification and
publishing the seven crates together.
