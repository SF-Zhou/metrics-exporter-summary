# Benchmark guide

Run these commands from the workspace root. `Cargo.lock` is generated locally
and is not tracked. Resolve dependencies once before starting the measurements:

```sh
cargo generate-lockfile
```

`cargo bench` uses the release profile; subsequent `--locked` commands keep this
resolution fixed. Retain the generated lockfile with the results, alongside the
revision, Rust version, CPU/cgroup limits, available memory and server
configuration. A later fresh resolution may choose different dependency versions.
Linux is needed for meaningful `/proc` RSS and CPU measurements.

## Recording cost

```sh
cargo bench -p metrics-exporter-summary --bench recording --locked

SUMMARY_BENCH_SAMPLES=1000000 \
  cargo bench -p metrics-exporter-summary --bench recording --locked
```

`recording` measures one cached histogram handle on one thread, including the
recurring compression work. It defaults to 100,000 observations and disables
periodic collection. Output is text: throughput, record-call p50/p99/max, the
final shutdown report and diagnostics. Check the report for loss as well as the
reported speed; timing and latency-buffer bookkeeping contribute overhead.

## Recorder and memory sink capacity

```sh
cargo bench -p metrics-exporter-summary --bench capacity --locked \
  > /tmp/metrics-capacity.json

SUMMARY_BENCH_LABELS=2 \
  cargo bench -p metrics-exporter-summary --bench capacity --locked \
  > /tmp/metrics-capacity-labels.json
```

The workload prewarms 200 threads × 1,000 histogram series, creating 200,000
shards. It then schedules 100,000 observations/s for 30 seconds: 3,000,000
observations plus 200,000 warmup observations. Handles are cached, collection
runs every 10 seconds, digest compression is 100, and shard buffers hold 32
samples. Values represent a deterministic 5 ms exponential distribution with
approximately 0.1% additional one-second outliers, expressed in nanoseconds.

`SUMMARY_BENCH_LABELS` accepts only `0` (default) or `2`. The latter supplies
`uid=<series>` and `tag=read`; both modes also inherit `host` and `instance`.
The same setting applies to the backend benchmark below.

JSON output includes registration and shard creation costs, record and
collection timings, sink write timings, scheduling lag, diagnostics, memory
estimates and sample counts. The benchmark asserts successful delivery of all
3,200,000 observations, no retries or registration/shard rejection, and complete
retained snapshots without a retention gap.

## ClickHouse and remote backends

The benchmark requires ClickHouse **26.8.6.5**, matching
`metrics_summary_sink_clickhouse::TESTED_SERVER_VERSION`. Use a disposable server:
the benchmark creates a unique database, installs the packaged schema, and
truncates its distribution table between modes before dropping the database on completion.
The account needs permissions for these operations and schema inspection.

```sh
METRICS_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  cargo bench -p metrics-summary-collector --all-features \
  --bench backend_capacity --locked > /tmp/metrics-backend-capacity.json
```

Set `METRICS_CLICKHOUSE_USER` and `METRICS_CLICKHOUSE_PASSWORD` if authentication
is required. Without `METRICS_CLICKHOUSE_URL`, the executable prints a message
and exits without running the workload.

Alternatively, the local helper starts a temporary loopback server, supplies
those environment variables and removes the server data afterward. It requires
Bash, Python 3, curl and a ClickHouse binary:

```sh
deploy/clickhouse/run-local-tests.sh /absolute/path/to/clickhouse \
  cargo bench -p metrics-summary-collector --all-features \
  --bench backend_capacity --locked > /tmp/metrics-backend-capacity.json
```

The helper configures a 1 GiB server memory limit and per-query limits of two
threads and 512 MiB. These settings affect measured capacity; they are not
production sizing recommendations.

`backend_capacity` repeats the 30-second capacity workload for five modes:
direct ClickHouse, HTTP Enqueued, HTTP ClickHouseConfirmed, TCP Enqueued, and
TCP ClickHouseConfirmed. Allow at least 150 seconds of scheduled load plus
compilation, warmup and draining. HTTP/TCP carry MessagePack over authenticated
loopback connections; the source and collector share a process. Each remote
mode uses one collector writer with a 100 ms maximum grouping delay and also
submits a simultaneous burst of 32 source sessions × 1,000 rows.
The benchmark drains the collector and queries ClickHouse to check sample
counts, instance coverage and nonempty host labels. JSON includes database
checks, collector groups, drain timings and source completion boundaries.

## Interpreting results

- `achieved_qps` measures a scheduled observation workload, not maximum throughput
  or database rows/s. Late producers catch up; inspect `max_schedule_lag_ns`.
- Enqueued success means volatile collector admission. ClickHouseConfirmed and
  direct writes wait for storage confirmation. Compare ACK timings only at the
  same completion boundary; source shutdown alone does not drain an Enqueued
  collector.
- RSS includes threads and benchmark latency buffers. `output_estimated_bytes`
  and retained batch estimates are memory budget units, not wire sizes or RSS.
  Diagnostics are sampled every 10 ms and can miss short peaks. `VmHWM` covers
  the process lifetime, including earlier modes in a backend run. CPU ticks need
  the host's `getconf CLK_TCK` value for conversion to seconds.
- These finite runs do not establish cross-host/TLS costs, sustained overload
  fairness or long-duration memory behavior. Repeat on the intended deployment
  and compare distributions and loss diagnostics, not a single throughput value.

For quantile accuracy, run the independent empirical-CDF tests:

```sh
SUMMARY_QUANTILE_REPORT=1 cargo test -p metrics-exporter-summary \
  --test quantile_quality --locked -- --nocapture
```

The tests print per-quantile JSON diagnostics alongside the test output and
check fixed datasets and merge orders. Their rank-error checks do not imply a
uniform relative-value-error guarantee, especially for small sample counts.
