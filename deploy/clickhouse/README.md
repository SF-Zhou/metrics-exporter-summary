# ClickHouse deployment

The writer stores metric summaries in ClickHouse. The default database is
`metrics_summary`, with `counters` and `distributions`. `schema.sql` defines the
provided tables: MergeTree, daily partitions, the
`(metricName, host, pod, instance, TIMESTAMP)` primary/sorting key, three-month TTL,
and `index_granularity = 8192`. Provision explicitly with a schema administrator:

```sh
clickhouse-client --multiquery < deploy/clickhouse/schema.sql
```

The writer never creates or alters tables. Configure `ClickHouseConfig.database`
and `TableNames` for reviewed alternative names. Before its first nonempty write,
it reads `system.columns` and checks the required columns. `verify_schema` exposes
the same read-only check. Each write also validates its actual label columns;
a label missing from cached metadata triggers a refresh before rejection.
Metric names and labels may be `String` or
`LowCardinality(String)`; `TIMESTAMP` must be `DateTime` (an explicit timezone is
also accepted). Scalar `val` must be `Int64`, and all eight distribution value
columns must be `Float64`. Additional deployed columns are allowed. Engine,
partition, sorting key and TTL are controlled by the operator, not validated or
modified by the writer. Revalidate after intentional DDL changes.

Counter deltas and gauge snapshots both go to `counters`, with the metric name in
`metricName` and the value in `val`. A counter is the nonnegative increment since
the preceding collection and must fit `Int64`; a gauge is a signed `Int64` current
snapshot and does not reset. The recorder rejects fractional or out-of-range
floating-point gauge updates before conversion. Instrument names must distinguish
counter and gauge semantics because no instrument-kind column is stored.

Histograms go to `distributions`, with `count`, `mean`, `min`, `max`, `p50`, `p90`,
`p95`, and `p99` as `Float64`. Mean is the source-window sum divided by its count;
count is bounded by 2^53 to keep its integer value exact in Float64. Values retain
the recorded unit: no automatic scaling occurs. The provided latency examples and
dashboard use nanoseconds. Local quantiles cannot be combined into an overall
percentile.

`TIMESTAMP` contains collection-end Unix seconds. Nanosecond fractions are
truncated; timestamps outside 0 through UInt32::MAX seconds are rejected before
I/O. Duration, source session, sequence, internal metric ID, application, unit and
source attributes remain internal and are not written. There is no labels Map.
The supplied DDL provides nine flat labels: `host`, `pod`, `instance`, `tag`,
`thread`, `uid`, `statusCode`, `mount_name`, and `io`. The writer has no label key
allowlist. Explicit row values override defaults; missing `host` and `instance`
use Source.hostname and Source.instance. The effective host must be nonempty.
Other omitted columns use database defaults. This includes `type` on counters
and `method` on distributions unless those labels are explicitly provided.

To add a label, provision a matching String or LowCardinality(String) column in
each table that receives it. No `extra_labels` setting is needed in the recorder,
remote client, collector or writer; remove that former setting from configuration.
JSONEachRow escapes label keys as data, so Unicode and punctuation do not require
SQL identifier restrictions in the summary model. The storage adapter rejects
collisions with its timestamp, metric-name and value fields before I/O, and checks
the actual label columns before any INSERT. This does not change ORDER BY or any
other DDL. Include every label dimension in queries that distinguish series, and
keep omitted-column defaults consistent with your application's identity rules.

There is no database retry identity or deduplication guarantee. A two-table write
is not transactional: counters may succeed before distributions fail. Lost replies
and partial writes report an unknown commit outcome. Retrying original batches
may insert duplicates, including with async insertion; this crate performs one
attempt per call. Async inserts always wait with `wait_for_async_insert=1`.
Missing batches lose counter increments. Since per-row duration is not stored,
the exact interval rate cannot be reconstructed from these tables alone; a query
needs an externally known reporting interval. `queries.sql` provides raw scalar
windows and histogram examples without pretending retries can be deduplicated.

Use a dedicated writer identity with INSERT and metadata inspection permissions;
readers need SELECT on the two tables. The transport is ClickHouse HTTP, normally
port 8123, or HTTPS at the configured endpoint. ClickHouse's native protocol
port 9000 is not an HTTP endpoint; enable and configure an HTTP(S) endpoint.
Configure HTTPS across hosts, with a trusted local TLS proxy if needed. Credentials
use HTTP Basic auth, never URL parameters. For private PKI, use
`ClickHouseConfig.tls_ca_pem` or
`read_ca_bundle` (maximum 1 MiB and 32 certificates). Collector TOML accepts
`[clickhouse] ca_file = "/path/to/ca.pem"`. Certificate and hostname validation
remain enabled; there is no skip-verify option.

Defaults bound each group to 256 batches, 100,000 rows, 32 MiB of estimated source
bytes and 64 MiB of total encoded JSON. Encoding is additional storage; Vec growth
may reserve about twice its length. Both bodies are fully validated and encoded
before any INSERT. Collectors use `encoded_batch_bytes_with_limits` with matching
validation limits, byte budget and deadline before admission. Counting includes
JSON escaping and default labels, and stops at the byte budget or deadline.
Response bodies are limited to 64 KiB and excluded from error messages, because
server errors may echo data. Requests use the lesser of the operation deadline
and configured timeout. A successful write confirms server acceptance under the
server's durability settings; it does not promise protection against arbitrary
replica or disk loss. Monitor ingestion failures, queue drops, retry exhaustion,
parts, merge backlog and storage capacity. TTL removal is asynchronous;
`retention.sql` restates the supplied three-month policy as an optional operator
action.

The complete direct example requires an explicit endpoint:

```sh
METRICS_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  cargo run -p metrics-summary-sink-clickhouse --example direct
```

Optional variables are `METRICS_CLICKHOUSE_DATABASE`, `METRICS_CLICKHOUSE_USER`,
`METRICS_CLICKHOUSE_PASSWORD`, and `METRICS_CLICKHOUSE_CA_FILE`.

Real integration tests use **ClickHouse 26.8.6.5** and create/drop only a uniquely
named test database. This is the tested release, not a claim about the production
server version. Run against a disposable instance:

```sh
METRICS_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  cargo test -p metrics-summary-sink-clickhouse --test real_clickhouse -- --ignored
```

`run-local-tests.sh /absolute/path/to/clickhouse` starts a temporary loopback-only
server with small thread pools, runs the ClickHouse and collector suites, and
removes its data. An additional command after the binary runs with its connection
environment, for example to run a single suite or a benchmark. The tests cover
shared scalar storage, full distributions, seconds and Int64 boundaries, extra
columns and labels, direct/group/async confirmation, visible retry duplicates and
partial writes. HTTPS and mutual-TLS transport tests require HAProxy 2.8+ and
OpenSSL and exercise the real Rust clients with a private CA:

```sh
deploy/clickhouse/run-local-tests.sh /absolute/path/to/clickhouse \
  python3 deploy/clickhouse/test-https.py /absolute/path/to/haproxy
```

The harness removes TLS keys and databases. It checks missing-CA and wrong-hostname
rejection as well as successful writes.
