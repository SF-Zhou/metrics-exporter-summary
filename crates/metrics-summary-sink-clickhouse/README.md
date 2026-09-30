# metrics-summary-sink-clickhouse

Bounded, confirmation-aware ClickHouse HTTP storage for metric summaries.
Counters and signed integer gauges share `metrics_summary.counters`; histogram windows
use `metrics_summary.distributions`, including min, mean and p95. Collection-end times
are stored as DateTime seconds. Labels are preserved as flat columns; absent
`host` and `instance` values use the batch source, and explicit values win.

Required columns are verified without changing deployed DDL. Each supplied label
needs a String or LowCardinality(String) column in the table receiving its row;
there is no key whitelist or extra-label configuration. Other absent labels use
database defaults. A label missing from the metadata cache triggers a fresh schema
check before either table is written. Labels cannot overwrite that table's time,
metric-name, or numeric-value columns. Stored rows have no retry identity, so an
uncertain write retried by the caller may produce duplicates.

See the crate API and repository `deploy/clickhouse` for schema, TLS, resource
bounds, delivery semantics, examples and integration tests.

[API documentation](https://docs.rs/metrics-summary-sink-clickhouse).

Rust 1.95 or newer. Licensed under either MIT or Apache-2.0 at your option.
