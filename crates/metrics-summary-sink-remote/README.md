# metrics-summary-sink-remote

HTTP and TCP summary clients with explicit collector admission or ClickHouse confirmation boundaries.

Part of the metrics-exporter-summary workspace. Every batch includes a nonempty
hostname, application, instance and a stable source session identity.

Requests and acknowledgments use MessagePack on both transports. HTTP ingestion
uses `POST /v1/batches` with `Content-Type: application/msgpack`; TCP uses the
`MXS1` handshake and big-endian length-prefixed MessagePack frames. Encoded byte
limits and model validation apply before admission.

TLS is optional. HTTP endpoints may use any hostname or IPv4/IPv6 address with
`http://` or `https://`; HTTPS validates the server certificate and hostname and
supports additional private CA roots. TCP connects directly to any configured
socket address, with an optional external TLS tunnel. HTTP endpoint URLs must
have a host and cannot contain credentials, query strings or fragments.
Authentication is configured independently with `bearer_token`. HTTP redirects
and environment proxies are disabled.

HTTP and TCP preserve the source completion `timestamp` and monotonic statistical
`duration_ns` unchanged, including across retries. A retry never recomputes these
fields. The receiver requires both fields.

Counter payloads carry only the current interval's `CounterDelta.delta_value`,
in the range `0..=i64::MAX`. Retries preserve the same delta and batch identity. Aggregate
counts require summing distinct delivered intervals; a later delta cannot recover
a missing batch. Histogram summaries contain interval count/sum, exact min/max
and estimated p50/p90/p95/p99; count is bounded by `2^53`. Gauges remain exact
signed 64-bit current-value snapshots. The ClickHouse rows omit transport
identities, so a retry after an unknown outcome may store duplicate observations.

Label keys require no allowlist; custom labels, including Unicode and punctuation
in keys, are preserved without additional configuration. Count, length, nonempty
key and control-character validation still applies. Missing `host` and `instance`
use `Source.hostname` and `Source.instance` for identity and storage; explicit row
values override those defaults. No other labels are added. ClickHouse requires
matching string columns for the labels supplied by each row. Counter and gauge
names must differ for the same effective labels, including across processes,
because they share the scalar table.

See the crate API documentation for configuration and examples. The repository
README, DESIGN.md and deploy directory describe delivery guarantees, resource
limits, ClickHouse schema, TLS termination and operations.

[API documentation](https://docs.rs/metrics-summary-sink-remote).

Rust 1.95 or newer. Licensed under either MIT or Apache-2.0 at your option.
