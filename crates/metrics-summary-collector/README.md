# metrics-summary-collector

Authenticated, bounded HTTP/TCP ingestion, source-preserving group writes and graceful shutdown.

Part of the metrics-exporter-summary workspace. Every batch includes a nonempty
hostname, application, instance and a stable source session identity.

Requests and acknowledgments use MessagePack on both transports. HTTP ingestion
uses `POST /v1/batches` with `Content-Type: application/msgpack`; TCP uses the
`MXS1` handshake and big-endian length-prefixed MessagePack frames. Encoded byte
limits and model validation apply before admission.

HTTP and TCP listeners can bind any configured address, including all interfaces,
a private IP or loopback. Applications may connect directly. The provided service
binary requires bearer authentication on both transports; library users can
configure a token or use `None` to disable authentication. TLS is optional through
an HTTPS proxy or TCP TLS tunnel, and the collector has no built-in server TLS.
Plain connections carry the token and metrics without encryption; HTTPS clients
validate certificates and hostnames.

Label keys require no allowlist. The collector preserves custom labels, subject
to count, length and text validation. Missing `host` and `instance` inherit the
source hostname and instance for identity and storage; no other labels are added.
ClickHouse stores each supplied label in its matching string column, which must
already exist in the destination table. Conflicts with storage fields are rejected
before the collector accepts ownership.

See the crate API documentation for configuration and examples. The repository
README, DESIGN.md and deploy directory describe delivery guarantees, resource
limits, ClickHouse schema, optional TLS termination and operations.

[API documentation](https://docs.rs/metrics-summary-collector).

Rust 1.95 or newer. Licensed under either MIT or Apache-2.0 at your option.
