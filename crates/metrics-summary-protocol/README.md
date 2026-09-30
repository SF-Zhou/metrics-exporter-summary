# metrics-summary-protocol

Versioned MessagePack summaries, bounded decoding and HTTP/TCP acknowledgement messages.

Part of the metrics-exporter-summary workspace. Every batch includes a nonempty
hostname, application, instance and a stable source session identity.

Each request requires `timestamp` (signed Unix nanoseconds at collection
completion) and `duration_ns` (unsigned monotonic span since the previous
completion, or recorder creation for the first round). Explicit zero values are
valid. Duration is independent of wall-clock changes and is not scan/merge time.

The counter payload carries unsigned `CounterDelta.delta_value`,
including an explicit zero, with a semantic maximum of `i64::MAX`. It is the
increment for this collection interval.
Later batches do not repeat earlier increments or recover a missing batch's count.
The gauge payload preserves every signed 64-bit
current value exactly. Histogram rows contain interval observations and require
`min` and `p95` alongside count/sum/p50/p90/p99/max.
Their count must be in `1..=2^53`; all floating-point fields must be finite and
the quantiles must be ordered between min and max. All floating-point fields use
the MessagePack float64 marker (`0xcb`) and eight IEEE 754 bytes, preserving bits
without conversion to float32 or decimal text.

## Wire schema

Model and wire versions are 1. HTTP uses `application/msgpack`; TCP frames contain
the identical payload after a big-endian `u32` byte length and the existing `MXS1`
authenticated handshake. Requests and acknowledgements are uncompressed. There
is no legacy format compatibility.

DTOs are fixed-length arrays, with fields in the following order:

| DTO | Array contents |
| --- | --- |
| Request | `[wire_version, model_version, ack_policy, id, source, timestamp, duration_ns, rows]` |
| Ack | `[wire_version, id, ack_policy, status, message]` |
| Id | `[session, sequence]` |
| Source | `[application, instance, hostname, attributes]` |
| Entry | `[key, value]` |
| Row | `[metric_id, name, labels, unit, value]` |
| Value | `[kind, payload]` where `0 = Histogram`, `1 = Counter`, `2 = Gauge` |
| Histogram | `[count, sum, p50, p90, p99, max, min, p95]` |

`session` is a 16-byte MessagePack binary UUID. `rows`, `labels` and `attributes`
are arrays; label/attribute entries are two-element arrays rather than maps so
duplicate keys can be rejected. Strings use MessagePack UTF-8 string types.
Only `Row.unit` and `Ack.id` may be `nil`. All other fields are required, including
explicit zero values. `ack_policy` is `1 = Enqueued` or `2 = ClickHouseConfirmed`;
statuses are `0 = Ok`, `1 = Overloaded`, `2 = Invalid`, `3 = Unauthorized`,
`4 = Unavailable`, `5 = Unknown`.

Unsigned fields accept positive fixints and uint8/16/32/64 markers, never signed
integer markers. Signed fields accept signed integer markers or unsigned integer
markers whose values fit the signed field. No integer field accepts floats.
Histogram floating-point fields accept only float64 markers, never float32 or
integer substitutions. Fixed DTO lengths, types and enum values are validated;
maps, extension values, additional fields and trailing bytes are rejected.

`encode_request` and `decode_request` validate traffic. `encode_wire_request`
provides deterministic serialization for already validated canonical fingerprints
and invalid-message test fixtures; it deliberately skips model validation and
resource limits. `encode_ack` and `decode_ack` use a separate 4,096-byte budget
and limit diagnostic strings to 1,024 UTF-8 bytes. A decoded successful ACK must
also pass `validate_ack` against the original identity and requested policy.

## Labels and resource limits

Label keys do not require an allowlist. Nonempty keys, including Unicode and
punctuation, are accepted within the configured count and length limits; control
characters and duplicate keys are rejected. Missing `host` inherits
`Source.hostname`; missing `instance` inherits `Source.instance` for identity and
storage. No other labels are added. Encoding and decoding preserve all explicit
labels and reject duplicate storage-family/name/effective-label identities.
Counter and gauge share the scalar family; use distinct names for the same labels,
also across processes. Histograms use a separate distribution family.

The encoded-body limit and retained-model limit are separate. Decoding compacts
all model strings and the row vector before checking `max_batch_bytes`, so
allocation growth in MessagePack decoding cannot reject an otherwise valid sender
batch under identical limits. Both encoding and decoding apply the same preflight.
Its temporary wire/model workspace estimate is bounded by
`max_encoded_bytes + 2 * validation.max_batch_bytes`; this is a budget unit,
not an exact allocator/RSS peak. A nonallocating schema scan checks every array
count and string/binary length before deserialization, including row, label and
source-attribute limits. Its fixed schema is nonrecursive, rejecting unexpected
nested containers immediately; the deserializer additionally has a depth limit
of 16. Encoding counts bytes before allocating its exact-size output buffer and
applies the same preflight checks as decoding.
Account for encoded bodies, temporary decoding and retained batches separately
when choosing collector request concurrency.

See the crate API documentation for configuration and examples. The repository
README, DESIGN.md and deploy directory describe delivery guarantees, resource
limits, ClickHouse schema, TLS termination and operations.

[API documentation](https://docs.rs/metrics-summary-protocol).

Rust 1.95 or newer. Licensed under either MIT or Apache-2.0 at your option.
