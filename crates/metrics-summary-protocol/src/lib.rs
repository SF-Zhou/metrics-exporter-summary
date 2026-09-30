//! Validated MessagePack requests and acknowledgements for metric summaries.
//!
//! This crate converts [`metrics_summary_core::Batch`] values into the payloads
//! shared by the HTTP and TCP transports. It performs no network I/O or
//! authentication. Start with [`encode_request`] and [`decode_request`]; the
//! [`wire`] module exposes the lower-level MessagePack representation.
//!
//! # Request and acknowledgement round trip
//!
//! ```
//! use metrics_summary_core::{Batch, BatchId, MetricValue, Row, Source, MODEL_VERSION};
//! use metrics_summary_protocol::{
//!     ack, decode_ack, decode_request, encode_ack, encode_request, validate_ack,
//!     AckPolicy, ProtocolLimits, Status,
//! };
//! use std::collections::BTreeMap;
//! use uuid::Uuid;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let batch = Batch {
//!     model_version: MODEL_VERSION,
//!     id: BatchId {
//!         source_session_id: Uuid::new_v4(),
//!         sequence: 1,
//!     },
//!     source: Source {
//!         application: "example-server".into(),
//!         instance: "worker-1".into(),
//!         hostname: "host-a".into(),
//!         attributes: BTreeMap::new(),
//!     },
//!     timestamp: 1_700_000_000_000_000_000,
//!     duration_ns: 10_000_000_000,
//!     rows: vec![Row {
//!         metric_id: 1,
//!         name: "requests.completed".into(),
//!         labels: BTreeMap::from([("tag".into(), "success".into())]),
//!         unit: None,
//!         value: MetricValue::CounterDelta { delta_value: 42 },
//!     }],
//! };
//! let limits = ProtocolLimits::default();
//! let requested = AckPolicy::Enqueued;
//! let payload = encode_request(&batch, requested, &limits)?;
//!
//! // Receiver: decode, then admit the batch to its bounded queue.
//! let (received, policy) = decode_request(&payload, &limits)?;
//! assert_eq!(received, batch);
//! // Send this successful ACK only after admission succeeds.
//! let response = encode_ack(&ack(Some(received.id), policy, Status::Ok, ""))?;
//!
//! // Sender: decoding alone does not prove this request succeeded.
//! let response = decode_ack(&response)?;
//! validate_ack(&response, batch.id, requested)?;
//! # Ok(())
//! # }
//! ```
//!
//! [`AckPolicy::Enqueued`] confirms admission to a volatile collector queue;
//! [`AckPolicy::ClickHouseConfirmed`] waits for downstream write confirmation.
//! Neither level establishes exactly-once delivery. Preserve the original batch
//! identity and contents when retrying an uncertain result.
//!
//! # Model and wire contract
//!
//! Wire version [`WIRE_VERSION`] and model version
//! [`metrics_summary_core::MODEL_VERSION`] are currently both 1. They are checked
//! independently. Version 1 uses uncompressed MessagePack fixed-length arrays.
//! The complete positional schema is documented in the crate README. Integers
//! retain their full signed/unsigned 64-bit values; floating-point fields use
//! float64 exclusively. Maps, extra fields, float32 substitutions, missing
//! required fields and trailing data are rejected. There is no legacy format
//! compatibility.
//!
//! A batch carries a collection-completion wall timestamp in signed Unix
//! nanoseconds and an independent monotonic statistical duration in nanoseconds.
//! Both fields must be present, including when zero; duration is not collection
//! execution time. Counter values are interval increments in `0..=i64::MAX`.
//! Gauges are exact signed 64-bit current values. Histograms describe interval
//! observations, with count in `1..=2^53`, finite statistics and ordered quantiles
//! between min and max. Exported quantiles cannot be merged across batches.
//!
//! Labels do not require a key allowlist. Nonempty keys, including Unicode and
//! punctuation, are accepted within the configured length and count limits;
//! control characters and duplicate keys are rejected. Missing `host` and
//! `instance` inherit source values for identity and storage. No other labels
//! are added. Encoding and decoding preserve explicit labels rather than adding
//! these defaults. Duplicate effective identities are rejected; counters and
//! gauges share one storage family, while histograms use another.
//!
//! # Resource budgets
//!
//! [`ProtocolLimits`] separates encoded body bytes from retained model memory.
//! Both request helpers apply preflight count bounds and a temporary workspace
//! estimate capped at
//! `max_encoded_bytes + 2 * validation.max_batch_bytes`. This estimate is a budget
//! unit, not an exact allocator or RSS peak. The decoder runs this preflight before
//! MessagePack allocation. Decoding compacts model strings and
//! the row vector before final [`Batch::validate`] checks, so spare MessagePack
//! allocation capacity does not consume the sender's retained-model budget.
//! Transport implementations must bound body reads and request concurrency as
//! well; this crate only checks the byte slice it receives.
#![warn(missing_docs)]

use metrics_summary_core::{Batch, BatchId, MetricValue, Row, Source, ValidationLimits};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

/// Supported request and acknowledgement wire version.
pub const WIRE_VERSION: u32 = 1;
/// HTTP media type for uncompressed MessagePack request and acknowledgement bodies.
pub const CONTENT_TYPE: &str = "application/msgpack";
/// Four-byte prefix used to identify the version-1 TCP handshake.
pub const TCP_MAGIC: &[u8; 4] = b"MXS1";
/// Maximum encoded acknowledgement size accepted by [`decode_ack`].
pub const MAX_ACK_BYTES: usize = 4096;

/// Requested or achieved level of collector acknowledgement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i32)]
pub enum AckPolicy {
    /// Accepted into the collector's bounded, volatile queue; not yet durable.
    Enqueued = 1,
    /// The collector's downstream writer confirmed the batch's writes.
    /// This does not imply exactly-once delivery across retries or restarts.
    ClickHouseConfirmed = 2,
}
impl AckPolicy {
    /// Whether this achieved level is at least as strong as `requested`.
    pub fn satisfies(self, requested: Self) -> bool {
        self as i32 >= requested as i32
    }
}
impl TryFrom<i32> for AckPolicy {
    type Error = ProtocolError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Enqueued),
            2 => Ok(Self::ClickHouseConfirmed),
            _ => Err(invalid("unknown ACK policy")),
        }
    }
}

/// Collector outcome carried by an acknowledgement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum Status {
    /// The acknowledgement's stated policy was achieved.
    Ok = 0,
    /// A bounded collector resource could not admit the request.
    Overloaded = 1,
    /// The request, batch, or requested policy was rejected as invalid.
    Invalid = 2,
    /// Authentication or source authorization failed.
    Unauthorized = 3,
    /// The collector or downstream service could not complete the request.
    Unavailable = 4,
    /// The commit outcome is uncertain, including a confirmation timeout.
    /// An accepted batch may still be processing after this response.
    Unknown = 5,
}
impl TryFrom<i32> for Status {
    type Error = ProtocolError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Ok),
            1 => Ok(Self::Overloaded),
            2 => Ok(Self::Invalid),
            3 => Ok(Self::Unauthorized),
            4 => Ok(Self::Unavailable),
            5 => Ok(Self::Unknown),
            _ => Err(invalid("unknown ACK status")),
        }
    }
}

/// Independent limits for encoded requests and their validated retained models.
///
/// The default encoded-body limit is 8 MiB; model limits come from
/// [`ValidationLimits::default`]. Configure compatible limits on both peers.
#[derive(Clone, Debug)]
pub struct ProtocolLimits {
    /// Maximum encoded request size. Bodies and retained models have separate budgets.
    pub max_encoded_bytes: usize,
    /// Limits for the retained model, including its allocation capacities.
    /// Decoder preflight separately bounds temporary representations by
    /// `max_encoded_bytes + 2 * validation.max_batch_bytes` estimated bytes.
    pub validation: ValidationLimits,
}
impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_encoded_bytes: 8 * 1024 * 1024,
            validation: ValidationLimits::default(),
        }
    }
}

/// A malformed, unsupported, oversized, or semantically invalid protocol value.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ProtocolError(
    /// Human-readable diagnostic; callers should not depend on its exact wording.
    pub String,
);
fn invalid(message: impl Into<String>) -> ProtocolError {
    ProtocolError(message.into())
}

/// Low-level MessagePack DTOs. Struct fields encode as arrays in declaration order.
///
/// Requests contain 8 fields; acknowledgements 5, rows 5, sources 4, identities
/// and key/value entries 2, and histograms 8. Values encode as `[kind, payload]`
/// with kind 0 for histograms, 1 for counters and 2 for gauges. UUIDs use the
/// MessagePack binary type. Optional fields encode as `nil`; only a row's unit
/// and an acknowledgement's ID may be absent in validated traffic.
///
/// Direct Serde deserialization skips protocol limits and structural preflight.
/// Use [`crate::decode_request`] and [`crate::decode_ack`] for untrusted bytes.
/// [`crate::encode_wire_request`] supports trusted canonical encoding and fixtures;
/// use [`crate::encode_request`] for validated transport requests.
pub mod wire {
    /// One label or source attribute; duplicate keys are rejected during validation.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Entry {
        /// UTF-8 key, subject to the configured name and length restrictions.
        pub key: String,
        /// UTF-8 value, subject to the configured length limit.
        pub value: String,
    }
    /// Stable batch identity preserved across retries.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Id {
        /// Exactly 16 bytes containing a non-nil source-session UUID.
        #[serde(with = "serde_bytes")]
        pub session: Vec<u8>,
        /// Required, nonzero collection sequence within this session.
        pub sequence: Option<u64>,
    }
    /// Required source metadata carried by every request.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Source {
        /// Nonempty application or service name.
        pub application: String,
        /// Nonempty running-instance identity; fallback for the `instance` label.
        pub instance: String,
        /// Nonempty host name; fallback for the `host` label.
        pub hostname: String,
        /// Bounded source attributes, distinct from metric labels.
        pub attributes: Vec<Entry>,
    }
    /// One interval's histogram summary; all fields are required and finite.
    ///
    /// Quantiles must be ordered between `min` and `max`. An empty histogram
    /// window is represented by an absent row, not a zero-count summary.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Histogram {
        /// Observation count in `1..=2^53`.
        pub count: Option<u64>,
        /// Sum of interval observations, in the recorded unit.
        pub sum: Option<f64>,
        /// Estimated 50th percentile of interval observations.
        pub p50: Option<f64>,
        /// Estimated 90th percentile of interval observations.
        pub p90: Option<f64>,
        /// Estimated 99th percentile of interval observations.
        pub p99: Option<f64>,
        /// Maximum interval observation.
        pub max: Option<f64>,
        /// Minimum interval observation.
        pub min: Option<f64>,
        /// Estimated 95th percentile of interval observations.
        pub p95: Option<f64>,
    }
    /// One metric series with exactly one instrument-specific value.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Row {
        /// Required, nonzero recorder-local ID, unique within the batch.
        pub metric_id: Option<u64>,
        /// Nonempty metric name.
        pub name: String,
        /// Explicit labels; missing host/instance inherit source values for identity and storage.
        pub labels: Vec<Entry>,
        /// Optional declared unit; the protocol performs no unit conversion.
        pub unit: Option<String>,
        /// Required histogram summary, counter delta, or gauge snapshot.
        pub value: Option<Value>,
    }
    /// Instrument-specific payload for a metric row.
    #[derive(Clone, Debug, PartialEq)]
    pub enum Value {
        /// Statistics of observations collected during this interval.
        Histogram(Histogram),
        /// Counter delta for this collection interval, bounded to `i64::MAX`.
        Counter(u64),
        /// Exact current signed 64-bit integer gauge value.
        Gauge(i64),
    }
    impl serde::Serialize for Value {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeTuple;
            let mut tuple = serializer.serialize_tuple(2)?;
            match self {
                Self::Histogram(value) => {
                    tuple.serialize_element(&0u8)?;
                    tuple.serialize_element(value)?;
                }
                Self::Counter(value) => {
                    tuple.serialize_element(&1u8)?;
                    tuple.serialize_element(value)?;
                }
                Self::Gauge(value) => {
                    tuple.serialize_element(&2u8)?;
                    tuple.serialize_element(value)?;
                }
            }
            tuple.end()
        }
    }
    impl<'de> serde::Deserialize<'de> for Value {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct Visitor;
            impl<'de> serde::de::Visitor<'de> for Visitor {
                type Value = Value;
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("a [kind, payload] array")
                }
                fn visit_seq<A: serde::de::SeqAccess<'de>>(
                    self,
                    mut seq: A,
                ) -> Result<Value, A::Error> {
                    use serde::de::Error;
                    let kind: u8 = seq
                        .next_element()?
                        .ok_or_else(|| A::Error::custom("missing kind"))?;
                    let value = match kind {
                        0 => Value::Histogram(
                            seq.next_element()?
                                .ok_or_else(|| A::Error::custom("missing histogram"))?,
                        ),
                        1 => Value::Counter(
                            seq.next_element()?
                                .ok_or_else(|| A::Error::custom("missing counter"))?,
                        ),
                        2 => Value::Gauge(
                            seq.next_element()?
                                .ok_or_else(|| A::Error::custom("missing gauge"))?,
                        ),
                        _ => return Err(A::Error::custom("unknown instrument kind")),
                    };
                    if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                        return Err(A::Error::custom("extra instrument fields"));
                    }
                    Ok(value)
                }
            }
            deserializer.deserialize_tuple(2, Visitor)
        }
    }
    /// A complete collection batch and its requested acknowledgement policy.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Request {
        /// Protocol version; must equal [`super::WIRE_VERSION`].
        pub wire_version: u32,
        /// Data-model version; must equal [`metrics_summary_core::MODEL_VERSION`].
        pub model_version: u32,
        /// Numeric [`super::AckPolicy`]; unspecified and unknown values are invalid.
        pub ack_policy: i32,
        /// Required source-session and sequence identity.
        pub id: Option<Id>,
        /// Required source metadata.
        pub source: Option<Source>,
        /// Required collection-completion wall time in signed Unix nanoseconds.
        pub timestamp: Option<i64>,
        /// Required monotonic statistical span in nanoseconds; zero is valid.
        pub duration_ns: Option<u64>,
        /// Complete metric rows for the interval; empty batches are valid.
        pub rows: Vec<Row>,
    }
    /// Collector response; decode and validate it against the original request.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Ack {
        /// Protocol version; must equal [`super::WIRE_VERSION`].
        pub wire_version: u32,
        /// Original batch identity; may be absent if the request was not decoded.
        pub id: Option<Id>,
        /// Numeric [`super::AckPolicy`]; achieved level when status is successful.
        pub ack_policy: i32,
        /// Numeric [`super::Status`] describing the collector outcome.
        pub status: i32,
        /// Human-readable diagnostic; [`super::ack`] limits it to 1,024 UTF-8 bytes.
        pub message: String,
    }
}
fn entries(map: &BTreeMap<String, String>) -> Vec<wire::Entry> {
    map.iter()
        .map(|(key, value)| wire::Entry {
            key: key.clone(),
            value: value.clone(),
        })
        .collect()
}
fn map(entries: Vec<wire::Entry>) -> Result<BTreeMap<String, String>, ProtocolError> {
    let mut map = BTreeMap::new();
    for entry in entries {
        if map
            .insert(compact(entry.key), compact(entry.value))
            .is_some()
        {
            return Err(invalid("duplicate key"));
        }
    }
    Ok(map)
}
// Deserializers may grow strings/vectors while decoding. Spare capacity is not wire data:
// do not charge it to a sender whose original model fit the retained budget.
fn compact(value: String) -> String {
    value.into_boxed_str().into_string()
}
fn required<T>(v: Option<T>) -> Result<T, ProtocolError> {
    v.ok_or_else(|| invalid("missing required field"))
}
impl From<BatchId> for wire::Id {
    fn from(id: BatchId) -> Self {
        Self {
            session: id.source_session_id.as_bytes().to_vec(),
            sequence: Some(id.sequence),
        }
    }
}
impl TryFrom<wire::Id> for BatchId {
    type Error = ProtocolError;
    fn try_from(id: wire::Id) -> Result<Self, Self::Error> {
        Ok(Self {
            source_session_id: Uuid::from_slice(&id.session)
                .map_err(|_| invalid("invalid UUID"))?,
            sequence: required(id.sequence)?,
        })
    }
}
/// Builds a low-level request from a batch without validating either its contents
/// or its resource usage.
///
/// Prefer [`encode_request`] when preparing a transport payload. This helper is
/// useful for inspecting the wire representation and computing its encoded size.
pub fn request(batch: &Batch, policy: AckPolicy) -> wire::Request {
    wire::Request {
        wire_version: WIRE_VERSION,
        model_version: batch.model_version,
        ack_policy: policy as i32,
        id: Some(batch.id.into()),
        source: Some(wire::Source {
            application: batch.source.application.clone(),
            instance: batch.source.instance.clone(),
            hostname: batch.source.hostname.clone(),
            attributes: entries(&batch.source.attributes),
        }),
        timestamp: Some(batch.timestamp),
        duration_ns: Some(batch.duration_ns),
        rows: batch
            .rows
            .iter()
            .map(|r| wire::Row {
                metric_id: Some(r.metric_id),
                name: r.name.clone(),
                labels: entries(&r.labels),
                unit: r.unit.clone(),
                value: Some(match r.value {
                    MetricValue::HistogramSummary {
                        count,
                        sum,
                        min,
                        p50,
                        p90,
                        p95,
                        p99,
                        max,
                    } => wire::Value::Histogram(wire::Histogram {
                        count: Some(count),
                        sum: Some(sum),
                        min: Some(min),
                        p50: Some(p50),
                        p90: Some(p90),
                        p95: Some(p95),
                        p99: Some(p99),
                        max: Some(max),
                    }),
                    MetricValue::CounterDelta { delta_value } => wire::Value::Counter(delta_value),
                    MetricValue::GaugeSnapshot { current_value } => {
                        wire::Value::Gauge(current_value)
                    }
                }),
            })
            .collect(),
    }
}
/// Validates a batch and encodes an uncompressed MessagePack request.
///
/// Checks the model, retained-memory budget, encoded-body limit and the same
/// preflight workspace/count bounds used by [`decode_request`]. The output is
/// the message body only; HTTP headers and TCP framing belong to the transport.
///
/// # Errors
///
/// Returns [`ProtocolError`] if model validation or any protocol limit fails.
pub fn encode_request(
    batch: &Batch,
    policy: AckPolicy,
    limits: &ProtocolLimits,
) -> Result<Vec<u8>, ProtocolError> {
    batch
        .validate(&limits.validation)
        .map_err(|e| invalid(e.to_string()))?;
    let encoded = encode_with_limit(&request(batch, policy), limits.max_encoded_bytes)?;
    preflight(&encoded, limits)?;
    Ok(encoded)
}
/// Encodes a low-level request as a deterministic MessagePack array without model validation.
///
/// Intended for canonical fingerprints of already validated batches and malformed
/// test fixtures. This helper does not enforce request limits; use [`encode_request`]
/// for transport traffic. Identical DTOs always have identical encoded bytes.
pub fn encode_wire_request(request: &wire::Request) -> Result<Vec<u8>, ProtocolError> {
    encode_with_limit(request, usize::MAX)
}

fn encode_with_limit(value: &impl Serialize, limit: usize) -> Result<Vec<u8>, ProtocolError> {
    struct Counter {
        len: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.len) {
                return Err(std::io::Error::other("encoded message exceeds limit"));
            }
            self.len += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { len: 0, limit };
    value
        .serialize(&mut rmp_serde::Serializer::new(&mut counter).with_struct_tuple())
        .map_err(|e| invalid(e.to_string()))?;
    let mut encoded = Vec::with_capacity(counter.len);
    value
        .serialize(&mut rmp_serde::Serializer::new(&mut encoded).with_struct_tuple())
        .map_err(|e| invalid(e.to_string()))?;
    Ok(encoded)
}
mod preflight;
use preflight::{preflight, preflight_ack};

/// Decodes an uncompressed MessagePack request and validates its complete model.
///
/// Checks encoded size and repeated-field/workspace bounds before MessagePack
/// allocation, validates the wire version and policy, compacts retained strings
/// and vectors, and finally applies [`Batch::validate`]. Explicit labels are
/// preserved; their effective defaults are used for identity validation.
/// The fixed array schema rejects unknown fields, wrong scalar types and trailing data.
///
/// # Errors
///
/// Returns [`ProtocolError`] for invalid limits, malformed input, missing required
/// fields, unsupported versions/policies, duplicate map keys, invalid model data,
/// or exceeded resource bounds. No partially decoded batch is returned.
pub fn decode_request(
    bytes: &[u8],
    limits: &ProtocolLimits,
) -> Result<(Batch, AckPolicy), ProtocolError> {
    limits
        .validation
        .validate()
        .map_err(|error| invalid(error.to_string()))?;
    if bytes.len() > limits.max_encoded_bytes {
        return Err(invalid("encoded batch exceeds limit"));
    }
    preflight(bytes, limits)?;
    let mut decoder = rmp_serde::Deserializer::from_read_ref(bytes);
    decoder.set_max_depth(16);
    let r = wire::Request::deserialize(&mut decoder).map_err(|e| invalid(e.to_string()))?;
    if r.wire_version != WIRE_VERSION {
        return Err(invalid("unsupported wire version"));
    }
    let policy = AckPolicy::try_from(r.ack_policy).map_err(|_| invalid("unknown ACK policy"))?;
    let source = required(r.source)?;
    let batch = Batch {
        model_version: r.model_version,
        id: required(r.id)?.try_into()?,
        source: Source {
            application: compact(source.application),
            instance: compact(source.instance),
            hostname: compact(source.hostname),
            attributes: map(source.attributes)?,
        },
        timestamp: required(r.timestamp)?,
        duration_ns: required(r.duration_ns)?,
        rows: r
            .rows
            .into_iter()
            .map(|r| {
                Ok(Row {
                    metric_id: required(r.metric_id)?,
                    name: compact(r.name),
                    labels: map(r.labels)?,
                    unit: r.unit.map(compact),
                    value: match required(r.value)? {
                        wire::Value::Histogram(h) => MetricValue::HistogramSummary {
                            count: required(h.count)?,
                            sum: required(h.sum)?,
                            min: required(h.min)?,
                            p50: required(h.p50)?,
                            p90: required(h.p90)?,
                            p95: required(h.p95)?,
                            p99: required(h.p99)?,
                            max: required(h.max)?,
                        },
                        wire::Value::Counter(delta_value) => {
                            MetricValue::CounterDelta { delta_value }
                        }
                        wire::Value::Gauge(current_value) => {
                            MetricValue::GaugeSnapshot { current_value }
                        }
                    },
                })
            })
            .collect::<Result<Vec<_>, ProtocolError>>()?
            .into_boxed_slice()
            .into_vec(),
    };
    batch
        .validate(&limits.validation)
        .map_err(|e| invalid(e.to_string()))?;
    Ok((batch, policy))
}
/// Builds an acknowledgement using the current wire version.
///
/// `id` may be absent when the request could not be decoded. For a successful
/// response, `policy` must describe the level actually achieved. The diagnostic
/// is truncated to at most 1,024 bytes at a UTF-8 boundary. Encode the result using
/// [`encode_ack`].
///
/// This constructs a message; it does not perform queue admission or a write.
pub fn ack(
    id: Option<BatchId>,
    policy: AckPolicy,
    status: Status,
    message: impl Into<String>,
) -> wire::Ack {
    let mut message = message.into();
    let mut end = message.len().min(1024);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message.truncate(end);
    wire::Ack {
        wire_version: WIRE_VERSION,
        id: id.map(Into::into),
        ack_policy: policy as i32,
        status: status as i32,
        message,
    }
}
/// Encodes an acknowledgement after validating its independent size and shape limits.
///
/// Histories and request budgets do not affect ACK encoding. The message must
/// contain at most 1,024 UTF-8 diagnostic bytes and use supported version, status
/// and policy values. Successful delivery still requires [`validate_ack`].
pub fn encode_ack(ack: &wire::Ack) -> Result<Vec<u8>, ProtocolError> {
    let bytes = encode_with_limit(ack, MAX_ACK_BYTES)?;
    decode_ack(&bytes)?;
    Ok(bytes)
}
/// Decodes an acknowledgement after checking [`MAX_ACK_BYTES`], its wire version,
/// status and acknowledgement-policy enum values.
///
/// A decoded response may report an error or refer to another batch. Call
/// [`validate_ack`] before treating it as successful delivery of a request.
///
/// # Errors
///
/// Returns [`ProtocolError`] for an oversized or malformed message, unsupported
/// wire version, or unknown status/policy. Batch identity is checked separately
/// by [`validate_ack`].
pub fn decode_ack(bytes: &[u8]) -> Result<wire::Ack, ProtocolError> {
    if bytes.len() > MAX_ACK_BYTES {
        return Err(invalid("ACK too large"));
    }
    preflight_ack(bytes)?;
    let mut decoder = rmp_serde::Deserializer::from_read_ref(bytes);
    decoder.set_max_depth(16);
    let ack = wire::Ack::deserialize(&mut decoder).map_err(|e| invalid(e.to_string()))?;
    if ack.wire_version != WIRE_VERSION {
        return Err(invalid("unsupported ACK version"));
    }
    Status::try_from(ack.status).map_err(|_| invalid("unknown ACK status"))?;
    AckPolicy::try_from(ack.ack_policy).map_err(|_| invalid("unknown ACK policy"))?;
    Ok(ack)
}
/// Verifies that a decoded acknowledgement confirms this batch at the requested
/// level or a stronger one.
///
/// Pass the result of [`decode_ack`], which checks the version and enum values.
/// This function checks the batch identity, successful status, and achieved
/// policy; it does not itself decode or recheck the wire version.
///
/// # Errors
///
/// Returns [`ProtocolError`] if the identity is absent, malformed or mismatched,
/// if the collector reported failure, or if the acknowledgement is too weak.
pub fn validate_ack(ack: &wire::Ack, id: BatchId, policy: AckPolicy) -> Result<(), ProtocolError> {
    let actual: BatchId = required(ack.id.clone())?.try_into()?;
    if actual != id {
        return Err(invalid("ACK batch ID mismatch"));
    }
    let actual = AckPolicy::try_from(ack.ack_policy).map_err(|_| invalid("unknown ACK policy"))?;
    if ack.status != Status::Ok as i32 {
        return Err(invalid(format!("collector error: {}", ack.message)));
    }
    if !actual.satisfies(policy) {
        return Err(invalid("insufficient ACK level"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
