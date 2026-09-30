//! Wire fixtures generated independently by Python's MessagePack implementation.
//! This catches changes that a Rust encoder/decoder round trip alone cannot.
use metrics_summary_core::{Batch, BatchId, MetricValue, Row, Source, MODEL_VERSION};
use metrics_summary_protocol::{
    ack, decode_ack, decode_request, encode_ack, encode_request, validate_ack, AckPolicy,
    ProtocolLimits, Status,
};
use std::collections::BTreeMap;
use uuid::Uuid;

const REQUEST: &[u8] = include_bytes!("fixtures/request.msgpack");
const ACK: &[u8] = include_bytes!("fixtures/ack.msgpack");

fn batch() -> Batch {
    let smallest = f64::from_bits(1);
    Batch {
        model_version: MODEL_VERSION,
        id: BatchId {
            source_session_id: Uuid::from_bytes([
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
            ]),
            sequence: u64::MAX,
        },
        source: Source {
            application: "svc".into(),
            instance: "worker".into(),
            hostname: "node".into(),
            attributes: BTreeMap::from([("region".into(), "华东".into())]),
        },
        timestamp: i64::MIN,
        duration_ns: u64::MAX,
        rows: vec![
            Row {
                metric_id: u64::MAX,
                name: "latency".into(),
                labels: BTreeMap::from([
                    ("http.method".into(), "GET".into()),
                    ("route".into(), "/read".into()),
                    ("区域".into(), "华东".into()),
                ]),
                unit: Some("seconds".into()),
                value: MetricValue::HistogramSummary {
                    count: 3,
                    sum: 0.6000000000000001,
                    min: -0.0,
                    p50: 0.1,
                    p90: 0.25,
                    p95: 0.29,
                    p99: 0.3,
                    max: 0.30000000000000004,
                },
            },
            Row {
                metric_id: 2,
                name: "counter".into(),
                labels: BTreeMap::new(),
                unit: None,
                value: MetricValue::CounterDelta {
                    delta_value: i64::MAX as u64,
                },
            },
            Row {
                metric_id: 3,
                name: "gauge".into(),
                labels: BTreeMap::new(),
                unit: None,
                value: MetricValue::GaugeSnapshot {
                    current_value: i64::MIN,
                },
            },
            Row {
                metric_id: 4,
                name: "smallest".into(),
                labels: BTreeMap::new(),
                unit: None,
                value: MetricValue::HistogramSummary {
                    count: 1,
                    sum: smallest,
                    min: smallest,
                    p50: smallest,
                    p90: smallest,
                    p95: smallest,
                    p99: smallest,
                    max: smallest,
                },
            },
        ],
    }
}

#[test]
fn request_matches_python_messagepack_and_preserves_float_bits() {
    let expected = batch();
    let limits = ProtocolLimits::default();
    let (decoded, policy) = decode_request(REQUEST, &limits).unwrap();
    assert_eq!(policy, AckPolicy::ClickHouseConfirmed);
    assert_eq!(decoded, expected);
    let MetricValue::HistogramSummary { min, max, .. } = decoded.rows[0].value else {
        panic!("histogram expected");
    };
    assert_eq!(min.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(max.to_bits(), 0.30000000000000004_f64.to_bits());
    let MetricValue::HistogramSummary { sum, .. } = decoded.rows[3].value else {
        panic!("histogram expected");
    };
    assert_eq!(sum.to_bits(), 1);
    assert_eq!(
        encode_request(&expected, policy, &limits).unwrap(),
        REQUEST,
        "Rust output must match the independently generated MessagePack fixture"
    );
}

#[test]
fn acknowledgement_matches_python_messagepack() {
    let batch = batch();
    let decoded = decode_ack(ACK).unwrap();
    validate_ack(&decoded, batch.id, AckPolicy::ClickHouseConfirmed).unwrap();
    let expected = ack(
        Some(batch.id),
        AckPolicy::ClickHouseConfirmed,
        Status::Ok,
        "完成",
    );
    assert_eq!(decoded, expected);
    assert_eq!(encode_ack(&expected).unwrap(), ACK);
}

#[test]
fn complete_message_is_required_without_trailing_values() {
    let limits = ProtocolLimits::default();
    for length in 0..REQUEST.len() {
        assert!(decode_request(&REQUEST[..length], &limits).is_err());
    }
    for length in 0..ACK.len() {
        assert!(decode_ack(&ACK[..length]).is_err());
    }
    for suffix in [&[0xc0][..], REQUEST, ACK] {
        let mut request = REQUEST.to_vec();
        request.extend_from_slice(suffix);
        assert!(decode_request(&request, &limits).is_err());
        let mut ack = ACK.to_vec();
        ack.extend_from_slice(suffix);
        assert!(decode_ack(&ack).is_err());
    }
}
