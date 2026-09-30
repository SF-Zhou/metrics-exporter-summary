use super::*;
fn batch() -> Batch {
    Batch {
        model_version: 1,
        id: BatchId {
            source_session_id: Uuid::new_v4(),
            sequence: u64::MAX,
        },
        source: Source {
            application: "service".into(),
            instance: "pid-1".into(),
            hostname: "host-a".into(),
            attributes: BTreeMap::new(),
        },
        timestamp: 3,
        duration_ns: 1,
        rows: vec![Row {
            metric_id: u64::MAX,
            name: "counter".into(),
            labels: BTreeMap::from([("pod".into(), "a".into())]),
            unit: None,
            value: MetricValue::CounterDelta {
                delta_value: i64::MAX as u64,
            },
        }],
    }
}
#[test]
fn roundtrip_u64_uuid_hostname_and_all_kinds() {
    let mut batch = batch();
    batch.rows.push(Row {
        metric_id: 2,
        name: "gauge".into(),
        labels: BTreeMap::new(),
        unit: Some("seconds".into()),
        value: MetricValue::GaugeSnapshot { current_value: -1 },
    });
    batch.rows.push(Row {
        metric_id: 3,
        name: "histogram".into(),
        labels: BTreeMap::new(),
        unit: None,
        value: MetricValue::HistogramSummary {
            count: 2,
            sum: 3.0,
            min: 1.0,
            p50: 1.0,
            p90: 2.0,
            p95: 2.0,
            p99: 2.0,
            max: 2.0,
        },
    });
    let bytes = encode_request(
        &batch,
        AckPolicy::ClickHouseConfirmed,
        &ProtocolLimits::default(),
    )
    .unwrap();
    let (decoded, policy) = decode_request(&bytes, &ProtocolLimits::default()).unwrap();
    assert_eq!(decoded, batch);
    assert_eq!(policy, AckPolicy::ClickHouseConfirmed);
}
#[test]
fn roundtrip_preserves_retained_budget_at_vector_and_string_growth_boundaries() {
    for count in [0, 1, 2, 3, 17, 33, 1000, 2300] {
        for full_labels in [false, true] {
            let mut batch = batch();
            batch.source.application = "a".into();
            batch.source.instance = "i".into();
            batch.source.hostname = "h".into();
            batch.source.attributes = BTreeMap::from([("a".into(), "测试".into())]);
            let labels = if full_labels {
                metrics_summary_core::effective_labels(
                    &batch.source,
                    &BTreeMap::from([
                        ("pod".into(), String::new()),
                        ("tag".into(), String::new()),
                        ("thread".into(), String::new()),
                        ("uid".into(), String::new()),
                        ("statusCode".into(), String::new()),
                        ("mount_name".into(), String::new()),
                        ("io".into(), String::new()),
                    ]),
                    &ValidationLimits::default(),
                )
                .unwrap()
            } else {
                BTreeMap::new()
            };
            batch.rows = (0..count)
                .map(|index| Row {
                    metric_id: index + 1,
                    name: format!("m{index}"),
                    labels: labels.clone(),
                    unit: Some("秒".into()),
                    value: MetricValue::CounterDelta { delta_value: 1 },
                })
                .collect::<Vec<_>>()
                .into_boxed_slice()
                .into_vec();
            // A legal sender exactly fills its retained-memory quota. This
            // must work both for sparse labels and normalized Recorder rows.
            let mut limits = ProtocolLimits::default();
            limits.validation.max_batch_bytes = batch.estimated_bytes();
            limits.max_encoded_bytes = encode_wire_request(&request(&batch, AckPolicy::Enqueued))
                .unwrap()
                .len();
            let encoded = encode_request(&batch, AckPolicy::Enqueued, &limits).unwrap();
            let (decoded, _) = decode_request(&encoded, &limits).unwrap();
            assert_eq!(decoded, batch);
            assert!(decoded.estimated_bytes() <= batch.estimated_bytes());
            assert_eq!(decoded.rows.capacity(), decoded.rows.len());
            let mut too_small = limits.clone();
            too_small.validation.max_batch_bytes = decoded.estimated_bytes() - 1;
            assert!(decode_request(&encoded, &too_small).is_err());
        }
    }
}
#[test]
fn counter_delta_roundtrip_preserves_zero_and_int64_maximum() {
    let limits = ProtocolLimits::default();
    let mut batch = batch();
    for delta_value in [0, 1, i64::MAX as u64] {
        batch.rows[0].value = MetricValue::CounterDelta { delta_value };
        let encoded = encode_request(&batch, AckPolicy::Enqueued, &limits).unwrap();
        let (decoded, _) = decode_request(&encoded, &limits).unwrap();
        assert_eq!(decoded, batch);
    }
    // Fixed MessagePack row: [1, "m", [], nil, [1, 0]]. Zero is present.
    let bytes = [0x95, 0x01, 0xa1, b'm', 0x90, 0xc0, 0x92, 0x01, 0x00];
    let row: wire::Row = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(row.value, Some(wire::Value::Counter(0)));
    assert_eq!(encode_with_limit(&row, usize::MAX).unwrap(), bytes);
    for delta_value in [i64::MAX as u64 + 1, u64::MAX] {
        batch.rows[0].value = MetricValue::CounterDelta { delta_value };
        assert!(encode_request(&batch, AckPolicy::Enqueued, &limits).is_err());
        let raw = encode_wire_request(&request(&batch, AckPolicy::Enqueued)).unwrap();
        assert!(decode_request(&raw, &limits).is_err());
    }
}
#[test]
fn signed_gauges_preserve_all_integer_bits_and_zero_presence() {
    let limits = ProtocolLimits::default();
    let mut batch = batch();
    for current_value in [
        i64::MIN,
        -9_007_199_254_740_993,
        -1,
        0,
        1,
        9_007_199_254_740_993,
        i64::MAX,
    ] {
        batch.rows[0].value = MetricValue::GaugeSnapshot { current_value };
        let bytes = encode_request(&batch, AckPolicy::Enqueued, &limits).unwrap();
        assert_eq!(decode_request(&bytes, &limits).unwrap().0, batch);
    }
    for (value, bytes) in [(-1, [0x92, 0x02, 0xff]), (0, [0x92, 0x02, 0x00])] {
        let payload: wire::Value = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(payload, wire::Value::Gauge(value));
        assert_eq!(encode_with_limit(&payload, usize::MAX).unwrap(), bytes);
    }
}
#[test]
fn histogram_requires_min_and_p95_and_enforces_float_count_precision() {
    let limits = ProtocolLimits::default();
    let mut batch = batch();
    batch.rows[0].value = MetricValue::HistogramSummary {
        count: 2,
        sum: -3.0,
        min: -2.0,
        p50: -1.5,
        p90: -1.2,
        p95: -1.1,
        p99: -1.01,
        max: -1.0,
    };
    let original = request(&batch, AckPolicy::Enqueued);
    assert_eq!(
        decode_request(&encode_wire_request(&original).unwrap(), &limits)
            .unwrap()
            .0,
        batch
    );
    for mutate in [
        |histogram: &mut wire::Histogram| histogram.min = None,
        |histogram: &mut wire::Histogram| histogram.p95 = None,
        |histogram: &mut wire::Histogram| histogram.min = Some(f64::NAN),
        |histogram: &mut wire::Histogram| histogram.p95 = Some(f64::INFINITY),
        |histogram: &mut wire::Histogram| histogram.p95 = Some(-3.0),
        |histogram: &mut wire::Histogram| histogram.count = Some((1_u64 << 53) + 1),
    ] {
        let mut malformed = original.clone();
        let Some(wire::Value::Histogram(histogram)) = &mut malformed.rows[0].value else {
            unreachable!()
        };
        mutate(histogram);
        assert!(decode_request(&encode_wire_request(&malformed).unwrap(), &limits).is_err());
    }
}
#[test]
fn completion_timestamp_and_duration_preserve_full_ranges_and_zero_presence() {
    let mut batch = batch();
    for timestamp in [i64::MAX, 0, -1, i64::MIN] {
        for duration_ns in [0, 1, u64::MAX] {
            batch.timestamp = timestamp;
            batch.duration_ns = duration_ns;
            let bytes =
                encode_request(&batch, AckPolicy::Enqueued, &ProtocolLimits::default()).unwrap();
            let dto: wire::Request = rmp_serde::from_slice(&bytes).unwrap();
            assert_eq!(dto.timestamp, Some(timestamp));
            assert_eq!(dto.duration_ns, Some(duration_ns));
            let (decoded, _) = decode_request(&bytes, &ProtocolLimits::default()).unwrap();
            assert_eq!(decoded, batch);
        }
    }
}
#[test]
fn malformed_unknown_versions_kinds_missing_hostname_and_fields_rejected() {
    let original = request(&batch(), AckPolicy::Enqueued);
    for mutate in [
        |r: &mut wire::Request| r.wire_version = 99,
        |r: &mut wire::Request| r.model_version = 99,
        |r: &mut wire::Request| r.ack_policy = 99,
        |r: &mut wire::Request| r.id = None,
        |r: &mut wire::Request| r.source = None,
        |r: &mut wire::Request| r.rows[0].metric_id = None,
        |r: &mut wire::Request| r.rows[0].value = None,
        |r: &mut wire::Request| r.source.as_mut().unwrap().hostname.clear(),
        |r: &mut wire::Request| r.timestamp = None,
        |r: &mut wire::Request| r.duration_ns = None,
    ] {
        let mut r = original.clone();
        mutate(&mut r);
        assert!(decode_request(
            &encode_wire_request(&r).unwrap(),
            &ProtocolLimits::default()
        )
        .is_err());
    }
    let bytes = encode_wire_request(&original).unwrap();
    assert!(decode_request(&bytes[..bytes.len() - 1], &ProtocolLimits::default()).is_err());
    let mut duplicate = original;
    let label = duplicate.rows[0].labels[0].clone();
    duplicate.rows[0].labels.push(label);
    assert!(decode_request(
        &encode_wire_request(&duplicate).unwrap(),
        &ProtocolLimits::default()
    )
    .is_err());
}
#[test]
fn bounded_decode_preflights_rows_labels_and_encoded_size() {
    let request = request(&batch(), AckPolicy::Enqueued);
    let bytes = encode_wire_request(&request).unwrap();
    let mut limits = ProtocolLimits {
        max_encoded_bytes: bytes.len() - 1,
        ..Default::default()
    };
    assert!(decode_request(&bytes, &limits).is_err());
    limits.max_encoded_bytes = bytes.len();
    limits.validation.max_rows = 0;
    assert!(decode_request(&bytes, &limits).is_err());
    limits.validation.max_rows = 1;
    limits.validation.max_labels = 0;
    assert!(decode_request(&bytes, &limits).is_err());
    let mut bytes =
        encode_request(&batch(), AckPolicy::Enqueued, &ProtocolLimits::default()).unwrap();
    bytes.push(0xc0);
    assert!(
        decode_request(&bytes, &ProtocolLimits::default()).is_err(),
        "a second MessagePack value is never part of this request"
    );
}
#[test]
fn arbitrary_labels_roundtrip_without_configuration_and_duplicate_identities_fail() {
    let mut batch = batch();
    batch.rows[0].labels.extend([
        ("route".into(), "/read".into()),
        ("http.method".into(), "GET".into()),
        ("区域".into(), "华东".into()),
        ("metricName".into(), "ordinary protocol label".into()),
    ]);
    let limits = ProtocolLimits::default();
    let bytes = encode_request(&batch, AckPolicy::Enqueued, &limits).unwrap();
    assert_eq!(decode_request(&bytes, &limits).unwrap().0, batch);
    assert!(!batch.rows[0].labels.contains_key("host"));
    assert!(!batch.rows[0].labels.contains_key("instance"));
    let mut duplicate = batch.clone();
    let mut row = duplicate.rows[0].clone();
    row.metric_id = 3;
    row.labels
        .insert("host".into(), duplicate.source.hostname.clone());
    duplicate.rows.push(row);
    // Bypass encode validation to exercise the untrusted decoder path.
    assert!(decode_request(
        &encode_wire_request(&request(&duplicate, AckPolicy::Enqueued)).unwrap(),
        &limits
    )
    .is_err());
}
#[test]
fn invalid_label_keys_and_sizes_fail_at_both_protocol_boundaries() {
    let limits = ProtocolLimits::default();
    for key in [
        String::new(),
        "line\nbreak".into(),
        "x".repeat(limits.validation.max_label_key_bytes + 1),
    ] {
        let mut batch = batch();
        batch.rows[0].labels.insert(key, "value".into());
        assert!(encode_request(&batch, AckPolicy::Enqueued, &limits).is_err());
        let raw = encode_wire_request(&request(&batch, AckPolicy::Enqueued)).unwrap();
        assert!(decode_request(&raw, &limits).is_err());
    }
}
#[test]
fn ack_identity_and_strength_are_checked() {
    let id = batch().id;
    let a = ack(Some(id), AckPolicy::Enqueued, Status::Ok, "");
    assert!(validate_ack(&a, id, AckPolicy::Enqueued).is_ok());
    assert!(validate_ack(&a, id, AckPolicy::ClickHouseConfirmed).is_err());
    let mut other = id;
    other.sequence -= 1;
    assert!(validate_ack(&a, other, AckPolicy::Enqueued).is_err());
    assert!(decode_ack(&vec![0; MAX_ACK_BYTES + 1]).is_err());
}

// Independently assembled minimal positional envelope. Using explicit bytes
// here keeps malformed type/length coverage independent of Serde's encoder.
fn envelope(rows: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0x98, 1, 1, 1, 0x92, 0xc4, 16];
    bytes.extend_from_slice(&[1; 16]);
    bytes.extend_from_slice(&[1, 0x94, 0xa1, b'a', 0xa1, b'i', 0xa1, b'h', 0x90, 0, 0]);
    bytes.extend_from_slice(rows);
    bytes
}
fn envelope_with_value(value: &[u8]) -> Vec<u8> {
    let mut rows = vec![0x91, 0x95, 1, 0xa1, b'm', 0x90, 0xc0];
    rows.extend_from_slice(value);
    envelope(&rows)
}

#[test]
fn fixed_arrays_reject_maps_extra_fields_and_unknown_kinds() {
    let limits = ProtocolLimits::default();
    let original = envelope(&[0x90]);
    assert!(decode_request(&original, &limits).is_ok());
    for marker in [0x97, 0x99, 0x88] {
        let mut changed = original.clone();
        changed[0] = marker;
        assert!(decode_request(&changed, &limits).is_err());
    }
    let named = rmp_serde::to_vec_named(&request(&batch(), AckPolicy::Enqueued)).unwrap();
    assert!(decode_request(&named, &limits).is_err());
    for value in [&[0x92, 3, 0][..], &[0x91, 1], &[0x93, 1, 0, 0], &[0xc0]] {
        assert!(decode_request(&envelope_with_value(value), &limits).is_err());
    }
}

#[test]
fn floats_require_float64_and_integer_fields_never_convert_through_float() {
    let limits = ProtocolLimits::default();
    let mut one = vec![0xcb];
    one.extend_from_slice(&1.0_f64.to_be_bytes());
    for replacement in [&one[..], &[0xca, 0x3f, 0x80, 0, 0], &[1], &[0xc0]] {
        let mut value = vec![0x92, 0, 0x98, 1];
        value.extend_from_slice(replacement);
        for _ in 0..6 {
            value.extend_from_slice(&one);
        }
        let result = decode_request(&envelope_with_value(&value), &limits);
        assert_eq!(result.is_ok(), replacement == one);
    }
    let mut counter_float = vec![0x92, 1];
    counter_float.extend_from_slice(&one);
    assert!(decode_request(&envelope_with_value(&counter_float), &limits).is_err());
    // Gauge overflow and a signed marker used for an unsigned counter are rejected.
    for value in [
        &[
            0x92, 2, 0xcf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ][..],
        &[0x92, 1, 0xd0, 1],
    ] {
        assert!(decode_request(&envelope_with_value(value), &limits).is_err());
    }
}

#[test]
fn impossible_lengths_reserved_types_and_deep_nesting_fail_before_deserialization() {
    let limits = ProtocolLimits::default();
    for bytes in [
        vec![0xc1],
        vec![0xc7, 0xff, 0],
        envelope(&[0xdd, 0xff, 0xff, 0xff, 0xff]),
        envelope(&[0x91, 0x95, 1, 0xdb, 0xff, 0xff, 0xff, 0xff]),
        vec![0x98, 1, 1, 1, 0x92, 0xc6, 0xff, 0xff, 0xff, 0xff],
        vec![0x98; 100_000],
    ] {
        assert!(preflight(&bytes, &limits).is_err());
        assert!(decode_request(&bytes, &limits).is_err());
    }
}

#[test]
fn source_attributes_and_string_lengths_are_bounded_before_deserialization() {
    let mut dto = request(&batch(), AckPolicy::Enqueued);
    dto.source.as_mut().unwrap().attributes = vec![
        wire::Entry {
            key: "a".into(),
            value: "one".into(),
        },
        wire::Entry {
            key: "b".into(),
            value: "two".into(),
        },
    ];
    let bytes = encode_wire_request(&dto).unwrap();
    let mut limits = ProtocolLimits::default();
    limits.validation.max_source_attributes = 1;
    assert!(preflight(&bytes, &limits).is_err());
    limits.validation.max_source_attributes = 2;
    assert!(decode_request(&bytes, &limits).is_ok());
    limits.validation.max_attribute_value_bytes = 2;
    assert!(preflight(&bytes, &limits).is_err());
    dto.source.as_mut().unwrap().attributes[1].key = "a".into();
    assert!(decode_request(
        &encode_wire_request(&dto).unwrap(),
        &ProtocolLimits::default()
    )
    .is_err());
}

#[test]
fn workspace_and_announced_row_counts_are_bounded_before_deserialization() {
    let mut dto = request(&batch(), AckPolicy::Enqueued);
    dto.rows = vec![wire::Row::default(); 1000];
    let bytes = encode_wire_request(&dto).unwrap();
    let mut limits = ProtocolLimits {
        max_encoded_bytes: bytes.len(),
        validation: ValidationLimits {
            max_batch_bytes: 1024,
            ..Default::default()
        },
    };
    let error = preflight(&bytes, &limits).unwrap_err();
    assert!(error.0.contains("decoding workspace"), "{error}");
    assert!(decode_request(&bytes, &limits).is_err());
    limits.validation.max_rows = 16;
    assert!(preflight(&bytes, &limits)
        .unwrap_err()
        .0
        .contains("array count"));
}

#[test]
fn encoded_size_is_enforced_in_both_directions_and_raw_encoding_is_deterministic() {
    let batch = batch();
    let dto = request(&batch, AckPolicy::Enqueued);
    let bytes = encode_wire_request(&dto).unwrap();
    assert_eq!(bytes, encode_wire_request(&dto).unwrap());
    for (limit, valid) in [(bytes.len(), true), (bytes.len() - 1, false)] {
        let limits = ProtocolLimits {
            max_encoded_bytes: limit,
            ..Default::default()
        };
        assert_eq!(
            encode_request(&batch, AckPolicy::Enqueued, &limits).is_ok(),
            valid
        );
        assert_eq!(decode_request(&bytes, &limits).is_ok(), valid);
    }
}

#[test]
fn acknowledgement_shape_enum_budget_and_trailing_data_are_checked() {
    let a = ack(Some(batch().id), AckPolicy::Enqueued, Status::Ok, "ok");
    let bytes = encode_ack(&a).unwrap();
    assert_eq!(decode_ack(&bytes).unwrap(), a);
    for mutate in [
        |a: &mut wire::Ack| a.wire_version = 99,
        |a: &mut wire::Ack| a.ack_policy = 99,
        |a: &mut wire::Ack| a.status = 99,
        |a: &mut wire::Ack| a.message = "x".repeat(1025),
        |a: &mut wire::Ack| a.id.as_mut().unwrap().session = vec![0; 17],
    ] {
        let mut bad = a.clone();
        mutate(&mut bad);
        assert!(encode_ack(&bad).is_err());
        assert!(decode_ack(&rmp_serde::to_vec(&bad).unwrap()).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0xc0);
    assert!(decode_ack(&trailing).is_err());
    for end in 0..bytes.len() {
        assert!(decode_ack(&bytes[..end]).is_err());
    }
    for malformed in [
        &[0x95, 1, 0xc0, 1, 0, 0xdb, 0xff, 0xff, 0xff, 0xff][..],
        &[0x95, 1, 0xc0, 1, 0, 0x91, 0x91, 0x91],
    ] {
        assert!(decode_ack(malformed).is_err());
    }
    let no_id = ack(
        None,
        AckPolicy::Enqueued,
        Status::Invalid,
        "invalid request",
    );
    assert_eq!(decode_ack(&encode_ack(&no_id).unwrap()).unwrap(), no_id);
}

#[test]
fn truncated_and_deterministically_mutated_messages_never_panic() {
    let original =
        encode_request(&batch(), AckPolicy::Enqueued, &ProtocolLimits::default()).unwrap();
    let limits = ProtocolLimits::default();
    for end in 0..original.len() {
        assert!(decode_request(&original[..end], &limits).is_err());
    }
    let mut seed = 0x1234_5678u64;
    for length in 0..256 {
        let mut bytes = vec![0; length];
        for byte in &mut bytes {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (seed >> 32) as u8;
        }
        assert!(std::panic::catch_unwind(|| decode_request(&bytes, &limits)).is_ok());
        assert!(std::panic::catch_unwind(|| decode_ack(&bytes)).is_ok());
    }
}
