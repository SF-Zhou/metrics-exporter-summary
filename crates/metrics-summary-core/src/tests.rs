use super::*;

fn batch() -> Batch {
    Batch {
        model_version: MODEL_VERSION,
        id: BatchId {
            source_session_id: Uuid::new_v4(),
            sequence: 1,
        },
        source: Source {
            application: "service".into(),
            instance: "worker-1".into(),
            hostname: "node-1".into(),
            attributes: BTreeMap::new(),
        },
        timestamp: 80,
        duration_ns: 1,
        rows: vec![Row {
            metric_id: 1,
            name: "latency".into(),
            labels: BTreeMap::new(),
            unit: Some("seconds".into()),
            value: MetricValue::HistogramSummary {
                count: 2,
                sum: -3.0,
                min: -2.0,
                p50: -1.5,
                p90: -1.0,
                p95: -1.0,
                p99: -1.0,
                max: -1.0,
            },
        }],
    }
}

#[test]
fn accepts_negative_histograms_empty_rounds_and_independent_time_boundaries() {
    let mut value = batch();
    value.validate(&ValidationLimits::default()).unwrap();
    let bytes = value.estimated_bytes();
    for timestamp in [i64::MAX, 0, -1, i64::MIN] {
        for duration_ns in [0, 1, u64::MAX] {
            value.timestamp = timestamp;
            value.duration_ns = duration_ns;
            value.validate(&ValidationLimits::default()).unwrap();
            assert_eq!(value.estimated_bytes(), bytes);
        }
    }
    value.rows.clear();
    value.validate(&ValidationLimits::default()).unwrap();
}

#[test]
fn validates_identity_and_mandatory_hostname() {
    let valid = batch();
    for field in 0..7 {
        let mut invalid = valid.clone();
        match field {
            0 => invalid.source.hostname = " \t".into(),
            1 => invalid.id.source_session_id = Uuid::nil(),
            2 => invalid.id.sequence = 0,
            3 => invalid.rows[0].metric_id = 0,
            4 => invalid.model_version += 1,
            5 => invalid.rows.push(invalid.rows[0].clone()),
            6 => invalid.rows[0]
                .labels
                .insert("".into(), "value".into())
                .map_or((), |_| ()),
            _ => unreachable!(),
        }
        assert!(
            invalid.validate(&ValidationLimits::default()).is_err(),
            "case {field}"
        );
    }
}

#[test]
fn rejects_invalid_floating_point_and_summary_structure() {
    for statistic in 0..7 {
        for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut values = [1.0; 7];
            values[statistic] = invalid;
            let [sum, min, p50, p90, p95, p99, max] = values;
            assert!(MetricValue::HistogramSummary {
                count: 1,
                sum,
                min,
                p50,
                p90,
                p95,
                p99,
                max
            }
            .validate()
            .is_err());
        }
    }
    for index in 0..5 {
        let mut values = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0];
        values[index] = values[index + 1] + 0.5;
        let [min, p50, p90, p95, p99, max] = values;
        assert!(
            MetricValue::HistogramSummary {
                count: 1,
                sum: 1.0,
                min,
                p50,
                p90,
                p95,
                p99,
                max
            }
            .validate()
            .is_err(),
            "ordered pair {index}"
        );
    }
}

#[test]
fn histogram_count_stays_exact_in_float_storage_and_includes_min_and_p95() {
    for count in [0, 1, 1_u64 << 53, (1_u64 << 53) + 1, u64::MAX] {
        let value = MetricValue::HistogramSummary {
            count,
            sum: count as f64 * 2.0,
            min: 2.0,
            p50: 2.0,
            p90: 2.0,
            p95: 2.0,
            p99: 2.0,
            max: 2.0,
        };
        assert_eq!(
            value.validate().is_ok(),
            (1..=1_u64 << 53).contains(&count),
            "count={count}"
        );
        let json = serde_json::to_value(&value).unwrap();
        assert_eq!(json["HistogramSummary"]["min"], 2.0);
        assert_eq!(json["HistogramSummary"]["p95"], 2.0);
        assert_eq!(serde_json::from_value::<MetricValue>(json).unwrap(), value);
    }
}

#[test]
fn memory_estimate_counts_unused_capacity_and_saturates_samples() {
    let mut value = batch();
    let original = value.estimated_bytes();
    value.rows.reserve(20);
    assert!(value.estimated_bytes() >= original + 19 * size_of::<Row>());
    let original = value.estimated_bytes();
    value.source.hostname.reserve(4096);
    assert!(value.estimated_bytes() >= original + 4000);
    let mut row = value.rows[0].clone();
    row.metric_id = 2;
    row.name = "other.latency".into();
    row.value = MetricValue::HistogramSummary {
        count: u64::MAX,
        sum: 0.0,
        min: 0.0,
        p50: 0.0,
        p90: 0.0,
        p95: 0.0,
        p99: 0.0,
        max: 0.0,
    };
    value.rows.push(row);
    assert_eq!(value.histogram_samples(), u64::MAX);
    assert!(value.validate(&ValidationLimits::default()).is_err());
    if let MetricValue::HistogramSummary { count, .. } = &mut value.rows[1].value {
        *count = 1_u64 << 53;
    }
    let limits = ValidationLimits {
        max_batch_bytes: value.estimated_bytes() - 1,
        ..Default::default()
    };
    assert!(value.validate(&limits).is_err());
    value
        .validate(&ValidationLimits {
            max_batch_bytes: value.estimated_bytes(),
            ..limits
        })
        .unwrap();
}

#[test]
fn checks_every_variable_length_limit() {
    let mut value = batch();
    value
        .source
        .attributes
        .insert("region".into(), "test-east".into());
    value.rows[0].labels.insert("tag".into(), "POST".into());
    let defaults = ValidationLimits::default();
    let limits = [
        ValidationLimits {
            max_rows: 0,
            ..defaults.clone()
        },
        ValidationLimits {
            max_labels: 0,
            ..defaults.clone()
        },
        ValidationLimits {
            max_name_bytes: 1,
            ..defaults.clone()
        },
        ValidationLimits {
            max_label_key_bytes: 1,
            ..defaults.clone()
        },
        ValidationLimits {
            max_label_value_bytes: 1,
            ..defaults.clone()
        },
        ValidationLimits {
            max_unit_bytes: 1,
            ..defaults.clone()
        },
        ValidationLimits {
            max_source_attributes: 0,
            ..defaults.clone()
        },
        ValidationLimits {
            max_source_field_bytes: 1,
            ..defaults.clone()
        },
        ValidationLimits {
            max_attribute_key_bytes: 1,
            ..defaults.clone()
        },
        ValidationLimits {
            max_attribute_value_bytes: 1,
            ..defaults.clone()
        },
    ];
    for (index, limit) in limits.iter().enumerate() {
        assert!(value.validate(limit).is_err(), "limit {index}");
    }
    value.validate(&defaults).unwrap();
}

#[test]
fn serde_preserves_u64_edges_and_hostname() {
    let mut value = batch();
    value.id.sequence = u64::MAX;
    value.timestamp = i64::MIN;
    value.duration_ns = u64::MAX;
    value.rows[0].value = MetricValue::CounterDelta {
        delta_value: i64::MAX as u64,
    };
    let encoded = serde_json::to_vec(&value).unwrap();
    let decoded: Batch = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, value);
    decoded.validate(&ValidationLimits::default()).unwrap();
}

#[test]
fn counter_delta_accepts_zero_and_preserves_its_interval_value_in_json() {
    for delta_value in [0, 1, i64::MAX as u64] {
        let mut value = batch();
        value.rows[0].value = MetricValue::CounterDelta { delta_value };
        value.validate(&ValidationLimits::default()).unwrap();
        assert_eq!(value.rows[0].value.kind(), MetricKind::Counter);
        let json = serde_json::to_value(&value).unwrap();
        assert_eq!(
            json["rows"][0]["value"],
            serde_json::json!({"CounterDelta": {"delta_value": delta_value}})
        );
        assert_eq!(serde_json::from_value::<Batch>(json).unwrap(), value);
    }
}

#[test]
fn scalar_storage_preserves_signed_gauges_and_rejects_oversized_counter_deltas() {
    for delta_value in [i64::MAX as u64 + 1, u64::MAX] {
        assert!(MetricValue::CounterDelta { delta_value }
            .validate()
            .is_err());
    }
    for current_value in [
        i64::MIN,
        -9_007_199_254_740_993,
        -1,
        0,
        1,
        9_007_199_254_740_993,
        i64::MAX,
    ] {
        let value = MetricValue::GaugeSnapshot { current_value };
        value.validate().unwrap();
        let encoded = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            serde_json::from_slice::<MetricValue>(&encoded).unwrap(),
            value
        );
    }
}

#[test]
fn serialized_time_fields_are_required_even_when_zero() {
    let mut value = batch();
    value.timestamp = 0;
    value.duration_ns = 0;
    let encoded = serde_json::to_value(&value).unwrap();
    assert_eq!(
        serde_json::from_value::<Batch>(encoded.clone()).unwrap(),
        value
    );
    for field in ["timestamp", "duration_ns"] {
        let mut incomplete = encoded.clone();
        incomplete.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<Batch>(incomplete).is_err(),
            "{field}"
        );
    }
}

#[test]
fn discovers_real_hostname_and_classifies_write_errors() {
    let source = Source::new("service", "instance").unwrap();
    assert_eq!(
        source.hostname,
        hostname::get().unwrap().into_string().unwrap()
    );
    assert!(Source::new("", "instance").is_err());
    assert!(WriteError::new(ErrorKind::Timeout, CommitOutcome::Unknown, "timeout").is_retryable());
    assert!(
        !WriteError::new(ErrorKind::Permanent, CommitOutcome::NotCommitted, "invalid")
            .is_retryable()
    );
}

#[test]
fn effective_labels_preserve_arbitrary_keys_and_only_fill_host_and_instance() {
    let source = batch().source;
    let limits = ValidationLimits::default();
    let defaults = effective_labels(&source, &BTreeMap::new(), &limits).unwrap();
    assert_eq!(
        defaults,
        BTreeMap::from([
            ("host".into(), "node-1".into()),
            ("instance".into(), "worker-1".into()),
        ])
    );
    let labels = BTreeMap::from([
        ("host".into(), "override-host".into()),
        ("instance".into(), "override-instance".into()),
        ("hostname".into(), "independent-hostname".into()),
        ("区域".into(), "东区".into()),
        ("a.b/c-`".into(), "/path".into()),
        ("metricName".into(), "label-value".into()),
        ("Tag".into(), "upper".into()),
        ("tag".into(), "lower".into()),
        ("1name".into(), "value".into()),
    ]);
    assert_eq!(effective_labels(&source, &labels, &limits).unwrap(), labels);
    let mut value = batch();
    value.rows[0].labels = labels.clone();
    value.validate(&limits).unwrap();
    assert_eq!(
        value.rows[0].labels, labels,
        "validation does not mutate labels"
    );
}

#[test]
fn arbitrary_labels_still_obey_key_value_and_effective_count_limits() {
    let source = batch().source;
    let limits = ValidationLimits::default();
    for name in ["", " ", "bad\nkey", "bad\tkey", "bad\0key", "bad\u{7f}key"] {
        let labels = BTreeMap::from([(name.into(), "value".into())]);
        assert!(
            effective_labels(&source, &labels, &limits).is_err(),
            "{name:?}"
        );
        let mut row = batch().rows.remove(0);
        row.labels = labels;
        assert!(row.validate(&limits).is_err(), "{name:?}");
    }
    for host in ["", " \t", "bad\0host"] {
        let labels = BTreeMap::from([("host".into(), host.into())]);
        assert!(effective_labels(&source, &labels, &limits).is_err());
    }
    let labels = BTreeMap::from([("route".into(), "/".into())]);
    let two_labels = ValidationLimits {
        max_labels: 2,
        ..limits.clone()
    };
    assert!(effective_labels(&source, &labels, &two_labels).is_err());
    let mut value = batch();
    value.rows[0].labels = labels;
    assert!(value.validate(&two_labels).is_err());
    effective_labels(&source, &BTreeMap::new(), &two_labels).unwrap();
    for restricted in [
        ValidationLimits {
            max_label_key_bytes: 7,
            ..limits.clone()
        },
        ValidationLimits {
            max_label_value_bytes: 3,
            ..limits.clone()
        },
        ValidationLimits {
            max_labels: 1,
            ..limits
        },
    ] {
        assert!(
            effective_labels(&source, &BTreeMap::new(), &restricted).is_err(),
            "inherited labels must satisfy count, key and value limits"
        );
        assert!(batch().validate(&restricted).is_err());
    }
}

#[test]
fn validation_limits_are_resource_limits_with_no_label_schema() {
    let limits = ValidationLimits {
        max_rows: 0,
        max_labels: 0,
        ..Default::default()
    };
    limits.validate().unwrap();
    let mut empty = batch();
    empty.rows.clear();
    empty.validate(&limits).unwrap();
    assert!(ValidationLimits {
        max_batch_bytes: 0,
        ..limits
    }
    .validate()
    .is_err());
    let partial: ValidationLimits = serde_json::from_value(serde_json::json!({
        "max_labels": 2
    }))
    .unwrap();
    partial.validate().unwrap();
    assert_eq!(partial.max_rows, ValidationLimits::default().max_rows);
    assert_eq!(partial.max_labels, 2);
    assert!(!serde_json::to_value(&partial)
        .unwrap()
        .as_object()
        .unwrap()
        .contains_key("extra_labels"));
    assert!(
        serde_json::from_value::<ValidationLimits>(serde_json::json!({
            "extra_labels": ["region"]
        }))
        .is_err()
    );
}

#[test]
fn semantic_duplicates_use_storage_family_name_and_effective_labels_instead_of_metric_id() {
    let mut value = batch();
    let limits = ValidationLimits::default();
    let mut second = value.rows[0].clone();
    second.metric_id = 2;
    second.labels = effective_labels(&value.source, &second.labels, &limits).unwrap();
    value.rows.push(second);
    assert!(value
        .validate(&limits)
        .unwrap_err()
        .message
        .contains("effective labels"));
    value.rows[1].labels.insert("tag".into(), "distinct".into());
    value.validate(&limits).unwrap();
    value.rows[1].labels.insert("tag".into(), "".into());
    value.validate(&limits).unwrap(); // Missing and explicitly empty custom labels differ.
    value.rows[1].labels.remove("tag");
    value.rows[1].name = "other".into();
    value.validate(&limits).unwrap();
    value.rows[1].name = value.rows[0].name.clone();
    value.rows[1].value = MetricValue::GaugeSnapshot { current_value: 1 };
    value.validate(&limits).unwrap();
    value.rows[1].value = value.rows[0].value.clone();
    value.rows[1].unit = Some("different-unit".into());
    assert!(
        value.validate(&limits).is_err(),
        "unit is not part of storage identity"
    );
    value.rows[1]
        .labels
        .insert("host".into(), "different-host".into());
    value.validate(&limits).unwrap();
}

#[test]
fn counters_and_gauges_cannot_share_scalar_storage_identity() {
    let mut value = batch();
    value.rows[0].value = MetricValue::CounterDelta { delta_value: 1 };
    let mut gauge = value.rows[0].clone();
    gauge.metric_id = 2;
    gauge.value = MetricValue::GaugeSnapshot { current_value: 2 };
    value.rows.push(gauge);
    assert!(value
        .validate(&ValidationLimits::default())
        .unwrap_err()
        .message
        .contains("scalar"));
    value.rows[1]
        .labels
        .insert("tag".into(), "different".into());
    value.validate(&ValidationLimits::default()).unwrap();
    value.rows[1].labels.clear();
    value.rows[1].name = "different".into();
    value.validate(&ValidationLimits::default()).unwrap();
}
