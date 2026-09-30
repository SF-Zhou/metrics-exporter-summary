use super::*;
use metrics_summary_core::{BatchId, Source, MODEL_VERSION};
use std::{net::TcpListener, sync::mpsc, thread};

fn batch() -> Arc<Batch> {
    Arc::new(Batch {
        model_version: MODEL_VERSION,
        id: BatchId {
            source_session_id: Uuid::new_v4(),
            sequence: 9_007_199_254_740_993,
        },
        source: Source {
            application: "test".into(),
            instance: "pod-1".into(),
            hostname: "host-1".into(),
            attributes: BTreeMap::from([("zone".into(), "cn-a".into())]),
        },
        timestamp: 1_750_000_001_223_456_789,
        duration_ns: u64::MAX,
        rows: vec![
            Row {
                metric_id: 1,
                name: "latency".into(),
                labels: BTreeMap::new(),
                unit: Some("seconds".into()),
                value: MetricValue::HistogramSummary {
                    count: 3,
                    sum: 6.0,
                    min: 1.0,
                    p50: 2.0,
                    p90: 3.0,
                    p95: 3.0,
                    p99: 3.0,
                    max: 3.0,
                },
            },
            Row {
                metric_id: 2,
                name: "requests".into(),
                labels: BTreeMap::from([("tag".into(), "a\"b\nc".into())]),
                unit: None,
                value: MetricValue::CounterDelta {
                    delta_value: i64::MAX as u64,
                },
            },
            Row {
                metric_id: 3,
                name: "queue".into(),
                labels: BTreeMap::new(),
                unit: None,
                value: MetricValue::GaugeSnapshot { current_value: -1 },
            },
        ],
    })
}

struct Reply {
    status: u16,
    body: String,
    matching_id: bool,
    response_gate: Option<mpsc::Receiver<()>>,
    disconnect: bool,
}
impl Reply {
    fn success() -> Self {
        Self {
            status: 200,
            body: String::new(),
            matching_id: true,
            response_gate: None,
            disconnect: false,
        }
    }
    fn error(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.into(),
            ..Self::success()
        }
    }
}
struct Request {
    query: BTreeMap<String, String>,
    body: Vec<u8>,
}
fn mock(replies: Vec<Reply>) -> (String, mpsc::Receiver<Request>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        for reply in replies {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut data = Vec::new();
            let header_end = loop {
                let mut b = [0u8; 4096];
                let n = stream.read(&mut b).unwrap();
                assert!(n > 0);
                data.extend_from_slice(&b[..n]);
                if let Some(end) = data.windows(4).position(|s| s == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = std::str::from_utf8(&data[..header_end]).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|n| n.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            let path = headers
                .lines()
                .next()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap();
            let url = Url::parse(&format!("http://127.0.0.1{path}")).unwrap();
            let query: BTreeMap<String, String> = url.query_pairs().into_owned().collect();
            while data.len() - header_end < length {
                let mut b = [0u8; 4096];
                let n = stream.read(&mut b).unwrap();
                assert!(n > 0);
                data.extend_from_slice(&b[..n]);
            }
            let query_id = if reply.matching_id {
                query["query_id"].clone()
            } else {
                "other-request".into()
            };
            let _ = tx.send(Request {
                query,
                body: data[header_end..].to_vec(),
            });
            if let Some(gate) = reply.response_gate {
                gate.recv_timeout(Duration::from_secs(10))
                    .expect("test must release the response after the client finishes");
            }
            if reply.disconnect {
                continue;
            }
            let response = format!("HTTP/1.1 {} Test\r\nContent-Length: {}\r\nX-ClickHouse-Query-Id: {}\r\nConnection: close\r\n\r\n{}", reply.status, reply.body.len(), query_id, reply.body);
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (endpoint, rx, handle)
}
fn writer(endpoint: String) -> ClickHouseBatchWriter {
    let mut writer = ClickHouseBatchWriter::new(ClickHouseConfig {
        endpoint,
        ..ClickHouseConfig::default()
    })
    .unwrap();
    writer.schema_checked = true;
    for line in schema(&[]).lines() {
        let table: SchemaTable = serde_json::from_str(line).unwrap();
        for (name, column_type) in table.columns {
            writer
                .schema_columns
                .insert((table.table.clone(), name), column_type);
        }
    }
    writer.client = Some(
        Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
    );
    writer
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

fn schema(extra: &[(&str, &str, &str)]) -> String {
    let mut schema = String::new();
    for (index, table) in ["counters", "distributions"].into_iter().enumerate() {
        let columns: Vec<_> = [
            ("TIMESTAMP", "DateTime"),
            ("metricName", "LowCardinality(String)"),
            ("host", "String"),
            ("instance", "String"),
            ("tag", "String"),
            (
                if index == 0 { "type" } else { "method" },
                "LowCardinality(String)",
            ),
        ]
        .into_iter()
        .chain(VALUE_COLUMNS[index].iter().copied())
        .chain(
            extra
                .iter()
                .filter(|(target, _, _)| *target == table)
                .map(|(_, name, kind)| (*name, *kind)),
        )
        .collect();
        schema.push_str(&serde_json::json!({"table":table,"columns":columns}).to_string());
        schema.push('\n');
    }
    schema
}

#[test]
fn encoding_preflight_counts_escaped_flat_labels_and_enforces_limits() {
    let mut batch = (*batch()).clone();
    let limits = ValidationLimits::default();
    batch.rows[0].labels = BTreeMap::from([
        ("tag".into(), "backslash\\\n\u{0001}中文".into()),
        ("region".into(), "quote\"".into()),
    ]);
    batch.validate(&limits).unwrap();
    let timestamp = timestamp(batch.timestamp).unwrap();
    let mut body = Vec::new();
    for row in &batch.rows {
        serde_json::to_writer(
            &mut body,
            &EncodedRow::new(&batch, row, timestamp, &limits).unwrap(),
        )
        .unwrap();
        body.push(b'\n');
    }
    assert_eq!(encoded_batch_bytes(&batch, &limits).unwrap(), body.len());
    assert_eq!(
        encoded_batch_bytes_with_limits(&batch, &limits, body.len(), deadline()).unwrap(),
        body.len()
    );
    let error =
        encoded_batch_bytes_with_limits(&batch, &limits, body.len() - 1, deadline()).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Permanent);
    assert_eq!(error.outcome, CommitOutcome::NotCommitted);
    let error =
        encoded_batch_bytes_with_limits(&batch, &limits, usize::MAX, Instant::now()).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert_eq!(error.outcome, CommitOutcome::NotCommitted);

    // Control characters expand to six JSON bytes each. Charge the actual flat
    // label encoding, even when the validated source batch is under its limit.
    let gauge = batch.rows[2].clone();
    batch.rows = (1..=1000)
        .map(|id| Row {
            metric_id: id,
            labels: BTreeMap::from([("tag".into(), format!("{id}{}", "\u{0001}".repeat(1000)))]),
            ..gauge.clone()
        })
        .collect();
    batch.validate(&limits).unwrap();
    assert!(encoded_batch_bytes_with_limits(&batch, &limits, 4 * 1024 * 1024, deadline()).is_err());
    assert!(encoded_batch_bytes(&batch, &limits).unwrap() > 4 * 1024 * 1024);
}

#[test]
fn configuration_validation_redacts_credentials_and_rejects_sql_injection() {
    let config = ClickHouseConfig {
        endpoint: "http://user:secret@localhost".into(),
        password: Some("secret".into()),
        ..ClickHouseConfig::default()
    };
    assert!(config.validate().is_err());
    assert!(!format!("{config:?}").contains("secret"));
    for database in ["", "bad-db", "db;DROP TABLE anything", "foo.bar", "123db"] {
        assert!(ClickHouseConfig {
            database: database.into(),
            ..ClickHouseConfig::default()
        }
        .validate()
        .is_err());
    }
    assert!(ClickHouseConfig {
        request_timeout: Duration::ZERO,
        ..ClickHouseConfig::default()
    }
    .validate()
    .is_err());
    for pem in [
        vec![],
        b"not a PEM certificate".to_vec(),
        vec![b'a'; 1024 * 1024 + 1],
    ] {
        assert!(ClickHouseConfig {
            tls_ca_pem: Some(pem),
            ..ClickHouseConfig::default()
        }
        .validate()
        .is_err());
    }
}

fn json_rows(request: &Request) -> Vec<serde_json::Value> {
    request
        .body
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}

#[test]
fn writes_counter_and_gauge_together_and_full_distribution() {
    let (endpoint, rx, server) = mock(vec![Reply::success(), Reply::success()]);
    let mut writer = writer(endpoint);
    writer.config.async_insert = true;
    let batch = batch();
    writer
        .write_group(std::slice::from_ref(&batch), deadline())
        .unwrap();
    server.join().unwrap();
    let requests: Vec<_> = rx.try_iter().collect();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].query["query"].contains("`counters`"));
    assert!(requests[1].query["query"].contains("`distributions`"));
    assert_eq!(
        encoded_batch_bytes(&batch, &ValidationLimits::default()).unwrap(),
        requests.iter().map(|r| r.body.len()).sum::<usize>()
    );
    for request in &requests {
        assert_eq!(request.query["async_insert"], "1");
        assert_eq!(request.query["wait_for_async_insert"], "1");
        assert_eq!(request.query["wait_end_of_query"], "1");
        assert_eq!(
            request.query["input_format_defaults_for_omitted_fields"],
            "1"
        );
        for row in json_rows(request) {
            assert_eq!(row["host"], "host-1");
            assert_eq!(row["instance"], "pod-1");
            for label in ["pod", "uid", "thread", "statusCode", "mount_name", "io"] {
                assert!(row.get(label).is_none());
            }
            for omitted in [
                "application",
                "model_version",
                "source_attributes",
                "unit",
                "metric_id",
                "labels",
                "timestamp",
                "duration_ns",
                "source_session_id",
                "sequence",
                "type",
                "method",
            ] {
                assert!(
                    row.get(omitted).is_none(),
                    "unexpected storage column {omitted}"
                );
            }
            assert_eq!(row["TIMESTAMP"], 1_750_000_001u32);
        }
    }
    let scalars = json_rows(&requests[0]);
    assert_eq!(scalars.len(), 2);
    assert_eq!(scalars[0]["metricName"], "requests");
    assert_eq!(scalars[0]["val"].as_i64(), Some(i64::MAX));
    assert_eq!(scalars[0]["tag"], "a\"b\nc");
    assert_eq!(scalars[1]["metricName"], "queue");
    assert_eq!(scalars[1]["val"].as_i64(), Some(-1));
    let distributions = json_rows(&requests[1]);
    assert_eq!(distributions.len(), 1);
    for (name, value) in [
        ("count", 3.0),
        ("mean", 2.0),
        ("min", 1.0),
        ("max", 3.0),
        ("p50", 2.0),
        ("p90", 3.0),
        ("p95", 3.0),
        ("p99", 3.0),
    ] {
        assert_eq!(distributions[0][name].as_f64(), Some(value));
    }
}

#[test]
fn arbitrary_labels_are_preserved_per_table_and_per_row_without_configuration() {
    let quoted_label = "区域.\"'`\\column";
    let metadata = schema(&[
        ("counters", "application", "String"),
        ("counters", "region.name", "LowCardinality(String)"),
        ("counters", "count", "String"),
        ("distributions", "val", "String"),
        ("distributions", quoted_label, "String"),
    ]);
    let (endpoint, rx, server) = mock(vec![
        Reply::error(200, &metadata),
        Reply::success(),
        Reply::success(),
    ]);
    let mut writer = writer(endpoint);
    writer.schema_checked = false;
    let mut batch = (*batch()).clone();
    batch.rows[0].labels = BTreeMap::from([
        ("host".into(), "explicit-host".into()),
        ("method".into(), "GET".into()),
        ("val".into(), "distribution label".into()),
        (quoted_label.into(), "value\"\\\n".into()),
    ]);
    batch.rows[1].labels = BTreeMap::from([
        ("instance".into(), "explicit-instance".into()),
        ("region.name".into(), "cn-a".into()),
        ("count".into(), "scalar label".into()),
        ("type".into(), "counter".into()),
    ]);
    batch.rows[2].labels = BTreeMap::from([("application".into(), "label-app".into())]);
    batch.validate(&ValidationLimits::default()).unwrap();
    let encoded_bytes = encoded_batch_bytes(&batch, &ValidationLimits::default()).unwrap();
    writer.write_group(&[Arc::new(batch)], deadline()).unwrap();
    server.join().unwrap();
    let requests: Vec<_> = rx.try_iter().collect();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        encoded_bytes,
        requests
            .iter()
            .map(|request| request.body.len())
            .sum::<usize>()
    );
    assert!(requests[0].query["query"].starts_with("SELECT"));
    assert_eq!(
        requests[1].query["query"],
        "INSERT INTO `metrics_summary`.`counters` FORMAT JSONEachRow"
    );
    assert_eq!(
        requests[2].query["query"],
        "INSERT INTO `metrics_summary`.`distributions` FORMAT JSONEachRow"
    );
    let scalars = json_rows(&requests[1]);
    assert_eq!(scalars[0]["host"], "host-1");
    assert_eq!(scalars[0]["instance"], "explicit-instance");
    assert_eq!(scalars[0]["region.name"], "cn-a");
    assert_eq!(scalars[0]["count"], "scalar label");
    assert_eq!(scalars[0]["type"], "counter");
    assert!(scalars[0].get("application").is_none());
    assert_eq!(scalars[1]["application"], "label-app");
    assert_eq!(scalars[1]["instance"], "pod-1");
    assert!(scalars[1].get("region.name").is_none());
    let distributions = json_rows(&requests[2]);
    assert_eq!(distributions[0]["host"], "explicit-host");
    assert_eq!(distributions[0]["instance"], "pod-1");
    assert_eq!(distributions[0]["method"], "GET");
    assert_eq!(distributions[0]["val"], "distribution label");
    assert_eq!(distributions[0][quoted_label], "value\"\\\n");
    assert!(distributions[0].get("region.name").is_none());
}

#[test]
fn later_label_refreshes_missing_metadata_once_and_then_uses_the_cache() {
    let metadata = schema(&[("distributions", "new.route", "String")]);
    let (endpoint, rx, server) = mock(vec![
        Reply::error(200, &schema(&[])),
        Reply::success(),
        Reply::success(),
        Reply::error(200, &metadata),
        Reply::success(),
        Reply::success(),
        Reply::success(),
        Reply::success(),
    ]);
    let mut writer = writer(endpoint);
    writer.schema_checked = false;
    writer.write_group(&[batch()], deadline()).unwrap();
    let mut next = (*batch()).clone();
    next.rows[0]
        .labels
        .insert("new.route".into(), "/ready".into());
    let next = Arc::new(next);
    for _ in 0..2 {
        writer
            .write_group(std::slice::from_ref(&next), deadline())
            .unwrap();
    }
    server.join().unwrap();
    let requests: Vec<_> = rx.try_iter().collect();
    assert_eq!(requests.len(), 8);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.query["query"].starts_with("SELECT"))
            .count(),
        2
    );
    for index in [5, 7] {
        assert_eq!(json_rows(&requests[index])[0]["new.route"], "/ready");
    }
}

#[test]
fn missing_or_non_string_label_columns_reject_both_tables_before_insertion() {
    for extra in [
        vec![],
        vec![("counters", "region", "String")],
        vec![("distributions", "region", "UInt64")],
    ] {
        let (endpoint, rx, server) = mock(vec![Reply::error(200, &schema(&extra))]);
        let mut writer = writer(endpoint);
        let mut batch = (*batch()).clone();
        batch.rows[0].labels.insert("region".into(), "cn-a".into());
        assert!(encoded_batch_bytes(&batch, &ValidationLimits::default()).is_ok());
        let error = writer
            .write_group(&[Arc::new(batch)], deadline())
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Permanent);
        assert_eq!(error.outcome, CommitOutcome::NotCommitted);
        assert!(error.message.contains("label column"));
        server.join().unwrap();
        let requests: Vec<_> = rx.try_iter().collect();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].query["query"].starts_with("SELECT"));
    }
}

#[test]
fn metric_column_collisions_are_rejected_by_local_preflight_without_io() {
    let mut writer = ClickHouseBatchWriter::new(ClickHouseConfig {
        endpoint: "http://127.0.0.1:1".into(),
        ..ClickHouseConfig::default()
    })
    .unwrap();
    for row_index in 0..3 {
        let value_columns = VALUE_COLUMNS[usize::from(row_index == 0)];
        for name in ["TIMESTAMP", "metricName"]
            .into_iter()
            .chain(value_columns.iter().map(|(name, _)| *name))
        {
            let mut batch = (*batch()).clone();
            batch.rows[row_index]
                .labels
                .insert(name.into(), "conflicting label".into());
            batch.validate(&ValidationLimits::default()).unwrap();
            let error = encoded_batch_bytes(&batch, &ValidationLimits::default()).unwrap_err();
            assert_eq!(error.kind, ErrorKind::Permanent);
            assert_eq!(error.outcome, CommitOutcome::NotCommitted);
            assert!(error.message.contains("conflicts"));
            let error = writer
                .write_group(&[Arc::new(batch)], deadline())
                .unwrap_err();
            assert_eq!(error.outcome, CommitOutcome::NotCommitted);
            assert!(error.message.contains("conflicts"));
            assert!(writer.client.is_none());
        }
    }
}

#[test]
fn grouping_preserves_original_seconds_and_retry_body() {
    let (endpoint, rx, server) = mock((0..4).map(|_| Reply::success()).collect());
    let mut writer = writer(endpoint);
    let mut second = (*batch()).clone();
    second.source.hostname = "host-2".into();
    second.timestamp = i64::from(u32::MAX) * 1_000_000_000 + 999_999_999;
    second.duration_ns = 0;
    let batches = [batch(), Arc::new(second)];
    writer.write_group(&batches, deadline()).unwrap();
    writer.write_group(&batches, deadline()).unwrap();
    server.join().unwrap();
    let requests: Vec<_> = rx.try_iter().collect();
    for index in 0..2 {
        assert_eq!(requests[index].body, requests[index + 2].body);
        let rows = json_rows(&requests[index]);
        assert_eq!(rows.len(), if index == 0 { 4 } else { 2 });
        assert_eq!(rows.last().unwrap()["host"], "host-2");
        assert_eq!(rows.last().unwrap()["TIMESTAMP"], u32::MAX);
    }
}

#[test]
fn partial_success_and_late_http_errors_never_report_not_committed() {
    let (endpoint, rx, server) = mock(vec![
        Reply::success(),
        Reply::error(500, "Code: 60. DB::Exception: secret"),
    ]);
    let error = writer(endpoint)
        .write_group(&[batch()], deadline())
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Permanent);
    assert_eq!(error.outcome, CommitOutcome::Unknown);
    assert!(!error.message.contains("secret"));
    server.join().unwrap();
    assert_eq!(rx.try_iter().count(), 2);
    for reply in [
        Reply::error(200, "Code: 159. timeout"),
        Reply {
            matching_id: false,
            ..Reply::success()
        },
        Reply::error(200, &"a".repeat(MAX_RESPONSE_BYTES + 1)),
    ] {
        let (endpoint, _, server) = mock(vec![reply]);
        let error = writer(endpoint)
            .write_group(&[batch()], deadline())
            .unwrap_err();
        assert_eq!(error.outcome, CommitOutcome::Unknown);
        server.join().unwrap();
    }
}

#[test]
fn timeout_and_disconnect_are_unknown_and_do_not_retry() {
    for disconnect in [false, true] {
        let (release_response, response_gate) = mpsc::channel();
        let reply = Reply {
            response_gate: (!disconnect).then_some(response_gate),
            disconnect,
            ..Reply::success()
        };
        let (endpoint, rx, server) = mock(vec![reply]);
        let mut writer = writer(endpoint);
        if !disconnect {
            // Leave time to send the request on busy hosts; the server cannot
            // answer until write_group returns and we release its response gate.
            writer.config.request_timeout = Duration::from_secs(3);
        }
        let result = writer.write_group(&[batch()], Instant::now() + Duration::from_secs(10));
        if !disconnect {
            release_response.send(()).unwrap();
        }
        let request = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(request.query["query"].starts_with("INSERT"));
        assert_eq!(json_rows(&request).len(), 2);
        let error = result.unwrap_err();
        assert_eq!(
            error.kind,
            if disconnect {
                ErrorKind::Retryable
            } else {
                ErrorKind::Timeout
            }
        );
        assert_eq!(error.outcome, CommitOutcome::Unknown);
        assert!(error.is_retryable());
        server.join().unwrap();
        assert_eq!(rx.try_iter().count(), 0, "write_group must not retry");
    }
}

#[test]
fn retry_classification_preserves_commit_uncertainty() {
    for (status, kind) in [
        (429, ErrorKind::Retryable),
        (503, ErrorKind::Retryable),
        (401, ErrorKind::Permanent),
        (400, ErrorKind::Permanent),
        (408, ErrorKind::Timeout),
        (302, ErrorKind::Permanent),
    ] {
        let (endpoint, rx, server) = mock(vec![Reply::error(status, "")]);
        let error = writer(endpoint)
            .write_group(&[batch()], deadline())
            .unwrap_err();
        assert_eq!(error.kind, kind);
        assert_eq!(error.outcome, CommitOutcome::Unknown);
        server.join().unwrap();
        assert_eq!(rx.try_iter().count(), 1);
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let error = writer(format!("http://{address}"))
        .write_group(&[batch()], deadline())
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Retryable);
    assert_eq!(error.outcome, CommitOutcome::NotCommitted);
}

#[test]
fn bounded_preflight_and_expired_deadline_perform_no_io() {
    let mut writer = ClickHouseBatchWriter::new(ClickHouseConfig {
        max_encoded_bytes: 1,
        endpoint: "http://127.0.0.1:1".into(),
        ..ClickHouseConfig::default()
    })
    .unwrap();
    let error = writer.write_group(&[batch()], deadline()).unwrap_err();
    assert_eq!(error.outcome, CommitOutcome::NotCommitted);
    assert_eq!(error.kind, ErrorKind::Permanent);
    let original = batch();
    let mut conflict = original.as_ref().clone();
    conflict.source.hostname = "different-host".into();
    let error = writer
        .write_group(&[original, Arc::new(conflict)], deadline())
        .unwrap_err();
    assert!(error.message.contains("conflicting content"));
    let error = writer.write_group(&[batch()], Instant::now()).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert_eq!(error.outcome, CommitOutcome::NotCommitted);
    let mut invalid_batch = (*batch()).clone();
    invalid_batch.rows[1].value = MetricValue::CounterDelta {
        delta_value: u64::MAX,
    };
    assert_eq!(
        writer
            .write_group(&[Arc::new(invalid_batch)], deadline())
            .unwrap_err()
            .outcome,
        CommitOutcome::NotCommitted
    );
    let mut unsupported_time = (*batch()).clone();
    unsupported_time.timestamp = -1;
    unsupported_time.validate(&Default::default()).unwrap();
    assert!(
        encoded_batch_bytes(&unsupported_time, &ValidationLimits::default())
            .unwrap_err()
            .message
            .contains("timestamp")
    );
    let error = writer
        .write_group(&[Arc::new(unsupported_time)], deadline())
        .unwrap_err();
    assert!(error.message.contains("timestamp"));
    assert_eq!(error.outcome, CommitOutcome::NotCommitted);
    let mut empty = (*batch()).clone();
    empty.rows.clear();
    writer.write_group(&[Arc::new(empty)], deadline()).unwrap();
    assert!(writer.client.is_none());
}

#[test]
fn schema_verification_checks_required_subset_without_engine_constraints() {
    let schema = schema(&[]);
    for valid_schema in [
        schema.clone(),
        schema.replace("\"DateTime\"", "\"DateTime('UTC')\""),
    ] {
        let (endpoint, _, server) = mock(vec![Reply::error(200, &valid_schema)]);
        let mut writer = ClickHouseBatchWriter::new(ClickHouseConfig {
            endpoint,
            ..ClickHouseConfig::default()
        })
        .unwrap();
        writer.verify_schema(deadline()).unwrap();
        assert!(writer.schema_checked);
        server.join().unwrap();
    }
    for invalid_schema in [
        schema.replace("Int64", "UInt64"),
        schema.replace("DateTime", "DateTime64(9)"),
        schema.replace("p95", "unused"),
        schema.replace("host", "other_host"),
    ] {
        let (endpoint, _, server) = mock(vec![Reply::error(200, &invalid_schema)]);
        let error = ClickHouseBatchWriter::new(ClickHouseConfig {
            endpoint,
            ..ClickHouseConfig::default()
        })
        .unwrap()
        .verify_schema(deadline())
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Permanent);
        assert_eq!(error.outcome, CommitOutcome::NotCommitted);
        server.join().unwrap();
    }
}

#[test]
fn seconds_and_integer_boundaries_are_checked_without_rounding() {
    assert_eq!(timestamp(0).unwrap(), 0);
    assert_eq!(timestamp(999_999_999).unwrap(), 0);
    assert_eq!(
        timestamp(i64::from(u32::MAX) * 1_000_000_000 + 999_999_999).unwrap(),
        u32::MAX
    );
    for nanos in [
        -1,
        i64::MIN,
        (i64::from(u32::MAX) + 1) * 1_000_000_000,
        i64::MAX,
    ] {
        assert!(timestamp(nanos).is_err());
    }
    for value in [i64::MIN, -1, 0, 9_007_199_254_740_993, i64::MAX] {
        assert_eq!(
            scalar_value(&MetricValue::GaugeSnapshot {
                current_value: value
            })
            .unwrap(),
            Some(value)
        );
    }
    assert_eq!(
        scalar_value(&MetricValue::CounterDelta {
            delta_value: i64::MAX as u64
        })
        .unwrap(),
        Some(i64::MAX)
    );
    assert!(scalar_value(&MetricValue::CounterDelta {
        delta_value: i64::MAX as u64 + 1
    })
    .is_err());
}
