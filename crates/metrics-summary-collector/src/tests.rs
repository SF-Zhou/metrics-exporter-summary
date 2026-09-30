use super::*;
#[cfg(any(feature = "http", feature = "tcp"))]
use metrics_summary_core::Sink;
use metrics_summary_core::{ErrorKind, MetricValue, Row, Source};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
use uuid::Uuid;
fn batch(sequence: u64) -> Batch {
    Batch {
        model_version: 1,
        id: BatchId {
            source_session_id: Uuid::new_v4(),
            sequence,
        },
        source: Source {
            application: "svc".into(),
            instance: "pid-1".into(),
            hostname: "machine-42".into(),
            attributes: BTreeMap::new(),
        },
        timestamp: 1_790_000_001_123_456_789,
        duration_ns: 1_000_000_000,
        rows: vec![Row {
            metric_id: 1,
            name: "requests".into(),
            labels: BTreeMap::new(),
            unit: None,
            value: MetricValue::CounterDelta {
                delta_value: i64::MAX as u64,
            },
        }],
    }
}
fn config() -> CollectorConfig {
    CollectorConfig {
        group_max_delay_ms: 10,
        retry_backoff_ms: 1,
        request_timeout_ms: 3000,
        ..Default::default()
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
#[derive(Default)]
struct Record {
    groups: Mutex<Vec<Vec<Arc<Batch>>>>,
    attempts: AtomicUsize,
    failures: AtomicUsize,
    block: Mutex<bool>,
    changed: Condvar,
    permanent: bool,
}
struct Writer(Arc<Record>);
impl GroupWriter for Writer {
    fn write_group(&mut self, batches: &[Arc<Batch>], until: Instant) -> Result<(), WriteError> {
        self.0.attempts.fetch_add(1, Ordering::SeqCst);
        let mut block = self.0.block.lock().unwrap();
        while *block && Instant::now() < until {
            block = self
                .0
                .changed
                .wait_timeout(block, until.saturating_duration_since(Instant::now()))
                .unwrap()
                .0;
        }
        if *block {
            return Err(WriteError::new(
                ErrorKind::Timeout,
                CommitOutcome::Unknown,
                "injected timeout",
            ));
        }
        drop(block);
        if self
            .0
            .failures
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(WriteError::new(
                if self.0.permanent {
                    ErrorKind::Permanent
                } else {
                    ErrorKind::Retryable
                },
                CommitOutcome::Unknown,
                "injected lost commit ACK",
            ));
        }
        self.0.groups.lock().unwrap().push(batches.to_vec());
        Ok(())
    }
}
fn setup(c: CollectorConfig, record: Arc<Record>) -> Collector {
    Collector::new(c, Some("secret".into()), vec![Box::new(Writer(record))]).unwrap()
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn unblock(record: &Record) {
    *record.block.lock().unwrap() = false;
    record.changed.notify_all();
}
#[tokio::test]
async fn enqueued_precedes_database_confirmed_waits_and_cancelled_waiter_keeps_batch() {
    let record = Arc::new(Record::default());
    *record.block.lock().unwrap() = true;
    let collector = setup(config(), record.clone());
    let b = batch(1);
    let ack = collector
        .submit(b.clone(), AckPolicy::Enqueued, deadline())
        .await;
    assert_eq!(ack.status, Status::Ok as i32);
    assert!(record.groups.lock().unwrap().is_empty());
    let c = collector.clone();
    let task = tokio::spawn(async move {
        c.submit(b, AckPolicy::ClickHouseConfirmed, deadline())
            .await
    });
    until(|| record.attempts.load(Ordering::SeqCst) > 0).await;
    assert!(!task.is_finished());
    task.abort();
    unblock(&record);
    let report = collector.shutdown(deadline()).await;
    assert!(report.drained);
    assert_eq!(collector.diagnostics().accepted_batches, 1);
    assert_eq!(collector.diagnostics().clickhouse_confirmed_batches, 1);
}
#[tokio::test]
async fn confirmed_timeout_is_unknown_and_work_continues() {
    let record = Arc::new(Record::default());
    *record.block.lock().unwrap() = true;
    let collector = setup(config(), record.clone());
    let ack = collector
        .submit(
            batch(1),
            AckPolicy::ClickHouseConfirmed,
            Instant::now() + Duration::from_millis(30),
        )
        .await;
    assert_eq!(ack.status, Status::Unknown as i32);
    assert_eq!(collector.diagnostics().pending_batches, 1);
    unblock(&record);
    assert!(collector.shutdown(deadline()).await.drained);
    assert_eq!(collector.diagnostics().clickhouse_confirmed_batches, 1);
}
#[tokio::test]
async fn bounded_admission_identity_conflicts_and_stronger_duplicate_ack() {
    let record = Arc::new(Record::default());
    *record.block.lock().unwrap() = true;
    let mut cfg = config();
    cfg.max_pending_batches = 1;
    cfg.dedup_capacity = 1;
    let collector = setup(cfg, record.clone());
    let b = batch(1);
    assert_eq!(
        collector
            .submit(b.clone(), AckPolicy::Enqueued, deadline())
            .await
            .status,
        Status::Ok as i32
    );
    assert_eq!(
        collector
            .submit(batch(2), AckPolicy::Enqueued, deadline())
            .await
            .status,
        Status::Overloaded as i32
    );
    let mut changed_hostname = b.clone();
    changed_hostname.source.hostname = "changed".into();
    let mut changed_timestamp = b.clone();
    changed_timestamp.timestamp += 1;
    let mut changed_duration = b.clone();
    changed_duration.duration_ns += 1;
    for conflict in [changed_hostname, changed_timestamp, changed_duration] {
        assert_eq!(
            collector
                .submit(conflict, AckPolicy::Enqueued, deadline())
                .await
                .status,
            Status::Invalid as i32
        );
    }
    unblock(&record);
    let ack = collector
        .submit(b.clone(), AckPolicy::ClickHouseConfirmed, deadline())
        .await;
    assert_eq!(ack.status, Status::Ok as i32);
    assert_eq!(ack.ack_policy, AckPolicy::ClickHouseConfirmed as i32);
    let ack = collector
        .submit(b, AckPolicy::ClickHouseConfirmed, deadline())
        .await;
    assert_eq!(ack.status, Status::Ok as i32);
    assert_eq!(collector.diagnostics().accepted_batches, 1);
    assert!(collector.shutdown(deadline()).await.drained);
}
#[tokio::test]
async fn unsupported_policy_and_oversize_rejected_before_ownership_transfer() {
    let mut cfg = config();
    cfg.allow_confirmed = false;
    cfg.group_max_bytes = 1;
    let collector = setup(cfg, Arc::new(Record::default()));
    assert_eq!(
        collector
            .submit(batch(1), AckPolicy::ClickHouseConfirmed, deadline())
            .await
            .status,
        Status::Invalid as i32
    );
    assert_eq!(
        collector
            .submit(batch(1), AckPolicy::Enqueued, deadline())
            .await
            .status,
        Status::Invalid as i32
    );
    assert_eq!(collector.diagnostics().accepted_batches, 0);
    assert!(collector.shutdown(deadline()).await.drained);
}

#[tokio::test]
async fn arbitrary_labels_are_preserved_without_configuration() {
    let mut batch = batch(1);
    let mut custom = batch.rows[0].clone();
    custom.metric_id = 2;
    custom.labels.extend([
        ("route".into(), "/health".into()),
        ("http.method".into(), "GET".into()),
        ("区域".into(), "华东".into()),
    ]);
    batch.rows.push(custom);
    let record = Arc::new(Record::default());
    let collector = setup(config(), record.clone());
    assert_eq!(
        collector
            .submit(batch.clone(), AckPolicy::ClickHouseConfirmed, deadline())
            .await
            .status,
        Status::Ok as i32
    );
    assert_eq!(&*record.groups.lock().unwrap()[0][0], &batch);
    assert!(collector.shutdown(deadline()).await.drained);
}

#[tokio::test]
async fn invalid_labels_reject_whole_batch_before_ownership_transfer() {
    let record = Arc::new(Record::default());
    let collector = setup(config(), record.clone());
    for key in [
        String::new(),
        "route\n".into(),
        "x".repeat(collector.config().validation.max_label_key_bytes + 1),
    ] {
        let mut invalid = batch(1);
        let mut row = invalid.rows[0].clone();
        row.metric_id = 2;
        row.labels.insert(key, "/health".into());
        invalid.rows.push(row);
        assert_eq!(
            collector
                .submit(invalid, AckPolicy::Enqueued, deadline())
                .await
                .status,
            Status::Invalid as i32
        );
    }
    assert_eq!(collector.diagnostics().accepted_batches, 0);
    assert_eq!(record.attempts.load(Ordering::SeqCst), 0);
    assert!(collector.shutdown(deadline()).await.drained);
}

#[test]
fn validation_limits_must_match_storage() {
    let mut configured = config();
    configured.validation.max_label_key_bytes += 1;
    let writer = ClickHouseBatchWriter::new(Default::default()).unwrap();
    assert!(Collector::new(configured, None, vec![Box::new(writer)]).is_err());
}

#[tokio::test]
async fn storage_scalar_and_timestamp_limits_reject_before_admission() {
    let record = Arc::new(Record::default());
    let collector = setup(config(), record.clone());
    let mut overflow = batch(1);
    overflow.rows[0].value = MetricValue::CounterDelta {
        delta_value: i64::MAX as u64 + 1,
    };
    let mut before_epoch = batch(2);
    before_epoch.timestamp = -1;
    let mut after_datetime = batch(3);
    after_datetime.timestamp = (i64::from(u32::MAX) + 1) * 1_000_000_000;
    for invalid in [overflow, before_epoch, after_datetime] {
        assert_eq!(
            collector
                .submit(invalid, AckPolicy::Enqueued, deadline())
                .await
                .status,
            Status::Invalid as i32
        );
    }
    assert_eq!(collector.diagnostics().accepted_batches, 0);
    assert_eq!(record.attempts.load(Ordering::SeqCst), 0);
    assert!(collector.shutdown(deadline()).await.drained);
}
#[tokio::test]
async fn retries_keep_identity_and_permanent_failure_is_diagnosed_and_readmitted() {
    let record = Arc::new(Record::default());
    record.failures.store(2, Ordering::SeqCst);
    let collector = setup(config(), record.clone());
    let b = batch(1);
    assert_eq!(
        collector
            .submit(b.clone(), AckPolicy::ClickHouseConfirmed, deadline())
            .await
            .status,
        Status::Ok as i32
    );
    assert_eq!(record.attempts.load(Ordering::SeqCst), 3);
    assert_eq!(record.groups.lock().unwrap()[0][0].id, b.id);
    assert_eq!(collector.diagnostics().retries, 2);
    collector.shutdown(deadline()).await;
    let record = Arc::new(Record {
        permanent: true,
        ..Default::default()
    });
    record.failures.store(1, Ordering::SeqCst);
    let collector = setup(config(), record.clone());
    assert_eq!(
        collector
            .submit(b.clone(), AckPolicy::ClickHouseConfirmed, deadline())
            .await
            .status,
        Status::Unknown as i32
    );
    assert_eq!(collector.diagnostics().dropped_after_acceptance, 1);
    assert_eq!(
        collector
            .submit(b, AckPolicy::ClickHouseConfirmed, deadline())
            .await
            .status,
        Status::Ok as i32
    );
    assert_eq!(collector.diagnostics().accepted_batches, 2);
    collector.shutdown(deadline()).await;
}
#[tokio::test]
async fn retry_exhaustion_and_shutdown_timeout_report_accepted_data() {
    let record = Arc::new(Record::default());
    record.failures.store(100, Ordering::SeqCst);
    let mut cfg = config();
    cfg.retry_max_attempts = 2;
    let collector = setup(cfg, record.clone());
    assert_eq!(
        collector
            .submit(batch(1), AckPolicy::Enqueued, deadline())
            .await
            .status,
        Status::Ok as i32
    );
    let report = collector.shutdown(deadline()).await;
    assert_eq!(report.dropped_after_acceptance, 1);
    assert_eq!(record.attempts.load(Ordering::SeqCst), 2);
    let record = Arc::new(Record::default());
    *record.block.lock().unwrap() = true;
    let collector = setup(config(), record.clone());
    collector
        .submit(batch(2), AckPolicy::Enqueued, deadline())
        .await;
    until(|| record.attempts.load(Ordering::SeqCst) > 0).await;
    let report = collector
        .shutdown(Instant::now() + Duration::from_millis(5))
        .await;
    assert_eq!(
        report.unconfirmed_batches + report.dropped_after_acceptance as usize,
        1
    );
    unblock(&record);
    until(|| collector.diagnostics().pending_batches == 0).await;
}
#[tokio::test]
async fn multi_source_group_preserves_rows_and_low_traffic_age_flushes() {
    let record = Arc::new(Record::default());
    let mut cfg = config();
    cfg.group_max_delay_ms = 100;
    let collector = setup(cfg, record.clone());
    let a = batch(1);
    let b = batch(2);
    collector
        .submit(a.clone(), AckPolicy::Enqueued, deadline())
        .await;
    collector
        .submit(b.clone(), AckPolicy::Enqueued, deadline())
        .await;
    until(|| collector.diagnostics().clickhouse_confirmed_batches == 2).await;
    let groups = record.groups.lock().unwrap().clone();
    assert_eq!(groups.len(), 1);
    assert_eq!(&*groups[0][0], &a);
    assert_eq!(&*groups[0][1], &b);
    collector.shutdown(deadline()).await;
}
#[cfg(feature = "http")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_wildcard_bind_auth_limits_roundtrip_and_hostname() {
    use metrics_summary_sink_remote::{Endpoint, RemoteConfig, RemoteSink};
    let record = Arc::new(Record::default());
    let collector = setup(config(), record.clone());
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    // Connect locally while exercising a listener bound to every IPv4 interface.
    let address =
        std::net::SocketAddr::from(([127, 0, 0, 1], listener.local_addr().unwrap().port()));
    let (tx, rx) = tokio::sync::oneshot::channel();
    let c = collector.clone();
    let server = tokio::spawn(async move {
        c.serve_http(listener, async {
            let _ = rx.await;
        })
        .await
        .unwrap()
    });
    let url = format!("http://{address}/v1/batches");
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .post(&url)
            .body("invalid data")
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(&url)
            .bearer_auth("secret")
            .header("content-type", metrics_summary_protocol::CONTENT_TYPE)
            .header("content-encoding", "gzip")
            .body("invalid data")
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    let payload = metrics_summary_protocol::encode_request(
        &batch(8),
        AckPolicy::Enqueued,
        &collector.config().protocol_limits(),
    )
    .unwrap();
    assert_eq!(payload[0], 0x98, "requests must be MessagePack arrays");
    let mut trailing = payload.clone();
    trailing.push(0xc0);
    for (content_type, body) in [
        ("application/x-protobuf", payload),
        ("application/msgpack", vec![0xc1]),
        ("application/msgpack", trailing),
    ] {
        let response = client
            .post(&url)
            .bearer_auth("secret")
            .header("content-type", content_type)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        assert_eq!(response.headers()["content-type"], "application/msgpack");
        let bytes = response.bytes().await.unwrap();
        assert_eq!(bytes[0], 0x95, "ACKs must be MessagePack arrays");
        let ack = metrics_summary_protocol::decode_ack(&bytes).unwrap();
        assert_eq!(ack.status, Status::Invalid as i32);
    }
    let mut invalid = batch(9);
    invalid.rows[0]
        .labels
        .insert("route\n".into(), "/health".into());
    let invalid = metrics_summary_protocol::encode_wire_request(
        &metrics_summary_protocol::request(&invalid, AckPolicy::Enqueued),
    )
    .unwrap();
    assert_eq!(
        client
            .post(&url)
            .bearer_auth("secret")
            .header("content-type", metrics_summary_protocol::CONTENT_TYPE)
            .body(invalid)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(collector.diagnostics().accepted_batches, 0);
    let mut b = batch(1);
    // An instantaneous collection window is valid and must survive MessagePack.
    b.duration_ns = 0;
    b.rows[0].labels.extend([
        ("route".into(), "/health".into()),
        ("http.method".into(), "GET".into()),
        ("区域".into(), "华东".into()),
    ]);
    let expect = b.clone();
    tokio::task::spawn_blocking(move || {
        let mut cfg = RemoteConfig::new(Endpoint::Http(url));
        cfg.bearer_token = Some("secret".into());
        cfg.ack_policy = AckPolicy::ClickHouseConfirmed;
        let mut sink = RemoteSink::new(cfg).unwrap();
        sink.write(Arc::new(b), deadline()).unwrap();
    })
    .await
    .unwrap();
    assert_eq!(&*record.groups.lock().unwrap()[0][0], &expect);
    assert_eq!(
        record.groups.lock().unwrap()[0][0].source.hostname,
        "machine-42"
    );
    tx.send(()).unwrap();
    server.await.unwrap();
    assert!(collector.shutdown(deadline()).await.drained);
}
#[cfg(feature = "tcp")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_wildcard_bind_half_frame_reconnect_auth_and_large_values_roundtrip() {
    use metrics_summary_sink_remote::{Endpoint, RemoteConfig, RemoteSink};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let record = Arc::new(Record::default());
    let collector = setup(config(), record.clone());
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    // Connect locally while exercising a listener bound to every IPv4 interface.
    let address =
        std::net::SocketAddr::from(([127, 0, 0, 1], listener.local_addr().unwrap().port()));
    let (tx, rx) = tokio::sync::oneshot::channel();
    let c = collector.clone();
    let server = tokio::spawn(async move {
        c.serve_tcp(listener, async {
            let _ = rx.await;
        })
        .await
        .unwrap()
    });
    let mut magic = [0; 4];
    let mut unauthorized = tokio::net::TcpStream::connect(address).await.unwrap();
    unauthorized.write_all(b"MXS1\0\x05wrong").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), unauthorized.read_exact(&mut magic))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&magic, b"NOPE");
    drop(unauthorized);
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket.write_all(b"MXS1\0\x06secret").await.unwrap();
    socket.read_exact(&mut magic).await.unwrap();
    assert_eq!(&magic, b"MXS1");
    socket.write_u32(100).await.unwrap();
    socket.write_all(&[1, 2, 3]).await.unwrap();
    drop(socket);
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket.write_all(b"MXS1\0\x06secret").await.unwrap();
    socket.read_exact(&mut magic).await.unwrap();
    assert_eq!(&magic, b"MXS1");
    let mut trailing = metrics_summary_protocol::encode_request(
        &batch(8),
        AckPolicy::Enqueued,
        &collector.config().protocol_limits(),
    )
    .unwrap();
    trailing.push(0xc0);
    for body in [vec![0xc1], trailing] {
        socket.write_u32(body.len() as u32).await.unwrap();
        socket.write_all(&body).await.unwrap();
        let length = socket.read_u32().await.unwrap() as usize;
        assert!(length <= metrics_summary_protocol::MAX_ACK_BYTES);
        let mut bytes = vec![0; length];
        socket.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes[0], 0x95, "TCP ACKs must carry MessagePack");
        let ack = metrics_summary_protocol::decode_ack(&bytes).unwrap();
        assert_eq!(ack.status, Status::Invalid as i32);
    }
    assert_eq!(collector.diagnostics().accepted_batches, 0);
    drop(socket);
    let mut b = batch(1);
    // Exercise exact unsigned duration transport beyond the signed/Float64 ranges.
    b.duration_ns = u64::MAX;
    b.rows[0].labels.extend([
        ("route".into(), "/health".into()),
        ("http.method".into(), "GET".into()),
        ("区域".into(), "华东".into()),
    ]);
    let expect = b.clone();
    tokio::task::spawn_blocking(move || {
        let mut cfg = RemoteConfig::new(Endpoint::Tcp(address));
        cfg.bearer_token = Some("secret".into());
        cfg.ack_policy = AckPolicy::ClickHouseConfirmed;
        let mut sink = RemoteSink::new(cfg).unwrap();
        sink.write(Arc::new(b.clone()), deadline()).unwrap();
        sink.write(Arc::new(b), deadline()).unwrap();
    })
    .await
    .unwrap();
    assert_eq!(collector.diagnostics().accepted_batches, 1);
    assert_eq!(&*record.groups.lock().unwrap()[0][0], &expect);
    tx.send(()).unwrap();
    server.await.unwrap();
    assert!(collector.shutdown(deadline()).await.drained);
}

#[cfg(any(feature = "http", feature = "tcp"))]
async fn recorder_near_retained_limit_reaches_collector(use_tcp: bool) {
    use metrics_exporter_summary::{metrics, Builder, Config};
    use metrics_summary_sink_remote::{Endpoint, RemoteConfig, RemoteSink};

    let record = Arc::new(Record::default());
    let collector = setup(config(), record.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let c = collector.clone();
    let server = tokio::spawn(async move {
        let shutdown = async {
            let _ = rx.await;
        };
        if use_tcp {
            #[cfg(feature = "tcp")]
            c.serve_tcp(listener, shutdown).await.unwrap();
            #[cfg(not(feature = "tcp"))]
            unreachable!();
        } else {
            #[cfg(feature = "http")]
            c.serve_http(listener, shutdown).await.unwrap();
            #[cfg(not(feature = "http"))]
            unreachable!();
        }
    });
    tokio::task::spawn_blocking(move || {
        let endpoint = if use_tcp {
            #[cfg(feature = "tcp")]
            {
                Endpoint::Tcp(address)
            }
            #[cfg(not(feature = "tcp"))]
            unreachable!()
        } else {
            #[cfg(feature = "http")]
            {
                Endpoint::Http(format!("http://{address}/v1/batches"))
            }
            #[cfg(not(feature = "http"))]
            unreachable!()
        };
        let mut remote = RemoteConfig::new(endpoint);
        remote.bearer_token = Some("secret".into());
        remote.ack_policy = AckPolicy::ClickHouseConfirmed;
        let (recorder, control) = Builder::new(Source {
            application: "a".into(),
            instance: "i".into(),
            hostname: "h".into(),
            attributes: BTreeMap::new(),
        })
        .config(Config {
            collect_interval: None,
            max_write_attempts: 1,
            ..Default::default()
        })
        .build(RemoteSink::new(remote).unwrap())
        .unwrap();
        metrics::with_local_recorder(&recorder, || {
            for index in 0..2300 {
                metrics::counter!(
                    format!("metric{index}"),
                    "host" => "h",
                    "instance" => "i",
                    "pod" => "",
                    "tag" => "",
                    "thread" => "",
                    "uid" => "",
                    "statusCode" => "",
                    "mount_name" => "",
                    "io" => ""
                )
                .increment(1);
            }
        });
        assert_eq!(control.diagnostics().registered_series, 2300);
        assert_eq!(control.diagnostics().registrations_rejected, 0);
        let report = control.shutdown(Duration::from_secs(10)).unwrap();
        assert!(
            report.is_success(),
            "{report:?}; {:?}",
            control.last_write_error()
        );
        assert_eq!(report.written_batches, 1);
        assert_eq!(control.diagnostics().retries, 0);
    })
    .await
    .unwrap();
    {
        let groups = record.groups.lock().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 1);
        let received = &groups[0][0];
        assert_eq!(received.rows.len(), 2300);
        let limit = metrics_summary_core::ValidationLimits::default().max_batch_bytes;
        assert!(received.estimated_bytes() <= limit);
        assert!(received.estimated_bytes() >= limit * 9 / 10);
        for row in &received.rows {
            assert_eq!(row.labels.len(), 9);
            assert_eq!(row.value, MetricValue::CounterDelta { delta_value: 1 });
        }
    }
    assert_eq!(collector.diagnostics().clickhouse_confirmed_batches, 1);
    tx.send(()).unwrap();
    server.await.unwrap();
    assert!(collector.shutdown(deadline()).await.drained);
}

#[cfg(feature = "http")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_recorder_near_default_retained_limit_roundtrips() {
    recorder_near_retained_limit_reaches_collector(false).await;
}

#[cfg(feature = "tcp")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_recorder_near_default_retained_limit_roundtrips() {
    recorder_near_retained_limit_reaches_collector(true).await;
}

#[tokio::test]
async fn storage_encoding_budget_rejects_single_batch_and_splits_groups() {
    let a = batch(1);
    let size =
        metrics_summary_sink_clickhouse::encoded_batch_bytes(&a, &config().validation).unwrap();
    let mut cfg = config();
    cfg.group_max_encoded_bytes = size - 1;
    let collector = setup(cfg, Arc::new(Record::default()));
    assert_eq!(
        collector
            .submit(a.clone(), AckPolicy::Enqueued, deadline())
            .await
            .status,
        Status::Invalid as i32
    );
    assert_eq!(collector.diagnostics().accepted_batches, 0);
    collector.shutdown(deadline()).await;
    let record = Arc::new(Record::default());
    let mut cfg = config();
    cfg.group_max_encoded_bytes = size + 1;
    let collector = setup(cfg, record.clone());
    collector.submit(a, AckPolicy::Enqueued, deadline()).await;
    collector
        .submit(batch(2), AckPolicy::Enqueued, deadline())
        .await;
    assert!(collector.shutdown(deadline()).await.drained);
    assert_eq!(record.groups.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn diagnostic_error_reason_is_bounded_redacted_and_survives_recovery() {
    struct FailsOnce(bool);
    impl GroupWriter for FailsOnce {
        fn write_group(&mut self, _: &[Arc<Batch>], _: Instant) -> Result<(), WriteError> {
            if self.0 {
                self.0 = false;
                Err(WriteError::new(
                    ErrorKind::Retryable,
                    CommitOutcome::Unknown,
                    format!("token secret {}", "详细错误".repeat(2000)),
                ))
            } else {
                Ok(())
            }
        }
    }
    let collector = Collector::new(
        config(),
        Some("secret".into()),
        vec![Box::new(FailsOnce(true))],
    )
    .unwrap();
    assert_eq!(
        collector
            .submit(batch(1), AckPolicy::ClickHouseConfirmed, deadline())
            .await
            .status,
        Status::Ok as i32
    );
    let diagnostic = collector.diagnostics();
    let error = diagnostic.last_write_error.unwrap();
    assert_eq!(error.kind, ErrorKind::Retryable);
    assert_eq!(error.outcome, CommitOutcome::Unknown);
    assert!(error.message.len() <= 1024);
    assert!(!error.message.contains("secret"));
    assert!(error.unix_time_ms > 0);
    assert!(diagnostic.last_confirmed_unix_ms.is_some());
    collector.shutdown(deadline()).await;
}

#[test]
fn extreme_resource_configuration_is_rejected_before_semaphore_construction() {
    for cfg in [
        CollectorConfig {
            max_connections: usize::MAX,
            ..config()
        },
        CollectorConfig {
            max_requests: usize::MAX,
            ..config()
        },
        CollectorConfig {
            request_timeout_ms: u64::MAX,
            ..config()
        },
        CollectorConfig {
            max_encoded_bytes: usize::MAX,
            ..config()
        },
        CollectorConfig {
            max_pending_batches: usize::MAX,
            dedup_capacity: 1,
            ..config()
        },
    ] {
        let result = std::panic::catch_unwind(|| {
            Collector::new(
                cfg,
                None,
                vec![Box::new(Writer(Arc::new(Record::default())))],
            )
        });
        assert!(result.is_ok());
        assert!(result.unwrap().is_err());
    }
}

#[cfg(all(feature = "http", feature = "tcp"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_http_and_tcp_connections_release_the_single_shared_slot() {
    use metrics_summary_sink_remote::{Endpoint, RemoteConfig, RemoteSink};
    let mut cfg = config();
    cfg.max_connections = 1;
    let collector = setup(cfg, Arc::new(Record::default()));
    let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address = http.local_addr().unwrap();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_address = tcp.local_addr().unwrap();
    let (http_tx, http_rx) = tokio::sync::oneshot::channel();
    let (tcp_tx, tcp_rx) = tokio::sync::oneshot::channel();
    let c = collector.clone();
    let http_server = tokio::spawn(async move {
        c.serve_http(http, async {
            let _ = http_rx.await;
        })
        .await
        .unwrap()
    });
    let c = collector.clone();
    let tcp_server = tokio::spawn(async move {
        c.serve_tcp(tcp, async {
            let _ = tcp_rx.await;
        })
        .await
        .unwrap()
    });
    let client = reqwest::Client::new();
    for _ in 0..32 {
        let response = client
            .get(format!("http://{http_address}/healthz"))
            .header("connection", "close")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let _ = response.bytes().await.unwrap();
    }
    until(|| collector.shared().connections.available_permits() == 1).await;
    tokio::task::spawn_blocking(move || {
        for sequence in 1..=32 {
            let mut cfg = RemoteConfig::new(Endpoint::Tcp(tcp_address));
            cfg.bearer_token = Some("secret".into());
            let mut sink = RemoteSink::new(cfg).unwrap();
            sink.write(Arc::new(batch(sequence)), deadline()).unwrap();
        }
    })
    .await
    .unwrap();
    until(|| collector.shared().connections.available_permits() == 1).await;
    assert_eq!(collector.diagnostics().accepted_batches, 32);
    http_tx.send(()).unwrap();
    tcp_tx.send(()).unwrap();
    http_server.await.unwrap();
    tcp_server.await.unwrap();
    assert!(collector.shutdown(deadline()).await.drained);
}
