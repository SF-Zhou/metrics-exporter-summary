use super::*;
use metrics_summary_core::{BatchId, MetricValue, Row, Source};
use metrics_summary_protocol::{ack, encode_ack};
#[cfg(feature = "tcp")]
use std::net::TcpStream;
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::TcpListener,
    thread,
};
use uuid::Uuid;
fn batch() -> Arc<Batch> {
    Arc::new(Batch {
        model_version: 1,
        id: BatchId {
            source_session_id: Uuid::new_v4(),
            sequence: u64::MAX,
        },
        source: Source {
            application: "svc".into(),
            instance: "worker".into(),
            hostname: "host".into(),
            attributes: BTreeMap::new(),
        },
        timestamp: i64::MIN,
        duration_ns: u64::MAX,
        rows: vec![
            Row {
                metric_id: 1,
                name: "requests".into(),
                labels: BTreeMap::from([
                    ("route".into(), "/read".into()),
                    ("http.method".into(), "GET".into()),
                    ("区域".into(), "华东".into()),
                ]),
                unit: None,
                value: MetricValue::CounterDelta {
                    delta_value: i64::MAX as u64,
                },
            },
            Row {
                metric_id: 2,
                name: "signed_gauge".into(),
                labels: BTreeMap::new(),
                unit: None,
                value: MetricValue::GaugeSnapshot {
                    current_value: i64::MIN,
                },
            },
            Row {
                metric_id: 3,
                name: "latency_ns".into(),
                labels: BTreeMap::from([("statusCode".into(), "200".into())]),
                unit: Some("nanoseconds".into()),
                value: MetricValue::HistogramSummary {
                    count: 4,
                    sum: 100.0,
                    min: 10.0,
                    p50: 25.0,
                    p90: 35.0,
                    p95: 37.0,
                    p99: 39.0,
                    max: 40.0,
                },
            },
        ],
    })
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
#[cfg(feature = "http")]
fn http_once(status: u16, body: Vec<u8>) -> (String, thread::JoinHandle<()>) {
    http_once_with_content_type(status, body, "application/msgpack")
}
#[cfg(feature = "http")]
fn http_once_with_content_type(
    status: u16,
    body: Vec<u8>,
    content_type: &'static str,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1/batches", listener.local_addr().unwrap());
    let thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut one = [0u8];
        while !bytes.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut one).unwrap();
            bytes.push(one[0]);
        }
        let headers = String::from_utf8(bytes).unwrap();
        assert!(headers
            .lines()
            .any(|line| line.eq_ignore_ascii_case("content-type: application/msgpack")));
        let length: usize = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|n| n.trim().parse().unwrap())
            })
            .unwrap();
        let mut request = vec![0; length];
        stream.read_exact(&mut request).unwrap();
        assert_eq!(request[0], 0x98, "requests must be MessagePack arrays");
        let (decoded, _) =
            metrics_summary_protocol::decode_request(&request, &ProtocolLimits::default()).unwrap();
        assert_eq!(decoded.timestamp, i64::MIN);
        assert_eq!(decoded.duration_ns, u64::MAX);
        assert_eq!(decoded.rows, batch().rows);
        let headers=format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nContent-Type: {content_type}\r\nConnection: close\r\n\r\n",body.len());
        stream.write_all(headers.as_bytes()).unwrap();
        stream.write_all(&body).unwrap();
    });
    (endpoint, thread)
}
#[cfg(feature = "http")]
fn sink(url: String) -> RemoteSink {
    let mut c = RemoteConfig::new(Endpoint::Http(url));
    c.ack_policy = AckPolicy::ClickHouseConfirmed;
    RemoteSink::new(c).unwrap()
}
#[cfg(feature = "http")]
#[test]
fn http_errors_are_classified_and_2xx_requires_matching_strong_ack() {
    let b = batch();
    for (status, expected) in [
        (401, ErrorKind::Permanent),
        (400, ErrorKind::Permanent),
        (429, ErrorKind::Retryable),
        (503, ErrorKind::Retryable),
        (408, ErrorKind::Timeout),
    ] {
        let (url, server) = http_once(status, vec![]);
        let e = sink(url).write(b.clone(), deadline()).unwrap_err();
        assert_eq!(e.kind, expected);
        server.join().unwrap();
    }
    for a in [
        ack(Some(b.id), AckPolicy::Enqueued, Status::Ok, ""),
        ack(
            Some(batch().id),
            AckPolicy::ClickHouseConfirmed,
            Status::Ok,
            "",
        ),
    ] {
        let (url, server) = http_once(200, encode_ack(&a).unwrap());
        let e = sink(url).write(b.clone(), deadline()).unwrap_err();
        assert_eq!(e.kind, ErrorKind::Permanent);
        assert_eq!(e.outcome, CommitOutcome::Unknown);
        server.join().unwrap();
    }
    let (url, server) = http_once(200, vec![]);
    assert!(sink(url).write(b.clone(), deadline()).is_err());
    server.join().unwrap();
    let (url, server) = http_once(
        202,
        encode_ack(&ack(
            Some(b.id),
            AckPolicy::ClickHouseConfirmed,
            Status::Ok,
            "",
        ))
        .unwrap(),
    );
    sink(url).write(b, deadline()).unwrap();
    server.join().unwrap();
}
#[cfg(feature = "http")]
#[test]
fn http_rejects_other_media_types_malformed_and_trailing_ack_data() {
    let b = batch();
    let valid = encode_ack(&ack(
        Some(b.id),
        AckPolicy::ClickHouseConfirmed,
        Status::Ok,
        "",
    ))
    .unwrap();
    assert_eq!(valid[0], 0x95, "ACKs must be MessagePack arrays");
    let mut trailing = valid.clone();
    trailing.push(0xc0);
    for (content_type, body) in [
        ("application/x-protobuf", valid),
        ("application/msgpack", vec![0xc1]),
        ("application/msgpack", trailing),
    ] {
        let (url, server) = http_once_with_content_type(200, body, content_type);
        let error = sink(url).write(b.clone(), deadline()).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Permanent);
        assert_eq!(error.outcome, CommitOutcome::Unknown);
        server.join().unwrap();
    }
}
#[cfg(feature = "http")]
#[test]
fn invalid_credentials_debug_and_elapsed_deadlines() {
    let mut cfg = RemoteConfig::new(Endpoint::Http(
        "http://name:secret@example.org/v1/batches".into(),
    ));
    cfg.bearer_token = Some("top-secret-token".into());
    let debug = format!("{cfg:?}");
    assert!(!debug.contains("secret"));
    assert!(RemoteSink::new(cfg).is_err());
    let mut cfg = RemoteConfig::new(Endpoint::Http("https://example.org/v1/batches".into()));
    cfg.request_timeout = Duration::MAX;
    assert!(RemoteSink::new(cfg).is_err());
    let mut sink = sink("http://127.0.0.1:1/v1/batches".into());
    let error = sink.write(batch(), Instant::now()).unwrap_err();
    assert_eq!(error.outcome, CommitOutcome::NotCommitted);
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert!(sink.flush(Instant::now()).is_err());
    let mut invalid = RemoteConfig::new(Endpoint::Http("https://example.org/v1/batches".into()));
    invalid.limits.validation.max_batch_bytes = 0;
    assert!(RemoteSink::new(invalid).is_err());
}

#[cfg(feature = "http")]
#[test]
fn http_and_https_accept_hostnames_and_remote_ip_addresses_without_opt_in() {
    for endpoint in [
        "http://collector.example.com/v1/batches",
        "http://localhost:9091/v1/batches",
        "http://10.23.4.5:9091/v1/batches",
        "http://[fd00::42]:9091/v1/batches",
        "http://[2001:db8::42]:9091/v1/batches",
        "https://collector.example.com/v1/batches",
        "https://10.23.4.5:9443/v1/batches",
        "https://[fd00::42]:9443/v1/batches",
    ] {
        assert!(
            RemoteSink::new(RemoteConfig::new(Endpoint::Http(endpoint.into()))).is_ok(),
            "endpoint should be configurable without connecting: {endpoint}"
        );
    }
}

#[cfg(feature = "http")]
#[test]
fn http_endpoint_rejects_missing_hosts_unsupported_schemes_and_url_metadata() {
    for endpoint in [
        "",
        "/v1/batches",
        "http://",
        "https://",
        "http://:9091/v1/batches",
        "http://[invalid]/v1/batches",
        "http://collector.example.com:70000/v1/batches",
        "ftp://collector.example.com/v1/batches",
        "tcp://10.23.4.5:9092",
        "file:///v1/batches",
        "http://user:secret@collector.example.com/v1/batches",
        "https://user:secret@collector.example.com/v1/batches",
        "http://collector.example.com/v1/batches?token=secret",
        "https://collector.example.com/v1/batches?token=secret",
        "http://collector.example.com/v1/batches#fragment",
        "https://collector.example.com/v1/batches#fragment",
    ] {
        assert!(
            RemoteSink::new(RemoteConfig::new(Endpoint::Http(endpoint.into()))).is_err(),
            "invalid endpoint was accepted: {endpoint}"
        );
    }
}

#[cfg(feature = "tcp")]
#[test]
fn tcp_accepts_loopback_and_remote_socket_addresses_without_opt_in() {
    for address in [
        "127.0.0.1:9092",
        "10.23.4.5:9092",
        "[::1]:9092",
        "[fd00::42]:9092",
        "[2001:db8::42]:9092",
    ] {
        assert!(
            RemoteSink::new(RemoteConfig::new(Endpoint::Tcp(address.parse().unwrap()))).is_ok(),
            "socket address should be configurable without connecting: {address}"
        );
    }
}

#[cfg(feature = "http")]
#[test]
fn invalid_labels_are_rejected_before_remote_connection() {
    for key in ["", "route\n"] {
        let mut value = (*batch()).clone();
        value.rows[0].labels.insert(key.into(), "/read".into());
        let error = sink("http://127.0.0.1:1/v1/batches".into())
            .write(Arc::new(value), deadline())
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Permanent);
        assert_eq!(error.outcome, CommitOutcome::NotCommitted);
        assert!(error.message.contains("label"));
    }
}
#[cfg(feature = "tcp")]
fn handshake(stream: &mut TcpStream) {
    let mut magic = [0; 4];
    stream.read_exact(&mut magic).unwrap();
    assert_eq!(&magic, b"MXS1");
    let mut length = [0; 2];
    stream.read_exact(&mut length).unwrap();
    let mut token = vec![0; u16::from_be_bytes(length) as usize];
    stream.read_exact(&mut token).unwrap();
    stream.write_all(b"MXS1").unwrap();
}
#[cfg(feature = "tcp")]
fn frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut length = [0; 4];
    stream.read_exact(&mut length).unwrap();
    let mut body = vec![0; u32::from_be_bytes(length) as usize];
    stream.read_exact(&mut body).unwrap();
    body
}
#[cfg(feature = "tcp")]
#[test]
fn tcp_ack_loss_reconnect_reuses_exact_bytes_and_ids() {
    let b = batch();
    let ack = encode_ack(&ack(
        Some(b.id),
        AckPolicy::ClickHouseConfirmed,
        Status::Ok,
        "",
    ))
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let expected = b.clone();
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        handshake(&mut first);
        let original = frame(&mut first);
        assert_eq!(original[0], 0x98, "TCP frames must carry MessagePack");
        let (decoded, _) =
            metrics_summary_protocol::decode_request(&original, &ProtocolLimits::default())
                .unwrap();
        assert_eq!(decoded, *expected);
        drop(first);
        let (mut second, _) = listener.accept().unwrap();
        handshake(&mut second);
        assert_eq!(frame(&mut second), original);
        second.write_all(&(ack.len() as u32).to_be_bytes()).unwrap();
        second.write_all(&ack).unwrap();
    });
    let mut cfg = RemoteConfig::new(Endpoint::Tcp(address));
    cfg.ack_policy = AckPolicy::ClickHouseConfirmed;
    let mut sink = RemoteSink::new(cfg).unwrap();
    let error = sink.write(b.clone(), deadline()).unwrap_err();
    assert_eq!(error.outcome, CommitOutcome::Unknown);
    assert!(error.is_retryable());
    sink.write(b, deadline()).unwrap();
    server.join().unwrap();
}
#[cfg(feature = "tcp")]
#[test]
fn tcp_slow_trickle_obeys_total_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        handshake(&mut stream);
        let _ = frame(&mut stream);
        for byte in [0, 0, 0, 1] {
            thread::sleep(Duration::from_millis(30));
            if stream.write_all(&[byte]).is_err() {
                break;
            }
        }
    });
    let cfg = RemoteConfig::new(Endpoint::Tcp(address));
    let mut sink = RemoteSink::new(cfg).unwrap();
    let start = Instant::now();
    let error = sink
        .write(batch(), start + Duration::from_millis(55))
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);
    server.join().unwrap();
}
