//! Recorder -> HTTP/TCP -> collector -> real ClickHouse. Explicitly opt in.
#![cfg(all(feature = "http", feature = "tcp"))]
use metrics_exporter_summary::{metrics, Builder, Config};
use metrics_summary_collector::{Collector, CollectorConfig};
use metrics_summary_core::{
    Batch, BatchId, CompletionBoundary, MetricValue, Row, Sink, Source, MODEL_VERSION,
};
use metrics_summary_sink_clickhouse::{
    ClickHouseBatchWriter, ClickHouseConfig, TESTED_SERVER_VERSION,
};
use metrics_summary_sink_remote::{AckPolicy, Endpoint, RemoteConfig, RemoteSink};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;
#[path = "support/tls_proxy.rs"]
mod tls_proxy;

struct Database {
    client: reqwest::blocking::Client,
    config: ClickHouseConfig,
}
impl Database {
    fn query(&self, sql: &str) -> String {
        let mut request = self
            .client
            .post(&self.config.endpoint)
            .query(&[("wait_end_of_query", "1")])
            .body(sql.to_owned());
        if let Some(user) = &self.config.username {
            request = request.basic_auth(user, self.config.password.as_ref());
        }
        let response = request.send().expect("ClickHouse connection");
        let status = response.status();
        let text = response.text().unwrap();
        assert!(
            status.is_success() && !text.starts_with("Code:"),
            "SQL failed: {status}: {text}"
        );
        text
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        let mut request = self.client.post(&self.config.endpoint).body(format!(
            "DROP DATABASE IF EXISTS {} SYNC",
            self.config.database
        ));
        if let Some(user) = &self.config.username {
            request = request.basic_auth(user, self.config.password.as_ref());
        }
        let _ = request.send();
    }
}
fn remote(endpoint: Endpoint, policy: AckPolicy) -> RemoteSink {
    let mut config = RemoteConfig::new(endpoint);
    config.ack_policy = policy;
    config.bearer_token = Some("local-integration-test-token".into());
    config.request_timeout = Duration::from_secs(10);
    config.tls_ca_pem = std::env::var_os("METRICS_COLLECTOR_CA_FILE")
        .map(metrics_summary_sink_clickhouse::read_ca_bundle)
        .transpose()
        .unwrap();
    RemoteSink::new(config).unwrap()
}

#[test]
#[ignore = "requires METRICS_CLICKHOUSE_URL and disposable ClickHouse 26.8.6.5"]
fn recorder_http_tcp_both_ack_policies_and_retry_identity_reach_real_clickhouse() {
    let tls_ca_pem = std::env::var_os("METRICS_CLICKHOUSE_CA_FILE")
        .map(metrics_summary_sink_clickhouse::read_ca_bundle)
        .transpose()
        .unwrap();
    let mut client = reqwest::blocking::Client::builder().timeout(Duration::from_secs(20));
    if let Some(pem) = &tls_ca_pem {
        client = client.tls_certs_merge(reqwest::Certificate::from_pem_bundle(pem).unwrap());
    }
    let database = Database {
        client: client.build().unwrap(),
        config: ClickHouseConfig {
            endpoint: std::env::var("METRICS_CLICKHOUSE_URL").expect("set METRICS_CLICKHOUSE_URL"),
            database: format!("metrics_e2e_{}", Uuid::new_v4().simple()),
            username: std::env::var("METRICS_CLICKHOUSE_USER").ok(),
            password: std::env::var("METRICS_CLICKHOUSE_PASSWORD").ok(),
            tls_ca_pem,
            async_insert: true,
            ..ClickHouseConfig::default()
        },
    };
    assert_eq!(
        database.query("SELECT version()").trim(),
        TESTED_SERVER_VERSION
    );
    let ddl = include_str!("fixtures/schema.sql")
        .replace(
            "metrics_summary.",
            &format!("{}.", database.config.database),
        )
        .replace(
            "DATABASE IF NOT EXISTS metrics_summary",
            &format!("DATABASE IF NOT EXISTS {}", database.config.database),
        );
    for statement in ddl.split(';').filter(|s| !s.trim().is_empty()) {
        database.query(statement);
    }
    let collector = Collector::new(
        CollectorConfig {
            group_max_delay_ms: 5,
            ..CollectorConfig::default()
        },
        Some("local-integration-test-token".into()),
        vec![Box::new(
            ClickHouseBatchWriter::new(database.config.clone()).unwrap(),
        )],
    )
    .unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (http, tcp, stop, handles) = runtime.block_on(async {
        let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = format!("http://{}/v1/batches", http_listener.local_addr().unwrap());
        let tcp = tcp_listener.local_addr().unwrap();
        let (stop, rx) = tokio::sync::watch::channel(false);
        let http_collector = collector.clone();
        let mut http_rx = rx.clone();
        let tcp_collector = collector.clone();
        let mut tcp_rx = rx;
        let http_handle = tokio::spawn(async move {
            http_collector
                .serve_http(http_listener, async move {
                    let _ = http_rx.changed().await;
                })
                .await
                .unwrap();
        });
        let tcp_handle = tokio::spawn(async move {
            tcp_collector
                .serve_tcp(tcp_listener, async move {
                    let _ = tcp_rx.changed().await;
                })
                .await
                .unwrap();
        });
        (http, tcp, stop, [http_handle, tcp_handle])
    });

    let proxy = tls_proxy::Proxy::optional(&http, tcp);
    let (http, tcp) = proxy
        .as_ref()
        .map_or((http, tcp), |proxy| (proxy.http.clone(), proxy.tcp));

    // Real recorder instances exercise instrument capture and hostname discovery,
    // not only handcrafted protocol messages. Counter deltas reset each round;
    // unchanged gauges continue to appear and empty histogram windows do not.
    for (transport, endpoint) in [
        ("http", Endpoint::Http(http.clone())),
        ("tcp", Endpoint::Tcp(tcp)),
    ] {
        for policy in [AckPolicy::Enqueued, AckPolicy::ClickHouseConfirmed] {
            let sink = remote(endpoint.clone(), policy);
            let (recorder, control) = Builder::for_service(
                format!("e2e-{transport}-{policy:?}"),
                format!("{transport}-{policy:?}"),
            )
            .unwrap()
            .config(Config {
                collect_interval: None,
                ..Config::default()
            })
            .build(sink)
            .unwrap();
            metrics::with_local_recorder(&recorder, || {
                metrics::describe_histogram!(
                    "rpc.duration",
                    metrics::Unit::Nanoseconds,
                    "RPC duration"
                );
                for sample in [1_000_000.0, 2_000_000.0, 3_000_000.0] {
                    metrics::histogram!(
                        "rpc.duration",
                        "tag" => "/integration",
                        "method" => "Read"
                    )
                    .record(sample);
                    metrics::counter!("rpc.requests", "type" => "completed").increment(1);
                }
                metrics::gauge!("rpc.active").set(2.0);
            });
            assert!(control.flush(Duration::from_secs(15)).unwrap().is_success());
            metrics::with_local_recorder(&recorder, || {
                metrics::counter!("rpc.requests", "type" => "completed").increment(2);
            });
            assert!(control.flush(Duration::from_secs(15)).unwrap().is_success());
            let report = control.shutdown(Duration::from_secs(15)).unwrap();
            assert!(report.is_success(), "{transport} {policy:?}: {report:?}");
            assert_eq!(
                report.completion_boundary,
                if policy == AckPolicy::Enqueued {
                    CompletionBoundary::RemoteAccepted
                } else {
                    CompletionBoundary::StorageConfirmed
                }
            );
        }
    }

    // Replay within this collector's live dedup cache must not issue another write.
    let source = Source::new("retry-e2e", "synthetic-instance").unwrap();
    let actual_hostname = source.hostname.clone();
    let replay = Arc::new(Batch {
        model_version: MODEL_VERSION,
        id: BatchId {
            source_session_id: Uuid::new_v4(),
            sequence: 1,
        },
        source,
        timestamp: 1_790_000_001_123_456_789,
        duration_ns: 1_000_000_000,
        rows: vec![
            Row {
                metric_id: 1,
                name: "replayed.requests".into(),
                labels: BTreeMap::from([("uid".into(), "alpha".into())]),
                unit: None,
                value: MetricValue::CounterDelta {
                    delta_value: i64::MAX as u64,
                },
            },
            Row {
                metric_id: 2,
                name: "replayed.requests".into(),
                labels: BTreeMap::from([("uid".into(), "beta".into())]),
                unit: None,
                value: MetricValue::CounterDelta { delta_value: 42 },
            },
        ],
    });
    if proxy.is_some() {
        let mut missing_root = RemoteConfig::new(Endpoint::Http(http.clone()));
        missing_root.bearer_token = Some("local-integration-test-token".into());
        assert!(RemoteSink::new(missing_root)
            .unwrap()
            .write(replay.clone(), Instant::now() + Duration::from_secs(3))
            .is_err());
        let mut wrong_host = reqwest::Url::parse(&http).unwrap();
        wrong_host.set_host(Some("127.0.0.1")).unwrap();
        assert!(remote(
            Endpoint::Http(wrong_host.to_string()),
            AckPolicy::ClickHouseConfirmed
        )
        .write(replay.clone(), Instant::now() + Duration::from_secs(3))
        .is_err());
    }
    remote(Endpoint::Http(http), AckPolicy::ClickHouseConfirmed)
        .write(replay.clone(), Instant::now() + Duration::from_secs(15))
        .unwrap();
    remote(Endpoint::Tcp(tcp), AckPolicy::ClickHouseConfirmed)
        .write(replay.clone(), Instant::now() + Duration::from_secs(15))
        .unwrap();
    let diagnostics = collector.diagnostics();
    assert!(diagnostics.duplicate_requests >= 1);
    stop.send_replace(true);
    runtime.block_on(async {
        let report = collector
            .shutdown(Instant::now() + Duration::from_secs(15))
            .await;
        assert!(report.drained, "{report:?}");
        assert_eq!(report.dropped_after_acceptance, 0);
        for handle in handles {
            handle.await.unwrap();
        }
    });

    for (table, expected) in [("distributions", 4), ("counters", 26)] {
        assert_eq!(
            database
                .query(&format!(
                    "SELECT count() FROM {}.{table}",
                    database.config.database
                ))
                .trim(),
            expected.to_string()
        );
        let rows = database.query(&format!(
            "SELECT DISTINCT host FROM {}.{table} FORMAT JSONEachRow",
            database.config.database
        ));
        let parsed: Vec<serde_json::Value> = rows
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["host"].as_str(), Some(actual_hostname.as_str()));
    }
    let distribution_rows = database.query(&format!(
        "SELECT count, mean, min, max, p50, p90, p95, p99 FROM {}.distributions FORMAT JSONEachRow",
        database.config.database
    ));
    for row in distribution_rows.lines() {
        let row: serde_json::Value = serde_json::from_str(row).unwrap();
        assert_eq!(row["count"].as_f64(), Some(3.0));
        assert!((row["mean"].as_f64().unwrap() - 2_000_000.0).abs() < 1e-12);
        assert_eq!(row["min"].as_f64(), Some(1_000_000.0));
        assert_eq!(row["max"].as_f64(), Some(3_000_000.0));
        assert!(row["p90"].as_f64().unwrap() <= row["p95"].as_f64().unwrap());
        assert!(row["p95"].as_f64().unwrap() <= row["p99"].as_f64().unwrap());
    }
    // Existing String/LowCardinality(String) columns need no label allowlist.
    for (table, column, metric, expected) in [
        ("distributions", "method", "rpc.duration", "Read"),
        ("counters", "type", "rpc.requests", "completed"),
    ] {
        assert_eq!(
            database
                .query(&format!(
                    "SELECT DISTINCT {column} FROM {}.{table} WHERE metricName = '{metric}'",
                    database.config.database
                ))
                .trim(),
            expected
        );
    }
    // DateTime stores seconds, so multiple rounds may tie; compare multisets,
    // without claiming that storage retains a round's ordering or identity.
    for (name, expected) in [("rpc.requests", "[0,2,3]"), ("rpc.active", "[2,2,2]")] {
        let values = database.query(&format!(
            "SELECT arraySort(groupArray(val)) FROM {}.counters WHERE metricName = '{name}' GROUP BY instance",
            database.config.database
        ));
        let rows: Vec<_> = values.lines().collect();
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|row| *row == expected), "{values}");
    }
    assert_eq!(
        database
            .query(&format!(
            "SELECT uid, val FROM {}.counters WHERE metricName = 'replayed.requests' ORDER BY uid",
            database.config.database
        ))
            .trim(),
        format!("alpha\t{}\nbeta\t42", i64::MAX)
    );
    assert_eq!(
        database.query(&format!(
            "SELECT toUnixTimestamp(TIMESTAMP) FROM {}.counters WHERE metricName = 'replayed.requests' AND uid = 'alpha'",
            database.config.database
        )).trim(),
        "1790000001"
    );
    // Storage has no batch identity: a repeated physical insert is visible.
    ClickHouseBatchWriter::new(database.config.clone())
        .unwrap()
        .write_group(&[replay], Instant::now() + Duration::from_secs(15))
        .unwrap();
    assert_eq!(
        database
            .query(&format!(
                "SELECT count() FROM {}.counters WHERE metricName = 'replayed.requests'",
                database.config.database
            ))
            .trim(),
        "4"
    );
    println!("Real recorder -> HTTP/TCP -> collector -> ClickHouse counters/distributions verified interval increments, repeated integer gauges, min/p95, both ACK policies, host labels, live collector-cache suppression and visible repeated storage inserts");
}
