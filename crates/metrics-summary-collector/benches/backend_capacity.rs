//! Real-storage capacity runs. This opt-in benchmark creates and removes one
//! uniquely named database; set METRICS_CLICKHOUSE_URL to a disposable server.

#[path = "support/capacity_workload.rs"]
mod workload;

use metrics_summary_collector::{Collector, CollectorConfig, GroupWriter};
use metrics_summary_core::{
    Batch, BatchId, MetricValue, Row, Sink, Source, WriteError, MODEL_VERSION,
};
use metrics_summary_sink_clickhouse::{
    ClickHouseBatchWriter, ClickHouseConfig, ClickHouseSink, TESTED_SERVER_VERSION,
};
use metrics_summary_sink_remote::{AckPolicy, Endpoint, RemoteConfig, RemoteSink};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, Barrier, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

const TOKEN: &str = "local-capacity-benchmark-token";

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

    fn verify(&self, prefix: &str, expected_samples: u64, expected_instances: u64) -> Value {
        assert!(prefix
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.'));
        let result = self.query(&format!(
            "SELECT count(), sum(toUInt64(count)), uniqExact(instance), countIf(empty(host)) FROM {}.distributions WHERE startsWith(metricName, '{prefix}')",
            self.config.database
        ));
        let numbers: Vec<u64> = result
            .split_whitespace()
            .map(|n| n.parse().unwrap())
            .collect();
        assert_eq!(
            numbers[1], expected_samples,
            "database sample conservation for {prefix}"
        );
        assert_eq!(
            numbers[2], expected_instances,
            "instance coverage for {prefix}"
        );
        assert_eq!(numbers[3], 0, "host label missing in database");
        json!({"physical_rows": numbers[0], "samples": numbers[1], "distinct_instances": numbers[2], "missing_host_rows": numbers[3]})
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

struct ObservedGroupWriter {
    inner: ClickHouseBatchWriter,
    groups: Arc<Mutex<Vec<Value>>>,
}
impl GroupWriter for ObservedGroupWriter {
    fn validate_config(
        &self,
        config: &CollectorConfig,
    ) -> Result<(), metrics_summary_collector::ConfigError> {
        GroupWriter::validate_config(&self.inner, config)
    }
    fn write_group(&mut self, batches: &[Arc<Batch>], deadline: Instant) -> Result<(), WriteError> {
        let started = Instant::now();
        let result = self.inner.write_group(batches, deadline);
        self.groups.lock().unwrap().push(json!({
            "batches": batches.len(), "rows": batches.iter().map(|b| b.rows.len()).sum::<usize>(),
            "samples": batches.iter().map(|b| b.histogram_samples()).sum::<u64>(),
            "duration_ns": started.elapsed().as_nanos() as u64, "success": result.is_ok()
        }));
        result
    }
}

struct Service {
    runtime: tokio::runtime::Runtime,
    collector: Collector,
    http: String,
    tcp: SocketAddr,
    stop: tokio::sync::watch::Sender<bool>,
    handles: Vec<tokio::task::JoinHandle<()>>,
    groups: Arc<Mutex<Vec<Value>>>,
}
impl Service {
    fn start(database: &Database) -> Self {
        let groups = Arc::new(Mutex::new(Vec::new()));
        let writer = ObservedGroupWriter {
            inner: ClickHouseBatchWriter::new(database.config.clone()).unwrap(),
            groups: groups.clone(),
        };
        let collector = Collector::new(
            CollectorConfig::default(),
            Some(TOKEN.into()),
            vec![Box::new(writer)],
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
            let tcp_collector = collector.clone();
            let mut http_rx = rx.clone();
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
            (http, tcp, stop, vec![http_handle, tcp_handle])
        });
        Self {
            runtime,
            collector,
            http,
            tcp,
            stop,
            handles,
            groups,
        }
    }

    fn finish(self) -> Value {
        let before = self.collector.diagnostics();
        let started = Instant::now();
        self.stop.send_replace(true);
        let report = self.runtime.block_on(async {
            let report = self
                .collector
                .shutdown(Instant::now() + Duration::from_secs(60))
                .await;
            for handle in self.handles {
                handle.await.unwrap();
            }
            report
        });
        assert!(
            report.drained && report.dropped_after_acceptance == 0,
            "{report:?}"
        );
        let after = self.collector.diagnostics();
        assert_eq!(after.accepted_batches, after.clickhouse_confirmed_batches);
        assert_eq!(after.retries, 0);
        assert_eq!(after.rejected_requests, 0);
        json!({"drain_ns": started.elapsed().as_nanos() as u64, "before_drain": before, "after_drain": after, "shutdown": report, "groups": *self.groups.lock().unwrap()})
    }
}

fn remote(endpoint: Endpoint, policy: AckPolicy) -> RemoteSink {
    let mut config = RemoteConfig::new(endpoint);
    config.ack_policy = policy;
    config.bearer_token = Some(TOKEN.into());
    RemoteSink::new(config).unwrap()
}

fn burst(endpoint: Endpoint, policy: AckPolicy, application: &str) -> Value {
    const SOURCES: usize = 32;
    let start = Arc::new(Barrier::new(SOURCES));
    let mut workers = Vec::new();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    for source in 0..SOURCES {
        let start = start.clone();
        let endpoint = endpoint.clone();
        let source = Source::new(application, format!("source-{source}")).unwrap();
        workers.push(thread::spawn(move || {
            let source_session_id = Uuid::new_v4();
            let batch = Arc::new(Batch {
                model_version: MODEL_VERSION,
                id: BatchId {
                    source_session_id,
                    sequence: 1,
                },
                source,
                timestamp,
                // Synthetic instantaneous samples, not the later write/ACK latency.
                duration_ns: 0,
                rows: (1..=1000)
                    .map(|metric_id| Row {
                        metric_id,
                        name: format!("burst.{metric_id}"),
                        labels: BTreeMap::new(),
                        unit: None,
                        value: MetricValue::HistogramSummary {
                            count: 1,
                            sum: 1.0,
                            min: 1.0,
                            p50: 1.0,
                            p90: 1.0,
                            p95: 1.0,
                            p99: 1.0,
                            max: 1.0,
                        },
                    })
                    .collect(),
            });
            let mut sink = remote(endpoint, policy);
            start.wait();
            let started = Instant::now();
            sink.write(batch, Instant::now() + Duration::from_secs(30))
                .unwrap();
            (started.elapsed().as_nanos() as u64, source_session_id)
        }));
    }
    let (mut ack_ns, sessions): (Vec<_>, Vec<_>) = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .unzip();
    ack_ns.sort_unstable();
    json!({"sources": SOURCES, "source_session_ids": sessions, "rows_per_source": 1000, "ack_p50_ns": ack_ns[SOURCES / 2], "ack_p99_ns": ack_ns[SOURCES - 1], "ack_min_ns": ack_ns[0], "ack_max_ns": ack_ns[SOURCES - 1]})
}

fn main() {
    let Some(endpoint) = std::env::var("METRICS_CLICKHOUSE_URL").ok() else {
        eprintln!(
            "backend_capacity requires METRICS_CLICKHOUSE_URL for a disposable ClickHouse server"
        );
        return;
    };
    let database = Database {
        client: reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap(),
        config: ClickHouseConfig {
            endpoint,
            database: format!("metrics_capacity_{}", Uuid::new_v4().simple()),
            username: std::env::var("METRICS_CLICKHOUSE_USER").ok(),
            password: std::env::var("METRICS_CLICKHOUSE_PASSWORD").ok(),
            ..Default::default()
        },
    };
    let version = database.query("SELECT version()");
    assert_eq!(version.trim(), TESTED_SERVER_VERSION);
    let ddl = include_str!("../tests/fixtures/schema.sql")
        .replace(
            "metrics_summary.",
            &format!("{}.", database.config.database),
        )
        .replace(
            "DATABASE IF NOT EXISTS metrics_summary",
            &format!("DATABASE IF NOT EXISTS {}", database.config.database),
        );
    for statement in ddl
        .split(';')
        .filter(|statement| !statement.trim().is_empty())
    {
        database.query(statement);
    }
    let mut results = Vec::new();
    eprintln!("Capacity run 1/5: direct ClickHouse");
    let mut direct = workload::run(
        ClickHouseSink::new(database.config.clone()).unwrap(),
        "capacity-direct",
    );
    direct["database"] = database.verify("capacity.histogram.", 3_200_000, 1);
    direct["mode"] = "direct-clickhouse".into();
    results.push(direct);
    for (index, transport, policy) in [
        (2, "http", AckPolicy::Enqueued),
        (3, "http", AckPolicy::ClickHouseConfirmed),
        (4, "tcp", AckPolicy::Enqueued),
        (5, "tcp", AckPolicy::ClickHouseConfirmed),
    ] {
        let mode = format!(
            "{transport}-{}",
            if policy == AckPolicy::Enqueued {
                "enqueued"
            } else {
                "confirmed"
            }
        );
        let application = format!("capacity-{mode}");
        eprintln!("Capacity run {index}/5: {mode}");
        // The distributions table has no source-session column. Isolate each
        // finished mode in this disposable database, then distinguish main/burst
        // metric names.
        database.query(&format!(
            "TRUNCATE TABLE {}.distributions",
            database.config.database
        ));
        let service = Service::start(&database);
        let endpoint = if transport == "http" {
            Endpoint::Http(service.http.clone())
        } else {
            Endpoint::Tcp(service.tcp)
        };
        let mut run = workload::run(remote(endpoint.clone(), policy), &application);
        let after_source_shutdown = service.collector.diagnostics();
        let burst_application = format!("burst-{mode}");
        run["multi_source_burst"] = burst(endpoint, policy, &burst_application);
        run["collector_at_source_shutdown"] = serde_json::to_value(after_source_shutdown).unwrap();
        run["collector"] = service.finish();
        run["database"] = database.verify("capacity.histogram.", 3_200_000, 1);
        run["multi_source_burst"]["database"] = database.verify("burst.", 32_000, 32);
        let per_source = database.query(&format!(
            "SELECT sum(toUInt64(count)) FROM {}.distributions WHERE startsWith(metricName, 'burst.') GROUP BY instance",
            database.config.database
        ));
        assert_eq!(per_source.lines().count(), 32);
        assert!(
            per_source.lines().all(|samples| samples.trim() == "1000"),
            "one of the burst sources lost observations"
        );
        run["mode"] = mode.into();
        results.push(run);
    }
    println!("{}", serde_json::to_string_pretty(&json!({"clickhouse_version": version.trim(), "async_insert": database.config.async_insert, "collector_group_max_delay_ms": 100, "transport_security": "loopback cleartext with bearer authentication", "results": results})).unwrap());
}
