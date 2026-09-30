//! Explicit opt-in tests for a disposable real server; see deploy/clickhouse.
use metrics_summary_core::{
    Batch, BatchId, CommitOutcome, MetricValue, Row, Sink, Source, MODEL_VERSION,
};
use metrics_summary_sink_clickhouse::{
    ClickHouseBatchWriter, ClickHouseConfig, ClickHouseSink, TableNames, TESTED_SERVER_VERSION,
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

struct Database {
    client: reqwest::blocking::Client,
    endpoint: String,
    username: Option<String>,
    password: Option<String>,
    name: String,
    tls_ca_pem: Option<Vec<u8>>,
}
impl Database {
    fn query(&self, sql: &str) -> String {
        let mut request = self
            .client
            .post(&self.endpoint)
            .query(&[("wait_end_of_query", "1")])
            .body(sql.to_string());
        if let Some(user) = &self.username {
            request = request.basic_auth(user, self.password.as_ref());
        }
        let response = request
            .send()
            .unwrap_or_else(|error| panic!("ClickHouse HTTP request for {sql}: {error}"));
        let status = response.status();
        let body = response.text().expect("ClickHouse response");
        assert!(status.is_success(), "ClickHouse {status}: {body}");
        assert!(
            !body.starts_with("Code: "),
            "ClickHouse query error: {body}"
        );
        body
    }
    fn config(&self, async_insert: bool) -> ClickHouseConfig {
        ClickHouseConfig {
            endpoint: self.endpoint.clone(),
            database: self.name.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            tls_ca_pem: self.tls_ca_pem.clone(),
            async_insert,
            request_timeout: Duration::from_secs(15),
            ..ClickHouseConfig::default()
        }
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        let mut request = self
            .client
            .post(&self.endpoint)
            .body(format!("DROP DATABASE IF EXISTS `{}` SYNC", self.name));
        if let Some(user) = &self.username {
            request = request.basic_auth(user, self.password.as_ref());
        }
        let _ = request.send();
    }
}
fn batch(hostname: &str, sequence: u64) -> Arc<Batch> {
    Arc::new(Batch {
        model_version: MODEL_VERSION,
        id: BatchId {
            source_session_id: Uuid::new_v4(),
            sequence,
        },
        source: Source {
            application: "real-test".into(),
            instance: "pod".into(),
            hostname: hostname.into(),
            attributes: BTreeMap::from([("cluster".into(), "integration".into())]),
        },
        timestamp: 1_790_000_002_012_345_678,
        duration_ns: u64::MAX,
        rows: vec![
            Row {
                metric_id: 1,
                name: "rpc.client.duration".into(),
                labels: BTreeMap::from([("tag".into(), "/api".into())]),
                unit: Some("seconds".into()),
                value: MetricValue::HistogramSummary {
                    count: 3,
                    sum: 6.0,
                    min: 1.0,
                    p50: 2.0,
                    p90: 2.5,
                    p95: 2.75,
                    p99: 3.0,
                    max: 3.0,
                },
            },
            Row {
                metric_id: 2,
                name: "count".into(),
                labels: BTreeMap::new(),
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
                value: MetricValue::GaugeSnapshot {
                    current_value: i64::MIN,
                },
            },
        ],
    })
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

#[test]
#[ignore = "requires METRICS_CLICKHOUSE_URL pointing to a disposable ClickHouse 26.8.6.5 instance"]
fn actual_server_schema_precision_direct_group_retry_async_and_partial_failure() {
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
        tls_ca_pem,
        endpoint: std::env::var("METRICS_CLICKHOUSE_URL").expect("set METRICS_CLICKHOUSE_URL"),
        username: std::env::var("METRICS_CLICKHOUSE_USER").ok(),
        password: std::env::var("METRICS_CLICKHOUSE_PASSWORD").ok(),
        name: format!("metrics_test_{}", Uuid::new_v4().simple()),
    };
    let version = database.query("SELECT version()");
    assert_eq!(
        version.trim(),
        TESTED_SERVER_VERSION,
        "test the pinned server release"
    );
    let ddl = include_str!("fixtures/schema.sql")
        .replace("metrics_summary.", &format!("{}.", database.name))
        .replace(
            "DATABASE IF NOT EXISTS metrics_summary",
            &format!("DATABASE IF NOT EXISTS {}", database.name),
        );
    for statement in ddl.split(';').filter(|s| !s.trim().is_empty()) {
        database.query(statement);
    }
    if std::env::var_os("METRICS_TEST_TLS_SERVER_PEM").is_some() {
        ClickHouseBatchWriter::new(database.config(false))
            .unwrap()
            .verify_schema(deadline())
            .unwrap();
        let mut missing_root = database.config(false);
        missing_root.tls_ca_pem = None;
        assert!(
            ClickHouseBatchWriter::new(missing_root)
                .unwrap()
                .verify_schema(deadline())
                .is_err(),
            "private CA must not be trusted without configuration"
        );
        let mut wrong_hostname = database.config(false);
        let mut endpoint = reqwest::Url::parse(&wrong_hostname.endpoint).unwrap();
        endpoint.set_host(Some("127.0.0.1")).unwrap();
        wrong_hostname.endpoint = endpoint.to_string();
        assert!(
            ClickHouseBatchWriter::new(wrong_hostname)
                .unwrap()
                .verify_schema(deadline())
                .is_err(),
            "trusted CA must not disable server hostname verification"
        );
    }
    for table in ["counters", "distributions"] {
        database.query(&format!("SYSTEM STOP MERGES {}.{table}", database.name));
    }
    let first = batch("worker-a", 9_007_199_254_740_993);
    let mut second = batch("worker-b", 99);
    Arc::make_mut(&mut second).duration_ns = 0;
    let mut direct = ClickHouseSink::new(database.config(false)).unwrap();
    direct.write(first.clone(), deadline()).unwrap();
    direct.write(first.clone(), deadline()).unwrap();
    let mut group = ClickHouseBatchWriter::new(database.config(true)).unwrap();
    for _ in 0..2 {
        group
            .write_group(&[first.clone(), second.clone()], deadline())
            .unwrap();
    }
    // The supplied MergeTree has no batch identity: retries remain visible.
    assert_eq!(
        database
            .query(&format!("SELECT count() FROM {}.counters", database.name))
            .trim(),
        "12"
    );
    assert_eq!(
        database
            .query(&format!(
                "SELECT count() FROM {}.distributions",
                database.name
            ))
            .trim(),
        "6"
    );
    assert_eq!(database.query(&format!("SELECT metricName, val, host, instance, toUnixTimestamp(TIMESTAMP), type FROM {}.counters WHERE host = 'worker-a' ORDER BY metricName LIMIT 1", database.name)).trim_end_matches('\n'), "count\t9223372036854775807\tworker-a\tpod\t1790000002\t");
    assert_eq!(
        database
            .query(&format!(
                "SELECT DISTINCT val FROM {}.counters WHERE metricName = 'queue'",
                database.name
            ))
            .trim(),
        "-9223372036854775808"
    );
    assert_eq!(database.query(&format!("SELECT count, mean, min, max, p50, p90, p95, p99, method FROM {}.distributions LIMIT 1", database.name)).trim_end_matches('\n'), "3\t2\t1\t3\t2\t2.5\t2.75\t3\t");
    assert_eq!(database.query(&format!("SELECT count() FROM system.columns WHERE database = '{}' AND name IN ('duration_ns','source_session_id','sequence','model_version','metric_id','unit','labels')", database.name)).trim(), "0");

    // Force an actual counters-success / distributions-failure partial write.
    database.query(&format!(
        "RENAME TABLE {}.distributions TO {}.distributions_saved",
        database.name, database.name
    ));
    let third = batch("worker-c", 100);
    let error = group
        .write_group(std::slice::from_ref(&third), deadline())
        .unwrap_err();
    assert_eq!(error.outcome, CommitOutcome::Unknown);
    database.query(&format!(
        "RENAME TABLE {}.distributions_saved TO {}.distributions",
        database.name, database.name
    ));
    group.write_group(&[third], deadline()).unwrap();
    assert_eq!(
        database
            .query(&format!(
                "SELECT count() FROM {}.counters WHERE host = 'worker-c'",
                database.name
            ))
            .trim(),
        "4"
    );
    assert_eq!(
        database
            .query(&format!(
                "SELECT count() FROM {}.distributions WHERE host = 'worker-c'",
                database.name
            ))
            .trim(),
        "1"
    );

    // Labels need matching columns on the table receiving each row, without any
    // application-side key configuration. Missing columns reject the whole group.
    let mut missing = (*batch("missing-column", 499)).clone();
    missing.rows[0]
        .labels
        .insert("region".into(), "cn-a".into());
    assert_eq!(
        ClickHouseBatchWriter::new(database.config(false))
            .unwrap()
            .write_group(&[Arc::new(missing)], deadline())
            .unwrap_err()
            .outcome,
        CommitOutcome::NotCommitted
    );
    for table in ["counters", "distributions"] {
        database.query(&format!("CREATE TABLE {}.{table}_extra ENGINE = Memory AS SELECT *, CAST('' AS String) AS application, CAST('' AS String) AS region, CAST('' AS LowCardinality(String)) AS zone FROM {}.{table} WHERE 0", database.name, database.name));
    }
    let mut extra_config = database.config(false);
    extra_config.tables = TableNames {
        counters: "counters_extra".into(),
        distributions: "distributions_extra".into(),
    };
    let mut extra_writer = ClickHouseBatchWriter::new(extra_config).unwrap();
    // Memory is deliberate: engine/sort/partition/TTL choices are not our schema contract.
    extra_writer.verify_schema(deadline()).unwrap();
    let mut extra_batch = (*batch("source-host", 500)).clone();
    for row in &mut extra_batch.rows {
        row.labels = BTreeMap::from([
            ("host".into(), "label-host".into()),
            ("instance".into(), "label-instance".into()),
            ("pod".into(), "pod-x".into()),
            ("tag".into(), "operation".into()),
            ("uid".into(), "user-1".into()),
            ("region".into(), "cn-a".into()),
            ("application".into(), "user-label-app".into()),
        ]);
    }
    extra_batch.rows[0]
        .labels
        .insert("method".into(), "GET".into());
    extra_batch.rows[1]
        .labels
        .insert("type".into(), "requests".into());
    let mut second_gauge = extra_batch.rows[2].clone();
    second_gauge.metric_id = 4;
    second_gauge.labels.insert("region".into(), "cn-b".into());
    second_gauge.value = MetricValue::GaugeSnapshot {
        current_value: 9_007_199_254_740_993,
    };
    extra_batch.rows.push(second_gauge);
    let extra_batch = Arc::new(extra_batch);
    assert_eq!(
        direct
            .write(extra_batch.clone(), deadline())
            .unwrap_err()
            .outcome,
        CommitOutcome::NotCommitted
    );
    extra_writer
        .write_group(&[extra_batch], deadline())
        .unwrap();
    assert_eq!(database.query(&format!("SELECT host, pod, instance, tag, uid, application, region, zone, val FROM {}.counters_extra WHERE metricName = 'queue' ORDER BY region", database.name)).trim_end_matches('\n'), "label-host\tpod-x\tlabel-instance\toperation\tuser-1\tuser-label-app\tcn-a\t\t-9223372036854775808\nlabel-host\tpod-x\tlabel-instance\toperation\tuser-1\tuser-label-app\tcn-b\t\t9007199254740993");
    assert_eq!(
        database
            .query(&format!(
                "SELECT method FROM {}.distributions_extra",
                database.name
            ))
            .trim(),
        "GET"
    );
    assert_eq!(
        database
            .query(&format!(
                "SELECT type FROM {}.counters_extra WHERE metricName = 'count'",
                database.name
            ))
            .trim(),
        "requests"
    );

    // A new key after the first insert refreshes cached metadata, and needs a
    // column only in the row's target table. Different row key sets stay distinct.
    database.query(&format!(
        "ALTER TABLE {}.counters_extra ADD COLUMN `route.name` String DEFAULT '/default'",
        database.name
    ));
    let mut late = (*batch("late-key", 504)).clone();
    late.rows.remove(0);
    late.rows[0]
        .labels
        .insert("route.name".into(), "/ready".into());
    late.rows[1].labels.insert("zone".into(), "cn-b".into());
    extra_writer
        .write_group(&[Arc::new(late)], deadline())
        .unwrap();
    assert_eq!(database.query(&format!("SELECT metricName, host, instance, `route.name`, zone FROM {}.counters_extra WHERE host = 'late-key' ORDER BY metricName", database.name)).trim_end_matches('\n'), "count\tlate-key\tpod\t/ready\t\nqueue\tlate-key\tpod\t/default\tcn-b");
    assert_eq!(database.query(&format!("SELECT count() FROM system.columns WHERE database = '{}' AND table = 'distributions_extra' AND name = 'route.name'", database.name)).trim(), "0");

    database.query(&format!(
        "ALTER TABLE {}.counters_extra ADD COLUMN bad_label UInt64",
        database.name
    ));
    for label in ["bad_label", "val", "TIMESTAMP", "metricName"] {
        let mut rejected = (*batch("invalid-label", 505)).clone();
        rejected.rows[1].labels.insert(label.into(), "7".into());
        let error = extra_writer
            .write_group(&[Arc::new(rejected)], deadline())
            .unwrap_err();
        assert_eq!(error.outcome, CommitOutcome::NotCommitted);
    }
    for table in ["counters_extra", "distributions_extra"] {
        assert_eq!(
            database
                .query(&format!(
                    "SELECT count() FROM {}.{table} WHERE host = 'invalid-label'",
                    database.name
                ))
                .trim(),
            "0"
        );
    }

    // The seconds range includes both UInt32 endpoints. Duration stays internal,
    // including zero and UInt64::MAX, and fractional seconds are discarded.
    for (sequence, nanos, duration_ns) in [
        (501, 999_999_999, 0),
        (
            502,
            i64::from(u32::MAX) * 1_000_000_000 + 999_999_999,
            u64::MAX,
        ),
    ] {
        let mut boundary = (*batch("boundary", sequence)).clone();
        boundary.timestamp = nanos;
        boundary.duration_ns = duration_ns;
        extra_writer
            .write_group(&[Arc::new(boundary)], deadline())
            .unwrap();
    }
    assert_eq!(database.query(&format!("SELECT DISTINCT toUnixTimestamp(TIMESTAMP) FROM {}.counters_extra WHERE host = 'boundary' ORDER BY 1", database.name)).trim(), "0\n4294967295");
    assert_eq!(database.query(&format!("SELECT DISTINCT toUnixTimestamp(TIMESTAMP) FROM {}.distributions_extra WHERE host = 'boundary' ORDER BY 1", database.name)).trim(), "0\n4294967295");
    for nanos in [-1, (i64::from(u32::MAX) + 1) * 1_000_000_000] {
        let mut rejected = (*batch("out-of-range", 503)).clone();
        rejected.timestamp = nanos;
        assert_eq!(
            extra_writer
                .write_group(&[Arc::new(rejected)], deadline())
                .unwrap_err()
                .outcome,
            CommitOutcome::NotCommitted
        );
    }

    for template in [
        include_str!("fixtures/queries.sql"),
        include_str!("fixtures/retention.sql"),
    ] {
        let sql = template
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n")
            .replace("metrics_summary.", &format!("{}.", database.name));
        for statement in sql.split(';').filter(|s| !s.trim().is_empty()) {
            database.query(statement);
        }
    }
    // Missing percentile and incorrect signed scalar type fail before insertion.
    database.query(&format!("CREATE TABLE {}.distributions_bad ENGINE = Memory AS SELECT * EXCEPT p95 FROM {}.distributions WHERE 0", database.name, database.name));
    database.query(&format!("CREATE TABLE {}.counters_bad ENGINE = Memory AS SELECT * REPLACE toUInt64(0) AS val FROM {}.counters WHERE 0", database.name, database.name));
    for bad_table in ["distributions", "counters"] {
        let mut config = database.config(false);
        if bad_table == "distributions" {
            config.tables.distributions = "distributions_bad".into();
        } else {
            config.tables.counters = "counters_bad".into();
        }
        assert_eq!(
            ClickHouseBatchWriter::new(config)
                .unwrap()
                .verify_schema(deadline())
                .unwrap_err()
                .outcome,
            CommitOutcome::NotCommitted
        );
    }
    println!("ClickHouse {}: storage schema, shared Int64 scalars, full histogram fields, DateTime seconds/boundaries, label overrides/extras, unconstrained engine, retries visible, async ACK, partial failure and invalid schema rejection verified", version.trim());
}
