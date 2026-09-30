//! Apply deploy/clickhouse/schema.sql first, then set METRICS_CLICKHOUSE_URL.
use metrics_exporter_summary::{metrics, Builder, Config};
use metrics_summary_sink_clickhouse::{ClickHouseConfig, ClickHouseSink};
use std::{error::Error, time::Duration};

fn main() -> Result<(), Box<dyn Error>> {
    let endpoint = std::env::var("METRICS_CLICKHOUSE_URL")
        .map_err(|_| "set METRICS_CLICKHOUSE_URL explicitly before running this example")?;
    let sink = ClickHouseSink::new(ClickHouseConfig {
        endpoint,
        database: std::env::var("METRICS_CLICKHOUSE_DATABASE")
            .unwrap_or_else(|_| "metrics_summary".into()),
        username: std::env::var("METRICS_CLICKHOUSE_USER").ok(),
        password: std::env::var("METRICS_CLICKHOUSE_PASSWORD").ok(),
        tls_ca_pem: std::env::var_os("METRICS_CLICKHOUSE_CA_FILE")
            .map(metrics_summary_sink_clickhouse::read_ca_bundle)
            .transpose()?,
        ..ClickHouseConfig::default()
    })?;
    // Source::new discovers the OS hostname, independently of this instance ID.
    let (recorder, control) =
        Builder::for_service("direct-example", format!("pid-{}", std::process::id()))?
            .config(Config {
                collect_interval: None,
                ..Config::default()
            })
            .build(sink)?;
    metrics::with_local_recorder(&recorder, || {
        metrics::describe_histogram!(
            "rpc.client.duration",
            metrics::Unit::Nanoseconds,
            "Client RPC duration"
        );
        for value in [1_000_000.0, 2_000_000.0, 4_000_000.0, 8_000_000.0] {
            metrics::histogram!("rpc.client.duration", "tag" => "example").record(value);
            metrics::counter!("rpc.client.requests", "tag" => "example").increment(1);
        }
        metrics::gauge!("rpc.client.active").set(0.0);
    });
    // This collection stores a counter delta of 4; gauge snapshots are not reset.
    let report = control.shutdown(Duration::from_secs(35))?;
    if !report.is_success() {
        return Err(format!("ClickHouse shutdown was incomplete: {report:?}").into());
    }
    println!("ClickHouse confirmed collection {}", report.target.sequence);
    Ok(())
}
