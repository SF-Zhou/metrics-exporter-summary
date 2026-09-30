use clap::Parser;
use metrics_summary_collector::{Collector, CollectorConfig, GroupWriter};
use metrics_summary_sink_clickhouse::{ClickHouseBatchWriter, ClickHouseConfig};
use serde::Deserialize;
use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::sync::watch;

#[derive(Parser)]
#[command(
    version,
    about = "Bounded MessagePack metrics collector with optional TLS termination via a proxy"
)]
struct Args {
    #[arg(long, env = "METRICS_COLLECTOR_CONFIG")]
    config: PathBuf,
    /// Parse and validate configuration without binding listeners.
    #[arg(long)]
    check_config: bool,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    http_listen: Option<SocketAddr>,
    tcp_listen: Option<SocketAddr>,
    write_concurrency: usize,
    shutdown_timeout_ms: u64,
    collector: CollectorConfig,
    clickhouse: DatabaseConfig,
}
impl Default for FileConfig {
    fn default() -> Self {
        Self {
            http_listen: None,
            tcp_listen: None,
            write_concurrency: 1,
            shutdown_timeout_ms: 30_000,
            collector: CollectorConfig::default(),
            clickhouse: DatabaseConfig::default(),
        }
    }
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DatabaseConfig {
    endpoint: String,
    database: String,
    username: Option<String>,
    ca_file: Option<PathBuf>,
    async_insert: bool,
    connect_timeout_ms: u64,
}
impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:8123".into(),
            database: "metrics_summary".into(),
            username: None,
            ca_file: None,
            async_insert: false,
            connect_timeout_ms: 2000,
        }
    }
}
impl FileConfig {
    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.collector.validate()?;
        if self.write_concurrency == 0
            || self.write_concurrency > 64
            || self.shutdown_timeout_ms == 0
            || self.shutdown_timeout_ms > 3_600_000
        {
            return Err("invalid writer concurrency or shutdown timeout".into());
        }
        if self.http_listen.is_none() && self.tcp_listen.is_none() {
            return Err("at least one listener must be configured".into());
        }
        #[cfg(not(feature = "http"))]
        if self.http_listen.is_some() {
            return Err("rebuild with feature http for HTTP listener".into());
        }
        #[cfg(not(feature = "tcp"))]
        if self.tcp_listen.is_some() {
            return Err("rebuild with feature tcp for TCP listener".into());
        }
        Ok(())
    }
    fn database(&self) -> Result<ClickHouseConfig, Box<dyn std::error::Error>> {
        Ok(ClickHouseConfig {
            endpoint: self.clickhouse.endpoint.clone(),
            database: self.clickhouse.database.clone(),
            username: self.clickhouse.username.clone(),
            password: std::env::var("CLICKHOUSE_PASSWORD").ok(),
            tls_ca_pem: self
                .clickhouse
                .ca_file
                .as_ref()
                .map(metrics_summary_sink_clickhouse::read_ca_bundle)
                .transpose()?,
            connect_timeout: Duration::from_millis(self.clickhouse.connect_timeout_ms),
            request_timeout: Duration::from_millis(self.collector.db_write_timeout_ms),
            async_insert: self.clickhouse.async_insert,
            max_group_batches: self.collector.group_max_batches,
            max_group_bytes: self.collector.group_max_bytes,
            max_group_rows: self.collector.group_max_rows,
            max_encoded_bytes: self.collector.group_max_encoded_bytes,
            validation_limits: self.collector.validation.clone(),
            ..Default::default()
        })
    }
}
async fn signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {_ = tokio::signal::ctrl_c()=>{},_ = terminate.recv()=>{}}
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
#[cfg(any(feature = "http", feature = "tcp"))]
async fn stopped(mut rx: watch::Receiver<bool>) {
    if !*rx.borrow() {
        let _ = rx.changed().await;
    }
}
#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let config: FileConfig = toml::from_str(&std::fs::read_to_string(args.config)?)?;
    config.validate()?;
    let database = config.database()?;
    database.validate()?;
    if args.check_config {
        println!("configuration valid");
        return Ok(());
    }
    let token = std::env::var("METRICS_COLLECTOR_TOKEN").ok();
    if token.is_none() {
        return Err("METRICS_COLLECTOR_TOKEN is required by the service binary".into());
    }
    let writers = (0..config.write_concurrency)
        .map(|_| {
            ClickHouseBatchWriter::new(database.clone())
                .map(|writer| Box::new(writer) as Box<dyn GroupWriter>)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let collector = Collector::new(config.collector.clone(), token, writers)?;
    let (tx, _rx) = watch::channel(false);
    let mut servers: tokio::task::JoinSet<std::io::Result<()>> = tokio::task::JoinSet::new();
    #[cfg(feature = "http")]
    if let Some(address) = config.http_listen {
        let listener = tokio::net::TcpListener::bind(address).await?;
        let collector = collector.clone();
        let rx = _rx.clone();
        servers.spawn(async move { collector.serve_http(listener, stopped(rx)).await });
    }
    #[cfg(feature = "tcp")]
    if let Some(address) = config.tcp_listen {
        let listener = tokio::net::TcpListener::bind(address).await?;
        let collector = collector.clone();
        let rx = _rx.clone();
        servers.spawn(async move { collector.serve_tcp(listener, stopped(rx)).await });
    }
    tracing::info!(
        "collector listening; diagnostics at /diagnostics require bearer authentication"
    );
    let listener_failed = tokio::select! {_ = signal()=>false,result=servers.join_next()=>{tracing::error!(?result,"collector listener stopped");true}};
    tx.send_replace(true);
    let shutdown_deadline = Instant::now() + Duration::from_millis(config.shutdown_timeout_ms);
    let report = collector.shutdown(shutdown_deadline).await;
    let drain_network = async { while servers.join_next().await.is_some() {} };
    let _ = tokio::time::timeout_at(
        tokio::time::Instant::from_std(shutdown_deadline),
        drain_network,
    )
    .await;
    servers.abort_all();
    while servers.join_next().await.is_some() {}
    tracing::info!(report=%serde_json::to_string(&report)?,"collector shutdown");
    if listener_failed {
        return Err("collector listener failed".into());
    }
    if !report.drained || report.dropped_after_acceptance > 0 {
        return Err(
            "collector shutdown with unconfirmed or dropped accepted batches; inspect diagnostics"
                .into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_listeners_stay_disabled_after_toml_parsing() {
        let tcp: FileConfig = toml::from_str("tcp_listen = \"127.0.0.1:9092\"").unwrap();
        assert!(tcp.http_listen.is_none());
        assert_eq!(tcp.tcp_listen, Some("127.0.0.1:9092".parse().unwrap()));

        let http: FileConfig = toml::from_str("http_listen = \"127.0.0.1:9091\"").unwrap();
        assert!(http.tcp_listen.is_none());
        assert_eq!(http.http_listen, Some("127.0.0.1:9091".parse().unwrap()));
    }
}
