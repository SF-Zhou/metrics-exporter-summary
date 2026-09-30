//! METRICS_COLLECTOR_URL=http://collector.example/v1/batches
//! Use https:// for TLS, or tcp://10.0.0.8:9092 with the tcp feature for direct TCP.
//! METRICS_COLLECTOR_TOKEN is read from the environment and never logged.
use metrics_exporter_summary::{metrics, Builder, Config};
use metrics_summary_sink_remote::{AckPolicy, Endpoint, RemoteConfig, RemoteSink};
use std::time::Duration;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("METRICS_COLLECTOR_URL")?;
    #[cfg(feature = "tcp")]
    let endpoint = if let Some(address) = address.strip_prefix("tcp://") {
        Endpoint::Tcp(address.parse()?)
    } else {
        Endpoint::Http(address)
    };
    #[cfg(not(feature = "tcp"))]
    let endpoint = Endpoint::Http(address);
    let mut remote = RemoteConfig::new(endpoint);
    remote.bearer_token = Some(std::env::var("METRICS_COLLECTOR_TOKEN")?);
    remote.tls_ca_pem = std::env::var_os("METRICS_COLLECTOR_CA_FILE")
        .map(std::fs::read)
        .transpose()?;
    remote.ack_policy = match std::env::var("METRICS_ACK_POLICY").as_deref() {
        Ok("confirmed") => AckPolicy::ClickHouseConfirmed,
        Ok("enqueued") | Err(_) => AckPolicy::Enqueued,
        Ok(_) => return Err("METRICS_ACK_POLICY must be enqueued or confirmed".into()),
    };
    let sink = RemoteSink::new(remote)?;
    let config = Config {
        collect_interval: None,
        ..Default::default()
    };
    let (recorder, control) =
        Builder::for_service("remote-example", format!("pid-{}", std::process::id()))?
            .config(config)
            .build(sink)?;
    recorder.install()?;
    metrics::describe_histogram!(
        "request.latency_ns",
        metrics::Unit::Nanoseconds,
        "Request latency in nanoseconds"
    );
    let latency = metrics::histogram!("request.latency_ns", "tag" => "example");
    let requests = metrics::counter!("requests.total");
    let inflight = metrics::gauge!("requests.inflight");
    inflight.set(1.0);
    latency.record(12_000_000.0);
    requests.increment(1);
    inflight.set(0.0);
    let report = control.shutdown(Duration::from_secs(30))?;
    println!("summary shutdown: {report:?}");
    if !report.is_success() {
        return Err(
            "summary shutdown did not complete delivery; inspect the shutdown report".into(),
        );
    }
    Ok(())
}
