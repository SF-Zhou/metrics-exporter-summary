#![cfg(all(feature = "http", unix))]
use metrics_summary_core::{Batch, BatchId, Source};
use metrics_summary_protocol::{decode_ack, encode_request, AckPolicy, ProtocolLimits, Status};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};
use uuid::Uuid;
struct Process {
    child: Child,
    config: PathBuf,
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.config);
    }
}
fn launch(address: std::net::SocketAddr) -> Process {
    let path = std::env::temp_dir().join(format!("metrics-collector-{}.toml", Uuid::new_v4()));
    std::fs::write(&path,format!("http_listen = \"{address}\"\nshutdown_timeout_ms = 3000\n[collector]\ngroup_max_delay_ms = 5000\n[clickhouse]\nendpoint = \"http://127.0.0.1:1\"\n")).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_metrics-summary-collector"))
        .arg("--config")
        .arg(&path)
        .env("METRICS_COLLECTOR_TOKEN", "lifecycle-test-secret")
        .env_remove("CLICKHOUSE_PASSWORD")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    Process {
        child,
        config: path,
    }
}
async fn until(mut condition: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn sigterm_drains_confirmed_ack_and_exits_successfully() {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let mut process = launch(address);
    let client = reqwest::Client::new();
    let base = format!("http://{address}");
    until(async || {
        client
            .get(format!("{base}/readyz"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
    })
    .await;
    let batch = Batch {
        model_version: 1,
        id: BatchId {
            source_session_id: Uuid::new_v4(),
            sequence: 1,
        },
        source: Source {
            application: "lifecycle".into(),
            instance: "test".into(),
            hostname: "test-host".into(),
            attributes: BTreeMap::new(),
        },
        timestamp: 1_790_000_001_123_456_789,
        duration_ns: 1_000_000_000,
        rows: vec![],
    };
    let encoded = encode_request(
        &batch,
        AckPolicy::ClickHouseConfirmed,
        &ProtocolLimits::default(),
    )
    .unwrap();
    let response = tokio::spawn(
        client
            .post(format!("{base}/v1/batches"))
            .bearer_auth("lifecycle-test-secret")
            .header("content-type", metrics_summary_protocol::CONTENT_TYPE)
            .body(encoded)
            .send(),
    );
    until(async || {
        client
            .get(format!("{base}/diagnostics"))
            .bearer_auth("lifecycle-test-secret")
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()["accepted_batches"]
            == 1
    })
    .await;
    assert!(
        !response.is_finished(),
        "five-second group age has not elapsed"
    );
    assert!(Command::new("kill")
        .args(["-TERM", &process.child.id().to_string()])
        .status()
        .unwrap()
        .success());
    let response = tokio::time::timeout(Duration::from_secs(3), response)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    let ack = decode_ack(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(ack.status, Status::Ok as i32);
    metrics_summary_protocol::validate_ack(&ack, batch.id, AckPolicy::ClickHouseConfirmed).unwrap();
    until(async || process.child.try_wait().unwrap().is_some()).await;
    assert!(process.child.try_wait().unwrap().unwrap().success());
}
#[tokio::test]
async fn listener_bind_failure_exits_nonzero() {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut process = launch(reservation.local_addr().unwrap());
    until(async || process.child.try_wait().unwrap().is_some()).await;
    assert!(!process.child.try_wait().unwrap().unwrap().success());
}
