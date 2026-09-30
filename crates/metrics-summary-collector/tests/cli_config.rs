use std::{
    path::PathBuf,
    process::{Command, Output},
};
use uuid::Uuid;

struct ConfigFile(PathBuf);
impl ConfigFile {
    fn new(listeners: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("metrics-collector-config-{}.toml", Uuid::new_v4()));
        std::fs::write(&path, listeners).unwrap();
        Self(path)
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_metrics-summary-collector"));
        command
            .arg("--config")
            .arg(&self.0)
            .env_remove("CLICKHOUSE_PASSWORD")
            .env_remove("METRICS_COLLECTOR_TOKEN");
        command
    }
}
impl Drop for ConfigFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn check_config(listeners: &str, expected_error: Option<&str>) {
    let file = ConfigFile::new(listeners);
    let Output {
        status,
        stdout,
        stderr,
    } = file.command().arg("--check-config").output().unwrap();
    let stderr = String::from_utf8(stderr).unwrap();
    if let Some(expected) = expected_error {
        assert!(
            !status.success(),
            "unsupported configuration was accepted: {listeners}"
        );
        assert!(stderr.contains(expected), "unexpected error: {stderr}");
    } else {
        assert!(
            status.success(),
            "configuration rejected ({listeners}): {stderr}"
        );
        assert!(String::from_utf8(stdout)
            .unwrap()
            .contains("configuration valid"));
    }
}

#[test]
fn http_listener_addresses_require_only_http_feature() {
    for address in [
        "127.0.0.1:9091",
        "0.0.0.0:9091",
        "10.20.30.40:9091",
        "[::]:9091",
        "[fd00::1]:9091",
    ] {
        check_config(
            &format!("http_listen = \"{address}\""),
            (!cfg!(feature = "http")).then_some("rebuild with feature http for HTTP listener"),
        );
    }
}

#[test]
fn tcp_listener_addresses_require_only_tcp_feature() {
    for address in [
        "127.0.0.1:9092",
        "0.0.0.0:9092",
        "10.20.30.40:9092",
        "[::]:9092",
        "[fd00::1]:9092",
    ] {
        check_config(
            &format!("tcp_listen = \"{address}\""),
            (!cfg!(feature = "tcp")).then_some("rebuild with feature tcp for TCP listener"),
        );
    }
}

#[test]
fn both_listeners_require_both_features() {
    let error = if !cfg!(feature = "http") {
        Some("rebuild with feature http for HTTP listener")
    } else if !cfg!(feature = "tcp") {
        Some("rebuild with feature tcp for TCP listener")
    } else {
        None
    };
    check_config(
        "http_listen = \"127.0.0.1:9091\"\ntcp_listen = \"127.0.0.1:9092\"",
        error,
    );
}

#[test]
fn missing_listeners_are_rejected() {
    check_config("", Some("at least one listener must be configured"));
}

#[cfg(all(feature = "tcp", unix))]
#[test]
fn tcp_only_service_starts_authenticates_and_shuts_down() {
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        process::{Child, Stdio},
        thread,
        time::{Duration, Instant},
    };

    struct Process(Child);
    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    let file = ConfigFile::new(&format!("tcp_listen = \"{address}\""));
    drop(reservation);
    let token = "config-test-token";
    let mut process = Process(
        file.command()
            .env("METRICS_COLLECTOR_TOKEN", token)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut handshake = metrics_summary_protocol::TCP_MAGIC.to_vec();
    handshake.extend_from_slice(&(token.len() as u16).to_be_bytes());
    handshake.extend_from_slice(token.as_bytes());
    let socket = loop {
        let connected = (|| -> std::io::Result<TcpStream> {
            let mut socket = TcpStream::connect_timeout(&address, Duration::from_millis(100))?;
            socket.set_read_timeout(Some(Duration::from_millis(100)))?;
            socket.set_write_timeout(Some(Duration::from_millis(100)))?;
            socket.write_all(&handshake)?;
            let mut hello = [0; 4];
            socket.read_exact(&mut hello)?;
            assert_eq!(&hello, metrics_summary_protocol::TCP_MAGIC);
            Ok(socket)
        })();
        match connected {
            Ok(socket) => break socket,
            Err(error) => assert!(
                Instant::now() < deadline,
                "TCP listener did not become ready: {error}"
            ),
        }
        assert!(process.0.try_wait().unwrap().is_none(), "service exited");
        thread::sleep(Duration::from_millis(10));
    };
    drop(socket);
    assert!(Command::new("kill")
        .args(["-TERM", &process.0.id().to_string()])
        .status()
        .unwrap()
        .success());
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success(), "service shutdown failed: {status}");
            break;
        }
        assert!(Instant::now() < deadline, "service did not shut down");
        thread::sleep(Duration::from_millis(10));
    }
}
