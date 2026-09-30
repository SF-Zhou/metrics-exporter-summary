//! Optional actual TLS layer for the real end-to-end test.
use std::{
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

pub struct Proxy {
    child: Child,
    directory: PathBuf,
    pub http: String,
    pub tcp: SocketAddr,
}
fn port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
impl Proxy {
    pub fn optional(http: &str, tcp: SocketAddr) -> Option<Self> {
        let binary = std::env::var_os("METRICS_TEST_HAPROXY_BIN")?;
        let server_pem = std::env::var("METRICS_TEST_TLS_SERVER_PEM").unwrap();
        let client_pem = std::env::var("METRICS_TEST_TLS_CLIENT_PEM").unwrap();
        let ca = std::env::var("METRICS_COLLECTOR_CA_FILE").unwrap();
        let https_port = port();
        let tcp_tls_port = port();
        let local_tcp_port = port();
        let address = reqwest::Url::parse(http)
            .unwrap()
            .socket_addrs(|| None)
            .unwrap()[0];
        let directory =
            std::env::temp_dir().join(format!("metrics-rust-tls-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let config = directory.join("haproxy.cfg");
        std::fs::write(&config, format!("global\n    maxconn 64\n    nbthread 1\ndefaults\n    timeout connect 3s\n    timeout client 15s\n    timeout server 15s\nfrontend https\n    mode http\n    bind 127.0.0.1:{https_port} ssl crt {server_pem}\n    default_backend plain_http\nbackend plain_http\n    mode http\n    server collector {address}\nfrontend tls_tcp\n    mode tcp\n    bind 127.0.0.1:{tcp_tls_port} ssl crt {server_pem} ca-file {ca} verify required\n    default_backend plain_tcp\nbackend plain_tcp\n    mode tcp\n    server collector {tcp}\nfrontend client_tunnel\n    mode tcp\n    bind 127.0.0.1:{local_tcp_port}\n    default_backend verified_tls\nbackend verified_tls\n    mode tcp\n    server collector 127.0.0.1:{tcp_tls_port} ssl crt {client_pem} ca-file {ca} verify required verifyhost localhost sni str(localhost)\n")).unwrap();
        let child = Command::new(binary)
            .args(["-db", "-f"])
            .arg(config)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let proxy = Self {
            child,
            directory,
            http: format!("https://localhost:{https_port}/v1/batches"),
            tcp: ([127, 0, 0, 1], local_tcp_port).into(),
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while TcpStream::connect_timeout(&proxy.tcp, Duration::from_millis(50)).is_err() {
            assert!(Instant::now() < deadline, "TLS proxy failed to start");
            std::thread::sleep(Duration::from_millis(20));
        }
        Some(proxy)
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
