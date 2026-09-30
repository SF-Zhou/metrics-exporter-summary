#!/usr/bin/env python3
"""Run real Rust sinks with private-PKI HTTPS and mutual-TLS TCP tunnels.

Run inside run-local-tests.sh; pass a HAProxy 2.8+ binary as the only argument.
All CA keys, server/client certificates, proxies and databases are disposable.
"""
import contextlib
import os
import pathlib
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse


def openssl(*args):
    subprocess.run(["openssl", *map(str, args)], check=True, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)


def main():
    haproxy = str(pathlib.Path(sys.argv[1]).resolve())
    backend = urllib.parse.urlparse(os.environ["METRICS_CLICKHOUSE_URL"])
    assert backend.scheme == "http" and backend.hostname in ("127.0.0.1", "localhost")
    if not pathlib.Path("Cargo.lock").exists():
        subprocess.run(["cargo", "generate-lockfile"], check=True)
    with tempfile.TemporaryDirectory(prefix="metrics-rust-https-") as temporary:
        directory = pathlib.Path(temporary)
        ca, key = directory / "ca.crt", directory / "ca.key"
        openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                "-subj", "/CN=metrics-private-test-ca", "-keyout", key, "-out", ca)
        for name, usage in [("server", "serverAuth"), ("client", "clientAuth")]:
            cert_key, csr, cert = [directory / (name + suffix) for suffix in [".key", ".csr", ".crt"]]
            openssl("req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
                    "-keyout", cert_key, "-out", csr)
            extension = directory / (name + ".ext")
            # Deliberately no IP SAN: the tests verify that 127.0.0.1 is rejected.
            extension.write_text("basicConstraints=CA:FALSE\nextendedKeyUsage=" + usage +
                                 "\nsubjectAltName=DNS:localhost\n")
            openssl("x509", "-req", "-in", csr, "-CA", ca, "-CAkey", key,
                    "-CAcreateserial", "-days", "1", "-extfile", extension, "-out", cert)
            pem = directory / (name + ".pem")
            pem.write_bytes(cert.read_bytes() + cert_key.read_bytes())
            pem.chmod(0o600)
        with contextlib.closing(socket.socket()) as stream:
            stream.bind(("127.0.0.1", 0))
            port = stream.getsockname()[1]
        config = directory / "clickhouse-proxy.cfg"
        config.write_text(f"""global
    maxconn 64
    nbthread 1
defaults
    mode http
    timeout connect 3s
    timeout client 30s
    timeout server 30s
frontend clickhouse_https
    bind 127.0.0.1:{port} ssl crt {directory / 'server.pem'}
    default_backend clickhouse_plain
backend clickhouse_plain
    server clickhouse {backend.hostname}:{backend.port}
""")
        subprocess.run([haproxy, "-c", "-f", str(config)], check=True)
        process = subprocess.Popen([haproxy, "-db", "-f", str(config)], stdout=subprocess.DEVNULL)
        try:
            end = time.monotonic() + 5
            while True:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    assert time.monotonic() < end, "HTTPS proxy failed to start"
                    time.sleep(0.02)
            environment = dict(os.environ, METRICS_CLICKHOUSE_URL=f"https://localhost:{port}",
                               METRICS_CLICKHOUSE_CA_FILE=str(ca), METRICS_COLLECTOR_CA_FILE=str(ca),
                               METRICS_TEST_TLS_SERVER_PEM=str(directory / "server.pem"),
                               METRICS_TEST_TLS_CLIENT_PEM=str(directory / "client.pem"),
                               METRICS_TEST_HAPROXY_BIN=haproxy, NO_PROXY="localhost,127.0.0.1")
            subprocess.run(["cargo", "test", "-p", "metrics-summary-sink-clickhouse", "--test",
                            "real_clickhouse", "--locked", "--", "--ignored", "--nocapture"],
                           check=True, env=environment)
            subprocess.run(["cargo", "test", "-p", "metrics-summary-collector", "--all-features",
                            "--test", "real_clickhouse", "--locked", "--", "--ignored", "--nocapture"],
                           check=True, env=environment)
            print("Actual Rust sinks verified private CA trust and hostname rejection: ClickHouse HTTPS, Remote HTTPS, mutual-TLS TCP tunnel")
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


if __name__ == "__main__":
    main()
