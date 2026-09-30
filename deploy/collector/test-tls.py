#!/usr/bin/env python3
"""Validate the shipped HAProxy templates with temporary certificates and sockets.

Usage: python3 deploy/collector/test-tls.py /absolute/path/to/haproxy
Only loopback listeners are started. No system certificate store is modified.
The echo backends validate transport headers and opaque byte preservation only;
payloads are deliberately not application messages or collector ACKs.
"""
import contextlib
import http.client
import http.server
import pathlib
import socket
import socketserver
import ssl
import subprocess
import sys
import tempfile
import threading
import time


class HttpBackend(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        if self.headers.get("Authorization") != "Bearer tls-smoke-only":
            self.send_error(401)
            return
        if self.headers.get("Content-Type") != "application/msgpack":
            self.send_error(415)
            return
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.send_response(200)
        self.send_header("Content-Type", "application/msgpack")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


class TcpBackend(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(3)
        try:
            while True:
                data = self.request.recv(65536)
                if not data:
                    return
                self.request.sendall(data)
        except (OSError, TimeoutError):
            return


class ThreadedTcp(socketserver.ThreadingTCPServer):
    daemon_threads = True


def unused_port():
    with contextlib.closing(socket.socket()) as stream:
        stream.bind(("127.0.0.1", 0))
        return stream.getsockname()[1]


def openssl(*args):
    subprocess.run(["openssl", *map(str, args)], check=True, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)


def receive(stream, length):
    body = bytearray()
    while len(body) < length:
        chunk = stream.recv(length - len(body))
        if not chunk:
            raise AssertionError("unexpected EOF")
        body.extend(chunk)
    return bytes(body)


def main():
    binary = sys.argv[1]
    templates = pathlib.Path(__file__).resolve().parent
    with tempfile.TemporaryDirectory(prefix="metrics-tls-smoke-") as temporary:
        directory = pathlib.Path(temporary)
        ca = directory / "ca.crt"
        ca_key = directory / "ca.key"
        openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                "-subj", "/CN=metrics-test-ca", "-keyout", ca_key, "-out", ca)
        for name, usage in [("collector", "serverAuth"), ("client", "clientAuth")]:
            key, csr, cert = [directory / (name + suffix) for suffix in [".key", ".csr", ".crt"]]
            openssl("req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj",
                    "/CN=collector.example.internal" if name == "collector" else "/CN=metrics-test-client",
                    "-keyout", key, "-out", csr)
            extension = directory / (name + ".ext")
            extension.write_text("basicConstraints=CA:FALSE\nextendedKeyUsage=" + usage +
                                 "\nsubjectAltName=DNS:collector.example.internal,IP:127.0.0.1\n")
            openssl("x509", "-req", "-in", csr, "-CA", ca, "-CAkey", ca_key,
                    "-CAcreateserial", "-days", "1", "-extfile", extension, "-out", cert)
            pem = directory / (name + ".pem")
            pem.write_bytes(cert.read_bytes() + key.read_bytes())
            pem.chmod(0o600)
        http_backend = http.server.ThreadingHTTPServer(("127.0.0.1", 0), HttpBackend)
        tcp_backend = ThreadedTcp(("127.0.0.1", 0), TcpBackend)
        for server in [http_backend, tcp_backend]:
            threading.Thread(target=server.serve_forever, daemon=True).start()
        https_port, tls_tcp_port, local_tcp_port = unused_port(), unused_port(), unused_port()
        common = {"/etc/haproxy/tls/collector.pem": str(directory / "collector.pem"),
                  "/etc/haproxy/tls/clients-ca.pem": str(ca),
                  "/etc/haproxy/tls/client.pem": str(directory / "client.pem"),
                  "/etc/haproxy/tls/collector-ca.pem": str(ca)}
        server_text = (templates / "haproxy-server.cfg").read_text()
        server_text = server_text.replace("bind :9443", "bind 127.0.0.1:" + str(https_port))
        server_text = server_text.replace("bind :9444", "bind 127.0.0.1:" + str(tls_tcp_port))
        server_text = server_text.replace("127.0.0.1:9091", "127.0.0.1:" + str(http_backend.server_port))
        server_text = server_text.replace("127.0.0.1:9092", "127.0.0.1:" + str(tcp_backend.server_address[1]))
        client_text = (templates / "haproxy-tcp-client.cfg").read_text()
        client_text = client_text.replace("127.0.0.1:19092", "127.0.0.1:" + str(local_tcp_port))
        client_text = client_text.replace("collector.example.internal:9444", "127.0.0.1:" + str(tls_tcp_port))
        processes = []
        try:
            for name, text in [("server", server_text), ("client", client_text)]:
                for source, destination in common.items():
                    text = text.replace(source, destination)
                config = directory / (name + ".cfg")
                config.write_text(text)
                subprocess.run([binary, "-c", "-f", str(config)], check=True)
                output = open(directory / (name + ".log"), "w")
                processes.append((subprocess.Popen([binary, "-db", "-f", str(config)], stdout=output, stderr=output), output))
            end = time.monotonic() + 10
            while True:
                try:
                    with socket.create_connection(("127.0.0.1", local_tcp_port), timeout=1):
                        break
                except OSError:
                    if time.monotonic() >= end:
                        raise
                    time.sleep(0.05)
            context = ssl.create_default_context(cafile=str(ca))
            client = http.client.HTTPSConnection("127.0.0.1", https_port, context=context, timeout=3)
            # Deliberately opaque bytes: this checks the proxy, not the wire codec.
            payload = b"tls-smoke:opaque\x00\xff\r\n"
            client.request("POST", "/v1/batches", body=payload,
                           headers={"Authorization": "Bearer tls-smoke-only", "Content-Type": "application/msgpack"})
            response = client.getresponse()
            assert response.status == 200 and response.read() == payload
            assert response.getheader("Content-Type") == "application/msgpack"
            client.close()
            client = http.client.HTTPSConnection("127.0.0.1", https_port, context=context, timeout=3)
            client.request("POST", "/v1/batches", body=b"", headers={"Content-Length": "8388609"})
            response = client.getresponse()
            assert response.status == 413
            response.read()
            client.close()
            # The sender's loopback tunnel verifies server identity and presents a
            # client certificate. Multiple opaque payloads retain their exact bytes.
            with socket.create_connection(("127.0.0.1", local_tcp_port), timeout=3) as stream:
                for frame in [payload, bytes(range(256)) * 4]:
                    stream.sendall(frame)
                    assert receive(stream, len(frame)) == frame
            # The TLS endpoint must reject clients without the required certificate.
            rejected = False
            try:
                with context.wrap_socket(socket.create_connection(("127.0.0.1", tls_tcp_port), timeout=3),
                                         server_hostname="collector.example.internal") as stream:
                    stream.sendall(payload)
                    rejected = not stream.recv(1)
            except ssl.SSLError:
                rejected = True
            assert rejected, "mTLS endpoint accepted a client without a certificate"
            print("HAProxy templates verified: trusted HTTPS, bearer/MessagePack content-type forwarding, body limit, opaque bytes through mTLS, missing-certificate rejection")
        finally:
            for process, output in reversed(processes):
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                output.close()
            for server in [http_backend, tcp_backend]:
                server.shutdown()
                server.server_close()


if __name__ == "__main__":
    main()
