use super::*;
use axum::{
    body::{to_bytes, Body},
    extract::{Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use tokio::{net::TcpListener, task::JoinSet};
impl Collector {
    /// Serve MessagePack HTTP ingestion and operational endpoints on the supplied listener.
    ///
    /// Requires the `http` feature. Any bind address is accepted. The listener
    /// serves plaintext HTTP; optionally deploy an HTTPS proxy for TLS.
    /// Requests authenticate using the token supplied to [`Collector::new`],
    /// when configured.
    ///
    /// Routes are `POST /v1/batches`, authenticated `GET /diagnostics`, and
    /// unauthenticated `GET /healthz` and `GET /readyz`. Ingestion accepts
    /// uncompressed `application/msgpack` and an `Authorization: Bearer ...`
    /// header. Successful Enqueued acknowledgment uses HTTP 202; storage
    /// confirmation uses HTTP 200. Readiness reports admission capacity, not
    /// database health or durability.
    ///
    /// When `shutdown` resolves, stop accepting connections and allow active
    /// requests up to the configured request timeout before aborting them.
    /// Accepted storage work continues independently; call [`Collector::shutdown`]
    /// afterward to drain it. Returns an I/O error for a listener failure;
    /// individual connection errors do not stop the server.
    #[cfg_attr(docsrs, doc(cfg(feature = "http")))]
    pub async fn serve_http(
        &self,
        listener: TcpListener,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> std::io::Result<()> {
        let app = Router::new()
            .route("/v1/batches", post(ingest))
            .route("/healthz", get(health))
            .route("/readyz", get(ready))
            .route("/diagnostics", get(diagnostics))
            .with_state(self.clone());
        let mut tasks = JoinSet::new();
        let (close_tx, close_rx) = watch::channel(false);
        tokio::pin!(shutdown);
        loop {
            // Completed tasks keep their connection permit until reaped. Reap
            // before accepting so churn cannot build a hidden completed queue.
            while tasks.try_join_next().is_some() {}
            tokio::select! {
                _=&mut shutdown=>break,
                Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
                connection=listener.accept(),if tasks.len()<self.config().max_connections=>{
                    let(socket,_)=connection?;
                    let Ok(permit)=self.shared().connections.clone().try_acquire_owned() else {drop(socket);continue;};
                    let service=TowerToHyperService::new(app.clone());let mut close_rx=close_rx.clone();let timeout=Duration::from_millis(self.config().request_timeout_ms);
                    tasks.spawn(async move {
                        let mut builder=hyper::server::conn::http1::Builder::new();
                        builder.timer(TokioTimer::new()).header_read_timeout(timeout).max_headers(32).max_buf_size(16*1024);
                        let connection=builder.serve_connection(TokioIo::new(socket),service);
                        tokio::pin!(connection);
                        tokio::select! {
                            _=&mut connection=>{},
                            _=close_rx.changed()=>{connection.as_mut().graceful_shutdown();let _=tokio::time::timeout(timeout,connection).await;}
                        }
                        permit
                    });
                }
            }
        }
        close_tx.send_replace(true);
        let wait = async { while tasks.join_next().await.is_some() {} };
        let _ = tokio::time::timeout(
            Duration::from_millis(self.config().request_timeout_ms),
            wait,
        )
        .await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Ok(())
    }
}
fn authorized(collector: &Collector, request: &Request) -> bool {
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    collector.authenticate(token)
}
fn reply(ack: metrics_summary_protocol::wire::Ack) -> Response {
    let status = match Status::try_from(ack.status).unwrap_or(Status::Invalid) {
        Status::Ok => {
            if ack.ack_policy == AckPolicy::Enqueued as i32 {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            }
        }
        Status::Overloaded => StatusCode::TOO_MANY_REQUESTS,
        Status::Unauthorized => StatusCode::UNAUTHORIZED,
        Status::Invalid => StatusCode::BAD_REQUEST,
        Status::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        Status::Unknown => StatusCode::GATEWAY_TIMEOUT,
    };
    match metrics_summary_protocol::encode_ack(&ack) {
        Ok(bytes) => (
            status,
            [(header::CONTENT_TYPE, metrics_summary_protocol::CONTENT_TYPE)],
            bytes,
        )
            .into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "ACK encoding failed").into_response(),
    }
}
async fn ingest(State(collector): State<Collector>, request: Request) -> Response {
    if !authorized(&collector, &request) {
        return reply(collector.reject(
            None,
            AckPolicy::Enqueued,
            Status::Unauthorized,
            "authentication required",
        ));
    }
    let Ok(_permit) = collector.shared().requests.clone().try_acquire_owned() else {
        return reply(collector.reject(
            None,
            AckPolicy::Enqueued,
            Status::Overloaded,
            "too many requests",
        ));
    };
    if collector.is_closing() {
        return reply(collector.reject(
            None,
            AckPolicy::Enqueued,
            Status::Unavailable,
            "collector closing",
        ));
    }
    if request
        .headers()
        .get(header::CONTENT_ENCODING)
        .is_some_and(|h| h != "identity")
    {
        return reply(collector.reject(
            None,
            AckPolicy::Enqueued,
            Status::Invalid,
            "compression is not supported in wire v1",
        ));
    }
    if request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .and_then(|v| v.split(';').next())
        != Some(metrics_summary_protocol::CONTENT_TYPE)
    {
        return reply(collector.reject(
            None,
            AckPolicy::Enqueued,
            Status::Invalid,
            "application/msgpack required",
        ));
    }
    let deadline = Instant::now() + Duration::from_millis(collector.config().request_timeout_ms);
    let body = match tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        to_bytes(request.into_body(), collector.config().max_encoded_bytes),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => {
            return (StatusCode::PAYLOAD_TOO_LARGE, "encoded body limit exceeded").into_response()
        }
        Err(_) => return (StatusCode::REQUEST_TIMEOUT, "body receive timeout").into_response(),
    };
    let decoded =
        metrics_summary_protocol::decode_request(&body, &collector.config().protocol_limits());
    drop(body);
    let (batch, policy) = match decoded {
        Ok(v) => v,
        Err(e) => {
            return reply(collector.reject(
                None,
                AckPolicy::Enqueued,
                Status::Invalid,
                &e.to_string(),
            ))
        }
    };
    reply(collector.submit(batch, policy, deadline).await)
}
async fn health() -> Response {
    (StatusCode::OK, "ok").into_response()
}
async fn ready(State(collector): State<Collector>) -> Response {
    let state = collector.shared().state.lock().unwrap();
    if state.closing
        || state.pending_batches >= collector.config().max_pending_batches
        || state.pending_bytes >= collector.config().max_pending_bytes
    {
        (StatusCode::SERVICE_UNAVAILABLE, "closing or overloaded").into_response()
    } else {
        (StatusCode::OK, "ready").into_response()
    }
}
async fn diagnostics(State(collector): State<Collector>, request: Request) -> Response {
    if !authorized(&collector, &request) {
        return (StatusCode::UNAUTHORIZED, "authentication required").into_response();
    }
    let Ok(_permit) = collector.shared().requests.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    (
        [(header::CONTENT_TYPE, "application/json")],
        Body::from(serde_json::to_vec(&collector.diagnostics()).expect("diagnostic serialization")),
    )
        .into_response()
}
