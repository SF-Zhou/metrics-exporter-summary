//! A runnable HTTP service with periodic metric collection and graceful shutdown.
//!
//! cargo run -p metrics-exporter-summary --example server
//! curl http://127.0.0.1:3000/work
//! curl http://127.0.0.1:3000/metrics     # latest JSON snapshot; wait for collection
//! curl http://127.0.0.1:3000/diagnostics
//!
//! Optional environment: SERVER_ADDR, METRICS_INSTANCE, POD_NAME.
//! MemorySink makes the example self-contained. A service exporting to a collector
//! can pass a configured RemoteSink to the same Builder instead.

use axum::{
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use metrics_exporter_summary::{metrics, Builder, Config, Control, ValidationLimits};
use metrics_summary_sink_memory::{MemorySink, Retention, SnapshotReader};
use std::{
    error::Error,
    net::SocketAddr,
    time::{Duration, Instant},
};

#[derive(Clone)]
struct RequestMetrics {
    requests: metrics::Counter,
    in_flight: metrics::Gauge,
    duration: metrics::Histogram,
}

impl RequestMetrics {
    fn register(pod: &str) -> Self {
        metrics::describe_counter!(
            "server.requests.total",
            metrics::Unit::Count,
            "Requests admitted to the work handler during this collection window"
        );
        metrics::describe_gauge!(
            "server.requests.in_flight",
            metrics::Unit::Count,
            "Currently running work handlers"
        );
        metrics::describe_histogram!(
            "server.request.duration",
            metrics::Unit::Nanoseconds,
            "Work handler lifetime, including cancelled handlers"
        );

        // Register after installing the recorder, then clone/reuse these handles.
        // Use bounded route names, not request URLs, request IDs or user IDs.
        // host/instance inherit Source; uid defaults to an empty string.
        let labels = [("pod", pod.to_owned()), ("tag", "work".to_owned())];
        let handles = Self {
            requests: metrics::counter!("server.requests.total", &labels),
            in_flight: metrics::gauge!("server.requests.in_flight", &labels),
            duration: metrics::histogram!("server.request.duration", &labels),
        };
        handles.in_flight.set(0.0);
        handles
    }

    fn begin(&self) -> InFlightRequest {
        // Counter increments are exported once, in the next collection window.
        // The gauge retains its state even when this request spans collections.
        self.requests.increment(1);
        self.in_flight.increment(1.0);
        InFlightRequest {
            metrics: self.clone(),
            started: Instant::now(),
        }
    }
}

// A guard also finishes instrumentation if the handler future is cancelled.
struct InFlightRequest {
    metrics: RequestMetrics,
    started: Instant,
}

impl Drop for InFlightRequest {
    fn drop(&mut self) {
        self.metrics
            .duration
            .record(self.started.elapsed().as_nanos() as f64);
        self.metrics.in_flight.decrement(1.0);
    }
}

#[derive(Clone)]
struct AppState {
    metrics: RequestMetrics,
    snapshots: SnapshotReader,
    control: Control,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    let address: SocketAddr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3000".to_owned())
        .parse()?;
    let instance = std::env::var("METRICS_INSTANCE")
        .unwrap_or_else(|_| format!("server-{}", std::process::id()));
    let pod = std::env::var("POD_NAME").unwrap_or_default();
    let listener = tokio::net::TcpListener::bind(address).await?;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    let config = Config {
        // Two seconds makes the demo easy to observe; the library default is 10s.
        collect_interval: Some(Duration::from_secs(2)),
        max_series: 1_000,
        // Capacity for up to 200 recording threads × 1,000 histogram series.
        // This is an admission limit; the example itself uses four Tokio workers.
        max_shards: 200_000,
        validation: ValidationLimits {
            max_rows: 1_000,
            ..ValidationLimits::default()
        },
        // Queued and in-flight batches share both limits.
        queue_max_batches: 8,
        queue_max_bytes: 64 * 1024 * 1024,
        shutdown_timeout: Duration::from_secs(35),
        ..Config::default()
    };
    // Reject invalid deployment labels at startup instead of getting no-op handles.
    if pod.len() > config.validation.max_label_value_bytes {
        return Err("POD_NAME exceeds the metric label byte limit".into());
    }
    let (sink, snapshots) = MemorySink::with_validation_limits(
        Retention {
            max_snapshots: 60,
            max_retained_bytes: 64 * 1024 * 1024,
        },
        config.validation.clone(),
    )?;
    let (recorder, control) = Builder::for_service("server-example", instance)?
        .config(config)
        .build(sink)?;

    // Install once at process startup, before registering handles or serving work.
    // Keep Control until every task/thread that records metrics has stopped.
    recorder.install()?;
    let state = AppState {
        metrics: RequestMetrics::register(&pod),
        snapshots: snapshots.clone(),
        control: control.clone(),
    };
    let app = Router::new()
        .route("/work", get(work))
        .route("/metrics", get(latest_metrics))
        .route("/diagnostics", get(diagnostics))
        .with_state(state);

    let shutdown_signal = async move {
        let interrupt = async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("cannot wait for Ctrl-C: {error}");
            }
        };
        #[cfg(unix)]
        tokio::select! {
            () = interrupt => {},
            _ = terminate.recv() => {},
        }
        #[cfg(not(unix))]
        interrupt.await;
        eprintln!("stopping HTTP admission; waiting for active requests");
    };

    println!("listening on http://{}", listener.local_addr()?);
    let server_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await;

    // The server has drained its handlers. Stop/join any other producer tasks here.
    // shutdown collects the final window and waits for the writer, so a separate
    // flush is unnecessary. Control waits are blocking: keep them off Tokio workers.
    let shutdown_control = control.clone();
    let report = tokio::task::spawn_blocking(move || shutdown_control.shutdown_default()).await??;
    eprintln!("metrics shutdown: {report:?}");
    eprintln!("metrics diagnostics: {:?}", control.diagnostics());
    if let Some(error) = control.last_write_error() {
        eprintln!("last metrics write error: {error:?}");
    }
    if !report.is_success() {
        return Err("metrics shutdown completed with delivery losses".into());
    }
    // MemorySink confirms LocalPublished. RemoteSink's configured ACK policy
    // determines whether the same report confirms collector acceptance or storage.
    println!(
        "final snapshot: {}",
        serde_json::to_string(snapshots.get(report.target)?.as_ref())?
    );
    server_result?;
    Ok(())
}

async fn work(State(state): State<AppState>) -> &'static str {
    let _request = state.metrics.begin();
    // Replace this with the application's asynchronous work.
    tokio::time::sleep(Duration::from_millis(50)).await;
    "ok\n"
}

async fn latest_metrics(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Reading a snapshot never flushes or changes collection boundaries.
    let batch = state.snapshots.latest().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "No snapshot yet; wait for the first collection.\n".to_owned(),
        )
    })?;
    let json = serde_json::to_string(batch.as_ref())
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(([(header::CONTENT_TYPE, "application/json")], json))
}

async fn diagnostics(State(state): State<AppState>) -> String {
    // Inspect exporter health out of band; these endpoints do not record metrics.
    format!(
        "{:#?}\nlast_write_error: {:?}\n",
        state.control.diagnostics(),
        state.control.last_write_error()
    )
}
