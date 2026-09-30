use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
impl Collector {
    /// Serve authenticated framed MessagePack on the supplied TCP listener.
    ///
    /// Requires the `tcp` feature. Any bind address is accepted. TCP is
    /// plaintext; optionally deploy an authenticated TLS tunnel for encryption.
    ///
    /// Wire v1 begins with `MXS1`, a big-endian `u16` token length, and at most
    /// 4096 token bytes. The server responds with `MXS1` on acceptance. Each
    /// subsequent request and acknowledgment is a big-endian `u32` byte length
    /// followed by the same MessagePack payload used by HTTP. Requests are
    /// sequential per connection; lengths are checked before allocation.
    ///
    /// When `shutdown` resolves, stop accepting connections and wait up to the
    /// configured request timeout for existing tasks. Accepted batches continue
    /// processing; call [`Collector::shutdown`] to drain them. Returns an I/O
    /// error for a listener failure. This transport has no HTTP health or
    /// diagnostics endpoints; use [`Collector::diagnostics`] when embedding it.
    ///
    /// ```no_run
    /// # #[cfg(feature = "tcp")]
    /// # async fn run(collector: metrics_summary_collector::Collector)
    /// #     -> Result<(), Box<dyn std::error::Error>> {
    /// use std::time::{Duration, Instant};
    /// use tokio::net::TcpListener;
    ///
    /// // Construct `collector` with a token when authentication is needed.
    /// let listener = TcpListener::bind("0.0.0.0:9092").await?;
    /// collector.serve_tcp(listener, async {
    ///     tokio::signal::ctrl_c().await.expect("install Ctrl-C handler");
    /// }).await?;
    /// let report = collector.shutdown(Instant::now() + Duration::from_secs(30)).await;
    /// if !report.drained || report.dropped_after_acceptance != 0 {
    ///     return Err("collector shutdown did not complete delivery".into());
    /// }
    /// # Ok(())
    /// # }
    /// # fn main() {}
    /// ```
    #[cfg_attr(docsrs, doc(cfg(feature = "tcp")))]
    pub async fn serve_tcp(
        &self,
        listener: TcpListener,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> std::io::Result<()> {
        let mut tasks = JoinSet::new();
        let (close_tx, close_rx) = watch::channel(false);
        tokio::pin!(shutdown);
        loop {
            // Return permits as task outputs, retaining them until completion
            // is reaped; the explicit length guard also bounds panicked tasks.
            while tasks.try_join_next().is_some() {}
            tokio::select! {
                _=&mut shutdown=>break,
                Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
                connection=listener.accept(),if tasks.len()<self.config().max_connections=>{
                    let(socket,_)=connection?;
                    socket.set_nodelay(true)?;
                    let Ok(permit)=self.shared().connections.clone().try_acquire_owned() else {drop(socket);continue;};
                    let collector=self.clone();let close_rx=close_rx.clone();tasks.spawn(async move {let _=handle(collector,socket,close_rx).await;permit});
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
async fn handle(
    collector: Collector,
    mut socket: TcpStream,
    mut close_rx: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let timeout = Duration::from_millis(collector.config().request_timeout_ms);
    tokio::time::timeout(timeout, async {
        let mut magic = [0; 4];
        socket.read_exact(&mut magic).await?;
        let len = socket.read_u16().await? as usize;
        if magic != *metrics_summary_protocol::TCP_MAGIC || len > 4096 {
            socket.write_all(b"NOPE").await?;
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "handshake rejected",
            ));
        }
        let mut token = vec![0; len];
        socket.read_exact(&mut token).await?;
        if !collector.authenticate(std::str::from_utf8(&token).ok()) {
            socket.write_all(b"NOPE").await?;
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "authentication failed",
            ));
        }
        socket.write_all(metrics_summary_protocol::TCP_MAGIC).await
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "handshake deadline"))??;
    loop {
        let deadline = Instant::now() + timeout;
        if *close_rx.borrow() {
            return Ok(());
        }
        let length = tokio::select! {
            _=close_rx.changed()=>return Ok(()),
            result=tokio::time::timeout_at(tokio::time::Instant::from_std(deadline),socket.read_u32())=>result.map_err(|_|std::io::Error::new(std::io::ErrorKind::TimedOut,"frame deadline"))?? as usize,
        };
        if length > collector.config().max_encoded_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "frame too large",
            ));
        }
        let _permit = collector
            .shared()
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::WouldBlock, "request budget exhausted")
            })?;
        let mut body = vec![0; length];
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            socket.read_exact(&mut body),
        )
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "frame deadline"))??;
        let decoded =
            metrics_summary_protocol::decode_request(&body, &collector.config().protocol_limits());
        drop(body);
        let ack = match decoded {
            Ok((batch, policy)) => collector.submit(batch, policy, deadline).await,
            Err(e) => collector.reject(None, AckPolicy::Enqueued, Status::Invalid, &e.to_string()),
        };
        let bytes = metrics_summary_protocol::encode_ack(&ack).map_err(|error| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
        })?;
        tokio::time::timeout(
            Duration::from_millis(collector.config().request_timeout_ms),
            async {
                socket.write_u32(bytes.len() as u32).await?;
                socket.write_all(&bytes).await
            },
        )
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "ACK deadline"))??;
    }
}
