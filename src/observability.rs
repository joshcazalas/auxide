use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
    time,
};
use tokio_util::sync::CancellationToken;

const MAX_REQUEST_BYTES: usize = 4 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_CONNECTIONS: usize = 32;

#[derive(Clone, Debug, Default)]
pub struct ObservabilityState {
    inner: Arc<StateInner>,
}

#[derive(Debug, Default)]
struct StateInner {
    ready: AtomicBool,
    discord_connected: AtomicBool,
    guild_players: AtomicU64,
    interactions: AtomicU64,
    interaction_errors: AtomicU64,
    source_resolutions: AtomicU64,
    source_resolution_errors: AtomicU64,
}

impl ObservabilityState {
    pub fn set_ready(&self, ready: bool) {
        self.inner.ready.store(ready, Ordering::Relaxed);
    }

    pub fn set_discord_connected(&self, connected: bool) {
        self.inner
            .discord_connected
            .store(connected, Ordering::Relaxed);
    }

    pub fn set_guild_players(&self, players: u64) {
        self.inner.guild_players.store(players, Ordering::Relaxed);
    }

    pub fn record_interaction(&self, succeeded: bool) {
        self.inner.interactions.fetch_add(1, Ordering::Relaxed);
        if !succeeded {
            self.inner
                .interaction_errors
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_source_resolution(&self, succeeded: bool) {
        self.inner
            .source_resolutions
            .fetch_add(1, Ordering::Relaxed);
        if !succeeded {
            self.inner
                .source_resolution_errors
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.inner.ready.load(Ordering::Relaxed)
    }

    fn metrics(&self) -> String {
        format!(
            concat!(
                "# HELP auxide_ready Whether the bot can accept commands.\n",
                "# TYPE auxide_ready gauge\n",
                "auxide_ready {}\n",
                "# HELP auxide_discord_connected Whether the Discord gateway is connected.\n",
                "# TYPE auxide_discord_connected gauge\n",
                "auxide_discord_connected {}\n",
                "# HELP auxide_guild_players Configured per-guild player actors.\n",
                "# TYPE auxide_guild_players gauge\n",
                "auxide_guild_players {}\n",
                "# HELP auxide_interactions_total Discord interactions received.\n",
                "# TYPE auxide_interactions_total counter\n",
                "auxide_interactions_total {}\n",
                "# HELP auxide_interaction_errors_total Discord interactions that failed.\n",
                "# TYPE auxide_interaction_errors_total counter\n",
                "auxide_interaction_errors_total {}\n",
                "# HELP auxide_source_resolutions_total Source resolutions attempted.\n",
                "# TYPE auxide_source_resolutions_total counter\n",
                "auxide_source_resolutions_total {}\n",
                "# HELP auxide_source_resolution_errors_total Source resolutions that failed.\n",
                "# TYPE auxide_source_resolution_errors_total counter\n",
                "auxide_source_resolution_errors_total {}\n",
            ),
            u8::from(self.inner.ready.load(Ordering::Relaxed)),
            u8::from(self.inner.discord_connected.load(Ordering::Relaxed)),
            self.inner.guild_players.load(Ordering::Relaxed),
            self.inner.interactions.load(Ordering::Relaxed),
            self.inner.interaction_errors.load(Ordering::Relaxed),
            self.inner.source_resolutions.load(Ordering::Relaxed),
            self.inner.source_resolution_errors.load(Ordering::Relaxed),
        )
    }
}

#[derive(Debug)]
pub struct ObservabilityServer {
    pub local_address: SocketAddr,
    task: JoinHandle<io::Result<()>>,
}

impl ObservabilityServer {
    /// Waits for the server task to finish.
    ///
    /// # Errors
    ///
    /// Returns an error if the server task panics or its listener fails.
    pub async fn wait(self) -> io::Result<()> {
        self.task.await.map_err(io::Error::other)?
    }
}

/// Binds and starts the private health and metrics HTTP listener.
///
/// # Errors
///
/// Returns an error if the configured address cannot be bound or inspected.
pub async fn start_observability(
    address: SocketAddr,
    state: ObservabilityState,
    cancellation: CancellationToken,
) -> io::Result<ObservabilityServer> {
    let listener = TcpListener::bind(address).await?;
    let local_address = listener.local_addr()?;
    let task = tokio::spawn(run_server(listener, state, cancellation));
    Ok(ObservabilityServer {
        local_address,
        task,
    })
}

async fn run_server(
    listener: TcpListener,
    state: ObservabilityState,
    cancellation: CancellationToken,
) -> io::Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            result = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = result {
                    tracing::warn!(%error, "observability connection task failed");
                }
            }
            accepted = listener.accept(), if connections.len() < MAX_CONNECTIONS => {
                let (stream, peer) = accepted?;
                let state = state.clone();
                connections.spawn(async move {
                    if let Err(error) = serve_connection(stream, state).await {
                        tracing::debug!(%error, %peer, "observability request failed");
                    }
                });
            }
        }
    }

    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn serve_connection(mut stream: TcpStream, state: ObservabilityState) -> io::Result<()> {
    let mut request = Vec::with_capacity(1024);
    time::timeout(REQUEST_TIMEOUT, async {
        loop {
            let mut chunk = [0_u8; 1024];
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
            if request.len() > MAX_REQUEST_BYTES {
                return write_response(
                    &mut stream,
                    431,
                    "Request Header Fields Too Large",
                    "text/plain; charset=utf-8",
                    "request headers too large\n",
                )
                .await;
            }
        }

        let first_line = request
            .split(|byte| *byte == b'\n')
            .next()
            .unwrap_or_default();
        let first_line = std::str::from_utf8(first_line)
            .unwrap_or_default()
            .trim_end_matches('\r');
        match first_line {
            "GET /health/live HTTP/1.1" | "GET /health/live HTTP/1.0" => {
                write_response(
                    &mut stream,
                    200,
                    "OK",
                    "application/json",
                    "{\"status\":\"live\"}\n",
                )
                .await
            }
            "GET /health/ready HTTP/1.1" | "GET /health/ready HTTP/1.0" => {
                if state.is_ready() {
                    write_response(
                        &mut stream,
                        200,
                        "OK",
                        "application/json",
                        "{\"status\":\"ready\"}\n",
                    )
                    .await
                } else {
                    write_response(
                        &mut stream,
                        503,
                        "Service Unavailable",
                        "application/json",
                        "{\"status\":\"not_ready\"}\n",
                    )
                    .await
                }
            }
            "GET /metrics HTTP/1.1" | "GET /metrics HTTP/1.0" => {
                let metrics = state.metrics();
                write_response(
                    &mut stream,
                    200,
                    "OK",
                    "text/plain; version=0.0.4; charset=utf-8",
                    &metrics,
                )
                .await
            }
            _ => {
                write_response(
                    &mut stream,
                    404,
                    "Not Found",
                    "text/plain; charset=utf-8",
                    "not found\n",
                )
                .await
            }
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "observability request timed out"))?
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &str,
) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nX-Content-Type-Options: nosniff\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn request(address: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn readiness_is_fail_closed_and_metrics_are_bounded() {
        let cancellation = CancellationToken::new();
        let state = ObservabilityState::default();
        let server = start_observability(
            "127.0.0.1:0".parse().unwrap(),
            state.clone(),
            cancellation.clone(),
        )
        .await
        .unwrap();

        let unavailable = request(server.local_address, "/health/ready").await;
        assert!(unavailable.starts_with("HTTP/1.1 503"));
        state.set_ready(true);
        state.record_interaction(false);
        state.record_source_resolution(true);
        let ready = request(server.local_address, "/health/ready").await;
        assert!(ready.starts_with("HTTP/1.1 200"));
        let metrics = request(server.local_address, "/metrics").await;
        assert!(metrics.contains("auxide_ready 1"));
        assert!(metrics.contains("auxide_interaction_errors_total 1"));
        assert!(metrics.contains("auxide_source_resolutions_total 1"));

        cancellation.cancel();
        server.wait().await.unwrap();
    }
}
