#[cfg(unix)]
pub(crate) async fn shutdown_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).expect("SIGTERM handler is available");
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.expect("SIGINT handler is available");
            "SIGINT"
        }
        _ = terminate.recv() => "SIGTERM",
    }
}

#[cfg(not(unix))]
pub(crate) async fn shutdown_signal() -> &'static str {
    tokio::signal::ctrl_c()
        .await
        .expect("interrupt handler is available");
    "interrupt"
}
