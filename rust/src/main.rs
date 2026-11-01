use axum::serve::ListenerExt;
use paseo_relay::{Config, Relay};
use std::{error::Error, time::Duration};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::load(&std::env::vars().collect())?;
    let listener = TcpListener::bind(config.listen).await?;
    eprintln!("paseo-relay listening on {}", listener.local_addr()?);
    let relay = Relay::new(config);
    let listener = listener.tap_io(|stream| {
        if let Err(error) = stream.set_nodelay(true) {
            eprintln!("TCP_NODELAY: {error}");
        }
    });
    axum::serve(listener, relay.router())
        .with_graceful_shutdown(shutdown(relay))
        .await?;
    Ok(())
}

async fn shutdown(relay: Relay) {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("register SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
    relay.begin_drain();
    let _ = tokio::time::timeout(Duration::from_secs(30), async {
        while relay.active_connections() != 0 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    relay.close_all();
    // Let explicit retryable closes reach sockets before the runtime exits.
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while relay.active_connections() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}
