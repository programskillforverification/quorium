use futures::StreamExt;
use quorium::{NatsPubSub, PubSub};

const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";
const TOPIC_KEYGEN: &str = "mpc:generate";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "quorium=info".into()),
        )
        .init();

    let url = std::env::var("NATS_URL").unwrap_or_else(|_| DEFAULT_NATS_URL.to_owned());

    let pubsub = NatsPubSub::connect(&url).await?;
    tracing::info!(%url, "connected to NATS");

    let mut keygen = pubsub.subscribe(TOPIC_KEYGEN).await?;
    tracing::info!(topic = TOPIC_KEYGEN, "subscribed");

    loop {
        tokio::select! {
            msg = keygen.next() => match msg {
                Some(msg) => {
                    tracing::info!(
                        topic = %msg.topic,
                        payload = %String::from_utf8_lossy(&msg.payload),
                        "received",
                    );
                }
                // The server closed the subscription; nothing left to wait for.
                None => {
                    tracing::warn!(topic = TOPIC_KEYGEN, "subscription closed");
                    break;
                }
            },
            _ = shutdown_signal() => {
                tracing::info!("shutdown signal received");
                break;
            }
        }
    }

    Ok(())
}

/// Resolves on SIGINT or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
