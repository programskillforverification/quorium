//! Port of `cmd/nodes-backup/main.go`: make sure a peer roster exists, in both
//! the KV bucket and `peers.json`, and print what it settled on.

use anyhow::Context;
use quorium::{JsonPeerStore, KvPeerStore, NatsPubSub, peers};

const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";
const BUCKET: &str = "mpc-peers";
const PEERS_FILE: &str = "peers.json";
const DEFAULT_PEER_COUNT: usize = 3;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "quorium=info,nodes_backup=info".into()),
        )
        .init();

    let url = std::env::var("NATS_URL").unwrap_or_else(|_| DEFAULT_NATS_URL.to_owned());
    let want = match std::env::var("PEER_COUNT") {
        Ok(raw) => raw
            .parse()
            .with_context(|| format!("PEER_COUNT={raw} is not a number"))?,
        Err(_) => DEFAULT_PEER_COUNT,
    };

    let pubsub = NatsPubSub::connect(&url)
        .await
        .with_context(|| format!("connecting to NATS at {url}"))?;
    tracing::info!(%url, "connected to NATS");

    let kv = KvPeerStore::open(pubsub.client().clone(), BUCKET).await?;
    let file = JsonPeerStore::new(PEERS_FILE);

    let peers = peers::resolve(&kv, &file, want).await?;

    println!("Peers:");
    for peer in &peers {
        println!("  {peer}");
    }

    Ok(())
}
