use async_nats::jetstream::kv::{self, Store};
use bytes::Bytes;
use futures::StreamExt;
use uuid::Uuid;

const PEER_JSON_FILE_PATH: &str = "peers.json";
const NATS_URL: &str = "nats://127.0.0.1:4222";
const BUCKET: &str = "mpc-peers";

fn generate_unique_peer_id() -> String {
    Uuid::new_v4().to_string()
}

async fn load_peers_from_json() -> anyhow::Result<Vec<String>> {
    // A missing file is not fatal: the caller reports it and carries on with an
    // empty list.
    let data = tokio::fs::read(PEER_JSON_FILE_PATH).await?;

    if data.is_empty() {
        return Ok(Vec::new());
    }

    Ok(serde_json::from_slice(&data)?)
}

async fn load_peers_from_kv(store: &Store) -> anyhow::Result<Vec<String>> {
    let mut keys = store.keys().await?;

    println!("Node IDs in the '{BUCKET}' bucket:");
    let mut peers = Vec::new();
    while let Some(key) = keys.next().await {
        let key = key?;
        let Some(value) = store.get(key.as_str()).await? else {
            continue;
        };

        let value = String::from_utf8_lossy(&value).into_owned();
        println!("Key: {key}, Value: {value}");
        peers.push(value);
    }

    Ok(peers)
}

async fn store_peers_to_json(peers: &[String]) -> anyhow::Result<()> {
    let json = serde_json::to_vec_pretty(peers)?;
    tokio::fs::write(PEER_JSON_FILE_PATH, json).await?;

    println!("Peers data has been written to {PEER_JSON_FILE_PATH}");
    Ok(())
}

fn print_peers(peers: &[String]) {
    println!("Peers:");
    for peer in peers {
        println!("{peer}");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = async_nats::connect(NATS_URL).await?;
    let jetstream = async_nats::jetstream::new(client);
    let store = jetstream
        .create_key_value(kv::Config {
            bucket: BUCKET.to_owned(),
            history: 1,
            ..Default::default()
        })
        .await?;

    let peers = load_peers_from_kv(&store).await?;

    println!("Loaded peers from the bucket:");
    print_peers(&peers);

    if peers.is_empty() {
        let peers = match load_peers_from_json().await {
            Ok(peers) => peers,
            Err(err) => {
                println!("{err}");
                Vec::new()
            }
        };

        if peers.is_empty() {
            let node_ids = [
                generate_unique_peer_id(),
                generate_unique_peer_id(),
                generate_unique_peer_id(),
            ];

            let mut keys = Vec::new();
            for (id, node_id) in node_ids.iter().enumerate() {
                let key = format!("node{id}-{node_id}");
                keys.push(format!("{id}-{node_id}"));

                match store.put(&key, Bytes::from_static(b"ok")).await {
                    Ok(_) => eprintln!("Stored key {key}"),
                    Err(err) => eprintln!("Failed to store key {key}: {err}"),
                }
            }

            store_peers_to_json(&keys).await?;
        }
    }

    let peers = load_peers_from_kv(&store).await?;
    print_peers(&peers);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_peer_ids_are_distinct_uuids() {
        let a = generate_unique_peer_id();
        let b = generate_unique_peer_id();

        assert_ne!(a, b);
        assert!(a.parse::<Uuid>().is_ok());
    }
}
