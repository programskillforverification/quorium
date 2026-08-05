use async_nats::Client;
use async_nats::jetstream::kv::{self, Store};
use bytes::Bytes;
use futures::StreamExt;

use crate::error::PeerError;
use crate::peers::{PeerId, PeerStore};

/// Peer roster held in a NATS JetStream KV bucket.
pub struct KvPeerStore {
    store: Store,
    bucket: String,
}

impl KvPeerStore {
    /// Opens the bucket, creating it if this is the first node to start.
    pub async fn open(client: Client, bucket: &str) -> Result<Self, PeerError> {
        let jetstream = async_nats::jetstream::new(client);

        let store = jetstream
            .create_key_value(kv::Config {
                bucket: bucket.to_owned(),
                description: "MPC peer registry".to_owned(),
                history: 1,
                ..Default::default()
            })
            .await
            .map_err(|source| PeerError::OpenBucket {
                bucket: bucket.to_owned(),
                source,
            })?;

        Ok(Self {
            store,
            bucket: bucket.to_owned(),
        })
    }
}

impl PeerStore for KvPeerStore {
    async fn load(&self) -> Result<Vec<PeerId>, PeerError> {
        let mut keys = self
            .store
            .keys()
            .await
            .map_err(|source| PeerError::ListKeys {
                bucket: self.bucket.clone(),
                source,
            })?;

        let mut peers = Vec::new();
        while let Some(key) = keys.next().await {
            let key = key.map_err(|source| PeerError::StreamKeys {
                bucket: self.bucket.clone(),
                source,
            })?;

            let value = self
                .store
                .get(key.as_str())
                .await
                .map_err(|source| PeerError::ReadKey {
                    key: key.clone(),
                    source,
                })?;

            // Deleted between `keys()` and `get()`; nothing to record.
            let Some(value) = value else { continue };

            // The value is the peer id itself, so a round trip through the bucket
            // gives back what was written.
            let raw = std::str::from_utf8(&value)
                .map_err(|_| PeerError::InvalidPeerId(format!("{value:?}")))?;
            peers.push(raw.parse::<PeerId>()?);
        }

        peers.sort_by_key(|peer| peer.index);
        Ok(peers)
    }

    async fn save(&self, peers: &[PeerId]) -> Result<(), PeerError> {
        for peer in peers {
            let key = peer.key();

            self.store
                .put(&key, Bytes::from(peer.to_string()))
                .await
                .map_err(|source| PeerError::WriteKey {
                    key: key.clone(),
                    source,
                })?;

            tracing::info!(%key, peer = %peer, "stored peer");
        }

        Ok(())
    }
}
