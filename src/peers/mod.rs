use std::fmt;
use std::future::Future;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::PeerError;

mod json_store;
mod kv_store;

pub use json_store::JsonPeerStore;
pub use kv_store::KvPeerStore;

/// Identity of one MPC node, rendered as `<index>-<uuid>`.
///
/// Parsing at the boundary keeps malformed identities from propagating as
/// unchecked strings.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct PeerId {
    pub index: usize,
    pub uuid: Uuid,
}

impl PeerId {
    pub fn new(index: usize) -> Self {
        Self {
            index,
            uuid: Uuid::new_v4(),
        }
    }

    /// Key this peer occupies in the KV bucket.
    pub fn key(&self) -> String {
        format!("node{}", self.index)
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.index, self.uuid)
    }
}

impl FromStr for PeerId {
    type Err = PeerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // The UUID itself contains `-`, so only the first one separates the index.
        let (index, uuid) = s
            .split_once('-')
            .ok_or_else(|| PeerError::InvalidPeerId(s.to_owned()))?;

        Ok(Self {
            index: index
                .parse()
                .map_err(|_| PeerError::InvalidPeerId(s.to_owned()))?,
            uuid: uuid
                .parse()
                .map_err(|_| PeerError::InvalidPeerId(s.to_owned()))?,
        })
    }
}

impl From<PeerId> for String {
    fn from(peer: PeerId) -> Self {
        peer.to_string()
    }
}

impl TryFrom<String> for PeerId {
    type Error = PeerError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

/// Somewhere a peer roster can be persisted.
///
/// Two implementations back this trait; `resolve` below is the fallback chain
/// between them.
pub trait PeerStore: Send + Sync {
    fn load(&self) -> impl Future<Output = Result<Vec<PeerId>, PeerError>> + Send;

    fn save(&self, peers: &[PeerId]) -> impl Future<Output = Result<(), PeerError>> + Send;
}

/// Reads the roster from the KV bucket, falling back to disk, then to generating
/// a fresh set. Whichever source wins is mirrored to the other one, so the two
/// backends cannot drift apart.
pub async fn resolve<K, F>(kv: &K, file: &F, want: usize) -> Result<Vec<PeerId>, PeerError>
where
    K: PeerStore,
    F: PeerStore,
{
    let peers = kv.load().await?;
    if !peers.is_empty() {
        tracing::info!(count = peers.len(), "loaded peers from the KV bucket");
        file.save(&peers).await?;
        return Ok(peers);
    }

    let peers = file.load().await?;
    if !peers.is_empty() {
        tracing::info!(count = peers.len(), "loaded peers from disk, backfilling the bucket");
        kv.save(&peers).await?;
        return Ok(peers);
    }

    let peers: Vec<PeerId> = (0..want).map(PeerId::new).collect();
    tracing::info!(count = peers.len(), "no peers on record, generating a fresh set");
    kv.save(&peers).await?;
    file.save(&peers).await?;
    Ok(peers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_id_round_trips_through_its_string_form() {
        let peer = PeerId::new(2);
        let rendered = peer.to_string();

        assert!(rendered.starts_with("2-"));
        assert_eq!(rendered.parse::<PeerId>().unwrap(), peer);
    }

    #[test]
    fn peer_id_parses_the_format_written_by_the_go_script() {
        let peer: PeerId = "0-dc354115-1e98-437f-bfbe-12a34ad0f669".parse().unwrap();

        assert_eq!(peer.index, 0);
        assert_eq!(peer.uuid.to_string(), "dc354115-1e98-437f-bfbe-12a34ad0f669");
        assert_eq!(peer.key(), "node0");
    }

    #[test]
    fn malformed_peer_ids_are_rejected() {
        for input in ["", "nodeone", "abc-dc354115-1e98-437f-bfbe-12a34ad0f669", "0-nope"] {
            assert!(input.parse::<PeerId>().is_err(), "{input} should not parse");
        }
    }
}
