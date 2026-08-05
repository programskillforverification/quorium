use std::io::ErrorKind;
use std::path::PathBuf;

use crate::error::PeerError;
use crate::peers::{PeerId, PeerStore};

/// Peer roster persisted to a local `peers.json` file.
pub struct JsonPeerStore {
    path: PathBuf,
}

impl JsonPeerStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl PeerStore for JsonPeerStore {
    async fn load(&self) -> Result<Vec<PeerId>, PeerError> {
        let bytes = match tokio::fs::read(&self.path).await {
            Ok(bytes) => bytes,
            // A missing file just means "no peers yet", not a failure.
            Err(source) if source.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(PeerError::ReadFile {
                    path: self.path.clone(),
                    source,
                });
            }
        };

        if bytes.is_empty() {
            return Ok(Vec::new());
        }

        serde_json::from_slice(&bytes).map_err(|source| PeerError::ParseFile {
            path: self.path.clone(),
            source,
        })
    }

    async fn save(&self, peers: &[PeerId]) -> Result<(), PeerError> {
        let mut json = serde_json::to_vec_pretty(peers).map_err(PeerError::Serialize)?;
        json.push(b'\n');

        tokio::fs::write(&self.path, json)
            .await
            .map_err(|source| PeerError::WriteFile {
                path: self.path.clone(),
                source,
            })?;

        tracing::info!(path = %self.path.display(), count = peers.len(), "wrote peers to disk");
        Ok(())
    }
}
