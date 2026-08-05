use std::path::PathBuf;

use thiserror::Error;

/// Errors surfaced by the pub/sub layer.
///
/// Every fallible path returns a typed error so the binary, not the library,
/// decides what is fatal.
#[derive(Debug, Error)]
pub enum PubSubError {
    #[error("failed to connect to NATS at `{url}`")]
    Connect {
        url: String,
        #[source]
        source: async_nats::ConnectError,
    },

    #[error("failed to publish to topic `{topic}`")]
    Publish {
        topic: String,
        #[source]
        source: async_nats::PublishError,
    },

    #[error("failed to subscribe to topic `{topic}`")]
    Subscribe {
        topic: String,
        #[source]
        source: async_nats::SubscribeError,
    },

    #[error("failed to flush the NATS connection")]
    Flush(#[from] async_nats::client::FlushError),
}

/// Errors surfaced while resolving the peer roster.
#[derive(Debug, Error)]
pub enum PeerError {
    #[error("`{0}` is not a valid peer id (expected `<index>-<uuid>`)")]
    InvalidPeerId(String),

    #[error("failed to open key-value bucket `{bucket}`")]
    OpenBucket {
        bucket: String,
        #[source]
        source: async_nats::jetstream::context::CreateKeyValueError,
    },

    #[error("failed to list keys in bucket `{bucket}`")]
    ListKeys {
        bucket: String,
        #[source]
        source: async_nats::jetstream::kv::HistoryError,
    },

    #[error("failed while streaming keys from bucket `{bucket}`")]
    StreamKeys {
        bucket: String,
        #[source]
        source: async_nats::jetstream::kv::WatcherError,
    },

    #[error("failed to read key `{key}`")]
    ReadKey {
        key: String,
        #[source]
        source: async_nats::jetstream::kv::EntryError,
    },

    #[error("failed to write key `{key}`")]
    WriteKey {
        key: String,
        #[source]
        source: async_nats::jetstream::kv::PutError,
    },

    #[error("failed to read `{}`", path.display())]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to write `{}`", path.display())]
    WriteFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse `{}` as a peer list", path.display())]
    ParseFile {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error("failed to serialize the peer list")]
    Serialize(#[source] serde_json::Error),
}
