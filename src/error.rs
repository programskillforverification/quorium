use async_nats::{
    ConnectError,
    client::{FlushError, PublishError, SubscribeError},
};
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
        source: ConnectError,
    },

    #[error("failed to publish to topic `{topic}`")]
    Publish {
        topic: String,
        #[source]
        source: PublishError,
    },

    #[error("failed to subscribe to topic `{topic}`")]
    Subscribe {
        topic: String,
        #[source]
        source: SubscribeError,
    },

    #[error("failed to flush the NATS connection")]
    Flush(#[from] FlushError),
}
