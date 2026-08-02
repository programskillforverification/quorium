use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};

use crate::error::PubSubError;

/// A message received from a topic.
///
/// Keeping the subject and reply inbox
/// around is what lets a request/response MPC round work later.
#[derive(Debug, Clone)]
pub struct Message {
    pub topic: String,
    pub payload: Bytes,
    pub reply: Option<String>,
}

/// Transport abstraction over the message bus.
///
/// `subscribe` hands back a stream instead of taking a callback: the NATS
/// subscriber already is a `Stream`, and a stream composes with `tokio::select!`
/// for shutdown without boxing a closure into an `Arc`.
#[async_trait]
pub trait PubSub: Send + Sync + 'static {
    async fn publish(&self, topic: &str, payload: Bytes) -> Result<(), PubSubError>;

    async fn subscribe(&self, topic: &str) -> Result<BoxStream<'static, Message>, PubSubError>;
}

#[derive(Clone)]
pub struct NatsPubSub {
    client: async_nats::Client,
}

impl NatsPubSub {
    pub async fn connect(url: &str) -> Result<Self, PubSubError> {
        let client = async_nats::connect(url)
            .await
            .map_err(|source| PubSubError::Connect {
                url: url.to_owned(),
                source,
            })?;

        Ok(Self { client })
    }

    /// Escape hatch for the JetStream / KV APIs, which need the raw client.
    pub fn client(&self) -> &async_nats::Client {
        &self.client
    }
}

#[async_trait]
impl PubSub for NatsPubSub {
    async fn publish(&self, topic: &str, payload: Bytes) -> Result<(), PubSubError> {
        tracing::info!(%topic, bytes = payload.len(), "publishing");

        self.client
            .publish(topic.to_owned(), payload)
            .await
            .map_err(|source| PubSubError::Publish {
                topic: topic.to_owned(),
                source,
            })?;

        // Publishing is buffered; without this a short-lived process exits
        // before the write hits the socket.
        self.client.flush().await?;

        Ok(())
    }

    async fn subscribe(&self, topic: &str) -> Result<BoxStream<'static, Message>, PubSubError> {
        let subscriber =
            self.client
                .subscribe(topic.to_owned())
                .await
                .map_err(|source| PubSubError::Subscribe {
                    topic: topic.to_owned(),
                    source,
                })?;

        let stream = subscriber
            .map(|msg| Message {
                topic: msg.subject.to_string(),
                payload: msg.payload,
                reply: msg.reply.map(|subject| subject.to_string()),
            })
            .boxed();

        Ok(stream)
    }
}
