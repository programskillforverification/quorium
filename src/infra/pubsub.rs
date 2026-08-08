use std::future::Future;

use async_nats::Client;
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
pub trait PubSub: Send + Sync + 'static {
    fn publish(
        &self,
        topic: &str,
        message: Bytes,
    ) -> impl Future<Output = Result<(), PubSubError>> + Send;

    fn subscribe(
        &self,
        topic: &str,
    ) -> impl Future<Output = Result<BoxStream<'static, Message>, PubSubError>> + Send;
}

#[derive(Clone)]
pub struct NatsPubSub {
    client: Client,
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

    pub fn client(&self) -> &Client {
        &self.client
    }
}

impl PubSub for NatsPubSub {
    async fn publish(&self, topic: &str, message: Bytes) -> Result<(), PubSubError> {
        tracing::info!(%topic, bytes = message.len(), "publishing");

        self.client
            .publish(topic.to_owned(), message)
            .await
            .map_err(|source| PubSubError::Publish {
                topic: topic.to_owned(),
                source,
            })?;

        self.client.flush().await?;

        Ok(())
    }

    async fn subscribe(&self, topic: &str) -> Result<BoxStream<'static, Message>, PubSubError> {
        let subscriber = self
            .client
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
