use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[derive(Clone)]
pub struct NatsClient {
    client: async_nats::Client,
}

impl NatsClient {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let client = async_nats::connect(url)
            .await
            .with_context(|| format!("failed to connect to nats at {url}"))?;
        Ok(Self { client })
    }

    pub async fn publish_json<T: Serialize>(&self, subject: &str, payload: &T) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(payload).context("failed to serialize NATS payload")?;
        self.client
            .publish(subject.to_string(), bytes.into())
            .await
            .with_context(|| format!("failed to publish to subject {subject}"))?;
        Ok(())
    }

    pub async fn subscribe(&self, subject: &str) -> anyhow::Result<async_nats::Subscriber> {
        self.client
            .subscribe(subject.to_string())
            .await
            .with_context(|| format!("failed to subscribe to subject {subject}"))
    }

    pub fn decode_json<T: DeserializeOwned>(&self, message: &async_nats::Message) -> anyhow::Result<T> {
        serde_json::from_slice::<T>(&message.payload).context("failed to decode NATS payload")
    }
}
