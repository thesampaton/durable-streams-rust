use crate::client::Client;
use crate::error::{Error, ErrorKind};
use crate::instrumentation as trace;
use crate::model::{AppendRequest, CloseStreamRequest, ProducerRequest, RequestOptions};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{Instrument, Span, debug};

#[derive(Debug)]
struct ProducerState {
    epoch: i64,
    next_seq: i64,
    closed: bool,
}

/// Idempotent producer configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotentProducerConfig {
    pub producer_id: String,
    pub epoch: i64,
    pub auto_claim: bool,
    pub max_batch_bytes: usize,
    pub max_batch_items: Option<usize>,
}

impl Default for IdempotentProducerConfig {
    fn default() -> Self {
        Self {
            producer_id: String::new(),
            epoch: 0,
            auto_claim: false,
            max_batch_bytes: 1024 * 1024,
            max_batch_items: None,
        }
    }
}

impl IdempotentProducerConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if self.producer_id.trim().is_empty() {
            return Err(Error::invalid_argument("producerId must not be empty"));
        }
        if self.epoch < 0 {
            return Err(Error::invalid_argument("epoch must not be negative"));
        }
        if self.max_batch_bytes == 0 {
            return Err(Error::invalid_argument(
                "maxBatchBytes must be greater than zero",
            ));
        }
        if let Some(max_batch_items) = self.max_batch_items {
            if max_batch_items == 0 {
                return Err(Error::invalid_argument(
                    "maxBatchItems must be greater than zero",
                ));
            }
        }
        Ok(())
    }
}

/// Producer helper that manages producer headers and sequence advancement.
pub struct IdempotentProducer {
    client: Client,
    path: String,
    content_type: String,
    options: RequestOptions,
    config: IdempotentProducerConfig,
    state: Mutex<ProducerState>,
}

impl IdempotentProducer {
    pub fn new(
        client: Client,
        path: impl Into<String>,
        content_type: impl Into<String>,
        options: RequestOptions,
        config: IdempotentProducerConfig,
    ) -> Result<Self, Error> {
        let path = path.into();
        let content_type = content_type.into();
        let span = trace::producer_span("producer_init", &path);
        let _guard = span.enter();
        config.validate()?;
        debug!(
            event = "producer.constructed",
            "producer.auto_claim" = config.auto_claim,
            "producer.epoch" = config.epoch,
            "producer.max_batch_bytes" = config.max_batch_bytes,
            "producer.max_batch_items" = config.max_batch_items.unwrap_or(0) as u64
        );
        Ok(Self {
            client,
            path,
            content_type,
            options,
            state: Mutex::new(ProducerState {
                epoch: config.epoch,
                next_seq: 0,
                closed: false,
            }),
            config,
        })
    }

    pub async fn append(&self, body: Vec<u8>) -> Result<crate::model::AppendResponse, Error> {
        let span = trace::producer_span("producer_append", &self.path);
        async {
            let mut state = self.state.lock().await;
            Span::current().record("producer.epoch", state.epoch);
            Span::current().record("producer.seq", state.next_seq);
            let response = self.append_with_state(&mut state, body).await?;
            state.next_seq += 1;
            debug!(
                event = "producer.append_completed",
                "producer.epoch" = state.epoch,
                "producer.seq" = state.next_seq
            );
            Ok(response)
        }
        .instrument(span)
        .await
    }

    pub async fn append_batch(
        &self,
        bodies: &[Vec<u8>],
    ) -> Result<crate::model::AppendResponse, Error> {
        let combined = if self.content_type.starts_with("application/json") {
            let mut items = Vec::with_capacity(bodies.len());
            for body in bodies {
                let value: serde_json::Value = serde_json::from_slice(body)?;
                items.push(value);
            }
            serde_json::to_vec(&items)?
        } else {
            let mut out = Vec::new();
            for body in bodies {
                out.extend_from_slice(body);
            }
            out
        };

        self.append(combined).await
    }

    pub async fn close(
        &self,
        body: Option<Vec<u8>>,
    ) -> Result<crate::model::CloseStreamResponse, Error> {
        let span = trace::producer_span("producer_close", &self.path);
        async {
            let mut state = self.state.lock().await;
            Span::current().record("producer.epoch", state.epoch);
            Span::current().record("producer.seq", state.next_seq);
            if state.closed {
                return self
                    .client
                    .close(
                        &self.path,
                        &CloseStreamRequest {
                            body,
                            content_type: Some(self.content_type.clone()),
                            producer: Some(ProducerRequest {
                                producer_id: self.config.producer_id.clone(),
                                producer_epoch: state.epoch,
                                producer_seq: state.next_seq,
                            }),
                            options: self.options.clone(),
                        },
                    )
                    .await
                    .or_else(|error| match error {
                        Error::Http(http)
                            if matches!(
                                http.kind,
                                ErrorKind::Conflict | ErrorKind::StreamClosed
                            ) =>
                        {
                            Ok(crate::model::CloseStreamResponse {
                                status: 200,
                                final_offset: http.next_offset.unwrap_or_default(),
                                stream_closed: true,
                            })
                        }
                        other => Err(other),
                    });
            }

            let response = self.close_with_state(&mut state, body.clone()).await?;
            state.closed = true;
            debug!(
                event = "producer.close_completed",
                "producer.epoch" = state.epoch,
                "producer.seq" = state.next_seq
            );
            Ok(response)
        }
        .instrument(span)
        .await
    }

    pub async fn detach(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn append_with_state(
        &self,
        state: &mut ProducerState,
        body: Vec<u8>,
    ) -> Result<crate::model::AppendResponse, Error> {
        let mut retried = false;

        loop {
            let request = AppendRequest {
                body: body.clone(),
                content_type: Some(self.content_type.clone()),
                stream_seq: None,
                producer: Some(ProducerRequest {
                    producer_id: self.config.producer_id.clone(),
                    producer_epoch: state.epoch,
                    producer_seq: state.next_seq,
                }),
                options: self.options.clone(),
            };

            match self.client.append(&self.path, &request).await {
                Ok(response) => return Ok(response),
                Err(Error::Http(http))
                    if self.config.auto_claim
                        && matches!(http.kind, ErrorKind::Forbidden)
                        && !retried =>
                {
                    debug!(
                        event = "producer.auto_claim",
                        "producer.epoch.previous" = state.epoch,
                        "producer.epoch.claimed" = http.producer_epoch.unwrap_or(state.epoch) + 1
                    );
                    state.epoch = http.producer_epoch.unwrap_or(state.epoch) + 1;
                    state.next_seq = 0;
                    retried = true;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn close_with_state(
        &self,
        state: &mut ProducerState,
        body: Option<Vec<u8>>,
    ) -> Result<crate::model::CloseStreamResponse, Error> {
        let mut retried = false;

        loop {
            let request = CloseStreamRequest {
                body: body.clone(),
                content_type: Some(self.content_type.clone()),
                producer: Some(ProducerRequest {
                    producer_id: self.config.producer_id.clone(),
                    producer_epoch: state.epoch,
                    producer_seq: state.next_seq,
                }),
                options: self.options.clone(),
            };

            match self.client.close(&self.path, &request).await {
                Ok(response) => return Ok(response),
                Err(Error::Http(http))
                    if self.config.auto_claim
                        && matches!(http.kind, ErrorKind::Forbidden)
                        && !retried =>
                {
                    debug!(
                        event = "producer.auto_claim",
                        "producer.epoch.previous" = state.epoch,
                        "producer.epoch.claimed" = http.producer_epoch.unwrap_or(state.epoch) + 1
                    );
                    state.epoch = http.producer_epoch.unwrap_or(state.epoch) + 1;
                    state.next_seq = 0;
                    retried = true;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IdempotentProducerConfig;

    #[test]
    fn rejects_zero_max_batch_bytes() {
        let config = IdempotentProducerConfig {
            producer_id: "producer".to_string(),
            epoch: 0,
            auto_claim: false,
            max_batch_bytes: 0,
            max_batch_items: None,
        };

        assert!(config.validate().is_err());
    }
}
