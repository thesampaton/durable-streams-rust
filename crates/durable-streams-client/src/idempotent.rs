//! Idempotent producer helper for sequence-aware append workflows.
//!
//! [`IdempotentProducer`] wraps [`crate::Client`] and manages producer headers,
//! local sequence advancement, and optional epoch auto-claim behavior.

use crate::client::{Client, ProducerHeaders};
use crate::error::{Error, ErrorKind};
use crate::instrumentation as trace;
use crate::journal::ProducerJournalProgress;
use crate::model::RequestOptions;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{Instrument, Span, debug};

#[derive(Debug)]
struct ProducerState {
    epoch: i64,
    next_seq: i64,
    closed: bool,
    acked_server_offset: Option<String>,
    acked_local_seq: Option<u64>,
}

/// Idempotent producer configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotentProducerConfig {
    /// Producer identifier sent in `Producer-Id`.
    pub producer_id: String,
    /// Starting epoch used for the first append.
    pub epoch: i64,
    /// Whether 403 fencing responses should trigger one local epoch claim retry.
    pub auto_claim: bool,
    /// Maximum bytes allowed in a batched append payload.
    pub max_batch_bytes: usize,
    /// Optional maximum number of logical items per batch.
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
    /// Validate producer configuration before construction.
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
///
/// This type is intended for callers that want a higher-level append API than
/// constructing [`crate::raw::ProducerRequest`] manually on every call.
pub struct IdempotentProducer {
    client: Client,
    path: String,
    content_type: String,
    options: RequestOptions,
    config: IdempotentProducerConfig,
    state: Mutex<ProducerState>,
}

impl IdempotentProducer {
    /// Create a producer bound to one stream path and content type.
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
                acked_server_offset: None,
                acked_local_seq: None,
            }),
            config,
        })
    }

    /// Append one payload and advance the local producer sequence on success.
    pub async fn append(&self, body: Vec<u8>) -> Result<crate::model::AppendResponse, Error> {
        let span = trace::producer_span("producer_append", &self.path);
        async {
            let mut state = self.state.lock().await;
            Span::current().record("producer.epoch", state.epoch);
            Span::current().record("producer.seq", state.next_seq);
            let response = self
                .append_with_state(&mut state, Bytes::from(body))
                .await?;
            state.acked_server_offset = response.next_offset.clone();
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

    /// Append multiple logical payloads as one request body.
    ///
    /// JSON content types are wrapped into one JSON array. Other content types
    /// are concatenated as raw bytes.
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
            let total_bytes = bodies.iter().map(Vec::len).sum();
            let mut out = Vec::with_capacity(total_bytes);
            for body in bodies {
                out.extend_from_slice(body);
            }
            out
        };

        self.append(combined).await
    }

    /// Append multiple JSON values as one `application/json` batch.
    ///
    /// This is the producer-oriented counterpart to the shared JSON ingest
    /// loader: callers can normalize input into `serde_json::Value` items and
    /// then send them as one durable-streams JSON append.
    pub async fn append_json_values(
        &self,
        values: &[serde_json::Value],
    ) -> Result<crate::model::AppendResponse, Error> {
        if !self.content_type.starts_with("application/json") {
            return Err(Error::invalid_argument(
                "append_json_values requires an application/json content type",
            ));
        }
        let body = serde_json::to_vec(values)?;
        self.append(body).await
    }

    /// Close the stream using the current producer state.
    pub async fn close(
        &self,
        body: Option<Vec<u8>>,
    ) -> Result<crate::model::CloseStreamResponse, Error> {
        let span = trace::producer_span("producer_close", &self.path);
        async {
            let mut state = self.state.lock().await;
            Span::current().record("producer.epoch", state.epoch);
            Span::current().record("producer.seq", state.next_seq);
            let body = body.map(Bytes::from);
            if state.closed {
                return self
                    .client
                    .close_parts(
                        &self.path,
                        &self.options,
                        Some(self.content_type.as_str()),
                        Some(ProducerHeaders {
                            producer_id: self.config.producer_id.as_str(),
                            producer_epoch: state.epoch,
                            producer_seq: state.next_seq,
                        }),
                        body,
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

            let response = self.close_with_state(&mut state, body).await?;
            state.acked_server_offset = Some(response.final_offset.clone());
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

    /// Release the producer handle without sending any protocol call.
    ///
    /// This currently exists as an explicit lifecycle no-op for API symmetry.
    pub async fn detach(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Snapshot the current in-memory producer progress for later persistence.
    ///
    /// The returned value matches the reserved producer fields in the JSONL
    /// journal schema so a later phase can persist producer recovery state
    /// without changing the format.
    pub async fn progress(&self) -> ProducerJournalProgress {
        let state = self.state.lock().await;
        ProducerJournalProgress {
            producer_id: self.config.producer_id.clone(),
            epoch: state.epoch,
            next_seq: state.next_seq,
            acked_server_offset: state.acked_server_offset.clone(),
            acked_local_seq: state.acked_local_seq,
        }
    }

    async fn append_with_state(
        &self,
        state: &mut ProducerState,
        body: Bytes,
    ) -> Result<crate::model::AppendResponse, Error> {
        let mut retried = false;

        loop {
            match self
                .client
                .append_parts(
                    &self.path,
                    &self.options,
                    Some(self.content_type.as_str()),
                    None,
                    Some(ProducerHeaders {
                        producer_id: self.config.producer_id.as_str(),
                        producer_epoch: state.epoch,
                        producer_seq: state.next_seq,
                    }),
                    body.clone(),
                )
                .await
            {
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
        body: Option<Bytes>,
    ) -> Result<crate::model::CloseStreamResponse, Error> {
        let mut retried = false;

        loop {
            match self
                .client
                .close_parts(
                    &self.path,
                    &self.options,
                    Some(self.content_type.as_str()),
                    Some(ProducerHeaders {
                        producer_id: self.config.producer_id.as_str(),
                        producer_epoch: state.epoch,
                        producer_seq: state.next_seq,
                    }),
                    body.clone(),
                )
                .await
            {
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
