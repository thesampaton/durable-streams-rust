#![forbid(unsafe_code)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![doc = include_str!("../README.md")]

pub mod auth;
pub mod client;
pub mod config;
pub mod error;
pub mod idempotent;
pub mod ingest;
mod instrumentation;
pub mod journal;
pub mod model;
mod protocol;
pub mod replica;
pub mod retry;
pub mod types;

/// Explicit protocol-shaped request and response models.
///
/// Most applications should prefer the stream-first ergonomic API on
/// [`Client`] and [`StreamHandle`]. Reach for this module when you need direct
/// control over headers, query parameters, or other wire-level protocol fields.
pub mod raw {
    pub use crate::model::{
        AppendRequest, AppendResponse, CloseStreamRequest, CloseStreamResponse, ConnectRequest,
        ConnectResponse, CreateStreamRequest, CreateStreamResponse, DeleteRequest, DeleteResponse,
        HeadRequest, HeadResponse, ProducerRequest, ReadChunk, ReadPayload, ReadRequest,
        ReadResponse, RequestOptions, RetryOptions, SubscribeRequest, SubscriptionEvent,
    };
}

pub use auth::AuthConfig;
pub use client::{
    AppendBuilder, Client, ClientBuilder, CloseBuilder, CreateBuilder, ReadBuilder, StreamHandle,
    Subscription,
};
pub use config::{
    ClientConfig, ClientConfigLoader, ClientConfigLoaderError, DefaultsConfig, TransportConfig,
};
pub use error::{Error, ErrorCode, ErrorKind, HttpError};
pub use idempotent::{IdempotentProducer, IdempotentProducerConfig};
pub use ingest::{JsonInput, JsonInputFormat, load_json_input, parse_json_input};
pub use journal::{
    JournalDirection, JournalRecord, JournalStreamIdentity, JsonJournal, ProducerJournalProgress,
};
pub use model::{
    AppendRequest, CreateStreamRequest, LiveMode, ReadPayload, ReadRequest, RequestOptions,
    RetryOptions, SubscriptionEvent,
};
pub use replica::{ReadReplica, ReadReplicaResult};
pub use types::{
    AppendOutcome, CloseOutcome, CreateOutcome, Offset, ReadPage, StreamChunk, StreamInfo,
};
