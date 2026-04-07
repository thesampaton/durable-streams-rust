#![forbid(unsafe_code)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![doc = include_str!("../README.md")]

//! Production-oriented Durable Streams client library.
//!
//! This crate is intended for normal backend and service usage rather than as a
//! thin protocol demo. The public API is centered on:
//!
//! - [`Client`] for configured HTTP access to a Durable Streams server
//! - typed request and response models in [`model`]
//! - explicit protocol and transport errors in [`error`]
//! - [`ClientConfig`] and [`ClientConfigLoader`] for config-first construction
//! - [`IdempotentProducer`] for producer fencing and sequence-aware appends
//!
//! # Quick Start
//!
//! ```no_run
//! use durable_streams_client::Client;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), durable_streams_client::Error> {
//! let client = Client::builder()
//!     .base_url("http://127.0.0.1:8080")
//!     .default_content_type("application/json")
//!     .build()?;
//! let orders = client.stream("/orders");
//!
//! orders.create().send().await?;
//!
//! let page = orders.read().send().await?;
//! let _next_offset = page.next_offset;
//! # Ok(())
//! # }
//! ```
//!
//! # Config Loading
//!
//! [`ClientConfigLoader`] assembles config from layered TOML files and
//! environment-variable overrides. By default it loads:
//!
//! - `config/default.toml`
//! - `config/<profile>.toml`
//! - `config/local.toml`
//!
//! and then applies overrides with the
//! `DURABLE_STREAMS_CLIENT__...` prefix, for example:
//!
//! - `DURABLE_STREAMS_CLIENT__CLIENT__BASE_URL`
//! - `DURABLE_STREAMS_CLIENT__AUTH__TYPE`
//! - `DURABLE_STREAMS_CLIENT__AUTH__BEARER_TOKEN`
//! - `DURABLE_STREAMS_CLIENT__TRANSPORT__REQUEST_TIMEOUT_MS`
//! - `DURABLE_STREAMS_CLIENT__DEFAULTS__HEADERS_JSON`
//!
//! The config loader is intentionally separate from the HTTP client so
//! applications can either adopt the built-in operational model or construct
//! [`ClientConfig`] directly.

//! # Library Entry Points
//!
//! Most users will start with:
//!
//! - [`Client`] for top-level stream operations
//! - [`StreamHandle`] for path-bound operations on one stream
//! - [`ClientConfig`] or [`ClientConfigLoader`] for construction
//! - [`IdempotentProducer`] when producer fencing and sequence management matter
//! - [`raw`] for protocol-shaped request and response models when you need them
//!
//! The public modules remain available when you want to browse one area of the
//! API in rustdoc by concern: auth, config, error handling, models, and retry.

pub mod auth;
pub mod client;
pub mod config;
pub mod error;
pub mod idempotent;
mod instrumentation;
pub mod model;
mod protocol;
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
pub use model::{LiveMode, ReadPayload, RequestOptions, RetryOptions, SubscriptionEvent};
pub use types::{
    AppendOutcome, CloseOutcome, CreateOutcome, Offset, ReadPage, StreamChunk, StreamInfo,
};
