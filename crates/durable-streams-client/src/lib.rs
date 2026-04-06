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
//! use durable_streams_client::{
//!     Client, ClientConfig, CreateStreamRequest, ReadRequest,
//! };
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), durable_streams_client::Error> {
//! let client = Client::new(ClientConfig::default())?;
//!
//! client
//!     .create(
//!         "/orders",
//!         &CreateStreamRequest {
//!             content_type: "application/json".to_string(),
//!             body: None,
//!             ttl_seconds: None,
//!             expires_at: None,
//!             closed: false,
//!             options: Default::default(),
//!         },
//!     )
//!     .await?;
//!
//! let response = client.read("/orders", &ReadRequest::default()).await?;
//! let _next_offset = response.next_offset;
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
//! - request and response types re-exported at crate root for ergonomic imports
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

pub use auth::AuthConfig;
pub use client::{Client, StreamHandle, Subscription};
pub use config::{
    ClientConfig, ClientConfigLoader, ClientConfigLoaderError, DefaultsConfig, TransportConfig,
};
pub use error::{Error, ErrorCode, ErrorKind, HttpError};
pub use idempotent::{IdempotentProducer, IdempotentProducerConfig};
pub use model::{
    AppendRequest, AppendResponse, CloseStreamRequest, CloseStreamResponse, ConnectRequest,
    ConnectResponse, CreateStreamRequest, CreateStreamResponse, DeleteRequest, DeleteResponse,
    HeadRequest, HeadResponse, LiveMode, ProducerRequest, ReadChunk, ReadPayload, ReadRequest,
    ReadResponse, RequestOptions, RetryOptions, SubscribeRequest, SubscriptionEvent,
};
