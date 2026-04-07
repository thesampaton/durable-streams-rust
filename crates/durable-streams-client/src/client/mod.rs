//! High-level asynchronous client and stream-scoped handles.
//!
//! [`Client`] owns the configured HTTP transport and exposes the full Durable
//! Streams operation set. [`StreamHandle`] binds a stream path so callers can
//! reuse one client for many operations without repeating the path argument.
//!
//! # Common Workflows
//!
//! Create a client and bind a stream handle:
//!
//! ```no_run
//! use durable_streams_client::Client;
//!
//! # fn main() -> Result<(), durable_streams_client::Error> {
//! let client = Client::builder().base_url("http://127.0.0.1:8080").build()?;
//! let orders = client.stream("/orders");
//! # let _ = orders;
//! # Ok(())
//! # }
//! ```
//!
//! Create a stream and append one JSON event:
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
//! orders.append_json(&serde_json::json!({ "type": "created" })).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Read from the beginning of the stream:
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
//! let page = orders.read().send().await?;
//!
//! for chunk in page.chunks {
//!     println!("{}", chunk.offset);
//! }
//! # Ok(())
//! # }
//! ```

mod builders;
mod raw_ops;
mod stream_handle;
mod subscription;

#[cfg(test)]
mod tests;

use crate::config::ClientConfig;
use crate::error::Error;
use crate::model::{LiveMode, RequestOptions, SubscriptionEvent};
use crate::types::Offset;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Clone)]
struct ClientInner {
    config: ClientConfig,
    http: reqwest::Client,
    default_headers: reqwest::header::HeaderMap,
    server_address: String,
    auth_type: &'static str,
}

/// Durable Streams HTTP client.
///
/// This is the main integration entry point for applications. It owns the
/// configured `reqwest` client, default headers, auth behavior, and retry
/// policy derived from [`ClientConfig`].
///
/// Most application code should construct a client once and then create one or
/// more [`StreamHandle`]s with [`Client::stream`].
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

/// Stream-scoped view over a [`Client`].
///
/// Use this when one caller performs repeated operations on a single stream.
///
/// The handle is lightweight and cloneable. It does not hold an open
/// connection; each operation performs an HTTP request on demand.
#[derive(Clone)]
pub struct StreamHandle {
    client: Client,
    path: String,
}

/// Background subscription handle returned by [`Client::subscribe_raw`].
///
/// The receiver yields collected subscription events until the background task
/// completes or is aborted.
pub struct Subscription {
    receiver: mpsc::Receiver<Result<SubscriptionEvent, Error>>,
    task: JoinHandle<()>,
}

/// Fluent builder for constructing a [`Client`] without assembling a full
/// [`ClientConfig`] manually.
///
/// # Example
///
/// ```rust
/// use durable_streams_client::Client;
/// use std::time::Duration;
///
/// # fn main() -> Result<(), durable_streams_client::Error> {
/// let client = Client::builder()
///     .base_url("http://127.0.0.1:8080")
///     .bearer_auth("replace-me")
///     .request_timeout(Duration::from_secs(30))
///     .default_content_type("application/json")
///     .build()?;
/// # let _ = client;
/// # Ok(())
/// # }
/// ```
pub struct ClientBuilder {
    config: ClientConfig,
    pending_error: Option<Error>,
}

/// Builder for ergonomic stream creation.
///
/// Obtain this from [`StreamHandle::create`], configure the options you need,
/// and finish with [`CreateBuilder::send`].
#[derive(Clone)]
pub struct CreateBuilder {
    stream: StreamHandle,
    content_type: Option<String>,
    ttl_seconds: Option<u64>,
    expires_at: Option<String>,
    closed: bool,
    body: Option<Bytes>,
    options: RequestOptions,
}

/// Builder for ergonomic appends.
///
/// Obtain this from [`StreamHandle::append`] and finish with
/// [`AppendBuilder::send`].
#[derive(Clone)]
pub struct AppendBuilder {
    stream: StreamHandle,
    body: Bytes,
    content_type: Option<String>,
    expected_seq: Option<String>,
    options: RequestOptions,
}

/// Builder for ergonomic closes.
///
/// Obtain this from [`StreamHandle::close`] and finish with
/// [`CloseBuilder::send`].
#[derive(Clone)]
pub struct CloseBuilder {
    stream: StreamHandle,
    body: Option<Bytes>,
    content_type: Option<String>,
    options: RequestOptions,
}

/// Builder for ergonomic reads.
///
/// Obtain this from [`StreamHandle::read`] and finish with
/// [`ReadBuilder::send`].
#[derive(Clone)]
pub struct ReadBuilder {
    stream: StreamHandle,
    offset: Offset,
    live: LiveMode,
    timeout: Option<Duration>,
    max_chunks: Option<usize>,
    wait_for_up_to_date: bool,
    cursor: Option<String>,
    if_none_match: Option<String>,
    options: RequestOptions,
}

#[derive(Clone, Copy)]
pub(crate) struct ProducerHeaders<'a> {
    pub producer_id: &'a str,
    pub producer_epoch: i64,
    pub producer_seq: i64,
}
