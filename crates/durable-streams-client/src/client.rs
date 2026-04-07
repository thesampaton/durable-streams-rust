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
//! use durable_streams_client::{Client, ClientConfig};
//!
//! # fn main() -> Result<(), durable_streams_client::Error> {
//! let client = Client::new(ClientConfig::default())?;
//! let orders = client.stream("/orders");
//! # let _ = orders;
//! # Ok(())
//! # }
//! ```
//!
//! Create a stream and append one JSON event:
//!
//! ```no_run
//! use durable_streams_client::{Client, ClientConfig};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), durable_streams_client::Error> {
//! let client = Client::new(ClientConfig::default())?;
//! let orders = client.stream("/orders");
//!
//! orders.create().content_type("application/json").send().await?;
//! orders.append_json(&serde_json::json!({ "type": "created" })).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Read from the beginning of the stream:
//!
//! ```no_run
//! use durable_streams_client::{Client, ClientConfig, LiveMode, Offset};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), durable_streams_client::Error> {
//! let client = Client::new(ClientConfig::default())?;
//! let orders = client.stream("/orders");
//!
//! let page = orders
//!     .read()
//!     .offset(Offset::Beginning)
//!     .live(LiveMode::CatchUp)
//!     .send()
//!     .await?;
//!
//! for chunk in page.chunks {
//!     println!("{}", chunk.next_offset);
//! }
//! # Ok(())
//! # }
//! ```

use crate::auth::AuthConfig;
use crate::config::ClientConfig;
use crate::error::Error;
use crate::instrumentation as trace;
use crate::model::{
    AppendRequest, AppendResponse, CloseStreamRequest, CloseStreamResponse, ConnectRequest,
    ConnectResponse, CreateStreamRequest, CreateStreamResponse, DeleteRequest, DeleteResponse,
    HeadRequest, HeadResponse, LiveMode, ReadRequest, ReadResponse, RequestOptions,
    SubscribeRequest, SubscriptionEvent,
};
use crate::protocol::{
    PRODUCER_EPOCH, PRODUCER_ID, PRODUCER_SEQ, STREAM_CLOSED, STREAM_EXPIRES_AT,
    STREAM_NEXT_OFFSET, STREAM_SEQ, STREAM_TTL, collect_catch_up, collect_sse, header_value,
    parse_bool_header, parse_i64_header, response_error, response_to_event,
};
use crate::retry::RetryPolicy;
use crate::types::{AppendAck, CloseAck, CreateAck, Offset, ReadPage, StreamInfo};
use bytes::Bytes;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode, Url};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{Instrument, Span, debug, error};

#[derive(Clone)]
struct ClientInner {
    config: ClientConfig,
    http: reqwest::Client,
    default_headers: HeaderMap,
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

impl Client {
    /// Start building a client with ergonomic fluent configuration.
    #[must_use]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Build a client from validated configuration.
    pub fn from_config(config: ClientConfig) -> Result<Self, Error> {
        config.validate()?;

        let server_address = trace::server_address(&config.base_url).to_string();
        let auth_type = trace::auth_type(&config.auth);
        let default_headers = build_default_headers(&config)?;
        let span = tracing::info_span!(
            "durable_streams.client",
            "ds.operation" = "client_init",
            "server.address" = server_address.as_str(),
            "auth.type" = auth_type,
            "error.kind" = tracing::field::Empty,
            "error.message" = tracing::field::Empty
        );
        let _guard = span.enter();
        debug!(
            event = "client.constructing",
            "transport.connect_timeout_ms" = config.transport.connect_timeout.as_millis() as u64,
            "transport.request_timeout_ms" = config.transport.request_timeout.as_millis() as u64,
            "transport.proxy_enabled" = config.transport.proxy_url.is_some(),
            user_agent = config.transport.user_agent.as_str()
        );

        let mut builder = reqwest::Client::builder()
            .connect_timeout(config.transport.connect_timeout)
            .timeout(config.transport.request_timeout)
            .user_agent(config.transport.user_agent.clone());

        if let Some(proxy_url) = &config.transport.proxy_url {
            builder = builder.proxy(reqwest::Proxy::all(proxy_url)?);
        }

        let http = match builder.build() {
            Ok(http) => http,
            Err(error) => {
                let error = Error::from(error);
                trace::record_current_error(&error);
                error!(event = "client.construct_failed");
                return Err(error);
            }
        };
        debug!(event = "client.constructed");
        Ok(Self {
            inner: Arc::new(ClientInner {
                config,
                http,
                default_headers,
                server_address,
                auth_type,
            }),
        })
    }

    /// Alias for [`Client::from_config`].
    pub fn new(config: ClientConfig) -> Result<Self, Error> {
        Self::from_config(config)
    }

    /// Return the resolved configuration used by this client.
    #[must_use]
    pub fn config(&self) -> &ClientConfig {
        &self.inner.config
    }

    /// Bind a stream path and return a path-scoped handle.
    #[must_use]
    pub fn stream(&self, path: impl Into<String>) -> StreamHandle {
        StreamHandle {
            client: self.clone(),
            path: path.into(),
        }
    }

    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::new(self.inner.config.retry)
    }

    fn server_address(&self) -> &str {
        &self.inner.server_address
    }

    fn auth_type(&self) -> &'static str {
        self.inner.auth_type
    }

    fn default_content_type(&self) -> Option<&str> {
        self.inner.config.defaults.default_content_type.as_deref()
    }

    async fn send_request<F>(
        &self,
        operation: &'static str,
        stream_id: &str,
        method: Method,
        url: Url,
        options: &RequestOptions,
        customize: F,
    ) -> Result<reqwest::Response, Error>
    where
        F: FnOnce(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
    {
        let span = trace::http_request_span(operation, stream_id, &method, &url, self.auth_type());
        async move {
            debug!(event = "request.started");
            let builder = customize(self.request(method, url, options));
            let response = builder.send().await?;
            Span::current().record("http.status_code", response.status().as_u16());
            debug!(
                event = "response.received",
                "http.status_code" = response.status().as_u16()
            );
            Ok(response)
        }
        .instrument(span)
        .await
    }

    async fn send_retrying_request<F>(
        &self,
        operation: &'static str,
        stream_id: &str,
        method: Method,
        url: Url,
        expected: &[StatusCode],
        options: &RequestOptions,
        mut customize: F,
    ) -> Result<reqwest::Response, Error>
    where
        F: FnMut(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
    {
        self.retry_policy()
            .run(|| {
                let operation = operation;
                let stream_id = stream_id;
                let method = method.clone();
                let url = url.clone();
                let span =
                    trace::http_request_span(operation, stream_id, &method, &url, self.auth_type());
                let builder = span.in_scope(|| {
                    debug!(event = "request.started");
                    customize(self.request(method, url, options))
                });
                async move {
                    let response = builder.send().await?;
                    let status = response.status();
                    Span::current().record("http.status_code", status.as_u16());
                    debug!(
                        event = "response.received",
                        "http.status_code" = status.as_u16()
                    );
                    if expected.contains(&status) {
                        Ok(response)
                    } else {
                        let error = Error::from(response_error(response).await);
                        trace::record_current_error(&error);
                        Err(error)
                    }
                }
                .instrument(span)
            })
            .await
    }

    async fn require_status(
        response: reqwest::Response,
        expected: &[StatusCode],
    ) -> Result<reqwest::Response, Error> {
        let status = response.status();
        Span::current().record("http.status_code", status.as_u16());
        if expected.contains(&status) {
            Ok(response)
        } else {
            Err(response_error(response).await.into())
        }
    }

    async fn require_success(response: reqwest::Response) -> Result<reqwest::Response, Error> {
        let status = response.status();
        Span::current().record("http.status_code", status.as_u16());
        if status.is_success() {
            Ok(response)
        } else {
            Err(response_error(response).await.into())
        }
    }

    /// Create a stream.
    ///
    /// Maps to `PUT /v1/stream/{name}` and returns the created or idempotently
    /// reused stream state, including the next offset when the server provides it.
    pub async fn create_raw(
        &self,
        path: &str,
        request: &CreateStreamRequest,
    ) -> Result<CreateStreamResponse, Error> {
        let span = trace::client_operation_span(
            "create_stream",
            path,
            self.server_address(),
            self.auth_type(),
        );
        async {
            debug!(event = "operation.started");
            let result = async {
                let url = self.stream_url(path, &request.options)?;
                let ttl_seconds = request.ttl_seconds.map(|value| value.to_string());
                let body = request.body.clone();
                let response = self
                    .send_retrying_request(
                        "create_stream",
                        path,
                        Method::PUT,
                        url,
                        &[StatusCode::OK, StatusCode::CREATED],
                        &request.options,
                        |mut builder| {
                            builder = builder.header(CONTENT_TYPE, &request.content_type);
                            if let Some(ttl_seconds) = ttl_seconds.as_deref() {
                                builder = builder.header(STREAM_TTL, ttl_seconds);
                            }
                            if let Some(expires_at) = &request.expires_at {
                                builder = builder.header(STREAM_EXPIRES_AT, expires_at);
                            }
                            if request.closed {
                                builder = builder.header(STREAM_CLOSED, "true");
                            }
                            if let Some(body) = &body {
                                builder = builder.body(body.clone());
                            }
                            builder
                        },
                    )
                    .await?;
                Ok(CreateStreamResponse {
                    status: response.status().as_u16(),
                    next_offset: header_value(&response, STREAM_NEXT_OFFSET),
                    stream_closed: parse_bool_header(&response, STREAM_CLOSED),
                })
            }
            .await;
            match &result {
                Ok(response) => {
                    Span::current().record("http.status_code", response.status);
                    Span::current().record("ds.stream_closed", response.stream_closed);
                    trace::record_current_optional_str(
                        "ds.resume_offset",
                        response.next_offset.as_deref(),
                    );
                    debug!(event = "operation.completed");
                }
                Err(error) => {
                    trace::record_current_error(error);
                    error!(event = "operation.failed");
                }
            }
            result
        }
        .instrument(span)
        .await
    }

    /// Fetch metadata for an existing stream without reading message bodies.
    pub async fn connect_raw(
        &self,
        path: &str,
        request: &ConnectRequest,
    ) -> Result<ConnectResponse, Error> {
        let span = trace::client_operation_span(
            "connect_stream",
            path,
            self.server_address(),
            self.auth_type(),
        );
        async {
            debug!(event = "operation.started");
            let result = async {
                let head = self
                    .head_once(
                        path,
                        &HeadRequest {
                            options: request.options.clone(),
                        },
                    )
                    .await?;

                Ok(ConnectResponse {
                    status: head.status,
                    offset: head.offset,
                    content_type: head.content_type,
                    stream_closed: head.stream_closed,
                })
            }
            .await;
            match &result {
                Ok(response) => {
                    Span::current().record("http.status_code", response.status);
                    Span::current().record("ds.stream_closed", response.stream_closed);
                    trace::record_current_optional_str(
                        "ds.resume_offset",
                        response.offset.as_deref(),
                    );
                    debug!(event = "operation.completed");
                }
                Err(error) => {
                    trace::record_current_error(error);
                    error!(event = "operation.failed");
                }
            }
            result
        }
        .instrument(span)
        .await
    }

    /// Append one request body to an existing stream.
    ///
    /// When `request.producer` is set, the append uses the protocol's idempotent
    /// producer headers and returns any acknowledged producer sequence state.
    pub async fn append_raw(
        &self,
        path: &str,
        request: &AppendRequest,
    ) -> Result<AppendResponse, Error> {
        self.append_parts(
            path,
            &request.options,
            request.content_type.as_deref(),
            request.stream_seq.as_deref(),
            request.producer.as_ref().map(|producer| ProducerHeaders {
                producer_id: producer.producer_id.as_str(),
                producer_epoch: producer.producer_epoch,
                producer_seq: producer.producer_seq,
            }),
            request.body.clone(),
        )
        .await
    }

    pub(crate) async fn append_parts(
        &self,
        path: &str,
        options: &RequestOptions,
        content_type: Option<&str>,
        stream_seq: Option<&str>,
        producer: Option<ProducerHeaders<'_>>,
        body: Bytes,
    ) -> Result<AppendResponse, Error> {
        let span = trace::client_operation_span(
            "append_stream",
            path,
            self.server_address(),
            self.auth_type(),
        );
        async {
            debug!(event = "operation.started");
            let result = async {
                let url = self.stream_url(path, options)?;
                let producer_headers = producer.map(|value| {
                    (
                        value.producer_id,
                        value.producer_epoch.to_string(),
                        value.producer_seq.to_string(),
                    )
                });
                let response = self
                    .send_retrying_request(
                        "append_stream",
                        path,
                        Method::POST,
                        url,
                        &[StatusCode::OK, StatusCode::NO_CONTENT],
                        options,
                        |mut builder| {
                            if let Some(content_type) = content_type {
                                builder = builder.header(CONTENT_TYPE, content_type);
                            }
                            if let Some(stream_seq) = stream_seq {
                                builder = builder.header(STREAM_SEQ, stream_seq);
                            }
                            if let Some((producer_id, producer_epoch, producer_seq)) =
                                &producer_headers
                            {
                                builder = builder
                                    .header(PRODUCER_ID, *producer_id)
                                    .header(PRODUCER_EPOCH, producer_epoch.as_str())
                                    .header(PRODUCER_SEQ, producer_seq.as_str());
                            }
                            builder.body(body.clone())
                        },
                    )
                    .await?;
                Ok(AppendResponse {
                    status: response.status().as_u16(),
                    next_offset: header_value(&response, STREAM_NEXT_OFFSET),
                    stream_closed: parse_bool_header(&response, STREAM_CLOSED),
                    producer_epoch: parse_i64_header(&response, PRODUCER_EPOCH),
                    producer_seq: parse_i64_header(&response, PRODUCER_SEQ),
                })
            }
            .await;
            match &result {
                Ok(response) => {
                    Span::current().record("http.status_code", response.status);
                    Span::current().record("ds.stream_closed", response.stream_closed);
                    trace::record_current_optional_str(
                        "ds.resume_offset",
                        response.next_offset.as_deref(),
                    );
                    debug!(event = "operation.completed");
                }
                Err(error) => {
                    trace::record_current_error(error);
                    error!(event = "operation.failed");
                }
            }
            result
        }
        .instrument(span)
        .await
    }

    /// Close a stream, optionally including a final body payload.
    pub async fn close_raw(
        &self,
        path: &str,
        request: &CloseStreamRequest,
    ) -> Result<CloseStreamResponse, Error> {
        self.close_parts(
            path,
            &request.options,
            request.content_type.as_deref(),
            request.producer.as_ref().map(|producer| ProducerHeaders {
                producer_id: producer.producer_id.as_str(),
                producer_epoch: producer.producer_epoch,
                producer_seq: producer.producer_seq,
            }),
            request.body.clone(),
        )
        .await
    }

    pub(crate) async fn close_parts(
        &self,
        path: &str,
        options: &RequestOptions,
        content_type: Option<&str>,
        producer: Option<ProducerHeaders<'_>>,
        body: Option<Bytes>,
    ) -> Result<CloseStreamResponse, Error> {
        let span = trace::client_operation_span(
            "close_stream",
            path,
            self.server_address(),
            self.auth_type(),
        );
        async {
            debug!(event = "operation.started");
            let result = async {
                let url = self.stream_url(path, options)?;
                let producer_headers = producer.map(|value| {
                    (
                        value.producer_id,
                        value.producer_epoch.to_string(),
                        value.producer_seq.to_string(),
                    )
                });
                let response = self
                    .send_retrying_request(
                        "close_stream",
                        path,
                        Method::POST,
                        url,
                        &[StatusCode::OK, StatusCode::NO_CONTENT],
                        options,
                        |mut builder| {
                            builder = builder.header(STREAM_CLOSED, "true");
                            if let Some(content_type) = content_type {
                                builder = builder.header(CONTENT_TYPE, content_type);
                            }
                            if let Some((producer_id, producer_epoch, producer_seq)) =
                                &producer_headers
                            {
                                builder = builder
                                    .header(PRODUCER_ID, *producer_id)
                                    .header(PRODUCER_EPOCH, producer_epoch.as_str())
                                    .header(PRODUCER_SEQ, producer_seq.as_str());
                            }
                            if let Some(body) = &body {
                                builder = builder.body(body.clone());
                            }
                            builder
                        },
                    )
                    .await?;
                Ok(CloseStreamResponse {
                    status: response.status().as_u16(),
                    final_offset: header_value(&response, STREAM_NEXT_OFFSET)
                        .ok_or_else(|| Error::parse("missing Stream-Next-Offset header"))?,
                    stream_closed: parse_bool_header(&response, STREAM_CLOSED),
                })
            }
            .await;
            match &result {
                Ok(response) => {
                    Span::current().record("http.status_code", response.status);
                    Span::current().record("ds.stream_closed", response.stream_closed);
                    trace::record_current_optional_str(
                        "ds.resume_offset",
                        Some(response.final_offset.as_str()),
                    );
                    debug!(event = "operation.completed");
                }
                Err(error) => {
                    trace::record_current_error(error);
                    error!(event = "operation.failed");
                }
            }
            result
        }
        .instrument(span)
        .await
    }

    async fn head_once(&self, path: &str, request: &HeadRequest) -> Result<HeadResponse, Error> {
        let url = self.stream_url(path, &request.options)?;
        let response = self
            .send_request(
                "head_stream",
                path,
                Method::HEAD,
                url,
                &request.options,
                |builder| builder,
            )
            .await?;
        let response = Self::require_success(response).await?;

        Ok(HeadResponse {
            status: response.status().as_u16(),
            offset: header_value(&response, STREAM_NEXT_OFFSET),
            content_type: header_value(&response, CONTENT_TYPE.as_str()),
            ttl_seconds: header_value(&response, STREAM_TTL)
                .and_then(|value| value.parse::<u64>().ok()),
            expires_at: header_value(&response, STREAM_EXPIRES_AT),
            stream_closed: parse_bool_header(&response, STREAM_CLOSED),
            etag: header_value(&response, reqwest::header::ETAG.as_str()),
        })
    }

    /// Read from a stream in catch-up, long-poll, or SSE-backed collection mode.
    pub async fn head_raw(&self, path: &str, request: &HeadRequest) -> Result<HeadResponse, Error> {
        let span = trace::client_operation_span(
            "head_stream",
            path,
            self.server_address(),
            self.auth_type(),
        );
        async {
            debug!(event = "operation.started");
            let result = self.head_once(path, request).await;
            match &result {
                Ok(response) => {
                    Span::current().record("http.status_code", response.status);
                    Span::current().record("ds.stream_closed", response.stream_closed);
                    trace::record_current_optional_str(
                        "ds.resume_offset",
                        response.offset.as_deref(),
                    );
                    debug!(event = "operation.completed");
                }
                Err(error) => {
                    trace::record_current_error(error);
                    error!(event = "operation.failed");
                }
            }
            result
        }
        .instrument(span)
        .await
    }

    /// Delete a stream.
    pub async fn delete_raw(
        &self,
        path: &str,
        request: &DeleteRequest,
    ) -> Result<DeleteResponse, Error> {
        let span = trace::client_operation_span(
            "delete_stream",
            path,
            self.server_address(),
            self.auth_type(),
        );
        async {
            debug!(event = "operation.started");
            let result = async {
                let url = self.stream_url(path, &request.options)?;
                let response = self
                    .send_request(
                        "delete_stream",
                        path,
                        Method::DELETE,
                        url,
                        &request.options,
                        |builder| builder,
                    )
                    .await?;
                let response =
                    Self::require_status(response, &[StatusCode::OK, StatusCode::NO_CONTENT])
                        .await?;
                Ok(DeleteResponse {
                    status: response.status().as_u16(),
                })
            }
            .await;
            match &result {
                Ok(response) => {
                    Span::current().record("http.status_code", response.status);
                    debug!(event = "operation.completed");
                }
                Err(error) => {
                    trace::record_current_error(error);
                    error!(event = "operation.failed");
                }
            }
            result
        }
        .instrument(span)
        .await
    }

    /// Collect a read response into owned chunks and payload metadata.
    ///
    /// For `live = sse`, this method consumes the SSE stream and returns the
    /// collected chunks rather than exposing the raw event stream directly.
    pub async fn read_raw(&self, path: &str, request: &ReadRequest) -> Result<ReadResponse, Error> {
        let span = trace::client_operation_span(
            "read_stream",
            path,
            self.server_address(),
            self.auth_type(),
        );
        span.record("ds.live_mode", trace::live_mode_name(request.live));
        trace::record_optional_str(&span, "ds.offset", request.offset.as_deref());
        trace::record_optional_str(&span, "ds.cursor", request.cursor.as_deref());
        async {
            debug!(event = "operation.started");
            let result = self
                .retry_policy()
                .run(|| self.read_once(path, request))
                .await;
            match &result {
                Ok(response) => {
                    Span::current().record("http.status_code", response.status);
                    Span::current().record("ds.up_to_date", response.up_to_date);
                    Span::current().record("ds.stream_closed", response.stream_closed);
                    trace::record_current_optional_str(
                        "ds.resume_offset",
                        Some(response.next_offset.as_str()),
                    );
                    trace::record_current_optional_str("ds.cursor", response.cursor.as_deref());
                    debug!(
                        event = "operation.completed",
                        "ds.chunk_count" = response.chunks.len()
                    );
                }
                Err(error) => {
                    trace::record_current_error(error);
                    error!(event = "operation.failed");
                }
            }
            result
        }
        .instrument(span)
        .await
    }

    async fn read_once(&self, path: &str, request: &ReadRequest) -> Result<ReadResponse, Error> {
        match request.live {
            LiveMode::CatchUp => self.read_http(path, request, None).await,
            LiveMode::LongPoll => self.read_long_poll(path, request).await,
            LiveMode::Sse => self.read_sse(path, request).await,
            LiveMode::Auto => self.read_auto(path, request).await,
        }
    }

    async fn read_long_poll(
        &self,
        path: &str,
        request: &ReadRequest,
    ) -> Result<ReadResponse, Error> {
        let started_at = Instant::now();
        let budget = request.timeout;
        let max_chunks = request.max_chunks.unwrap_or(usize::MAX);
        let mut current_request = request.clone();
        current_request.live = LiveMode::LongPoll;

        let mut aggregate: Option<ReadResponse> = None;

        loop {
            if let Some(budget) = budget {
                let elapsed = started_at.elapsed();
                if elapsed >= budget {
                    debug!(event = "read.long_poll_budget_exhausted");
                    return Ok(aggregate.unwrap_or(ReadResponse {
                        status: 200,
                        next_offset: current_request
                            .offset
                            .clone()
                            .unwrap_or_else(|| "-1".to_string()),
                        up_to_date: true,
                        stream_closed: false,
                        cursor: None,
                        content_type: None,
                        etag: None,
                        chunks: Vec::new(),
                        payload: None,
                    }));
                }
                current_request.timeout = Some(budget - elapsed);
            }

            let response = self
                .read_http(path, &current_request, Some("long-poll"))
                .await?;
            let next_offset_for_cursor = response.next_offset.clone();
            match &mut aggregate {
                Some(collected) => {
                    collected.status = response.status;
                    collected.next_offset = response.next_offset;
                    collected.up_to_date = response.up_to_date;
                    collected.stream_closed = response.stream_closed;
                    collected.cursor = response.cursor;
                    if response.content_type.is_some() {
                        collected.content_type = response.content_type;
                    }
                    if response.etag.is_some() {
                        collected.etag = response.etag;
                    }
                    collected.chunks.extend(response.chunks);
                    if collected.payload.is_none() {
                        collected.payload = response.payload;
                    }
                }
                None => aggregate = Some(response),
            }

            let done = aggregate.as_ref().is_some_and(|collected| {
                collected.stream_closed
                    || collected.chunks.len() >= max_chunks
                    || (request.wait_for_up_to_date && collected.up_to_date)
            });
            if done {
                return Ok(aggregate.expect("aggregate exists"));
            }

            current_request.offset = Some(next_offset_for_cursor);
            trace::record_current_optional_str(
                "ds.resume_offset",
                current_request.offset.as_deref(),
            );
            debug!(event = "read.resume_updated");
        }
    }

    async fn read_http(
        &self,
        path: &str,
        request: &ReadRequest,
        live: Option<&str>,
    ) -> Result<ReadResponse, Error> {
        let url = self.read_url(path, request, live)?;
        let future = async {
            let response = self
                .send_request(
                    "read_stream",
                    path,
                    Method::GET,
                    url,
                    &request.options,
                    |builder| {
                        if let Some(etag) = &request.if_none_match {
                            builder.header(reqwest::header::IF_NONE_MATCH, etag)
                        } else {
                            builder
                        }
                    },
                )
                .await?;
            let response = Self::require_success(response).await?;
            collect_catch_up(response).await
        };

        match request.timeout {
            Some(timeout) => match tokio::time::timeout(timeout, future).await {
                Ok(result) => result,
                Err(_) if matches!(request.live, LiveMode::LongPoll) => {
                    debug!(event = "read.long_poll_timeout");
                    Ok(ReadResponse {
                        status: 204,
                        next_offset: request.offset.clone().unwrap_or_else(|| "-1".to_string()),
                        up_to_date: true,
                        stream_closed: false,
                        cursor: None,
                        content_type: None,
                        etag: None,
                        chunks: Vec::new(),
                        payload: None,
                    })
                }
                Err(_) => Err(Error::parse("timed out waiting for response")),
            },
            None => future.await,
        }
    }

    async fn read_sse(&self, path: &str, request: &ReadRequest) -> Result<ReadResponse, Error> {
        let url = self.read_url(path, request, Some("sse"))?;
        let future = async {
            let response = self
                .send_request(
                    "read_stream",
                    path,
                    Method::GET,
                    url,
                    &request.options,
                    |builder| builder,
                )
                .await?;
            let response = Self::require_success(response).await?;
            collect_sse(response, request.max_chunks, request.wait_for_up_to_date).await
        };

        match request.timeout {
            Some(timeout) => tokio::time::timeout(timeout, future)
                .await
                .map_err(|_| Error::invalid_argument("timed out waiting for SSE response"))?,
            None => future.await,
        }
    }

    async fn read_auto(&self, path: &str, request: &ReadRequest) -> Result<ReadResponse, Error> {
        let mut catch_up = request.clone();
        catch_up.live = LiveMode::CatchUp;
        let initial = self.read_http(path, &catch_up, None).await?;

        if initial.stream_closed || !initial.chunks.is_empty() {
            return Ok(initial);
        }

        debug!(
            event = "read.auto_fallback",
            "ds.resume_offset" = initial.next_offset.as_str()
        );
        let mut live = request.clone();
        live.live = LiveMode::LongPoll;
        live.offset = Some(initial.next_offset.clone());
        let follow = self.read_http(path, &live, Some("long-poll")).await?;
        if follow.chunks.is_empty() {
            Ok(initial)
        } else {
            Ok(follow)
        }
    }

    /// Start a background subscription for one stream.
    ///
    /// The returned [`Subscription`] can be polled with [`Subscription::next`]
    /// or aborted explicitly with [`Subscription::abort`].
    pub fn subscribe_raw(&self, path: &str, request: SubscribeRequest) -> Subscription {
        let client = self.clone();
        let path = path.to_string();
        let server_address = self.server_address().to_string();
        let auth_type = self.auth_type();
        let subscription_span = trace::subscription_span(&path, server_address.as_str(), auth_type);
        subscription_span.record("ds.live_mode", trace::live_mode_name(request.read.live));
        trace::record_optional_str(
            &subscription_span,
            "ds.offset",
            request.read.offset.as_deref(),
        );
        trace::record_optional_str(
            &subscription_span,
            "ds.cursor",
            request.read.cursor.as_deref(),
        );
        let (sender, receiver) = mpsc::channel(32);
        let task = tokio::spawn(
            async move {
                debug!(event = "subscription.started");
                let mut next_request = request.read;
                loop {
                    match client.read_raw(&path, &next_request).await {
                        Ok(response) => {
                            Span::current().record("ds.up_to_date", response.up_to_date);
                            Span::current().record("ds.stream_closed", response.stream_closed);
                            trace::record_current_optional_str(
                                "ds.resume_offset",
                                Some(response.next_offset.as_str()),
                            );
                            trace::record_current_optional_str(
                                "ds.cursor",
                                response.cursor.as_deref(),
                            );
                            debug!(
                                event = "subscription.event",
                                "ds.chunk_count" = response.chunks.len()
                            );
                            let event = response_to_event(&response);
                            if sender.send(Ok(event)).await.is_err() {
                                debug!(event = "subscription.consumer_dropped");
                                break;
                            }
                            if response.stream_closed {
                                debug!(event = "subscription.completed");
                                break;
                            }
                            next_request.offset = Some(response.next_offset);
                            next_request.live = if matches!(next_request.live, LiveMode::CatchUp) {
                                LiveMode::LongPoll
                            } else {
                                next_request.live
                            };
                            trace::record_current_optional_str(
                                "ds.offset",
                                next_request.offset.as_deref(),
                            );
                            Span::current()
                                .record("ds.live_mode", trace::live_mode_name(next_request.live));
                            debug!(event = "subscription.resume_updated");
                        }
                        Err(error) => {
                            let _ = sender.send(Err(error)).await;
                            debug!(event = "subscription.terminated");
                            break;
                        }
                    }
                }
            }
            .instrument(subscription_span),
        );

        Subscription { receiver, task }
    }

    fn request(
        &self,
        method: Method,
        url: Url,
        options: &RequestOptions,
    ) -> reqwest::RequestBuilder {
        let mut builder = self.inner.http.request(method, url);
        builder = self.apply_default_headers(builder);
        builder = self.apply_request_headers(builder, options);
        self.apply_auth(builder)
    }

    fn apply_default_headers(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.inner.default_headers.is_empty() {
            builder
        } else {
            builder.headers(self.inner.default_headers.clone())
        }
    }

    fn apply_request_headers(
        &self,
        mut builder: reqwest::RequestBuilder,
        options: &RequestOptions,
    ) -> reqwest::RequestBuilder {
        for (name, value) in &options.headers {
            builder = builder.header(name, value);
        }
        builder
    }

    fn apply_auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.inner.config.auth {
            AuthConfig::None => builder,
            auth => auth.apply(builder),
        }
    }

    fn stream_url(&self, path: &str, options: &RequestOptions) -> Result<Url, Error> {
        let mut url = self
            .inner
            .config
            .base_url
            .join(path.trim_start_matches('/'))?;
        {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in &self.inner.config.defaults.query {
                pairs.append_pair(key, value);
            }
            for (key, value) in &options.query {
                pairs.append_pair(key, value);
            }
        }
        Ok(url)
    }

    fn read_url(
        &self,
        path: &str,
        request: &ReadRequest,
        live: Option<&str>,
    ) -> Result<Url, Error> {
        let mut url = self.stream_url(path, &request.options)?;
        {
            let mut pairs = url.query_pairs_mut();
            if let Some(offset) = &request.offset {
                pairs.append_pair("offset", offset);
            } else if live.is_some() {
                pairs.append_pair("offset", "-1");
            }
            if let Some(live) = live {
                pairs.append_pair("live", live);
            }
            if let Some(cursor) = &request.cursor {
                pairs.append_pair("cursor", cursor);
            }
        }
        Ok(url)
    }
}

fn build_default_headers(config: &ClientConfig) -> Result<HeaderMap, Error> {
    let mut headers = HeaderMap::with_capacity(config.defaults.headers.len());
    for (name, value) in &config.defaults.headers {
        let header_name = HeaderName::try_from(name.as_str()).map_err(|error| {
            Error::invalid_argument(format!("invalid default header name '{name}': {error}"))
        })?;
        let header_value = HeaderValue::try_from(value.as_str()).map_err(|error| {
            Error::invalid_argument(format!(
                "invalid default header value for '{name}': {error}"
            ))
        })?;
        headers.append(header_name, header_value);
    }
    Ok(headers)
}

impl ClientBuilder {
    /// Create a builder with the crate defaults.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: ClientConfig::default(),
            pending_error: None,
        }
    }

    /// Set the Durable Streams server base URL.
    #[must_use]
    pub fn base_url(mut self, url: impl AsRef<str>) -> Self {
        if self.pending_error.is_none() {
            match Url::parse(url.as_ref()) {
                Ok(parsed) => self.config.base_url = parsed,
                Err(error) => self.pending_error = Some(error.into()),
            }
        }
        self
    }

    /// Configure bearer token authentication.
    #[must_use]
    pub fn bearer_auth(mut self, token: impl Into<String>) -> Self {
        self.config.auth = AuthConfig::Bearer {
            token: token.into(),
        };
        self
    }

    /// Configure basic authentication.
    #[must_use]
    pub fn basic_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.config.auth = AuthConfig::Basic {
            username: username.into(),
            password: password.into(),
        };
        self
    }

    /// Configure static header authentication.
    #[must_use]
    pub fn header_auth(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.auth = AuthConfig::Header {
            name: name.into(),
            value: value.into(),
        };
        self
    }

    /// Set the connect timeout for new requests.
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.transport.connect_timeout = timeout;
        self
    }

    /// Set the per-request timeout.
    #[must_use]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.transport.request_timeout = timeout;
        self
    }

    /// Set the default user agent.
    #[must_use]
    pub fn user_agent(mut self, value: impl Into<String>) -> Self {
        self.config.transport.user_agent = value.into();
        self
    }

    /// Set the proxy URL for the underlying HTTP client.
    #[must_use]
    pub fn proxy_url(mut self, value: impl Into<String>) -> Self {
        self.config.transport.proxy_url = Some(value.into());
        self
    }

    /// Set the default content type used by the ergonomic stream API.
    #[must_use]
    pub fn default_content_type(mut self, value: impl Into<String>) -> Self {
        self.config.defaults.default_content_type = Some(value.into());
        self
    }

    /// Add a default header applied to every request.
    #[must_use]
    pub fn default_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.config
            .defaults
            .headers
            .insert(name.into(), value.into());
        self
    }

    /// Add a default query parameter applied to every request.
    #[must_use]
    pub fn default_query(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.defaults.query.insert(name.into(), value.into());
        self
    }

    /// Override the retry policy.
    #[must_use]
    pub fn retry(mut self, retry: crate::model::RetryOptions) -> Self {
        self.config.retry = retry;
        self
    }

    /// Build the configured client.
    pub fn build(self) -> Result<Client, Error> {
        if let Some(error) = self.pending_error {
            return Err(error);
        }
        Client::from_config(self.config)
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CreateBuilder {
    /// Override the content type used for stream creation.
    #[must_use]
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.content_type = Some(value.into());
        self
    }

    /// Set the stream TTL in seconds.
    #[must_use]
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl_seconds = Some(ttl.as_secs());
        self
    }

    /// Set the stream TTL in seconds directly.
    #[must_use]
    pub fn ttl_seconds(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = Some(ttl_seconds);
        self
    }

    /// Set an explicit expiry timestamp string.
    #[must_use]
    pub fn expires_at(mut self, value: impl Into<String>) -> Self {
        self.expires_at = Some(value.into());
        self
    }

    /// Mark the stream as closed immediately after creation.
    #[must_use]
    pub fn closed(mut self, closed: bool) -> Self {
        self.closed = closed;
        self
    }

    /// Add an initial body payload.
    #[must_use]
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Add one request header to this operation.
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.headers.insert(name.into(), value.into());
        self
    }

    /// Add one query parameter to this operation.
    #[must_use]
    pub fn query(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.query.insert(name.into(), value.into());
        self
    }

    /// Replace the low-level request options for this operation.
    #[must_use]
    pub fn request_options(mut self, options: RequestOptions) -> Self {
        self.options = options;
        self
    }

    fn into_raw(self) -> CreateStreamRequest {
        CreateStreamRequest {
            content_type: self
                .content_type
                .unwrap_or_else(|| "application/octet-stream".to_string()),
            ttl_seconds: self.ttl_seconds,
            expires_at: self.expires_at,
            closed: self.closed,
            body: self.body,
            options: self.options,
        }
    }

    /// Execute the create operation.
    pub async fn send(self) -> Result<CreateAck, Error> {
        let stream = self.stream.clone();
        let request = self.into_raw();
        let response = stream.create_raw(&request).await?;
        Ok(CreateAck::from(response))
    }
}

impl AppendBuilder {
    /// Override the content type used for this append.
    #[must_use]
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.content_type = Some(value.into());
        self
    }

    /// Set the expected stream sequence token.
    #[must_use]
    pub fn expected_seq(mut self, value: impl Into<String>) -> Self {
        self.expected_seq = Some(value.into());
        self
    }

    /// Add one request header to this operation.
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.headers.insert(name.into(), value.into());
        self
    }

    /// Add one query parameter to this operation.
    #[must_use]
    pub fn query(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.query.insert(name.into(), value.into());
        self
    }

    /// Replace the low-level request options for this operation.
    #[must_use]
    pub fn request_options(mut self, options: RequestOptions) -> Self {
        self.options = options;
        self
    }

    fn into_raw(self) -> AppendRequest {
        AppendRequest {
            body: self.body,
            content_type: self.content_type,
            stream_seq: self.expected_seq,
            producer: None,
            options: self.options,
        }
    }

    /// Execute the append operation.
    pub async fn send(self) -> Result<AppendAck, Error> {
        let stream = self.stream.clone();
        let request = self.into_raw();
        let response = stream.append_raw(&request).await?;
        Ok(AppendAck::from(response))
    }
}

impl CloseBuilder {
    /// Add a final body payload.
    #[must_use]
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Override the content type used for the final close payload.
    #[must_use]
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.content_type = Some(value.into());
        self
    }

    /// Add one request header to this operation.
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.headers.insert(name.into(), value.into());
        self
    }

    /// Add one query parameter to this operation.
    #[must_use]
    pub fn query(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.query.insert(name.into(), value.into());
        self
    }

    /// Replace the low-level request options for this operation.
    #[must_use]
    pub fn request_options(mut self, options: RequestOptions) -> Self {
        self.options = options;
        self
    }

    fn into_raw(self) -> CloseStreamRequest {
        CloseStreamRequest {
            body: self.body,
            content_type: self.content_type,
            producer: None,
            options: self.options,
        }
    }

    /// Execute the close operation.
    pub async fn send(self) -> Result<CloseAck, Error> {
        let stream = self.stream.clone();
        let request = self.into_raw();
        let response = stream.close_raw(&request).await?;
        Ok(CloseAck::from(response))
    }
}

impl ReadBuilder {
    /// Start reading from a different offset.
    #[must_use]
    pub fn offset(mut self, offset: impl Into<Offset>) -> Self {
        self.offset = offset.into();
        self
    }

    /// Set the live mode used for this read.
    #[must_use]
    pub fn live(mut self, live: LiveMode) -> Self {
        self.live = live;
        self
    }

    /// Bound the request time budget.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Limit the number of chunks collected.
    #[must_use]
    pub fn max_chunks(mut self, max_chunks: usize) -> Self {
        self.max_chunks = Some(max_chunks);
        self
    }

    /// Keep reading until the server reports that the stream is up to date.
    #[must_use]
    pub fn until_up_to_date(mut self) -> Self {
        self.wait_for_up_to_date = true;
        self
    }

    /// Send an initial request-collapsing cursor.
    #[must_use]
    pub fn cursor(mut self, value: impl Into<String>) -> Self {
        self.cursor = Some(value.into());
        self
    }

    /// Set an `If-None-Match` precondition.
    #[must_use]
    pub fn if_none_match(mut self, value: impl Into<String>) -> Self {
        self.if_none_match = Some(value.into());
        self
    }

    /// Add one request header to this operation.
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.headers.insert(name.into(), value.into());
        self
    }

    /// Add one query parameter to this operation.
    #[must_use]
    pub fn query(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.query.insert(name.into(), value.into());
        self
    }

    /// Replace the low-level request options for this operation.
    #[must_use]
    pub fn request_options(mut self, options: RequestOptions) -> Self {
        self.options = options;
        self
    }

    fn to_raw_request(&self) -> ReadRequest {
        ReadRequest {
            offset: Some(self.offset.to_string()),
            live: self.live,
            timeout: self.timeout,
            max_chunks: self.max_chunks,
            wait_for_up_to_date: self.wait_for_up_to_date,
            cursor: self.cursor.clone(),
            if_none_match: self.if_none_match.clone(),
            options: self.options.clone(),
        }
    }

    /// Execute the collected read operation.
    pub async fn send(self) -> Result<ReadPage, Error> {
        let stream = self.stream.clone();
        let request = self.to_raw_request();
        let response = stream.read_raw(&request).await?;
        Ok(ReadPage::from(response))
    }
}

impl StreamHandle {
    /// Return the stream path bound to this handle.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Start building a stream creation request.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// orders
    ///     .create()
    ///     .content_type("application/json")
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn create(&self) -> CreateBuilder {
        CreateBuilder {
            stream: self.clone(),
            content_type: self.client.default_content_type().map(ToOwned::to_owned),
            ttl_seconds: None,
            expires_at: None,
            closed: false,
            body: None,
            options: RequestOptions::default(),
        }
    }

    /// Fetch stream metadata.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// let info = orders.head().await?;
    /// println!("{:?}", info.content_type);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn head(&self) -> Result<StreamInfo, Error> {
        let response = self
            .client
            .head_raw(&self.path, &HeadRequest::default())
            .await?;
        Ok(StreamInfo::from(response))
    }

    /// Append raw bytes to this stream.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// orders
    ///     .append("hello world")
    ///     .content_type("text/plain")
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn append(&self, body: impl Into<Bytes>) -> AppendBuilder {
        AppendBuilder {
            stream: self.clone(),
            body: body.into(),
            content_type: self.client.default_content_type().map(ToOwned::to_owned),
            expected_seq: None,
            options: RequestOptions::default(),
        }
    }

    /// Serialize one JSON value and append it with `application/json`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// orders
    ///     .append_json(&serde_json::json!({ "type": "created" }))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn append_json<T>(&self, value: &T) -> Result<AppendAck, Error>
    where
        T: Serialize,
    {
        let body = serde_json::to_vec(value)?;
        self.append(body)
            .content_type("application/json")
            .send()
            .await
    }

    /// Start building a close request.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// let close = orders.close().send().await?;
    /// println!("{}", close.final_offset);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn close(&self) -> CloseBuilder {
        CloseBuilder {
            stream: self.clone(),
            body: None,
            content_type: self.client.default_content_type().map(ToOwned::to_owned),
            options: RequestOptions::default(),
        }
    }

    /// Start building a collected read request.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig, LiveMode, Offset};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// let page = orders
    ///     .read()
    ///     .offset(Offset::Beginning)
    ///     .live(LiveMode::CatchUp)
    ///     .send()
    ///     .await?;
    ///
    /// println!("{}", page.next_offset);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn read(&self) -> ReadBuilder {
        ReadBuilder {
            stream: self.clone(),
            offset: Offset::Beginning,
            live: LiveMode::CatchUp,
            timeout: None,
            max_chunks: None,
            wait_for_up_to_date: false,
            cursor: None,
            if_none_match: None,
            options: RequestOptions::default(),
        }
    }

    /// Delete this stream.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// orders.delete().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(&self) -> Result<(), Error> {
        self.client
            .delete_raw(&self.path, &DeleteRequest::default())
            .await?;
        Ok(())
    }

    /// Create this stream using the protocol-shaped raw request API.
    pub async fn create_raw(
        &self,
        request: &CreateStreamRequest,
    ) -> Result<CreateStreamResponse, Error> {
        self.client.create_raw(&self.path, request).await
    }

    /// Fetch metadata for this stream using the protocol-shaped raw API.
    pub async fn connect_raw(&self, request: &ConnectRequest) -> Result<ConnectResponse, Error> {
        self.client.connect_raw(&self.path, request).await
    }

    /// Append to this stream using the protocol-shaped raw API.
    pub async fn append_raw(&self, request: &AppendRequest) -> Result<AppendResponse, Error> {
        self.client.append_raw(&self.path, request).await
    }

    /// Read from this stream using the protocol-shaped raw API.
    pub async fn read_raw(&self, request: &ReadRequest) -> Result<ReadResponse, Error> {
        self.client.read_raw(&self.path, request).await
    }

    /// Close this stream using the protocol-shaped raw API.
    pub async fn close_raw(
        &self,
        request: &CloseStreamRequest,
    ) -> Result<CloseStreamResponse, Error> {
        self.client.close_raw(&self.path, request).await
    }

    /// Fetch metadata for this stream using a raw HEAD request.
    pub async fn head_raw(&self, request: &HeadRequest) -> Result<HeadResponse, Error> {
        self.client.head_raw(&self.path, request).await
    }

    /// Delete this stream using the protocol-shaped raw API.
    pub async fn delete_raw(&self, request: &DeleteRequest) -> Result<DeleteResponse, Error> {
        self.client.delete_raw(&self.path, request).await
    }

    /// Start a background subscription using the raw read request model.
    #[must_use]
    pub fn subscribe_raw(&self, request: SubscribeRequest) -> Subscription {
        self.client.subscribe_raw(&self.path, request)
    }
}

impl Subscription {
    /// Receive the next subscription event, or `None` when the task has finished.
    pub async fn next(&mut self) -> Option<Result<SubscriptionEvent, Error>> {
        self.receiver.recv().await
    }

    /// Abort the background subscription task.
    pub fn abort(&self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, LiveMode, Offset};

    #[test]
    fn create_builder_uses_client_default_content_type() {
        let client = Client::builder()
            .default_content_type("application/json")
            .build()
            .expect("client builds");
        let request = client.stream("/orders").create().into_raw();

        assert_eq!(request.content_type, "application/json");
    }

    #[test]
    fn append_builder_maps_expected_seq_to_raw_stream_seq() {
        let client = Client::builder().build().expect("client builds");
        let request = client
            .stream("/orders")
            .append("payload")
            .expected_seq("42-0")
            .into_raw();

        assert_eq!(request.stream_seq.as_deref(), Some("42-0"));
    }

    #[test]
    fn read_builder_uses_typed_offsets() {
        let client = Client::builder().build().expect("client builds");
        let request = client
            .stream("/orders")
            .read()
            .offset(Offset::Now)
            .live(LiveMode::Auto)
            .until_up_to_date()
            .to_raw_request();

        assert_eq!(request.offset.as_deref(), Some("now"));
        assert_eq!(request.live, LiveMode::Auto);
        assert!(request.wait_for_up_to_date);
    }
}
