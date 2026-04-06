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
use bytes::Bytes;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode, Url};
use std::sync::Arc;
use std::time::Instant;
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
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

/// Stream-scoped view over a client.
#[derive(Clone)]
pub struct StreamHandle {
    client: Client,
    path: String,
}

/// Background subscription handle.
pub struct Subscription {
    receiver: mpsc::Receiver<Result<SubscriptionEvent, Error>>,
    task: JoinHandle<()>,
}

#[derive(Clone, Copy)]
pub(crate) struct ProducerHeaders<'a> {
    pub producer_id: &'a str,
    pub producer_epoch: i64,
    pub producer_seq: i64,
}

impl Client {
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

    pub fn new(config: ClientConfig) -> Result<Self, Error> {
        Self::from_config(config)
    }

    #[must_use]
    pub fn config(&self) -> &ClientConfig {
        &self.inner.config
    }

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

    pub async fn create(
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

    pub async fn connect(
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

    pub async fn append(
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

    pub async fn close(
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

    pub async fn head(&self, path: &str, request: &HeadRequest) -> Result<HeadResponse, Error> {
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

    pub async fn delete(
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

    pub async fn read(&self, path: &str, request: &ReadRequest) -> Result<ReadResponse, Error> {
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

    pub fn subscribe(&self, path: &str, request: SubscribeRequest) -> Subscription {
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
                    match client.read(&path, &next_request).await {
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

impl StreamHandle {
    pub async fn create(
        &self,
        request: &CreateStreamRequest,
    ) -> Result<CreateStreamResponse, Error> {
        self.client.create(&self.path, request).await
    }

    pub async fn connect(&self, request: &ConnectRequest) -> Result<ConnectResponse, Error> {
        self.client.connect(&self.path, request).await
    }

    pub async fn append(&self, request: &AppendRequest) -> Result<AppendResponse, Error> {
        self.client.append(&self.path, request).await
    }

    pub async fn read(&self, request: &ReadRequest) -> Result<ReadResponse, Error> {
        self.client.read(&self.path, request).await
    }

    pub async fn close(&self, request: &CloseStreamRequest) -> Result<CloseStreamResponse, Error> {
        self.client.close(&self.path, request).await
    }

    pub async fn head(&self, request: &HeadRequest) -> Result<HeadResponse, Error> {
        self.client.head(&self.path, request).await
    }

    pub async fn delete(&self, request: &DeleteRequest) -> Result<DeleteResponse, Error> {
        self.client.delete(&self.path, request).await
    }
}

impl Subscription {
    pub async fn next(&mut self) -> Option<Result<SubscriptionEvent, Error>> {
        self.receiver.recv().await
    }

    pub fn abort(&self) {
        self.task.abort();
    }
}
