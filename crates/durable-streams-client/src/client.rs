use crate::auth::AuthConfig;
use crate::config::ClientConfig;
use crate::error::Error;
use crate::model::{
    AppendRequest, AppendResponse, CloseStreamRequest, CloseStreamResponse, ConnectRequest,
    ConnectResponse, CreateStreamRequest, CreateStreamResponse, DeleteRequest, DeleteResponse,
    HeadRequest, HeadResponse, LiveMode, ReadRequest, ReadResponse, RequestOptions,
    SubscribeRequest, SubscriptionEvent,
};
use crate::protocol::{
    collect_catch_up, collect_sse, header_value, parse_bool_header, parse_i64_header,
    response_error, response_to_event, PRODUCER_EPOCH, PRODUCER_SEQ, STREAM_CLOSED,
    STREAM_EXPIRES_AT, STREAM_NEXT_OFFSET, STREAM_SEQ, STREAM_TTL,
};
use crate::retry::RetryPolicy;
use reqwest::header::CONTENT_TYPE;
use reqwest::{Method, StatusCode, Url};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Clone)]
struct ClientInner {
    config: ClientConfig,
    http: reqwest::Client,
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

impl Client {
    pub fn from_config(config: ClientConfig) -> Result<Self, Error> {
        config.validate()?;

        let mut builder = reqwest::Client::builder()
            .connect_timeout(config.transport.connect_timeout)
            .timeout(config.transport.request_timeout)
            .user_agent(config.transport.user_agent.clone());

        if let Some(proxy_url) = &config.transport.proxy_url {
            builder = builder.proxy(reqwest::Proxy::all(proxy_url)?);
        }

        let http = builder.build()?;
        Ok(Self {
            inner: Arc::new(ClientInner { config, http }),
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

    pub async fn create(&self, path: &str, request: &CreateStreamRequest) -> Result<CreateStreamResponse, Error> {
        let url = self.stream_url(path, &request.options)?;
        let retry = RetryPolicy::new(self.inner.config.retry);
        retry
            .run(|| async {
                let mut builder = self.request(Method::PUT, url.clone(), &request.options);
                builder = builder.header(CONTENT_TYPE, &request.content_type);
                if let Some(ttl_seconds) = request.ttl_seconds {
                    builder = builder.header(STREAM_TTL, ttl_seconds.to_string());
                }
                if let Some(expires_at) = &request.expires_at {
                    builder = builder.header(STREAM_EXPIRES_AT, expires_at);
                }
                if request.closed {
                    builder = builder.header(STREAM_CLOSED, "true");
                }
                if let Some(body) = &request.body {
                    builder = builder.body(body.clone());
                }
                let response = builder.send().await?;
                if !matches!(response.status(), StatusCode::OK | StatusCode::CREATED) {
                    return Err(response_error(response).await.into());
                }
                Ok(CreateStreamResponse {
                    status: response.status().as_u16(),
                    next_offset: header_value(&response, STREAM_NEXT_OFFSET),
                    stream_closed: parse_bool_header(&response, STREAM_CLOSED),
                })
            })
            .await
    }

    pub async fn connect(&self, path: &str, request: &ConnectRequest) -> Result<ConnectResponse, Error> {
        let head = self.head(
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

    pub async fn append(&self, path: &str, request: &AppendRequest) -> Result<AppendResponse, Error> {
        let url = self.stream_url(path, &request.options)?;
        let retry = RetryPolicy::new(self.inner.config.retry);
        retry
            .run(|| async {
                let mut builder = self.request(Method::POST, url.clone(), &request.options);
                if let Some(content_type) = &request.content_type {
                    builder = builder.header(CONTENT_TYPE, content_type);
                }
                if let Some(stream_seq) = &request.stream_seq {
                    builder = builder.header(STREAM_SEQ, stream_seq);
                }
                if let Some(producer) = &request.producer {
                    builder = builder
                        .header(crate::protocol::PRODUCER_ID, &producer.producer_id)
                        .header(PRODUCER_EPOCH, producer.producer_epoch.to_string())
                        .header(crate::protocol::PRODUCER_SEQ, producer.producer_seq.to_string());
                }
                let response = builder.body(request.body.clone()).send().await?;
                if !matches!(response.status(), StatusCode::OK | StatusCode::NO_CONTENT) {
                    return Err(response_error(response).await.into());
                }
                Ok(AppendResponse {
                    status: response.status().as_u16(),
                    next_offset: header_value(&response, STREAM_NEXT_OFFSET),
                    stream_closed: parse_bool_header(&response, STREAM_CLOSED),
                    producer_epoch: parse_i64_header(&response, PRODUCER_EPOCH),
                    producer_seq: parse_i64_header(&response, PRODUCER_SEQ),
                })
            })
            .await
    }

    pub async fn close(&self, path: &str, request: &CloseStreamRequest) -> Result<CloseStreamResponse, Error> {
        let url = self.stream_url(path, &request.options)?;
        let retry = RetryPolicy::new(self.inner.config.retry);
        retry
            .run(|| async {
                let mut builder = self.request(Method::POST, url.clone(), &request.options);
                builder = builder.header(STREAM_CLOSED, "true");
                if let Some(content_type) = &request.content_type {
                    builder = builder.header(CONTENT_TYPE, content_type);
                }
                if let Some(producer) = &request.producer {
                    builder = builder
                        .header(crate::protocol::PRODUCER_ID, &producer.producer_id)
                        .header(PRODUCER_EPOCH, producer.producer_epoch.to_string())
                        .header(crate::protocol::PRODUCER_SEQ, producer.producer_seq.to_string());
                }
                if let Some(body) = &request.body {
                    builder = builder.body(body.clone());
                }
                let response = builder.send().await?;
                if !matches!(response.status(), StatusCode::OK | StatusCode::NO_CONTENT) {
                    return Err(response_error(response).await.into());
                }
                Ok(CloseStreamResponse {
                    status: response.status().as_u16(),
                    final_offset: header_value(&response, STREAM_NEXT_OFFSET)
                        .ok_or_else(|| Error::parse("missing Stream-Next-Offset header"))?,
                    stream_closed: parse_bool_header(&response, STREAM_CLOSED),
                })
            })
            .await
    }

    pub async fn head(&self, path: &str, request: &HeadRequest) -> Result<HeadResponse, Error> {
        let url = self.stream_url(path, &request.options)?;
        let response = self.request(Method::HEAD, url, &request.options).send().await?;
        if !response.status().is_success() {
            return Err(response_error(response).await.into());
        }

        Ok(HeadResponse {
            status: response.status().as_u16(),
            offset: header_value(&response, STREAM_NEXT_OFFSET),
            content_type: header_value(&response, CONTENT_TYPE.as_str()),
            ttl_seconds: header_value(&response, STREAM_TTL).and_then(|value| value.parse::<u64>().ok()),
            expires_at: header_value(&response, STREAM_EXPIRES_AT),
            stream_closed: parse_bool_header(&response, STREAM_CLOSED),
            etag: header_value(&response, reqwest::header::ETAG.as_str()),
        })
    }

    pub async fn delete(&self, path: &str, request: &DeleteRequest) -> Result<DeleteResponse, Error> {
        let url = self.stream_url(path, &request.options)?;
        let response = self.request(Method::DELETE, url, &request.options).send().await?;
        if !matches!(response.status(), StatusCode::OK | StatusCode::NO_CONTENT) {
            return Err(response_error(response).await.into());
        }
        Ok(DeleteResponse {
            status: response.status().as_u16(),
        })
    }

    pub async fn read(&self, path: &str, request: &ReadRequest) -> Result<ReadResponse, Error> {
        let retry = RetryPolicy::new(self.inner.config.retry);
        retry.run(|| self.read_once(path, request)).await
    }

    async fn read_once(&self, path: &str, request: &ReadRequest) -> Result<ReadResponse, Error> {
        match request.live {
            LiveMode::CatchUp => self.read_http(path, request, None).await,
            LiveMode::LongPoll => self.read_long_poll(path, request).await,
            LiveMode::Sse => self.read_sse(path, request).await,
            LiveMode::Auto => self.read_auto(path, request).await,
        }
    }

    async fn read_long_poll(&self, path: &str, request: &ReadRequest) -> Result<ReadResponse, Error> {
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
                    return Ok(aggregate.unwrap_or(ReadResponse {
                        status: 200,
                        next_offset: current_request.offset.clone().unwrap_or_else(|| "-1".to_string()),
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

            let response = self.read_http(path, &current_request, Some("long-poll")).await?;
            match &mut aggregate {
                Some(collected) => {
                    collected.status = response.status;
                    collected.next_offset = response.next_offset.clone();
                    collected.up_to_date = response.up_to_date;
                    collected.stream_closed = response.stream_closed;
                    collected.cursor = response.cursor.clone();
                    if response.content_type.is_some() {
                        collected.content_type = response.content_type.clone();
                    }
                    if response.etag.is_some() {
                        collected.etag = response.etag.clone();
                    }
                    collected.chunks.extend(response.chunks.clone());
                    if collected.payload.is_none() {
                        collected.payload = response.payload.clone();
                    }
                }
                None => aggregate = Some(response.clone()),
            }

            let done = aggregate.as_ref().is_some_and(|collected| {
                collected.stream_closed
                    || collected.chunks.len() >= max_chunks
                    || (request.wait_for_up_to_date && collected.up_to_date)
            });
            if done {
                return Ok(aggregate.expect("aggregate exists"));
            }

            current_request.offset = Some(response.next_offset);
        }
    }

    async fn read_http(
        &self,
        path: &str,
        request: &ReadRequest,
        live: Option<&str>,
    ) -> Result<ReadResponse, Error> {
        let url = self.read_url(path, request, live)?;
        let mut builder = self.request(Method::GET, url, &request.options);
        if let Some(etag) = &request.if_none_match {
            builder = builder.header(reqwest::header::IF_NONE_MATCH, etag);
        }

        let future = async {
            let response = builder.send().await?;
            if !response.status().is_success() {
                return Err(response_error(response).await.into());
            }
            collect_catch_up(response).await
        };

        match request.timeout {
            Some(timeout) => match tokio::time::timeout(timeout, future).await {
                Ok(result) => result,
                Err(_) if matches!(request.live, LiveMode::LongPoll) => Ok(ReadResponse {
                    status: 204,
                    next_offset: request.offset.clone().unwrap_or_else(|| "-1".to_string()),
                    up_to_date: true,
                    stream_closed: false,
                    cursor: None,
                    content_type: None,
                    etag: None,
                    chunks: Vec::new(),
                    payload: None,
                }),
                Err(_) => Err(Error::parse("timed out waiting for response")),
            },
            None => future.await,
        }
    }

    async fn read_sse(&self, path: &str, request: &ReadRequest) -> Result<ReadResponse, Error> {
        let url = self.read_url(path, request, Some("sse"))?;
        let builder = self.request(Method::GET, url, &request.options);
        let future = async {
            let response = builder.send().await?;
            if !response.status().is_success() {
                return Err(response_error(response).await.into());
            }
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
        let (sender, receiver) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            let mut next_request = request.read;
            loop {
                match client.read(&path, &next_request).await {
                    Ok(response) => {
                        let event = response_to_event(&response);
                        if sender.send(Ok(event)).await.is_err() {
                            break;
                        }
                        if response.stream_closed {
                            break;
                        }
                        next_request.offset = Some(response.next_offset);
                        next_request.live = if matches!(next_request.live, LiveMode::CatchUp) {
                            LiveMode::LongPoll
                        } else {
                            next_request.live
                        };
                    }
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        break;
                    }
                }
            }
        });

        Subscription { receiver, task }
    }

    fn request(&self, method: Method, url: Url, options: &RequestOptions) -> reqwest::RequestBuilder {
        let mut builder = self.inner.http.request(method, url);
        builder = self.apply_default_headers(builder);
        builder = self.apply_request_headers(builder, options);
        self.apply_auth(builder)
    }

    fn apply_default_headers(&self, mut builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        for (name, value) in &self.inner.config.defaults.headers {
            builder = builder.header(name, value);
        }
        builder
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
        let mut url = self.inner.config.base_url.join(path.trim_start_matches('/'))?;
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

    fn read_url(&self, path: &str, request: &ReadRequest, live: Option<&str>) -> Result<Url, Error> {
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

impl StreamHandle {
    pub async fn create(&self, request: &CreateStreamRequest) -> Result<CreateStreamResponse, Error> {
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
