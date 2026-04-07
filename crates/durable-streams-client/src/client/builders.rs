use super::{AppendBuilder, Client, ClientBuilder, CloseBuilder, CreateBuilder, ReadBuilder};
use crate::auth::AuthConfig;
use crate::config::ClientConfig;
use crate::error::Error;
use crate::model::{
    AppendRequest, CloseStreamRequest, CreateStreamRequest, LiveMode, ReadRequest, RequestOptions,
};
use crate::types::{AppendOutcome, CloseOutcome, CreateOutcome, Offset, ReadPage};
use bytes::Bytes;
use reqwest::Url;
use std::time::Duration;

impl ClientBuilder {
    /// Create a builder with the crate defaults.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: ClientConfig::default(),
            pending_error: None,
        }
    }

    /// Set the Durable Streams stream collection base URL.
    ///
    /// Stream paths from [`crate::Client::stream`] are joined directly onto
    /// this base URL. For the workspace server, that means using a value like
    /// `http://127.0.0.1:4437/v1/stream/` rather than just the server origin.
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

    /// Replace all low-level request options for this operation.
    ///
    /// This overwrites any headers or query parameters already added on this
    /// builder.
    #[must_use]
    pub fn replace_options(mut self, options: RequestOptions) -> Self {
        self.options = options;
        self
    }

    pub(super) fn into_raw(self) -> CreateStreamRequest {
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
    ///
    /// Content type precedence is:
    /// 1. [`CreateBuilder::content_type`]
    /// 2. the client `default_content_type`
    /// 3. `application/octet-stream`
    pub async fn send(self) -> Result<CreateOutcome, Error> {
        let stream = self.stream.clone();
        let request = self.into_raw();
        let response = stream.create_raw(&request).await?;
        Ok(CreateOutcome::from(response))
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

    /// Replace all low-level request options for this operation.
    ///
    /// This overwrites any headers or query parameters already added on this
    /// builder.
    #[must_use]
    pub fn replace_options(mut self, options: RequestOptions) -> Self {
        self.options = options;
        self
    }

    pub(super) fn into_raw(self) -> AppendRequest {
        AppendRequest {
            body: self.body,
            content_type: self.content_type,
            stream_seq: self.expected_seq,
            producer: None,
            options: self.options,
        }
    }

    /// Execute the append operation.
    pub async fn send(self) -> Result<AppendOutcome, Error> {
        let stream = self.stream.clone();
        let request = self.into_raw();
        let response = stream.append_raw(&request).await?;
        Ok(AppendOutcome::from(response))
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

    /// Replace all low-level request options for this operation.
    ///
    /// This overwrites any headers or query parameters already added on this
    /// builder.
    #[must_use]
    pub fn replace_options(mut self, options: RequestOptions) -> Self {
        self.options = options;
        self
    }

    pub(super) fn into_raw(self) -> CloseStreamRequest {
        CloseStreamRequest {
            body: self.body,
            content_type: self.content_type,
            producer: None,
            options: self.options,
        }
    }

    /// Execute the close operation.
    pub async fn send(self) -> Result<CloseOutcome, Error> {
        let stream = self.stream.clone();
        let request = self.into_raw();
        let response = stream.close_raw(&request).await?;
        Ok(CloseOutcome::from(response))
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

    /// Replace all low-level request options for this operation.
    ///
    /// This overwrites any headers or query parameters already added on this
    /// builder.
    #[must_use]
    pub fn replace_options(mut self, options: RequestOptions) -> Self {
        self.options = options;
        self
    }

    pub(super) fn to_raw_request(&self) -> ReadRequest {
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
