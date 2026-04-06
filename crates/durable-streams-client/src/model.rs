use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Shared per-request headers and query parameters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestOptions {
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub query: HashMap<String, String>,
}

/// Client retry options.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RetryOptions {
    pub max_retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub backoff_multiplier: f64,
}

/// Producer headers for idempotent appends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerRequest {
    pub producer_id: String,
    pub producer_epoch: i64,
    pub producer_seq: i64,
}

/// Read mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LiveMode {
    #[default]
    CatchUp,
    LongPoll,
    Sse,
    Auto,
}

/// Request body representation.
#[derive(Debug, Clone, PartialEq)]
pub enum ReadPayload {
    Bytes(Bytes),
    Json(Vec<serde_json::Value>),
}

/// Single read chunk for collected read responses or subscriptions.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadChunk {
    pub data: Bytes,
    pub next_offset: String,
}

/// Event yielded by subscriptions.
#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionEvent {
    pub chunk: Option<ReadChunk>,
    pub next_offset: String,
    pub up_to_date: bool,
    pub stream_closed: bool,
    pub cursor: Option<String>,
}

/// Request to create a stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateStreamRequest {
    pub content_type: String,
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub body: Option<Bytes>,
    #[serde(default)]
    pub options: RequestOptions,
}

/// Create response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamResponse {
    pub status: u16,
    pub next_offset: Option<String>,
    pub stream_closed: bool,
}

/// Request to connect to a known stream.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectRequest {
    #[serde(default)]
    pub options: RequestOptions,
}

/// Connect response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectResponse {
    pub status: u16,
    pub offset: Option<String>,
    pub content_type: Option<String>,
    pub stream_closed: bool,
}

/// Request to append to a stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendRequest {
    pub body: Bytes,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub stream_seq: Option<String>,
    #[serde(default)]
    pub producer: Option<ProducerRequest>,
    #[serde(default)]
    pub options: RequestOptions,
}

/// Append response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResponse {
    pub status: u16,
    pub next_offset: Option<String>,
    pub stream_closed: bool,
    pub producer_epoch: Option<i64>,
    pub producer_seq: Option<i64>,
}

/// Read request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadRequest {
    #[serde(default)]
    pub offset: Option<String>,
    #[serde(default)]
    pub live: LiveMode,
    #[serde(default)]
    pub timeout: Option<Duration>,
    #[serde(default)]
    pub max_chunks: Option<usize>,
    #[serde(default)]
    pub wait_for_up_to_date: bool,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub if_none_match: Option<String>,
    #[serde(default)]
    pub options: RequestOptions,
}

impl Default for ReadRequest {
    fn default() -> Self {
        Self {
            offset: None,
            live: LiveMode::CatchUp,
            timeout: None,
            max_chunks: None,
            wait_for_up_to_date: false,
            cursor: None,
            if_none_match: None,
            options: RequestOptions::default(),
        }
    }
}

/// Collected read response.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadResponse {
    pub status: u16,
    pub next_offset: String,
    pub up_to_date: bool,
    pub stream_closed: bool,
    pub cursor: Option<String>,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub chunks: Vec<ReadChunk>,
    pub payload: Option<ReadPayload>,
}

/// HEAD request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadRequest {
    #[serde(default)]
    pub options: RequestOptions,
}

/// HEAD response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadResponse {
    pub status: u16,
    pub offset: Option<String>,
    pub content_type: Option<String>,
    pub ttl_seconds: Option<u64>,
    pub expires_at: Option<String>,
    pub stream_closed: bool,
    pub etag: Option<String>,
}

/// DELETE request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteRequest {
    #[serde(default)]
    pub options: RequestOptions,
}

/// DELETE response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteResponse {
    pub status: u16,
}

/// Close request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseStreamRequest {
    #[serde(default)]
    pub body: Option<Bytes>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub producer: Option<ProducerRequest>,
    #[serde(default)]
    pub options: RequestOptions,
}

/// Close response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseStreamResponse {
    pub status: u16,
    pub final_offset: String,
    pub stream_closed: bool,
}

/// Subscription request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeRequest {
    pub read: ReadRequest,
}
