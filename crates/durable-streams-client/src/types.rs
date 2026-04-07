//! Application-facing types for the ergonomic client API.

use crate::model::{
    AppendResponse, CloseStreamResponse, CreateStreamResponse, HeadResponse, ReadChunk,
    ReadPayload, ReadResponse,
};
use bytes::Bytes;
use std::fmt;

/// Typed stream offset used by the ergonomic API.
///
/// This hides protocol sentinels like `"-1"` and `"now"` from normal callers
/// while keeping offsets explicit and resumable.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum Offset {
    /// Start from the beginning of the stream.
    #[default]
    Beginning,
    /// Start from the current tail of the stream.
    Now,
    /// Resume from a previously returned offset token.
    At(String),
}

impl Offset {
    /// Create an offset at a specific position.
    #[must_use]
    pub fn at(value: impl Into<String>) -> Self {
        Self::At(value.into())
    }

    /// Parse a protocol offset token.
    #[must_use]
    pub fn parse(value: &str) -> Self {
        match value {
            "" | "-1" => Self::Beginning,
            "now" => Self::Now,
            other => Self::At(other.to_string()),
        }
    }

    /// Convert this offset to the protocol token sent on the wire.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Beginning => "-1",
            Self::Now => "now",
            Self::At(value) => value.as_str(),
        }
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for Offset {
    fn from(value: String) -> Self {
        Self::parse(&value)
    }
}

impl From<&str> for Offset {
    fn from(value: &str) -> Self {
        Self::parse(value)
    }
}

/// Metadata for an existing stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamInfo {
    pub next_offset: Option<Offset>,
    pub content_type: Option<String>,
    pub ttl_seconds: Option<u64>,
    pub expires_at: Option<String>,
    pub closed: bool,
    pub etag: Option<String>,
}

/// Result of creating a stream through the ergonomic API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateOutcome {
    pub created: bool,
    pub next_offset: Option<Offset>,
    pub closed: bool,
}

/// Result of appending to a stream through the ergonomic API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendOutcome {
    pub next_offset: Option<Offset>,
}

/// Result of closing a stream through the ergonomic API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseOutcome {
    pub final_offset: Offset,
}

/// One chunk of stream data in the ergonomic read API.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamChunk {
    pub data: Bytes,
    pub offset: Offset,
}

/// Collected read result in the ergonomic API.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadPage {
    pub next_offset: Offset,
    pub up_to_date: bool,
    pub closed: bool,
    pub cursor: Option<String>,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub chunks: Vec<StreamChunk>,
    pub payload: Option<ReadPayload>,
}

impl From<HeadResponse> for StreamInfo {
    fn from(value: HeadResponse) -> Self {
        Self {
            next_offset: value.offset.map(Offset::from),
            content_type: value.content_type,
            ttl_seconds: value.ttl_seconds,
            expires_at: value.expires_at,
            closed: value.stream_closed,
            etag: value.etag,
        }
    }
}

impl From<CreateStreamResponse> for CreateOutcome {
    fn from(value: CreateStreamResponse) -> Self {
        Self {
            created: value.status == 201,
            next_offset: value.next_offset.map(Offset::from),
            closed: value.stream_closed,
        }
    }
}

impl From<AppendResponse> for AppendOutcome {
    fn from(value: AppendResponse) -> Self {
        Self {
            next_offset: value.next_offset.map(Offset::from),
        }
    }
}

impl From<CloseStreamResponse> for CloseOutcome {
    fn from(value: CloseStreamResponse) -> Self {
        Self {
            final_offset: Offset::from(value.final_offset),
        }
    }
}

impl From<ReadChunk> for StreamChunk {
    fn from(value: ReadChunk) -> Self {
        Self {
            data: value.data,
            offset: Offset::from(value.next_offset),
        }
    }
}

impl From<ReadResponse> for ReadPage {
    fn from(value: ReadResponse) -> Self {
        Self {
            next_offset: Offset::from(value.next_offset),
            up_to_date: value.up_to_date,
            closed: value.stream_closed,
            cursor: value.cursor,
            content_type: value.content_type,
            etag: value.etag,
            chunks: value.chunks.into_iter().map(StreamChunk::from).collect(),
            payload: value.payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Offset;

    #[test]
    fn parses_protocol_offset_sentinels() {
        assert_eq!(Offset::parse("-1"), Offset::Beginning);
        assert_eq!(Offset::parse("now"), Offset::Now);
        assert_eq!(Offset::parse("42-0"), Offset::At("42-0".to_string()));
    }

    #[test]
    fn displays_protocol_offset_tokens() {
        assert_eq!(Offset::Beginning.to_string(), "-1");
        assert_eq!(Offset::Now.to_string(), "now");
        assert_eq!(Offset::at("42-0").to_string(), "42-0");
    }
}
