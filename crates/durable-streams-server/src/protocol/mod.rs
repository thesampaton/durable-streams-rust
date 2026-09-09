//! Durable Streams protocol parsing, validation, and wire-format helpers.
//!
//! Most integrations will use [`error`], [`offset`], and [`producer`].
//! The remaining modules support the HTTP implementation and are kept crate-private
//! to avoid exposing server internals as the primary public surface.

pub(crate) mod cursor;
pub mod error;
pub(crate) mod headers;
pub(crate) mod json_mode;
pub mod offset;
pub mod problem;
pub mod producer;
pub(crate) mod sse;
pub(crate) mod stream_name;

/// Split an initial/append body into records, leaving empty-array policy to callers.
pub(crate) fn extract_messages(
    body: bytes::Bytes,
    content_type: &str,
) -> error::Result<Vec<bytes::Bytes>> {
    if body.is_empty() {
        Ok(Vec::new())
    } else if json_mode::is_json_content_type(content_type) {
        json_mode::process_append(&body)
    } else {
        Ok(vec![body])
    }
}
