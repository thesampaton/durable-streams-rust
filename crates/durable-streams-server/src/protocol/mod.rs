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
