use crate::protocol::problem::{Result, request_instance};
use crate::protocol::stream_name::StreamName;
use crate::storage::Storage;
use axum::{
    extract::{OriginalUri, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use std::sync::Arc;

/// DELETE handler for deleting streams
///
/// Deletes a stream and all its data. Returns 204 on success.
/// Returns 404 if the stream does not exist.
///
/// # Errors
///
/// Returns `Error::NotFound` if the stream does not exist.
pub async fn delete_stream<S: Storage>(
    State(storage): State<Arc<S>>,
    StreamName(name): StreamName,
    original_uri: OriginalUri,
) -> Result<Response> {
    let instance = request_instance(&original_uri);
    let result = (|| -> Result<Response> {
        // Delete stream (idempotent - no error if doesn't exist)
        storage.delete(&name)?;

        Ok(StatusCode::NO_CONTENT.into_response())
    })();

    result.map_err(|problem| problem.with_instance(instance))
}
