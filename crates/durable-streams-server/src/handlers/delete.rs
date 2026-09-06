use crate::handlers::common::with_instance;
use crate::protocol::problem::ProblemResult;
use crate::protocol::stream_name::StreamName;
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
pub async fn delete_stream(
    State(storage): State<Arc<crate::execution::AsyncStreams>>,
    StreamName(name): StreamName,
    original_uri: OriginalUri,
) -> ProblemResult<Response> {
    with_instance(original_uri, || async move {
        storage
            .run("delete", move |service| service.delete(&name))
            .await?;
        Ok(StatusCode::NO_CONTENT.into_response())
    })
    .await
}
