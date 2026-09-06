use crate::handlers::common::with_instance;
use crate::protocol::problem::ProblemResult;
use axum::{
    Json,
    extract::{OriginalUri, State},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

/// GET handler that lists all non-expired streams with their metadata.
///
/// Returns a JSON array of [`crate::streams::StreamListEntry`] objects, sorted
/// by stream name.
/// The response is consumed by the CLI `list` command and any operator tooling
/// that talks HTTP rather than opening storage directly.
///
/// # Errors
///
/// Returns 500 if the underlying storage backend cannot be read.
pub async fn list_streams(
    State(storage): State<Arc<crate::streams::StreamService>>,
    original_uri: OriginalUri,
) -> ProblemResult<Response> {
    with_instance(original_uri, || async move {
        Ok(Json(storage.list_entries()?).into_response())
    })
    .await
}
