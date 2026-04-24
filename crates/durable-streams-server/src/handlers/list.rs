use crate::handlers::common::with_instance;
use crate::protocol::problem::ProblemResult;
use crate::storage::Storage;
use crate::streams::StreamService;
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
pub async fn list_streams<S: Storage>(
    State(storage): State<Arc<S>>,
    original_uri: OriginalUri,
) -> ProblemResult<Response> {
    with_instance(original_uri, || async move {
        let service = StreamService::new(Arc::clone(&storage));
        Ok(Json(service.list_entries()?).into_response())
    })
    .await
}
