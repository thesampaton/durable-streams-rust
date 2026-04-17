use crate::handlers::common::{StreamResponse, header_value, with_instance};
use crate::protocol::{headers::names, problem::Result, stream_name::StreamName};
use crate::storage::Storage;
use axum::{
    extract::{OriginalUri, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::Utc;
use std::sync::Arc;

/// HEAD handler for stream metadata
///
/// Returns stream metadata in headers without a body.
///
/// # Errors
///
/// Returns 404 if stream doesn't exist or has expired.
pub async fn stream_metadata<S: Storage>(
    State(storage): State<Arc<S>>,
    StreamName(name): StreamName,
    original_uri: OriginalUri,
) -> Result<Response> {
    with_instance(original_uri, || async move {
        let metadata = storage.head(&name)?;
        let mut response = StreamResponse::new(StatusCode::OK)
            .content_type(&metadata.config.content_type)
            .next_offset(&metadata.next_offset)
            .closed_if(metadata.closed);

        if let Some(expires_at) = metadata.config.expires_at {
            let now = Utc::now();
            let remaining_seconds = if metadata.config.ttl_seconds.is_some() {
                // Use ceiling division so sub-second drift doesn't report
                // a value lower than the configured TTL immediately.
                let remaining_ms = (expires_at - now).num_milliseconds();
                (remaining_ms + 999) / 1000
            } else {
                (expires_at - now).num_seconds()
            };

            if remaining_seconds > 0 {
                response = response
                    .header(
                        names::STREAM_TTL,
                        header_value(remaining_seconds.to_string()),
                    )
                    .header(
                        names::STREAM_EXPIRES_AT,
                        header_value(expires_at.to_rfc3339()),
                    );
            }
        }

        Ok(response.into_response())
    })
    .await
}
