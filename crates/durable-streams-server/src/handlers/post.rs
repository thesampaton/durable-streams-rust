use crate::handlers::common::{
    StreamResponse, extract_messages, header_value, parse_stream_closed, read_body, with_instance,
};
use crate::protocol::error::Error;
use crate::protocol::headers::{self, names};
use crate::protocol::problem::{ProblemResponse, Result};
use crate::protocol::producer;
use crate::protocol::stream_name::StreamName;
use crate::storage::{ProducerAppendResult, Storage};
use axum::{
    body::Body,
    extract::{OriginalUri, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

/// POST handler for appending data to streams
///
/// Appends data to an existing stream and returns the next offset.
/// Can also close a stream with the Stream-Closed header.
/// Supports idempotent producer semantics when Producer-Id/Epoch/Seq headers
/// are provided. Validates Stream-Seq lexicographic ordering when present.
///
/// # Errors
///
/// Returns error if stream doesn't exist, content-type mismatches,
/// stream is closed, or body is empty without Stream-Closed header.
pub async fn append_data<S: Storage>(
    State(storage): State<Arc<S>>,
    StreamName(name): StreamName,
    original_uri: OriginalUri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response> {
    with_instance(original_uri, || async move {
        let body_bytes = read_body(body).await?;
        let should_close = parse_stream_closed(&headers);
        let is_close_only = body_bytes.is_empty() && should_close;

        // Content-Type: required when body is present, optional for close-only
        let content_type_raw = headers.get("content-type").and_then(|v| v.to_str().ok());

        let normalized_ct = if is_close_only {
            content_type_raw
                .map(headers::normalize_content_type)
                .unwrap_or_default()
        } else {
            let ct = content_type_raw.ok_or_else(|| Error::InvalidHeader {
                header: "Content-Type".to_string(),
                reason: "missing required header".to_string(),
            })?;
            headers::normalize_content_type(ct)
        };

        let producer_headers = producer::parse_producer_headers(&headers)?;

        let stream_seq = headers
            .get(names::STREAM_SEQ)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        if body_bytes.is_empty() && !should_close {
            return Err(ProblemResponse::from(Error::EmptyBody));
        }

        let messages = extract_messages(body_bytes, &normalized_ct)?;

        // After the empty-body guard above, an empty `messages` here can only
        // mean a JSON empty array was sent. POST rejects that unless the
        // request is also closing the stream.
        if messages.is_empty() && !should_close {
            return Err(ProblemResponse::from(Error::EmptyArray));
        }

        let seq_ref = stream_seq.as_deref();

        if let Some(ref prod) = producer_headers {
            handle_producer_append(ProducerAppendArgs {
                storage: &storage,
                name: &name,
                messages,
                content_type: &normalized_ct,
                producer: prod,
                should_close,
                is_close_only,
                seq: seq_ref,
            })
        } else {
            handle_non_producer_append(
                &storage,
                &name,
                messages,
                &normalized_ct,
                should_close,
                seq_ref,
            )
        }
    })
    .await
}

/// Non-producer append path. Always returns 204 No Content on success.
fn handle_non_producer_append<S: Storage>(
    storage: &Arc<S>,
    name: &str,
    messages: Vec<bytes::Bytes>,
    content_type: &str,
    should_close: bool,
    seq: Option<&str>,
) -> Result<Response> {
    let next_offset = if messages.is_empty() {
        storage.head(name)?.next_offset
    } else {
        match storage.batch_append(name, messages, content_type, seq) {
            Ok(next_offset) => next_offset,
            Err(Error::StreamClosed) => {
                return Err(stream_closed_response(storage, name));
            }
            Err(e) => return Err(e.into()),
        }
    };

    if should_close {
        storage.close_stream(name)?;
    }

    Ok(StreamResponse::new(StatusCode::NO_CONTENT)
        .next_offset(&next_offset)
        .closed_if(should_close)
        .into_response())
}

struct ProducerAppendArgs<'a, S: Storage> {
    storage: &'a Arc<S>,
    name: &'a str,
    messages: Vec<bytes::Bytes>,
    content_type: &'a str,
    producer: &'a producer::ProducerHeaders,
    should_close: bool,
    is_close_only: bool,
    seq: Option<&'a str>,
}

/// Producer append path with idempotent sequencing.
///
/// Returns 200 OK for accepted appends, 204 No Content for duplicates
/// or close-only operations.
fn handle_producer_append<S: Storage>(args: ProducerAppendArgs<'_, S>) -> Result<Response> {
    let ProducerAppendArgs {
        storage,
        name,
        messages,
        content_type,
        producer,
        should_close,
        is_close_only,
        seq,
    } = args;

    match storage.append_with_producer(name, messages, content_type, producer, should_close, seq) {
        Ok(result) => {
            let (status, epoch, seq, next_offset, closed) = match result {
                ProducerAppendResult::Accepted {
                    epoch,
                    seq,
                    next_offset,
                    closed,
                } => {
                    let status = if is_close_only {
                        StatusCode::NO_CONTENT
                    } else {
                        StatusCode::OK
                    };
                    (status, epoch, seq, next_offset, closed)
                }
                ProducerAppendResult::Duplicate {
                    epoch,
                    seq,
                    next_offset,
                    closed,
                } => (StatusCode::NO_CONTENT, epoch, seq, next_offset, closed),
            };

            Ok(StreamResponse::new(status)
                .next_offset(&next_offset)
                .header(names::PRODUCER_EPOCH, header_value(epoch.to_string()))
                .header(names::PRODUCER_SEQ, header_value(seq.to_string()))
                .closed_if(closed)
                .into_response())
        }
        Err(Error::StreamClosed) => Err(stream_closed_response(storage, name)),
        Err(Error::EpochFenced { current, .. }) => Err(ProblemResponse::from(Error::EpochFenced {
            current,
            received: producer.epoch,
        })
        .with_header(names::PRODUCER_EPOCH, header_value(current.to_string()))),
        Err(Error::SequenceGap { expected, actual }) => Err(ProblemResponse::from(
            Error::SequenceGap { expected, actual },
        )
        .with_header(
            names::PRODUCER_EXPECTED_SEQ,
            header_value(expected.to_string()),
        )
        .with_header(
            names::PRODUCER_RECEIVED_SEQ,
            header_value(actual.to_string()),
        )),
        Err(e) => Err(e.into()),
    }
}

/// Build the 409 Conflict response for a closed stream.
fn stream_closed_response<S: Storage>(storage: &Arc<S>, name: &str) -> ProblemResponse {
    let response = ProblemResponse::from(Error::StreamClosed)
        .with_header(names::STREAM_CLOSED, header_value("true"));

    if let Ok(metadata) = storage.head(name) {
        response.with_header(
            names::STREAM_NEXT_OFFSET,
            header_value(metadata.next_offset.as_str()),
        )
    } else {
        response
    }
}
