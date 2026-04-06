use crate::error::{Error, ErrorKind, HttpError};
use crate::model::{ReadChunk, ReadPayload, ReadResponse, SubscriptionEvent};
use base64::Engine;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::StreamExt;
use reqwest::header::{CONTENT_TYPE, ETAG};
use reqwest::{Response, StatusCode};
use serde::Deserialize;
use std::collections::HashMap;

pub(crate) const STREAM_TTL: &str = "stream-ttl";
pub(crate) const STREAM_EXPIRES_AT: &str = "stream-expires-at";
pub(crate) const STREAM_CLOSED: &str = "stream-closed";
pub(crate) const STREAM_NEXT_OFFSET: &str = "stream-next-offset";
pub(crate) const STREAM_CURSOR: &str = "stream-cursor";
pub(crate) const STREAM_UP_TO_DATE: &str = "stream-up-to-date";
pub(crate) const STREAM_SEQ: &str = "stream-seq";
pub(crate) const STREAM_SSE_DATA_ENCODING: &str = "stream-sse-data-encoding";
pub(crate) const PRODUCER_ID: &str = "producer-id";
pub(crate) const PRODUCER_EPOCH: &str = "producer-epoch";
pub(crate) const PRODUCER_SEQ: &str = "producer-seq";
pub(crate) const PRODUCER_EXPECTED_SEQ: &str = "producer-expected-seq";
pub(crate) const PRODUCER_RECEIVED_SEQ: &str = "producer-received-seq";

pub(crate) fn header_value(response: &Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

pub(crate) fn parse_bool_header(response: &Response, name: &str) -> bool {
    header_value(response, name)
        .is_some_and(|value| value.eq_ignore_ascii_case("true") || value.is_empty())
}

pub(crate) fn parse_i64_header(response: &Response, name: &str) -> Option<i64> {
    header_value(response, name)?.parse::<i64>().ok()
}

pub(crate) fn build_http_error_from_parts(
    status: StatusCode,
    headers: HashMap<String, String>,
    body: String,
) -> HttpError {
    let stream_closed = headers
        .get(STREAM_CLOSED)
        .is_some_and(|value| value.eq_ignore_ascii_case("true") || value.is_empty());
    let lower_message = body.to_ascii_lowercase();
    let kind = if status == StatusCode::NOT_FOUND {
        ErrorKind::NotFound
    } else if status == StatusCode::FORBIDDEN {
        ErrorKind::Forbidden
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        ErrorKind::RateLimited
    } else if status == StatusCode::BAD_REQUEST
        && (lower_message.contains("offset") || lower_message.contains("invalid"))
    {
        ErrorKind::InvalidOffset
    } else if status == StatusCode::CONFLICT && stream_closed {
        ErrorKind::StreamClosed
    } else if status == StatusCode::CONFLICT {
        ErrorKind::Conflict
    } else {
        ErrorKind::UnexpectedStatus
    };

    let next_offset = headers.get(STREAM_NEXT_OFFSET).cloned();
    let stream_cursor = headers.get(STREAM_CURSOR).cloned();
    let producer_epoch = headers
        .get(PRODUCER_EPOCH)
        .and_then(|value| value.parse::<i64>().ok());
    let producer_seq = headers
        .get(PRODUCER_SEQ)
        .and_then(|value| value.parse::<i64>().ok());
    let producer_expected_seq = headers
        .get(PRODUCER_EXPECTED_SEQ)
        .and_then(|value| value.parse::<i64>().ok());
    let producer_received_seq = headers
        .get(PRODUCER_RECEIVED_SEQ)
        .and_then(|value| value.parse::<i64>().ok());

    HttpError {
        status,
        kind,
        message: if body.is_empty() {
            status
                .canonical_reason()
                .unwrap_or("request failed")
                .to_string()
        } else {
            body
        },
        headers,
        next_offset,
        stream_closed,
        stream_cursor,
        producer_epoch,
        producer_seq,
        producer_expected_seq,
        producer_received_seq,
    }
}

pub(crate) async fn response_error(response: Response) -> HttpError {
    let status = response.status();
    let relevant_headers = [
        STREAM_CLOSED,
        STREAM_NEXT_OFFSET,
        STREAM_CURSOR,
        PRODUCER_EPOCH,
        PRODUCER_SEQ,
        PRODUCER_EXPECTED_SEQ,
        PRODUCER_RECEIVED_SEQ,
    ];
    let headers = relevant_headers
        .iter()
        .filter_map(|&name| {
            let value = response.headers().get(name)?.to_str().ok()?;
            Some((name.to_string(), value.to_string()))
        })
        .collect::<HashMap<_, _>>();
    let body = response.text().await.unwrap_or_else(|_| status.to_string());
    build_http_error_from_parts(status, headers, body)
}

pub(crate) fn read_response_from_http(
    response: &Response,
    payload: Option<ReadPayload>,
    chunks: Vec<ReadChunk>,
) -> Result<ReadResponse, Error> {
    Ok(ReadResponse {
        status: response.status().as_u16(),
        next_offset: header_value(response, STREAM_NEXT_OFFSET)
            .ok_or_else(|| Error::parse("missing Stream-Next-Offset header"))?,
        up_to_date: parse_bool_header(response, STREAM_UP_TO_DATE),
        stream_closed: parse_bool_header(response, STREAM_CLOSED),
        cursor: header_value(response, STREAM_CURSOR),
        content_type: header_value(response, CONTENT_TYPE.as_str()),
        etag: header_value(response, ETAG.as_str()),
        chunks,
        payload,
    })
}

#[derive(Debug, Deserialize)]
struct ControlEvent {
    #[serde(rename = "streamNextOffset")]
    stream_next_offset: String,
    #[serde(rename = "streamCursor")]
    stream_cursor: Option<String>,
    #[serde(rename = "upToDate", default)]
    up_to_date: bool,
    #[serde(rename = "streamClosed", default)]
    stream_closed: bool,
}

pub(crate) async fn collect_sse(
    response: Response,
    max_chunks: Option<usize>,
    wait_for_up_to_date: bool,
) -> Result<ReadResponse, Error> {
    let content_type = header_value(&response, CONTENT_TYPE.as_str());
    let etag = header_value(&response, ETAG.as_str());
    let base64_mode = header_value(&response, STREAM_SSE_DATA_ENCODING)
        .is_some_and(|value| value.eq_ignore_ascii_case("base64"));

    let mut parser = SseParser::default();
    let mut stream = response.bytes_stream();
    let mut chunks = Vec::new();
    let mut next_offset = None::<String>;
    let mut cursor = None::<String>;
    let mut up_to_date = false;
    let mut stream_closed = false;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        for event in parser.push(&chunk)? {
            match event.kind.as_deref().unwrap_or("data") {
                "data" => {
                    let data = if base64_mode {
                        let flattened = event.data.replace(['\n', '\r'], "");
                        Bytes::from(base64::engine::general_purpose::STANDARD.decode(flattened)?)
                    } else {
                        Bytes::from(event.data.into_bytes())
                    };
                    let current_offset = next_offset.clone().unwrap_or_else(|| "-1".to_string());
                    chunks.push(ReadChunk {
                        data,
                        next_offset: current_offset,
                    });
                    if max_chunks.is_some_and(|limit| chunks.len() >= limit) {
                        break;
                    }
                }
                "control" => {
                    let control: ControlEvent =
                        serde_json::from_str(&event.data).map_err(|error| {
                            Error::parse(format!("invalid SSE control event JSON: {error}"))
                        })?;
                    next_offset = Some(control.stream_next_offset);
                    cursor = control.stream_cursor;
                    up_to_date = control.up_to_date || control.stream_closed;
                    stream_closed = control.stream_closed;
                    if stream_closed || (wait_for_up_to_date && up_to_date) {
                        break;
                    }
                }
                _ => {}
            }
        }

        if stream_closed
            || (wait_for_up_to_date && up_to_date)
            || max_chunks.is_some_and(|limit| chunks.len() >= limit)
        {
            break;
        }
    }

    let next_offset = next_offset
        .ok_or_else(|| Error::parse("missing SSE control event with streamNextOffset"))?;
    Ok(ReadResponse {
        status: 200,
        next_offset,
        up_to_date,
        stream_closed,
        cursor,
        content_type,
        etag,
        chunks,
        payload: None,
    })
}

pub(crate) async fn collect_catch_up(response: Response) -> Result<ReadResponse, Error> {
    let content_type = header_value(&response, CONTENT_TYPE.as_str());
    let is_json = content_type
        .as_deref()
        .is_some_and(|value| value.starts_with("application/json"));
    let status = response.status().as_u16();
    let next_offset = header_value(&response, STREAM_NEXT_OFFSET)
        .ok_or_else(|| Error::parse("missing Stream-Next-Offset header"))?;
    let up_to_date =
        parse_bool_header(&response, STREAM_UP_TO_DATE) || status == 200 || status == 204;
    let stream_closed = parse_bool_header(&response, STREAM_CLOSED);
    let cursor = header_value(&response, STREAM_CURSOR);
    let etag = header_value(&response, ETAG.as_str());
    let bytes = response.bytes().await?;

    let (payload, chunks) = if is_json {
        let values = if bytes.is_empty() {
            Vec::new()
        } else {
            serde_json::from_slice::<Vec<serde_json::Value>>(&bytes)?
        };
        let chunks = if values.is_empty() {
            Vec::new()
        } else {
            vec![ReadChunk {
                data: bytes,
                next_offset: next_offset.clone(),
            }]
        };
        (Some(ReadPayload::Json(values)), chunks)
    } else {
        let chunks = if bytes.is_empty() {
            Vec::new()
        } else {
            vec![ReadChunk {
                data: bytes.clone(),
                next_offset: next_offset.clone(),
            }]
        };
        (Some(ReadPayload::Bytes(bytes)), chunks)
    };

    Ok(ReadResponse {
        status,
        next_offset,
        up_to_date,
        stream_closed,
        cursor,
        content_type,
        etag,
        chunks,
        payload,
    })
}

#[derive(Debug, Default)]
struct SseParser {
    buffer: BytesMut,
    current_event: Option<String>,
    data_lines: Vec<String>,
}

#[derive(Debug)]
struct ParsedEvent {
    kind: Option<String>,
    data: String,
}

impl SseParser {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<ParsedEvent>, Error> {
        self.buffer.put(bytes);
        let mut events = Vec::new();

        loop {
            let Some(line_end) = self.buffer.windows(1).position(|window| window == b"\n") else {
                break;
            };

            let mut line = self.buffer.split_to(line_end + 1);
            if line.ends_with(b"\n") {
                line.truncate(line.len() - 1);
            }
            if line.ends_with(b"\r") {
                line.truncate(line.len() - 1);
            }

            if line.is_empty() {
                if !self.data_lines.is_empty() || self.current_event.is_some() {
                    events.push(ParsedEvent {
                        kind: self.current_event.take(),
                        data: self.data_lines.join("\n"),
                    });
                    self.data_lines.clear();
                }
                continue;
            }

            let text = std::str::from_utf8(&line)
                .map_err(|_| Error::parse("SSE stream contained invalid UTF-8"))?;
            if let Some(rest) = text.strip_prefix("event:") {
                self.current_event = Some(rest.trim().to_string());
            } else if let Some(rest) = text.strip_prefix("data:") {
                let rest = rest.strip_prefix(' ').unwrap_or(rest);
                self.data_lines.push(rest.to_string());
            }
        }

        Ok(events)
    }
}

pub(crate) fn response_to_event(read: &ReadResponse) -> SubscriptionEvent {
    SubscriptionEvent {
        chunk: read.chunks.last().cloned(),
        next_offset: read.next_offset.clone(),
        up_to_date: read.up_to_date,
        stream_closed: read.stream_closed,
        cursor: read.cursor.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::SseParser;

    #[test]
    fn parses_multiline_sse_event() {
        let mut parser = SseParser::default();
        let events = parser
            .push(
                b"event: data\ndata: [\ndata: {\"k\":1}\ndata: ]\n\nevent: control\ndata: {\"streamNextOffset\":\"1\",\"upToDate\":true}\n\n",
            )
            .expect("parser works");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind.as_deref(), Some("data"));
        assert_eq!(events[1].kind.as_deref(), Some("control"));
    }
}
