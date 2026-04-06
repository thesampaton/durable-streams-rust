use crate::auth::AuthConfig;
use crate::error::{Error, ErrorKind};
use crate::model::LiveMode;
use reqwest::Method;
use tracing::{Span, debug_span, field, info_span};
use url::Url;

pub(crate) fn auth_type(auth: &AuthConfig) -> &'static str {
    match auth {
        AuthConfig::None => "none",
        AuthConfig::Bearer { .. } => "bearer",
        AuthConfig::Basic { .. } => "basic",
        AuthConfig::Header { .. } => "header",
    }
}

pub(crate) fn live_mode_name(mode: LiveMode) -> &'static str {
    match mode {
        LiveMode::CatchUp => "catch_up",
        LiveMode::LongPoll => "long_poll",
        LiveMode::Sse => "sse",
        LiveMode::Auto => "auto",
    }
}

pub(crate) fn server_address(url: &Url) -> &str {
    url.host_str().unwrap_or("unknown")
}

pub(crate) fn client_operation_span(
    operation: &'static str,
    stream_id: &str,
    server_address: &str,
    auth_type: &'static str,
) -> Span {
    info_span!(
        "durable_streams.client",
        "ds.operation" = operation,
        "ds.stream_id" = stream_id,
        "ds.offset" = field::Empty,
        "ds.resume_offset" = field::Empty,
        "ds.cursor" = field::Empty,
        "ds.live_mode" = field::Empty,
        "ds.up_to_date" = field::Empty,
        "ds.stream_closed" = field::Empty,
        "server.address" = server_address,
        "auth.type" = auth_type,
        "http.status_code" = field::Empty,
        "error.kind" = field::Empty,
        "error.message" = field::Empty
    )
}

pub(crate) fn http_request_span(
    operation: &'static str,
    stream_id: &str,
    method: &Method,
    url: &Url,
    auth_type: &'static str,
) -> Span {
    debug_span!(
        "durable_streams.http",
        "ds.operation" = operation,
        "ds.stream_id" = stream_id,
        "server.address" = server_address(url),
        "http.method" = method.as_str(),
        "auth.type" = auth_type,
        "http.status_code" = field::Empty,
        "error.kind" = field::Empty,
        "error.message" = field::Empty
    )
}

pub(crate) fn retry_span(attempt: u32, max_retries: u32) -> Span {
    debug_span!(
        "durable_streams.retry",
        "retry.attempt" = attempt,
        "retry.max" = max_retries,
        "retry.backoff_ms" = field::Empty,
        "error.kind" = field::Empty,
        "error.message" = field::Empty
    )
}

pub(crate) fn subscription_span(
    stream_id: &str,
    server_address: &str,
    auth_type: &'static str,
) -> Span {
    info_span!(
        "durable_streams.subscription",
        "ds.operation" = "subscribe",
        "ds.stream_id" = stream_id,
        "ds.offset" = field::Empty,
        "ds.resume_offset" = field::Empty,
        "ds.cursor" = field::Empty,
        "ds.live_mode" = field::Empty,
        "ds.up_to_date" = field::Empty,
        "ds.stream_closed" = field::Empty,
        "server.address" = server_address,
        "auth.type" = auth_type,
        "error.kind" = field::Empty,
        "error.message" = field::Empty
    )
}

pub(crate) fn producer_span(operation: &'static str, stream_id: &str) -> Span {
    info_span!(
        "durable_streams.producer",
        "ds.operation" = operation,
        "ds.stream_id" = stream_id,
        "producer.epoch" = field::Empty,
        "producer.seq" = field::Empty,
        "error.kind" = field::Empty,
        "error.message" = field::Empty
    )
}

pub(crate) fn record_optional_str(span: &Span, field_name: &'static str, value: Option<&str>) {
    if let Some(value) = value {
        span.record(field_name, value);
    }
}

pub(crate) fn record_current_optional_str(field_name: &'static str, value: Option<&str>) {
    record_optional_str(&Span::current(), field_name, value);
}

pub(crate) fn record_error(span: &Span, error: &Error) {
    span.record("error.kind", field::display(error_kind_name(error.kind())));
    span.record("error.message", field::display(error));
}

pub(crate) fn record_current_error(error: &Error) {
    record_error(&Span::current(), error);
}

fn error_kind_name(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::InvalidArgument => "invalid_argument",
        ErrorKind::Config => "config",
        ErrorKind::Network => "network",
        ErrorKind::Timeout => "timeout",
        ErrorKind::NotFound => "not_found",
        ErrorKind::Conflict => "conflict",
        ErrorKind::StreamClosed => "stream_closed",
        ErrorKind::InvalidOffset => "invalid_offset",
        ErrorKind::Forbidden => "forbidden",
        ErrorKind::RateLimited => "rate_limited",
        ErrorKind::UnexpectedStatus => "unexpected_status",
        ErrorKind::Parse => "parse",
        ErrorKind::Io => "io",
    }
}
