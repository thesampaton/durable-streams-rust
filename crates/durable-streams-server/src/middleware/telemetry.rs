use crate::middleware::proxy_trust::ProxyTrustResult;
use crate::protocol::problem::ProblemTelemetry;
use axum::{
    body::Body,
    extract::MatchedPath,
    http::{Request, Version},
    middleware::Next,
    response::Response,
};
use std::time::Instant;
use tracing::{Instrument, Span, field, info_span};

/// Bundles values extracted from an inbound HTTP request so that
/// [`request_span`] stays under the clippy argument-count threshold.
struct RequestContext<'a> {
    operation: &'static str,
    method: &'a str,
    path: &'a str,
    query: Option<&'a str>,
    route: Option<&'a str>,
    live_mode: Option<&'a str>,
    stream_id: Option<&'a str>,
    server_address: Option<&'a str>,
    client_address: Option<&'a str>,
    user_agent: Option<&'a str>,
    http_version: &'static str,
    proxy_trust: Option<&'a ProxyTrustResult>,
}

/// Record request/response telemetry using tracing field names that align with
/// OpenTelemetry semantic conventions and ECS where practical.
pub async fn track_requests(request: Request<Body>, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(ToOwned::to_owned);
    let matched_path = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .map(ToOwned::to_owned);
    let route = matched_path.as_deref();
    let live_mode = query
        .as_deref()
        .and_then(|query| query_param(query, "live"))
        .map(ToOwned::to_owned);
    let stream_id = route
        .and_then(|route| resource_id_from_path(route, &path))
        .filter(|stream_id| !stream_id.is_empty());
    let proxy_trust = request.extensions().get::<ProxyTrustResult>().cloned();
    let host = proxy_trust
        .as_ref()
        .and_then(|origin| origin.authority.clone());
    let client_address = proxy_trust
        .as_ref()
        .and_then(|origin| origin.client_address.clone());
    let user_agent = request
        .headers()
        .get("user-agent")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let operation = operation_name(method.as_str(), route, live_mode.as_deref());
    let http_version = http_version(request.version());

    let ctx = RequestContext {
        operation,
        method: method.as_str(),
        path: &path,
        query: query.as_deref(),
        route,
        live_mode: live_mode.as_deref(),
        stream_id,
        server_address: host.as_deref(),
        client_address: client_address.as_deref(),
        user_agent: user_agent.as_deref(),
        http_version,
        proxy_trust: proxy_trust.as_ref(),
    };
    let span = request_span(&ctx);
    let started = Instant::now();

    let response = next.run(request).instrument(span.clone()).await;
    emit_response_event(&span, &response, started.elapsed());

    response
}

fn request_span(ctx: &RequestContext<'_>) -> Span {
    let span = info_span!(
        "durable_streams.server",
        "ds.operation" = ctx.operation,
        "ds.live_mode" = field::Empty,
        "ds.stream_id" = field::Empty,
        "ds.error_code" = field::Empty,
        "ds.error_class" = field::Empty,
        "ds.storage.backend" = field::Empty,
        "ds.storage.operation" = field::Empty,
        "ds.proxy.peer_ip" = field::Empty,
        "ds.proxy.trusted" = field::Empty,
        "http.request.method" = ctx.method,
        "http.route" = field::Empty,
        "http.response.status_code" = field::Empty,
        "http.response.header.retry_after" = field::Empty,
        "url.path" = ctx.path,
        "url.query" = field::Empty,
        "server.address" = field::Empty,
        "client.address" = field::Empty,
        "user_agent.original" = field::Empty,
        "network.protocol.version" = ctx.http_version,
        "event.duration" = field::Empty,
        "error.type" = field::Empty,
        "error.message" = field::Empty
    );

    if let Some(query) = ctx.query {
        span.record("url.query", query);
    }
    if let Some(route) = ctx.route {
        span.record("http.route", route);
    }
    if let Some(live_mode) = ctx.live_mode {
        span.record("ds.live_mode", live_mode);
    }
    if let Some(stream_id) = ctx.stream_id {
        span.record("ds.stream_id", stream_id);
    }
    if let Some(server_address) = ctx.server_address {
        span.record("server.address", server_address);
    }
    if let Some(client_address) = ctx.client_address {
        span.record("client.address", client_address);
    }
    if let Some(user_agent) = ctx.user_agent {
        span.record("user_agent.original", user_agent);
    }
    if let Some(trust) = ctx.proxy_trust {
        if let Some(ip) = trust.peer_ip {
            span.record("ds.proxy.peer_ip", field::display(ip));
        }
        span.record("ds.proxy.trusted", trust.trusted);
    }

    span
}

fn emit_response_event(span: &Span, response: &Response, elapsed: std::time::Duration) {
    span.record(
        "http.response.status_code",
        field::display(response.status().as_u16()),
    );
    span.record(
        "event.duration",
        u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
    );

    if let Some(problem) = response.extensions().get::<ProblemTelemetry>() {
        span.record("ds.error_code", problem.code.as_str());
        if let Some(error_class) = &problem.error_class {
            span.record("ds.error_class", error_class.as_str());
        }
        if let Some(storage_backend) = &problem.storage_backend {
            span.record("ds.storage.backend", storage_backend.as_str());
        }
        if let Some(storage_operation) = &problem.storage_operation {
            span.record("ds.storage.operation", storage_operation.as_str());
        }
        if let Some(retry_after_secs) = problem.retry_after_secs {
            span.record("http.response.header.retry_after", retry_after_secs);
        }
        span.record("error.type", problem.problem_type.as_str());
        span.record(
            "error.message",
            problem
                .internal_detail
                .as_deref()
                .or(problem.detail.as_deref())
                .unwrap_or(problem.title.as_str()),
        );
    } else if response.status().is_server_error() {
        span.record("error.type", "server_error");
        span.record("error.message", "request failed");
    } else if response.status().is_client_error() {
        span.record("error.type", "client_error");
    }

    if response.status().is_server_error() {
        tracing::error!("request failed");
    } else if response.status().is_client_error() {
        tracing::warn!("request completed with client error");
    } else {
        tracing::info!("request completed");
    }
}

fn operation_name(method: &str, route: Option<&str>, live_mode: Option<&str>) -> &'static str {
    if matches!(route, Some("/healthz")) {
        return "health";
    }
    if matches!(route, Some("/readyz")) {
        return "readiness";
    }
    if !route.is_some_and(has_resource_segment) {
        return "request";
    }

    match (method, live_mode) {
        ("PUT", _) => "create",
        ("POST", _) => "append",
        ("HEAD", _) => "head",
        ("DELETE", _) => "delete",
        ("GET", Some("sse")) => "subscribe",
        ("GET", Some("long-poll")) => "poll",
        ("GET", _) => "read",
        _ => "request",
    }
}

fn has_resource_segment(route: &str) -> bool {
    route
        .split('/')
        .filter(|segment| !segment.is_empty())
        .any(is_route_param_segment)
}

fn resource_id_from_path<'a>(route: &str, path: &'a str) -> Option<&'a str> {
    let parameter_index = route
        .split('/')
        .filter(|segment| !segment.is_empty())
        .enumerate()
        .filter_map(|(index, segment)| is_route_param_segment(segment).then_some(index))
        .last()?;

    path.split('/')
        .filter(|segment| !segment.is_empty())
        .nth(parameter_index)
}

fn is_route_param_segment(segment: &str) -> bool {
    segment.starts_with('{') && segment.ends_with('}') && segment.len() > 2
}

fn query_param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (key == name).then_some(value)
    })
}

fn http_version(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 => "0.9",
        Version::HTTP_10 => "1.0",
        Version::HTTP_11 => "1.1",
        Version::HTTP_2 => "2",
        Version::HTTP_3 => "3",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::{operation_name, query_param, resource_id_from_path};

    #[test]
    fn operation_name_distinguishes_live_read_modes() {
        assert_eq!(
            operation_name("GET", Some("/streams/{name}"), Some("long-poll")),
            "poll"
        );
        assert_eq!(
            operation_name("GET", Some("/streams/{name}"), Some("sse")),
            "subscribe"
        );
        assert_eq!(operation_name("GET", Some("/streams/{name}"), None), "read");
        assert_eq!(
            operation_name("GET", Some("/documents/{uuid}"), None),
            "read"
        );
    }

    #[test]
    fn operation_name_handles_nested_subresources() {
        assert_eq!(
            operation_name("GET", Some("/documents/{uuid}/slides/{slide_id}"), None),
            "read"
        );
        assert_eq!(
            operation_name("POST", Some("/documents/{uuid}/ack"), None),
            "append"
        );
    }

    #[test]
    fn resource_id_from_path_uses_last_parameter_segment() {
        assert_eq!(
            resource_id_from_path("/documents/{uuid}", "/documents/doc-1"),
            Some("doc-1")
        );
        assert_eq!(
            resource_id_from_path(
                "/documents/{doc_id}/slides/{slide_id}",
                "/documents/doc-1/slides/slide-2"
            ),
            Some("slide-2")
        );
        assert_eq!(
            resource_id_from_path("/documents/{uuid}/ack", "/documents/doc-1/ack"),
            Some("doc-1")
        );
    }

    #[test]
    fn query_param_extracts_named_values() {
        assert_eq!(query_param("offset=now&live=sse", "live"), Some("sse"));
        assert_eq!(query_param("offset=now", "live"), None);
    }
}
