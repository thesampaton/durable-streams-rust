use axum::{
    http::{HeaderValue, Request, Response},
    middleware::Next,
};

/// Security headers middleware
///
/// Adds standard headers to all responses:
/// - `Cache-Control: no-store` (`no-cache` for SSE; a five-minute public cache for JWKS)
/// - `X-Content-Type-Options: nosniff` - Prevents MIME type sniffing
/// - `Cross-Origin-Resource-Policy: cross-origin` - Allows cross-origin access
///
/// # Panics
///
/// Panics if the hardcoded header values fail to parse, which should never happen.
pub async fn add_security_headers(
    request: Request<axum::body::Body>,
    next: Next,
) -> Response<axum::body::Body> {
    let mut response = next.run(request).await;

    let headers = response.headers_mut();

    let is_sse = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("text/event-stream"));

    let is_jwks = headers
        .get("content-type")
        .is_some_and(|ct| ct == "application/jwk-set+json");
    let cache_control = if is_jwks {
        "public, max-age=300"
    } else if is_sse {
        "no-cache"
    } else {
        "no-store"
    };
    headers.insert("cache-control", HeaderValue::from_static(cache_control));
    headers.insert(
        "X-Content-Type-Options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        "Cross-Origin-Resource-Policy",
        HeaderValue::from_static("cross-origin"),
    );

    response
}
