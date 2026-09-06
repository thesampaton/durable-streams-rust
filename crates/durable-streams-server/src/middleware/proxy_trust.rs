//! Proxy trust middleware: forwarded-header gating, origin derivation, and
//! identity enforcement.
//!
//! When proxy trust is **disabled** (the default), all forwarded headers are
//! stripped from every request — preventing clients from spoofing origin
//! information.
//!
//! When proxy trust is **enabled**, headers are only preserved from peers whose
//! IP matches one of the configured trusted proxy addresses. The configured
//! [`ForwardedHeadersMode`] determines which header family is kept; the
//! opposing family is always stripped, even from trusted peers.

use crate::config::{ForwardedHeadersMode, ProxyIdentityMode, TransportMode};
use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{HeaderName, Request},
    middleware::Next,
    response::Response,
};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

// ── Header constants ──────────────────────────────────────────────

const X_FORWARDED_FOR: &str = "x-forwarded-for";
const X_FORWARDED_HOST: &str = "x-forwarded-host";
const X_FORWARDED_PROTO: &str = "x-forwarded-proto";
const X_FORWARDED_PORT: &str = "x-forwarded-port";
const FORWARDED: &str = "forwarded";

// ── Trusted peers ─────────────────────────────────────────────────

/// Parsed set of trusted proxy peers for O(n) IP/CIDR matching.
#[derive(Debug, Clone)]
pub(crate) struct TrustedPeers {
    entries: Vec<PeerEntry>,
}

#[derive(Debug, Clone)]
pub(crate) enum PeerEntry {
    Exact(IpAddr),
    Cidr { network: IpAddr, prefix_len: u8 },
}

impl PeerEntry {
    /// Use the same syntax and prefix bounds for configuration and matching.
    pub(crate) fn parse(raw: &str) -> Option<Self> {
        if let Ok(ip) = raw.parse::<IpAddr>() {
            return Some(Self::Exact(ip));
        }
        let (address, prefix) = raw.split_once('/')?;
        let network = address.parse::<IpAddr>().ok()?;
        let prefix_len = prefix.parse::<u8>().ok()?;
        let max_prefix = if network.is_ipv4() { 32 } else { 128 };
        (prefix_len <= max_prefix).then_some(Self::Cidr {
            network,
            prefix_len,
        })
    }
}

impl TrustedPeers {
    /// Parse a list of validated IP/CIDR strings into a `TrustedPeers` set.
    ///
    /// Config validation guarantees all entries are syntactically valid by
    /// the time this constructor runs, so invalid entries are skipped with
    /// a warning rather than panicking.
    pub(crate) fn from_config(trusted_proxies: &[String]) -> Self {
        let entries = trusted_proxies
            .iter()
            .filter_map(|raw| {
                if let Some(entry) = PeerEntry::parse(raw) {
                    return Some(entry);
                }
                tracing::warn!(entry = raw, "skipping unparseable trusted_proxies entry");
                None
            })
            .collect();
        Self { entries }
    }

    /// Returns `true` if `addr` matches any entry in the trusted set.
    ///
    /// Handles IPv4-mapped IPv6 addresses (`::ffff:a.b.c.d`) by also
    /// checking the unwrapped IPv4 form against IPv4 entries.
    pub(crate) fn contains(&self, addr: IpAddr) -> bool {
        if self.entries.is_empty() {
            return false;
        }
        // Normalise: if the candidate is an IPv4-mapped IPv6, also try the
        // inner v4 address against v4-only entries.
        let v4_mapped = match addr {
            IpAddr::V6(v6) => v6.to_ipv4_mapped(),
            IpAddr::V4(_) => None,
        };

        for entry in &self.entries {
            match entry {
                PeerEntry::Exact(ip) => {
                    if *ip == addr {
                        return true;
                    }
                    if let Some(v4) = v4_mapped
                        && *ip == IpAddr::V4(v4)
                    {
                        return true;
                    }
                }
                PeerEntry::Cidr {
                    network,
                    prefix_len,
                } => {
                    if cidr_contains(*network, *prefix_len, addr) {
                        return true;
                    }
                    if let Some(v4) = v4_mapped
                        && cidr_contains(*network, *prefix_len, IpAddr::V4(v4))
                    {
                        return true;
                    }
                }
            }
        }
        false
    }
}

/// Check if `candidate` falls within the `network/prefix_len` CIDR range.
fn cidr_contains(network: IpAddr, prefix_len: u8, candidate: IpAddr) -> bool {
    match (network, candidate) {
        (IpAddr::V4(net), IpAddr::V4(cand)) => {
            let mask = ipv4_mask(prefix_len);
            (u32::from(net) & mask) == (u32::from(cand) & mask)
        }
        (IpAddr::V6(net), IpAddr::V6(cand)) => {
            let mask = ipv6_mask(prefix_len);
            let net_bits = u128::from(net);
            let cand_bits = u128::from(cand);
            (net_bits & mask) == (cand_bits & mask)
        }
        _ => false, // v4 vs v6 mismatch
    }
}

fn ipv4_mask(prefix_len: u8) -> u32 {
    if prefix_len == 0 {
        0
    } else if prefix_len >= 32 {
        u32::MAX
    } else {
        u32::MAX << (32 - prefix_len)
    }
}

fn ipv6_mask(prefix_len: u8) -> u128 {
    if prefix_len == 0 {
        0
    } else if prefix_len >= 128 {
        u128::MAX
    } else {
        u128::MAX << (128 - prefix_len)
    }
}

// ── Proxy trust result ─────────────────────────────────────────────

/// Per-request proxy trust decision, inserted as a request extension so that
/// downstream middleware (telemetry) can record the outcome.
#[derive(Debug, Clone)]
pub(crate) struct ProxyTrustResult {
    /// The connecting peer's IP address, if available.
    pub peer_ip: Option<IpAddr>,
    /// Whether the peer matched a trusted proxy entry.
    pub trusted: bool,
    /// Request scheme after proxy trust rules are applied.
    pub scheme: String,
    /// Request authority after proxy trust rules are applied.
    pub authority: Option<String>,
    /// Client address after proxy trust rules are applied.
    pub client_address: Option<String>,
}

// ── Proxy trust state ─────────────────────────────────────────────

/// Runtime proxy trust configuration, captured by the middleware closure.
#[derive(Debug, Clone)]
pub(crate) struct ProxyTrustState {
    pub enabled: bool,
    pub mode: ForwardedHeadersMode,
    pub trusted_peers: TrustedPeers,
    pub transport_mode: TransportMode,
    pub identity_mode: ProxyIdentityMode,
    pub identity_header: Option<HeaderName>,
    pub identity_require_tls: bool,
}

impl ProxyTrustState {
    /// Build from the resolved [`crate::config::ProxyConfig`].
    pub(crate) fn from_config(config: &crate::config::Config) -> Self {
        let proxy = &config.proxy;
        let identity_header = proxy
            .identity
            .header_name
            .as_deref()
            .and_then(|name| HeaderName::from_bytes(name.as_bytes()).ok());

        Self {
            enabled: proxy.enabled,
            mode: proxy.forwarded_headers,
            trusted_peers: TrustedPeers::from_config(&proxy.trusted_proxies),
            transport_mode: config.transport.mode,
            identity_mode: proxy.identity.mode,
            identity_header,
            identity_require_tls: proxy.identity.require_tls,
        }
    }
}

// ── Middleware ─────────────────────────────────────────────────────

/// Strip or pass forwarded headers based on proxy trust rules.
///
/// Called from a closure in the router that captures [`ProxyTrustState`].
/// Reads `ConnectInfo<SocketAddr>` directly from request extensions
/// (inserted by `axum::serve` or `into_make_service_with_connect_info`).
pub(crate) async fn enforce_proxy_trust(
    state: Arc<ProxyTrustState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let peer_ip = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip());

    if peer_ip.is_none() {
        tracing::warn!("ConnectInfo<SocketAddr> not available; treating peer as untrusted");
    }

    let trusted = state.enabled && peer_ip.is_some_and(|ip| state.trusted_peers.contains(ip));

    if trusted {
        // Trusted peer: keep the configured header family, strip the other.
        strip_opposing_family(&mut request, state.mode);
    } else {
        // Untrusted peer or proxy disabled: strip everything.
        let had_forwarded_headers = has_any_forwarded_header(&request);
        strip_all_forwarded(&mut request);
        if let Some(ref header) = state.identity_header {
            request.headers_mut().remove(header);
        }

        // Warn about potential spoofing attempts — only when proxy trust is
        // enabled but the peer is not in the trusted set.
        if state.enabled
            && had_forwarded_headers
            && let Some(ip) = peer_ip
        {
            tracing::warn!(
                peer_ip = %ip,
                "stripped forwarded headers from untrusted peer"
            );
        }
    }

    let identity_allowed = trusted
        && state.identity_mode == ProxyIdentityMode::Header
        && state.transport_mode == TransportMode::Mtls
        && (!state.identity_require_tls || state.transport_mode.uses_tls());
    if let Some(ref header) = state.identity_header
        && !identity_allowed
    {
        request.headers_mut().remove(header);
    }

    let origin = request_origin(&request, state.as_ref(), peer_ip, trusted);
    request.extensions_mut().insert(origin);

    next.run(request).await
}

fn request_origin(
    request: &Request<Body>,
    state: &ProxyTrustState,
    peer_ip: Option<IpAddr>,
    trusted: bool,
) -> ProxyTrustResult {
    let direct_scheme = if state.transport_mode.uses_tls() {
        "https"
    } else {
        "http"
    };

    let (scheme, authority, client_address) = if trusted {
        match state.mode {
            ForwardedHeadersMode::XForwarded => (
                forwarded_header_value(request, X_FORWARDED_PROTO)
                    .unwrap_or_else(|| direct_scheme.to_string()),
                forwarded_header_value(request, X_FORWARDED_HOST)
                    .or_else(|| host_header_value(request)),
                forwarded_for(request).or_else(|| peer_ip.map(|ip| ip.to_string())),
            ),
            ForwardedHeadersMode::Forwarded => {
                let forwarded = parse_forwarded(request);
                (
                    forwarded.proto.unwrap_or_else(|| direct_scheme.to_string()),
                    forwarded.host.or_else(|| host_header_value(request)),
                    forwarded
                        .for_value
                        .or_else(|| peer_ip.map(|ip| ip.to_string())),
                )
            }
            ForwardedHeadersMode::None => (
                direct_scheme.to_string(),
                host_header_value(request),
                peer_ip.map(|ip| ip.to_string()),
            ),
        }
    } else {
        (
            direct_scheme.to_string(),
            host_header_value(request),
            peer_ip.map(|ip| ip.to_string()),
        )
    };

    ProxyTrustResult {
        peer_ip,
        trusted,
        scheme,
        authority,
        client_address,
    }
}

/// Returns `true` if the request contains any forwarded header.
fn has_any_forwarded_header(request: &Request<Body>) -> bool {
    let headers = request.headers();
    headers.contains_key(X_FORWARDED_FOR)
        || headers.contains_key(X_FORWARDED_HOST)
        || headers.contains_key(X_FORWARDED_PROTO)
        || headers.contains_key(X_FORWARDED_PORT)
        || headers.contains_key(FORWARDED)
}

fn host_header_value(request: &Request<Body>) -> Option<String> {
    request
        .headers()
        .get("host")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn forwarded_header_value(request: &Request<Body>, name: &str) -> Option<String> {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn forwarded_for(request: &Request<Body>) -> Option<String> {
    request
        .headers()
        .get(X_FORWARDED_FOR)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[derive(Debug, Default)]
struct ParsedForwarded {
    proto: Option<String>,
    host: Option<String>,
    for_value: Option<String>,
}

fn parse_forwarded(request: &Request<Body>) -> ParsedForwarded {
    let mut parsed = ParsedForwarded::default();
    let Some(value) = request
        .headers()
        .get(FORWARDED)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
    else {
        return parsed;
    };

    let first_entry = value.split(',').next().unwrap_or_default();
    for part in first_entry.split(';') {
        let Some((raw_key, raw_value)) = part.split_once('=') else {
            continue;
        };
        let key = raw_key.trim().to_ascii_lowercase();
        let value = unquote_forwarded_value(raw_value.trim());
        if value.is_empty() {
            continue;
        }

        match key.as_str() {
            "proto" if parsed.proto.is_none() => parsed.proto = Some(value),
            "host" if parsed.host.is_none() => parsed.host = Some(value),
            "for" if parsed.for_value.is_none() => parsed.for_value = Some(value),
            _ => {}
        }
    }

    parsed
}

fn unquote_forwarded_value(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].replace("\\\"", "\"")
    } else {
        trimmed.to_string()
    }
}

/// Strip the forwarded-header family that is NOT the configured mode.
fn strip_opposing_family(request: &mut Request<Body>, mode: ForwardedHeadersMode) {
    let headers = request.headers_mut();
    match mode {
        ForwardedHeadersMode::XForwarded => {
            // Keep x-forwarded-*, strip RFC 7239 Forwarded
            headers.remove(FORWARDED);
        }
        ForwardedHeadersMode::Forwarded => {
            // Keep Forwarded, strip x-forwarded-*
            headers.remove(X_FORWARDED_FOR);
            headers.remove(X_FORWARDED_HOST);
            headers.remove(X_FORWARDED_PROTO);
            headers.remove(X_FORWARDED_PORT);
        }
        ForwardedHeadersMode::None => {
            // Should not happen when enabled=true (validation prevents it),
            // but defend anyway.
            strip_all_forwarded(request);
        }
    }
}

/// Remove all forwarded-header variants from the request.
fn strip_all_forwarded(request: &mut Request<Body>) {
    let headers = request.headers_mut();
    headers.remove(X_FORWARDED_FOR);
    headers.remove(X_FORWARDED_HOST);
    headers.remove(X_FORWARDED_PROTO);
    headers.remove(X_FORWARDED_PORT);
    headers.remove(FORWARDED);
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn exact_ipv4_match() {
        let peers = TrustedPeers::from_config(&["127.0.0.1".to_string(), "10.0.0.1".to_string()]);
        assert!(peers.contains(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(peers.contains(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(!peers.contains(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))));
    }

    #[test]
    fn exact_ipv6_match() {
        let peers = TrustedPeers::from_config(&["::1".to_string()]);
        assert!(peers.contains(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(!peers.contains(IpAddr::V6(Ipv6Addr::UNSPECIFIED)));
    }

    #[test]
    fn cidr_ipv4_24() {
        let peers = TrustedPeers::from_config(&["10.0.0.0/24".to_string()]);
        assert!(peers.contains(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0))));
        assert!(peers.contains(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 255))));
        assert!(!peers.contains(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 0))));
    }

    #[test]
    fn cidr_ipv4_32() {
        let peers = TrustedPeers::from_config(&["192.168.1.1/32".to_string()]);
        assert!(peers.contains(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))));
        assert!(!peers.contains(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2))));
    }

    #[test]
    fn cidr_ipv6_64() {
        let peers = TrustedPeers::from_config(&["fd00::/64".to_string()]);
        assert!(peers.contains("fd00::1".parse().unwrap()));
        assert!(peers.contains("fd00::ffff".parse().unwrap()));
        assert!(!peers.contains("fd01::1".parse().unwrap()));
    }

    #[test]
    fn ipv4_mapped_ipv6() {
        // When the server binds to [::], clients connecting from 127.0.0.1
        // may arrive as ::ffff:127.0.0.1.
        let peers = TrustedPeers::from_config(&["127.0.0.1".to_string()]);
        let mapped: IpAddr = "::ffff:127.0.0.1".parse().unwrap();
        assert!(peers.contains(mapped));
    }

    #[test]
    fn empty_peers_returns_false() {
        let peers = TrustedPeers::from_config(&[]);
        assert!(!peers.contains(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    }

    #[test]
    fn mixed_v4_and_v6_entries() {
        let peers = TrustedPeers::from_config(&["10.0.0.0/8".to_string(), "::1".to_string()]);
        assert!(peers.contains(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))));
        assert!(peers.contains(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(!peers.contains(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1))));
    }

    #[test]
    fn v4_v6_family_mismatch() {
        // An IPv4 entry should not match a non-mapped IPv6 address.
        let peers = TrustedPeers::from_config(&["127.0.0.1".to_string()]);
        assert!(!peers.contains(IpAddr::V6(Ipv6Addr::LOCALHOST)));
    }
}
