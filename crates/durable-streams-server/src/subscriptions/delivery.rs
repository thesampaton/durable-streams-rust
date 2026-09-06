use super::{ApiError, ApiResult};
use serde_json::Value;
use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

// Validate every resolved address, then pin the checked answers in the client.
// Redirects and environment proxies are disabled to prevent bypassing this check.
pub(super) async fn webhook_client(
    raw: &str,
    allow_local: bool,
) -> ApiResult<(reqwest::Client, reqwest::Url)> {
    let rejected = || {
        ApiError::bad(
            "WEBHOOK_URL_REJECTED",
            "webhook URL must resolve to an allowed HTTPS target",
        )
    };
    let url = reqwest::Url::parse(raw).map_err(|_| rejected())?;
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(rejected());
    }
    let host = url
        .host_str()
        .ok_or_else(rejected)?
        .trim_matches(['[', ']']);
    let local_name = host == "localhost"
        || host
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|ip| ip.octets()[..3] == [127, 0, 0]);
    let development = allow_local && local_name;
    if url.scheme() != "https" && !(development && url.scheme() == "http") {
        return Err(rejected());
    }
    let port = url.port_or_known_default().ok_or_else(rejected)?;
    let addresses: Vec<SocketAddr> = tokio::time::timeout(
        Duration::from_secs(3),
        tokio::net::lookup_host((host, port)),
    )
    .await
    .map_err(|_| rejected())?
    .map_err(|_| rejected())?
    .collect();
    if addresses.is_empty()
        || addresses
            .iter()
            .any(|a| !(public_ip(a.ip()) || development && a.ip().is_loopback()))
    {
        return Err(rejected());
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .resolve_to_addrs(host, &addresses)
        .build()
        .map_err(|_| ApiError::internal("could not create webhook client"))?;
    Ok((client, url))
}

fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, _, _] = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_broadcast()
                && !ip.is_documentation()
                && !ip.is_multicast()
                && a != 0
                && a < 240
                && !(a == 100 && (64..=127).contains(&b))
                && !(a == 198 && (b == 18 || b == 19))
                && !(a == 192 && b == 0)
        }
        IpAddr::V6(ip) => {
            // Only global unicast; reject mapped IPv4, ULA, link-local and documentation ranges.
            let seg = ip.segments();
            seg[0] & 0xe000 == 0x2000
                && !(seg[0] == 0x2001 && seg[1] < 0x200)
                && !(seg[0] == 0x2001 && seg[1] == 0xdb8)
                && seg[0] != 0x2002
        }
    }
}

pub(super) async fn deliver(
    url: &str,
    local: bool,
    body: Vec<u8>,
    signature: String,
) -> ApiResult<bool> {
    let (client, url) = webhook_client(url, local).await?;
    let mut response = client
        .post(url)
        .header("content-type", "application/json")
        .header("webhook-signature", signature)
        .body(body)
        .send()
        .await
        .map_err(|_| ApiError::internal("webhook delivery failed"))?;
    if !response.status().is_success() {
        return Err(ApiError::internal(
            "webhook returned an unsuccessful status",
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| ApiError::internal("webhook response failed"))?
    {
        if bytes.len() + chunk.len() > 65_536 {
            return Err(ApiError::internal("webhook response too large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice::<Value>(&bytes)
        .ok()
        .is_some_and(|body| body.get("done") == Some(&Value::Bool(true))))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_webhook_validation_rejects_local_targets_and_redirect_credentials() {
        for url in [
            "http://127.0.0.1/hook",
            "https://10.0.0.1/hook",
            "https://169.254.169.254/latest",
            "https://[::1]/hook",
            "https://[::ffff:127.0.0.1]/hook",
            "https://user:pass@example.com/hook",
        ] {
            assert!(webhook_client(url, false).await.is_err(), "accepted {url}");
        }
        assert!(webhook_client("http://127.0.0.1/hook", true).await.is_ok());
        assert!(webhook_client("http://10.0.0.1/hook", true).await.is_err());
    }
}
