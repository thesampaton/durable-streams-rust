use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::{
    digest,
    rand::{SecureRandom, SystemRandom},
    signature::{self, Ed25519KeyPair, KeyPair},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{ApiError, ApiResult};

pub(super) fn new_key() -> ApiResult<Vec<u8>> {
    Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map(|k| k.as_ref().to_vec())
        .map_err(|_| ApiError::internal("could not generate signing key"))
}

fn key(bytes: &[u8]) -> ApiResult<Ed25519KeyPair> {
    Ed25519KeyPair::from_pkcs8(bytes)
        .map_err(|_| ApiError::internal("invalid persisted signing key"))
}

pub(super) fn hash(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(digest::digest(&digest::SHA256, bytes))
}

pub(super) fn jwk(bytes: &[u8]) -> ApiResult<Value> {
    let key = key(bytes)?;
    let x = URL_SAFE_NO_PAD.encode(key.public_key().as_ref());
    let thumbprint =
        hash(format!("{{\"crv\":\"Ed25519\",\"kty\":\"OKP\",\"x\":\"{x}\"}}").as_bytes());
    Ok(
        json!({"kty":"OKP", "crv":"Ed25519", "kid":format!("ds_{thumbprint}"), "use":"sig", "alg":"EdDSA", "x":x}),
    )
}

pub(super) fn random_id() -> ApiResult<String> {
    let mut bytes = [0; 24];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| ApiError::internal("random source unavailable"))?;
    Ok(format!("w_{}", URL_SAFE_NO_PAD.encode(bytes)))
}

pub(super) fn retry_jitter() -> ApiResult<i64> {
    let mut bytes = [0; 2];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| ApiError::internal("random source unavailable"))?;
    Ok(i64::from(u16::from_le_bytes(bytes)) * 400 / i64::from(u16::MAX) - 200)
}

pub(super) fn signature(key_bytes: &[u8], data: &[u8]) -> ApiResult<String> {
    Ok(URL_SAFE_NO_PAD.encode(key(key_bytes)?.sign(data)))
}

#[derive(Serialize, Deserialize)]
pub(super) struct Claims {
    pub subscription: String,
    pub generation: u64,
    pub wake_id: String,
}

// The server validates the current persisted lease on every use. A heartbeat can
// extend that lease without replacing the token; release/expiry revokes it.
pub(super) fn token(key: &[u8], claims: &Claims) -> ApiResult<String> {
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).map_err(ApiError::json)?);
    Ok(format!("{payload}.{}", signature(key, payload.as_bytes())?))
}

pub(super) fn verify(key_bytes: &[u8], token: &str) -> ApiResult<Claims> {
    let invalid = || ApiError::fenced();
    let (payload, sig) = token.split_once('.').ok_or_else(invalid)?;
    let sig = URL_SAFE_NO_PAD.decode(sig).map_err(|_| invalid())?;
    let key = key(key_bytes)?;
    signature::UnparsedPublicKey::new(&signature::ED25519, key.public_key().as_ref())
        .verify(payload.as_bytes(), &sig)
        .map_err(|_| invalid())?;
    let bytes = URL_SAFE_NO_PAD.decode(payload).map_err(|_| invalid())?;
    serde_json::from_slice(&bytes).map_err(|_| invalid())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_tokens_require_the_original_key_and_unmodified_claims() {
        let key = new_key().unwrap();
        let claims = Claims {
            subscription: "one".into(),
            generation: 7,
            wake_id: "wake".into(),
        };
        let token = token(&key, &claims).unwrap();
        assert_eq!(verify(&key, &token).unwrap().generation, 7);
        assert!(verify(&new_key().unwrap(), &token).is_err());
        let (_, signature) = token.split_once('.').unwrap();
        let substituted =
            URL_SAFE_NO_PAD.encode(br#"{"subscription":"two","generation":7,"wake_id":"wake"}"#);
        assert!(verify(&key, &format!("{substituted}.{signature}")).is_err());
    }
}
