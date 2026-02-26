use std::time::{SystemTime, UNIX_EPOCH};

use k256::ecdsa::{signature::Signer, Signature, SigningKey};

/// Create an ES256K JWT bearer token for hub.rs ACP operations.
///
/// The token uses the `did:key:z...` format for the issuer, derived from the
/// compressed secp256k1 public key with multicodec prefix.
pub fn create_bearer_token(
    signing_key: &SigningKey,
    subject: &str,
    expiry_secs: u64,
) -> Result<String, BearerError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| BearerError::Time(e.to_string()))?
        .as_secs();

    let issuer = did_from_signing_key(signing_key);

    let header = base64url_encode(br#"{"alg":"ES256K","typ":"JWT"}"#);
    let payload_json = format!(
        r#"{{"iss":"{}","sub":"{}","exp":{}}}"#,
        issuer,
        subject,
        now + expiry_secs
    );
    let payload = base64url_encode(payload_json.as_bytes());

    let message = format!("{}.{}", header, payload);
    let signature: Signature = signing_key.sign(message.as_bytes());
    let sig_b64 = base64url_encode(&signature.to_bytes());

    Ok(format!("{}.{}", message, sig_b64))
}

fn did_from_signing_key(key: &SigningKey) -> String {
    let verifying_key = key.verifying_key();
    let compressed = verifying_key.to_sec1_bytes();
    // multicodec prefix for secp256k1-pub: 0xe7 0x01
    let mut multicodec = vec![0xe7, 0x01];
    multicodec.extend_from_slice(&compressed);
    let encoded = bs58::encode(&multicodec).into_string();
    format!("did:key:z{}", encoded)
}

fn base64url_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

#[derive(Debug, thiserror::Error)]
pub enum BearerError {
    #[error("system time error: {0}")]
    Time(String),
}
