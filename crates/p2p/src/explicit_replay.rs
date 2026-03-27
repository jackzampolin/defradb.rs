use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use crypto::{did::parse_did_key, public_key_from_bytes};
use identity::FullIdentity;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

const CAPABILITY_VERSION: u8 = 1;
const CAPABILITY_PURPOSE: &str = "explicit-replay";
pub const DEFAULT_CAPABILITY_TTL: Duration = Duration::from_secs(365 * 24 * 60 * 60);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExplicitReplayCapabilityClaims {
    pub version: u8,
    pub purpose: String,
    pub source_peer_id: String,
    pub target_peer_id: String,
    pub collection_id: String,
    pub authorizer_did: String,
    pub expires_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ExplicitReplayCapabilityEnvelope {
    claims: ExplicitReplayCapabilityClaims,
    #[serde(with = "serde_bytes")]
    signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplicitReplayAuthorization {
    pub source_peer_id: String,
    pub target_peer_id: String,
    pub collection_id: String,
    pub authorizer_did: String,
    pub expires_at: u64,
}

fn now_unix() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| Error::SystemClock(error.to_string()))
}

fn encode_claims(claims: &ExplicitReplayCapabilityClaims) -> Result<Vec<u8>> {
    serde_cbor::to_vec(claims).map_err(|error| {
        Error::ExplicitReplayCapability(format!("failed to encode claims: {error}"))
    })
}

fn decode_envelope(capability: &str) -> Result<ExplicitReplayCapabilityEnvelope> {
    let bytes = URL_SAFE_NO_PAD
        .decode(capability)
        .map_err(|error| Error::ExplicitReplayCapability(format!("invalid encoding: {error}")))?;

    serde_cbor::from_slice(&bytes)
        .map_err(|error| Error::ExplicitReplayCapability(format!("invalid payload: {error}")))
}

fn validate_claims(
    claims: &ExplicitReplayCapabilityClaims,
    transport_sender_peer_id: &str,
    target_peer_id: &str,
    collection_id: &str,
) -> Result<()> {
    if claims.version != CAPABILITY_VERSION {
        return Err(Error::ExplicitReplayCapability(format!(
            "unsupported version {}",
            claims.version
        )));
    }

    if claims.purpose != CAPABILITY_PURPOSE {
        return Err(Error::ExplicitReplayCapability(format!(
            "unexpected purpose {}",
            claims.purpose
        )));
    }

    if claims.source_peer_id != transport_sender_peer_id {
        return Err(Error::ExplicitReplayCapability(format!(
            "source {} did not match transport sender {}",
            claims.source_peer_id, transport_sender_peer_id
        )));
    }

    if claims.target_peer_id != target_peer_id {
        return Err(Error::ExplicitReplayCapability(format!(
            "target {} did not match local peer {}",
            claims.target_peer_id, target_peer_id
        )));
    }

    if claims.collection_id != collection_id {
        return Err(Error::ExplicitReplayCapability(format!(
            "collection {} did not match request collection {}",
            claims.collection_id, collection_id
        )));
    }

    if claims.authorizer_did.is_empty() {
        return Err(Error::ExplicitReplayCapability(
            "authorizer DID was empty".to_string(),
        ));
    }

    if claims.expires_at < now_unix()? {
        return Err(Error::ExplicitReplayCapability(format!(
            "expired at {}",
            claims.expires_at
        )));
    }

    Ok(())
}

pub fn generate_capability<I: FullIdentity>(
    authorizer: &I,
    source_peer_id: &str,
    target_peer_id: &str,
    collection_id: &str,
    lifetime: Duration,
) -> Result<String> {
    let issued_at = now_unix()?;
    let expires_at = issued_at.saturating_add(lifetime.as_secs());
    let authorizer_did = authorizer.did().map_err(|error| {
        Error::ExplicitReplayCapability(format!("failed to derive authorizer DID: {error}"))
    })?;
    let claims = ExplicitReplayCapabilityClaims {
        version: CAPABILITY_VERSION,
        purpose: CAPABILITY_PURPOSE.to_string(),
        source_peer_id: source_peer_id.to_string(),
        target_peer_id: target_peer_id.to_string(),
        collection_id: collection_id.to_string(),
        authorizer_did: authorizer_did.to_string(),
        expires_at,
    };
    generate_capability_from_claims(authorizer, claims)
}

pub fn generate_capability_from_claims<I: FullIdentity>(
    authorizer: &I,
    claims: ExplicitReplayCapabilityClaims,
) -> Result<String> {
    let claims_bytes = encode_claims(&claims)?;
    let signature = authorizer
        .sign(&claims_bytes)
        .map_err(|error| Error::ExplicitReplayCapability(format!("failed to sign: {error}")))?;

    let envelope = ExplicitReplayCapabilityEnvelope { claims, signature };

    let envelope_bytes = serde_cbor::to_vec(&envelope)
        .map_err(|error| Error::ExplicitReplayCapability(format!("failed to encode: {error}")))?;

    Ok(URL_SAFE_NO_PAD.encode(envelope_bytes))
}

fn decode_authorizer_public_key(authorizer_did: &str) -> Result<Box<dyn crypto::keys::PublicKey>> {
    let (key_type, public_key_bytes) = parse_did_key(authorizer_did).map_err(|error| {
        Error::ExplicitReplayCapability(format!("invalid authorizer DID: {error}"))
    })?;
    public_key_from_bytes(key_type, &public_key_bytes).map_err(|error| {
        Error::ExplicitReplayCapability(format!("invalid authorizer key: {error}"))
    })
}

pub fn verify_capability(
    capability: &str,
    transport_sender_peer_id: &str,
    target_peer_id: &str,
    collection_id: &str,
) -> Result<ExplicitReplayAuthorization> {
    let envelope = decode_envelope(capability)?;
    validate_claims(
        &envelope.claims,
        transport_sender_peer_id,
        target_peer_id,
        collection_id,
    )?;

    let public_key = decode_authorizer_public_key(&envelope.claims.authorizer_did)?;
    let claims_bytes = encode_claims(&envelope.claims)?;
    if !public_key
        .verify(&claims_bytes, &envelope.signature)
        .map_err(|error| {
            Error::ExplicitReplayCapability(format!("signature verification error: {error}"))
        })?
    {
        return Err(Error::ExplicitReplayCapability(
            "signature verification failed".to_string(),
        ));
    }

    Ok(ExplicitReplayAuthorization {
        source_peer_id: envelope.claims.source_peer_id,
        target_peer_id: envelope.claims.target_peer_id,
        collection_id: envelope.claims.collection_id,
        authorizer_did: envelope.claims.authorizer_did,
        expires_at: envelope.claims.expires_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crypto::generate_ed25519;
    use identity::{Identity, RawIdentity};

    fn authorizer() -> RawIdentity {
        RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap()
    }

    #[test]
    fn verify_capability_accepts_valid_sender_target_and_collection() {
        let authorizer = authorizer();
        let source_peer_id = "peer-source".to_string();
        let capability = generate_capability(
            &authorizer,
            &source_peer_id,
            "peer-target",
            "collection-a",
            Duration::from_secs(60),
        )
        .unwrap();

        let authorization =
            verify_capability(&capability, &source_peer_id, "peer-target", "collection-a").unwrap();

        assert_eq!(authorization.source_peer_id, source_peer_id);
        assert_eq!(authorization.target_peer_id, "peer-target");
        assert_eq!(authorization.collection_id, "collection-a");
        assert_eq!(
            authorization.authorizer_did,
            authorizer.did().unwrap().to_string()
        );
    }

    #[test]
    fn verify_capability_rejects_collection_mismatch() {
        let authorizer = authorizer();
        let source_peer_id = "peer-source".to_string();
        let capability = generate_capability(
            &authorizer,
            &source_peer_id,
            "peer-target",
            "collection-a",
            Duration::from_secs(60),
        )
        .unwrap();

        let error = verify_capability(&capability, &source_peer_id, "peer-target", "collection-b")
            .unwrap_err();

        let msg = error.to_string();
        assert!(
            msg.contains("did not match request collection"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn verify_capability_rejects_expired_capability() {
        let authorizer = authorizer();
        let source_peer_id = "peer-source".to_string();
        let capability = generate_capability_from_claims(
            &authorizer,
            ExplicitReplayCapabilityClaims {
                version: CAPABILITY_VERSION,
                purpose: CAPABILITY_PURPOSE.to_string(),
                source_peer_id: source_peer_id.clone(),
                target_peer_id: "peer-target".to_string(),
                collection_id: "collection-a".to_string(),
                authorizer_did: authorizer.did().unwrap().to_string(),
                expires_at: now_unix().unwrap().saturating_sub(1),
            },
        )
        .unwrap();

        let error = verify_capability(&capability, &source_peer_id, "peer-target", "collection-a")
            .unwrap_err();

        let msg = error.to_string();
        assert!(msg.contains("expired"), "unexpected error: {msg}");
    }

    #[test]
    fn verify_capability_rejects_wrong_authorizer_signature() {
        let claimed_authorizer = authorizer();
        let wrong_signer = authorizer();
        let source_peer_id = "peer-source".to_string();
        let capability = generate_capability_from_claims(
            &wrong_signer,
            ExplicitReplayCapabilityClaims {
                version: CAPABILITY_VERSION,
                purpose: CAPABILITY_PURPOSE.to_string(),
                source_peer_id: source_peer_id.clone(),
                target_peer_id: "peer-target".to_string(),
                collection_id: "collection-a".to_string(),
                authorizer_did: claimed_authorizer.did().unwrap().to_string(),
                expires_at: now_unix().unwrap().saturating_add(60),
            },
        )
        .unwrap();

        let error = verify_capability(&capability, &source_peer_id, "peer-target", "collection-a")
            .unwrap_err();

        let msg = error.to_string();
        assert!(msg.contains("signature"), "unexpected error: {msg}");
    }
}
