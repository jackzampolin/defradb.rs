use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use libp2p::identity::{Keypair, PublicKey};
use serde::{Deserialize, Serialize};

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
    source_pubkey: Vec<u8>,
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

fn now_unix() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| format!("system clock error: {error}"))
}

fn encode_claims(claims: &ExplicitReplayCapabilityClaims) -> Result<Vec<u8>, String> {
    serde_cbor::to_vec(claims)
        .map_err(|error| format!("failed to encode explicit replay claims: {error}"))
}

fn decode_envelope(capability: &str) -> Result<ExplicitReplayCapabilityEnvelope, String> {
    let bytes = URL_SAFE_NO_PAD
        .decode(capability)
        .map_err(|error| format!("invalid explicit replay capability encoding: {error}"))?;

    serde_cbor::from_slice(&bytes)
        .map_err(|error| format!("invalid explicit replay capability payload: {error}"))
}

fn validate_claims(
    claims: &ExplicitReplayCapabilityClaims,
    transport_sender_peer_id: &str,
    target_peer_id: &str,
    collection_id: &str,
) -> Result<(), String> {
    if claims.version != CAPABILITY_VERSION {
        return Err(format!(
            "unsupported explicit replay capability version {}",
            claims.version
        ));
    }

    if claims.purpose != CAPABILITY_PURPOSE {
        return Err(format!(
            "unexpected explicit replay capability purpose {}",
            claims.purpose
        ));
    }

    if claims.source_peer_id != transport_sender_peer_id {
        return Err(format!(
            "explicit replay capability source {} did not match transport sender {}",
            claims.source_peer_id, transport_sender_peer_id
        ));
    }

    if claims.target_peer_id != target_peer_id {
        return Err(format!(
            "explicit replay capability target {} did not match local peer {}",
            claims.target_peer_id, target_peer_id
        ));
    }

    if claims.collection_id != collection_id {
        return Err(format!(
            "explicit replay capability collection {} did not match request collection {}",
            claims.collection_id, collection_id
        ));
    }

    if claims.authorizer_did.is_empty() {
        return Err("explicit replay capability authorizer DID was empty".to_string());
    }

    if claims.expires_at < now_unix()? {
        return Err(format!(
            "explicit replay capability expired at {}",
            claims.expires_at
        ));
    }

    Ok(())
}

pub fn generate_capability(
    keypair: &Keypair,
    source_peer_id: &str,
    target_peer_id: &str,
    collection_id: &str,
    authorizer_did: &str,
    lifetime: Duration,
) -> Result<String, String> {
    let issued_at = now_unix()?;
    let expires_at = issued_at.saturating_add(lifetime.as_secs());
    let claims = ExplicitReplayCapabilityClaims {
        version: CAPABILITY_VERSION,
        purpose: CAPABILITY_PURPOSE.to_string(),
        source_peer_id: source_peer_id.to_string(),
        target_peer_id: target_peer_id.to_string(),
        collection_id: collection_id.to_string(),
        authorizer_did: authorizer_did.to_string(),
        expires_at,
    };
    generate_capability_from_claims(keypair, claims)
}

pub fn generate_capability_from_claims(
    keypair: &Keypair,
    claims: ExplicitReplayCapabilityClaims,
) -> Result<String, String> {
    let claims_bytes = encode_claims(&claims)?;
    let signature = keypair
        .sign(&claims_bytes)
        .map_err(|error| format!("failed to sign explicit replay capability: {error}"))?;

    let envelope = ExplicitReplayCapabilityEnvelope {
        claims,
        source_pubkey: keypair.public().encode_protobuf(),
        signature,
    };

    let envelope_bytes = serde_cbor::to_vec(&envelope)
        .map_err(|error| format!("failed to encode explicit replay capability: {error}"))?;

    Ok(URL_SAFE_NO_PAD.encode(envelope_bytes))
}

pub fn verify_capability(
    capability: &str,
    transport_sender_peer_id: &str,
    target_peer_id: &str,
    collection_id: &str,
) -> Result<ExplicitReplayAuthorization, String> {
    let envelope = decode_envelope(capability)?;
    validate_claims(
        &envelope.claims,
        transport_sender_peer_id,
        target_peer_id,
        collection_id,
    )?;

    let public_key = PublicKey::try_decode_protobuf(&envelope.source_pubkey)
        .map_err(|error| format!("invalid explicit replay capability public key: {error}"))?;

    let derived_peer_id = public_key.to_peer_id().to_string();
    if derived_peer_id != envelope.claims.source_peer_id {
        return Err(format!(
            "explicit replay capability source key derived peer {} did not match claim {}",
            derived_peer_id, envelope.claims.source_peer_id
        ));
    }

    let claims_bytes = encode_claims(&envelope.claims)?;
    if !public_key.verify(&claims_bytes, &envelope.signature) {
        return Err("explicit replay capability signature verification failed".to_string());
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

    fn keypair() -> Keypair {
        Keypair::generate_ed25519()
    }

    #[test]
    fn verify_capability_accepts_valid_sender_target_and_collection() {
        let keypair = keypair();
        let source_peer_id = keypair.public().to_peer_id().to_string();
        let capability = generate_capability(
            &keypair,
            &source_peer_id,
            "peer-target",
            "collection-a",
            "did:key:z6MkAuthorizer",
            Duration::from_secs(60),
        )
        .unwrap();

        let authorization =
            verify_capability(&capability, &source_peer_id, "peer-target", "collection-a").unwrap();

        assert_eq!(authorization.source_peer_id, source_peer_id);
        assert_eq!(authorization.target_peer_id, "peer-target");
        assert_eq!(authorization.collection_id, "collection-a");
        assert_eq!(authorization.authorizer_did, "did:key:z6MkAuthorizer");
    }

    #[test]
    fn verify_capability_rejects_collection_mismatch() {
        let keypair = keypair();
        let source_peer_id = keypair.public().to_peer_id().to_string();
        let capability = generate_capability(
            &keypair,
            &source_peer_id,
            "peer-target",
            "collection-a",
            "did:key:z6MkAuthorizer",
            Duration::from_secs(60),
        )
        .unwrap();

        let error = verify_capability(&capability, &source_peer_id, "peer-target", "collection-b")
            .unwrap_err();

        assert!(
            error.contains("did not match request collection"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn verify_capability_rejects_expired_capability() {
        let keypair = keypair();
        let source_peer_id = keypair.public().to_peer_id().to_string();
        let capability = generate_capability_from_claims(
            &keypair,
            ExplicitReplayCapabilityClaims {
                version: CAPABILITY_VERSION,
                purpose: CAPABILITY_PURPOSE.to_string(),
                source_peer_id: source_peer_id.clone(),
                target_peer_id: "peer-target".to_string(),
                collection_id: "collection-a".to_string(),
                authorizer_did: "did:key:z6MkAuthorizer".to_string(),
                expires_at: now_unix().unwrap().saturating_sub(1),
            },
        )
        .unwrap();

        let error = verify_capability(&capability, &source_peer_id, "peer-target", "collection-a")
            .unwrap_err();

        assert!(error.contains("expired"), "unexpected error: {error}");
    }
}
