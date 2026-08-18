//! Explicit replay capability signing, validation, and local revocation.
//!
//! Capabilities are authorizer-signed tokens that allow one transport peer to
//! replay encrypted data to one target peer for one collection. Verifiers always
//! enforce sender/target/collection binding, expiry, and a maximum remaining TTL
//! so compromised authorizer keys cannot mint effectively-eternal capabilities.
//!
//! Revocation is verifier-local: each process keeps an in-memory deny-list keyed
//! by a stable digest of the signed claims and signature. Deployments that need
//! early invalidation must distribute revoked capabilities to each verifier and
//! call [`revoke_capability`], or use [`ExplicitReplayRevocationRegistry`] with
//! [`verify_capability_with_revocations`] for a custom synced deny-list.

#[cfg(any(feature = "libp2p-transport", feature = "iroh-transport"))]
use std::collections::HashMap;
use std::collections::HashSet;
#[cfg(any(feature = "libp2p-transport", feature = "iroh-transport"))]
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use crypto::{did::parse_did_key, public_key_from_bytes, sha256, Sha256Hash};
use identity::FullIdentity;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

use crate::error::{Error, Result};

pub use defra_core::merge::ExplicitReplayAuthorization;

const CAPABILITY_VERSION: u8 = 1;
const CAPABILITY_PURPOSE: &str = "explicit-replay";
pub const DEFAULT_CAPABILITY_TTL: Duration = Duration::from_secs(365 * 24 * 60 * 60);
pub const MAX_CAPABILITY_TTL: Duration = DEFAULT_CAPABILITY_TTL;

static PROCESS_REVOCATIONS: LazyLock<ExplicitReplayRevocationRegistry> =
    LazyLock::new(ExplicitReplayRevocationRegistry::default);

#[cfg(any(feature = "libp2p-transport", feature = "iroh-transport"))]
#[derive(Debug, Clone)]
struct CachedExplicitReplayCapability {
    capability: String,
    authorizer_did: String,
}

#[cfg(any(feature = "libp2p-transport", feature = "iroh-transport"))]
#[derive(Debug, Clone, Default)]
pub(crate) struct ExplicitReplayCapabilityCache {
    capabilities: Arc<RwLock<HashMap<(String, String), CachedExplicitReplayCapability>>>,
}

#[cfg(any(feature = "libp2p-transport", feature = "iroh-transport"))]
impl ExplicitReplayCapabilityCache {
    pub(crate) fn set(
        &self,
        source_peer_id: &str,
        target_peer_id: &str,
        collection_id: &str,
        capability: &str,
    ) -> Result<()> {
        let authorization =
            verify_capability(capability, source_peer_id, target_peer_id, collection_id)?;
        self.capabilities.write().insert(
            (target_peer_id.to_string(), collection_id.to_string()),
            CachedExplicitReplayCapability {
                capability: capability.to_string(),
                authorizer_did: authorization.authorizer_did,
            },
        );
        Ok(())
    }

    pub(crate) fn clear(&self, target_peer_id: &str, collections: &[String]) {
        let mut capabilities = self.capabilities.write();
        for collection_id in collections {
            capabilities.remove(&(target_peer_id.to_string(), collection_id.clone()));
        }
    }

    pub(crate) fn clear_all(&self, target_peer_id: &str) {
        self.capabilities
            .write()
            .retain(|(stored_peer_id, _), _| stored_peer_id != target_peer_id);
    }

    pub(crate) fn matches(
        &self,
        target_peer_id: &str,
        collection_id: &str,
        capability: &str,
    ) -> bool {
        self.capabilities
            .read()
            .get(&(target_peer_id.to_string(), collection_id.to_string()))
            .is_some_and(|existing| existing.capability == capability)
    }

    pub(crate) fn attach(&self, target_peer_id: &str, request: &mut crate::PushLogRequest) {
        if request.explicit_replay_capability.is_some() {
            return;
        }

        let cached = self
            .capabilities
            .read()
            .get(&(target_peer_id.to_string(), request.collection_id.clone()))
            .cloned();
        if let Some(cached) = cached.filter(|cached| request.creator == cached.authorizer_did) {
            request.explicit_replay_capability = Some(cached.capability);
        }
    }
}

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

#[derive(Debug, Default)]
pub struct ExplicitReplayRevocationRegistry {
    revoked_capabilities: RwLock<HashSet<Sha256Hash>>,
}

impl ExplicitReplayRevocationRegistry {
    pub fn revoke_capability(&self, capability: &str) -> Result<bool> {
        let envelope = decode_envelope(capability)?;
        Ok(self
            .revoked_capabilities
            .write()
            .insert(capability_revocation_key(&envelope)?))
    }

    pub fn is_capability_revoked(&self, capability: &str) -> Result<bool> {
        let envelope = decode_envelope(capability)?;
        self.is_envelope_revoked(&envelope)
    }

    fn is_envelope_revoked(&self, envelope: &ExplicitReplayCapabilityEnvelope) -> Result<bool> {
        Ok(self
            .revoked_capabilities
            .read()
            .contains(&capability_revocation_key(envelope)?))
    }
}

fn now_unix() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| Error::SystemClock(error.to_string()))
}

fn encode_claims(claims: &ExplicitReplayCapabilityClaims) -> Result<Vec<u8>> {
    defra_core::cbor::to_vec(claims).map_err(|error| {
        Error::ExplicitReplayCapability(format!("failed to encode claims: {error}"))
    })
}

fn capability_revocation_key(envelope: &ExplicitReplayCapabilityEnvelope) -> Result<Sha256Hash> {
    let mut bytes = encode_claims(&envelope.claims)?;
    bytes.extend_from_slice(&envelope.signature);
    Ok(sha256(&bytes))
}

fn validate_expiration_cap(expires_at: u64) -> Result<()> {
    let now = now_unix()?;
    if expires_at.saturating_sub(now) > MAX_CAPABILITY_TTL.as_secs() {
        return Err(Error::ExplicitReplayCapability(format!(
            "expires_at {expires_at} exceeds max ttl of {} seconds",
            MAX_CAPABILITY_TTL.as_secs()
        )));
    }

    Ok(())
}

fn decode_envelope(capability: &str) -> Result<ExplicitReplayCapabilityEnvelope> {
    let bytes = URL_SAFE_NO_PAD
        .decode(capability)
        .map_err(|error| Error::ExplicitReplayCapability(format!("invalid encoding: {error}")))?;

    defra_core::cbor::from_slice(&bytes)
        .map_err(|error| Error::ExplicitReplayCapability(format!("invalid payload: {error}")))
}

fn validate_common_claims(claims: &ExplicitReplayCapabilityClaims) -> Result<()> {
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

    if claims.collection_id.is_empty() {
        return Err(Error::ExplicitReplayCapability(
            "collection was empty".to_string(),
        ));
    }

    if claims.authorizer_did.is_empty() {
        return Err(Error::ExplicitReplayCapability(
            "authorizer DID was empty".to_string(),
        ));
    }

    let now = now_unix()?;
    if claims.expires_at < now {
        return Err(Error::ExplicitReplayCapability(format!(
            "expired at {}",
            claims.expires_at
        )));
    }

    if claims.expires_at - now > MAX_CAPABILITY_TTL.as_secs() {
        return Err(Error::ExplicitReplayCapability(format!(
            "expires_at {} exceeds max ttl of {} seconds",
            claims.expires_at,
            MAX_CAPABILITY_TTL.as_secs()
        )));
    }

    Ok(())
}

fn validate_peer_binding(
    claims: &ExplicitReplayCapabilityClaims,
    source_peer_id: &str,
    target_peer_id: &str,
) -> Result<()> {
    if claims.source_peer_id != source_peer_id {
        return Err(Error::ExplicitReplayCapability(format!(
            "source {} did not match expected peer {}",
            claims.source_peer_id, source_peer_id
        )));
    }

    if claims.target_peer_id != target_peer_id {
        return Err(Error::ExplicitReplayCapability(format!(
            "target {} did not match expected peer {}",
            claims.target_peer_id, target_peer_id
        )));
    }

    Ok(())
}

fn validate_claims(
    claims: &ExplicitReplayCapabilityClaims,
    transport_sender_peer_id: &str,
    target_peer_id: &str,
    collection_id: &str,
) -> Result<()> {
    validate_common_claims(claims)?;
    validate_peer_binding(claims, transport_sender_peer_id, target_peer_id)?;
    if claims.collection_id != collection_id {
        return Err(Error::ExplicitReplayCapability(format!(
            "collection {} did not match request collection {}",
            claims.collection_id, collection_id
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
    if lifetime > MAX_CAPABILITY_TTL {
        return Err(Error::ExplicitReplayCapability(format!(
            "lifetime exceeds max ttl of {} seconds",
            MAX_CAPABILITY_TTL.as_secs()
        )));
    }

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
    validate_expiration_cap(claims.expires_at)?;

    let claims_bytes = encode_claims(&claims)?;
    let signature = authorizer
        .sign(&claims_bytes)
        .map_err(|error| Error::ExplicitReplayCapability(format!("failed to sign: {error}")))?;

    let envelope = ExplicitReplayCapabilityEnvelope { claims, signature };

    let envelope_bytes = defra_core::cbor::to_vec(&envelope)
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
    verify_capability_with_revocations(
        capability,
        transport_sender_peer_id,
        target_peer_id,
        collection_id,
        &PROCESS_REVOCATIONS,
    )
}

pub fn verify_capability_with_revocations(
    capability: &str,
    transport_sender_peer_id: &str,
    target_peer_id: &str,
    collection_id: &str,
    revocations: &ExplicitReplayRevocationRegistry,
) -> Result<ExplicitReplayAuthorization> {
    let envelope = decode_envelope(capability)?;
    validate_claims(
        &envelope.claims,
        transport_sender_peer_id,
        target_peer_id,
        collection_id,
    )?;

    verify_envelope(&envelope, revocations)?;

    Ok(authorization_from_envelope(envelope, capability))
}

/// Verify a capability presented by its target peer when requesting replay keys
/// from the source peer that originally sent the encrypted blocks.
pub fn verify_capability_for_key_request(
    capability: &str,
    source_peer_id: &str,
    transport_requester_peer_id: &str,
) -> Result<ExplicitReplayAuthorization> {
    let envelope = decode_envelope(capability)?;
    validate_common_claims(&envelope.claims)?;
    validate_peer_binding(
        &envelope.claims,
        source_peer_id,
        transport_requester_peer_id,
    )?;
    verify_envelope(&envelope, &PROCESS_REVOCATIONS)?;

    Ok(authorization_from_envelope(envelope, capability))
}

fn verify_envelope(
    envelope: &ExplicitReplayCapabilityEnvelope,
    revocations: &ExplicitReplayRevocationRegistry,
) -> Result<()> {
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

    if revocations.is_envelope_revoked(envelope)? {
        return Err(Error::ExplicitReplayCapability(
            "capability has been revoked".to_string(),
        ));
    }

    Ok(())
}

fn authorization_from_envelope(
    envelope: ExplicitReplayCapabilityEnvelope,
    capability: &str,
) -> ExplicitReplayAuthorization {
    ExplicitReplayAuthorization {
        source_peer_id: envelope.claims.source_peer_id,
        target_peer_id: envelope.claims.target_peer_id,
        collection_id: envelope.claims.collection_id,
        authorizer_did: envelope.claims.authorizer_did,
        expires_at: envelope.claims.expires_at,
        capability: Some(capability.to_string()),
    }
}

pub fn revoke_capability(capability: &str) -> Result<bool> {
    PROCESS_REVOCATIONS.revoke_capability(capability)
}

pub fn is_capability_revoked(capability: &str) -> Result<bool> {
    PROCESS_REVOCATIONS.is_capability_revoked(capability)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crypto::generate_ed25519;
    use identity::{Identity, RawIdentity};

    fn authorizer() -> RawIdentity {
        RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap()
    }

    fn generate_capability_from_claims_unchecked<I: FullIdentity>(
        authorizer: &I,
        claims: ExplicitReplayCapabilityClaims,
    ) -> String {
        let claims_bytes = encode_claims(&claims).unwrap();
        let signature = authorizer.sign(&claims_bytes).unwrap();
        let envelope = ExplicitReplayCapabilityEnvelope { claims, signature };
        URL_SAFE_NO_PAD.encode(defra_core::cbor::to_vec(&envelope).unwrap())
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
        assert_eq!(
            authorization.capability.as_deref(),
            Some(capability.as_str())
        );
    }

    #[test]
    fn verify_capability_for_key_request_reverses_source_and_target() {
        let authorizer = authorizer();
        let capability = generate_capability(
            &authorizer,
            "source-peer",
            "target-peer",
            "collection-a",
            Duration::from_secs(60),
        )
        .unwrap();

        let authorization =
            verify_capability_for_key_request(&capability, "source-peer", "target-peer").unwrap();
        assert_eq!(authorization.collection_id, "collection-a");
        assert!(
            verify_capability_for_key_request(&capability, "other-source", "target-peer").is_err()
        );
        assert!(
            verify_capability_for_key_request(&capability, "source-peer", "other-target").is_err()
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
    fn generate_capability_from_claims_rejects_effectively_eternal_expiry() {
        let authorizer = authorizer();
        let source_peer_id = "peer-source".to_string();
        let error = generate_capability_from_claims(
            &authorizer,
            ExplicitReplayCapabilityClaims {
                version: CAPABILITY_VERSION,
                purpose: CAPABILITY_PURPOSE.to_string(),
                source_peer_id,
                target_peer_id: "peer-target".to_string(),
                collection_id: "collection-a".to_string(),
                authorizer_did: authorizer.did().unwrap().to_string(),
                expires_at: u64::MAX,
            },
        )
        .unwrap_err();

        let msg = error.to_string();
        assert!(msg.contains("max ttl"), "unexpected error: {msg}");
    }

    #[test]
    fn verify_capability_rejects_effectively_eternal_expiry() {
        let authorizer = authorizer();
        let source_peer_id = "peer-source".to_string();
        let capability = generate_capability_from_claims_unchecked(
            &authorizer,
            ExplicitReplayCapabilityClaims {
                version: CAPABILITY_VERSION,
                purpose: CAPABILITY_PURPOSE.to_string(),
                source_peer_id: source_peer_id.clone(),
                target_peer_id: "peer-target".to_string(),
                collection_id: "collection-a".to_string(),
                authorizer_did: authorizer.did().unwrap().to_string(),
                expires_at: u64::MAX,
            },
        );

        let error = verify_capability(&capability, &source_peer_id, "peer-target", "collection-a")
            .unwrap_err();

        let msg = error.to_string();
        assert!(msg.contains("max ttl"), "unexpected error: {msg}");
    }

    #[test]
    fn generate_capability_rejects_lifetime_past_max_ttl() {
        let authorizer = authorizer();
        let source_peer_id = "peer-source".to_string();
        let error = generate_capability(
            &authorizer,
            &source_peer_id,
            "peer-target",
            "collection-a",
            MAX_CAPABILITY_TTL + Duration::from_secs(1),
        )
        .unwrap_err();

        let msg = error.to_string();
        assert!(msg.contains("max ttl"), "unexpected error: {msg}");
    }

    #[test]
    fn verify_capability_rejects_expiry_past_max_ttl() {
        let authorizer = authorizer();
        let source_peer_id = "peer-source".to_string();
        let capability = generate_capability_from_claims_unchecked(
            &authorizer,
            ExplicitReplayCapabilityClaims {
                version: CAPABILITY_VERSION,
                purpose: CAPABILITY_PURPOSE.to_string(),
                source_peer_id: source_peer_id.clone(),
                target_peer_id: "peer-target".to_string(),
                collection_id: "collection-a".to_string(),
                authorizer_did: authorizer.did().unwrap().to_string(),
                expires_at: now_unix()
                    .unwrap()
                    .saturating_add(MAX_CAPABILITY_TTL.as_secs())
                    .saturating_add(60),
            },
        );

        let error = verify_capability(&capability, &source_peer_id, "peer-target", "collection-a")
            .unwrap_err();

        let msg = error.to_string();
        assert!(msg.contains("max ttl"), "unexpected error: {msg}");
    }

    #[test]
    fn verify_capability_accepts_expiry_within_max_ttl() {
        let authorizer = authorizer();
        let source_peer_id = "peer-source".to_string();
        let capability = generate_capability(
            &authorizer,
            &source_peer_id,
            "peer-target",
            "collection-a",
            MAX_CAPABILITY_TTL,
        )
        .unwrap();

        let authorization =
            verify_capability(&capability, &source_peer_id, "peer-target", "collection-a").unwrap();

        assert_eq!(authorization.collection_id, "collection-a");
    }

    #[test]
    fn verify_capability_rejects_revoked_capability() {
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
        let revocations = ExplicitReplayRevocationRegistry::default();

        assert!(!revocations.is_capability_revoked(&capability).unwrap());
        assert!(revocations.revoke_capability(&capability).unwrap());
        assert!(revocations.is_capability_revoked(&capability).unwrap());

        let error = verify_capability_with_revocations(
            &capability,
            &source_peer_id,
            "peer-target",
            "collection-a",
            &revocations,
        )
        .unwrap_err();

        let msg = error.to_string();
        assert!(msg.contains("revoked"), "unexpected error: {msg}");
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
