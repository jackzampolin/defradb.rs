//! Block signing configuration for CRDT block signing.
//!
//! Provides the SigningConfig type and thread-local storage for passing
//! signing config from the FFI/query layer to the block builder layer.
//! Mirrors the pattern used by encryption.rs for EncryptionConfig.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use zeroize::Zeroize;

/// Remote signing delegate (e.g. Orbis, Secure Enclave host callbacks).
///
/// Implementations call an external service to produce signatures.
/// `sign_sync` must be callable from synchronous contexts (the block builder
/// runs inside `spawn_blocking`).
pub trait RemoteSigner: Send + Sync {
    fn sign_sync(
        &self,
        data: &[u8],
        authorization: Option<&SigningAuthorization>,
    ) -> Result<Vec<u8>, String>;
}

/// Optional authorization context passed to remote signers.
///
/// For Orbis-backed signing, this lets DefraDB attach ACP metadata to a sign
/// request so Orbis can authorize the signature ceremony before producing a
/// threshold signature.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SigningAuthorization {
    Policy {
        policy_id: String,
        resource: String,
        object_id: String,
        permission: String,
    },
    Decision {
        decision_id: String,
    },
}

/// Key type for signing operations.
///
/// Replaces raw string matching with a type-safe enum. Serializes to/from
/// lowercase strings ("ed25519", "secp256k1", "secp256r1", "bls") for
/// backward compatibility with JSON/FFI boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum SigningKeyType {
    Ed25519,
    Secp256k1,
    Secp256r1,
    Bls,
}

impl SigningKeyType {
    /// Return the string representation for backward compatibility.
    ///
    /// Callers that previously used `.key_type.as_str()` on the old String
    /// field can use this method without changes.
    pub fn as_str(&self) -> &'static str {
        match self {
            SigningKeyType::Ed25519 => "ed25519",
            SigningKeyType::Secp256k1 => "secp256k1",
            SigningKeyType::Secp256r1 => "secp256r1",
            SigningKeyType::Bls => "bls",
        }
    }

    /// Infallible conversion to the block-level `SignatureType`.
    pub fn to_signature_type(self) -> crate::block::SignatureType {
        match self {
            SigningKeyType::Ed25519 => crate::block::SignatureType::EdDSA,
            SigningKeyType::Secp256k1 => crate::block::SignatureType::ES256K,
            SigningKeyType::Secp256r1 => crate::block::SignatureType::ES256,
            SigningKeyType::Bls => crate::block::SignatureType::BLS,
        }
    }
}

impl fmt::Display for SigningKeyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SigningKeyType::Ed25519 => write!(f, "ed25519"),
            SigningKeyType::Secp256k1 => write!(f, "secp256k1"),
            SigningKeyType::Secp256r1 => write!(f, "secp256r1"),
            SigningKeyType::Bls => write!(f, "bls"),
        }
    }
}

impl std::str::FromStr for SigningKeyType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ed25519" => Ok(SigningKeyType::Ed25519),
            "secp256k1" => Ok(SigningKeyType::Secp256k1),
            "secp256r1" => Ok(SigningKeyType::Secp256r1),
            "bls" => Ok(SigningKeyType::Bls),
            other => Err(format!("unsupported signing key type: {other}")),
        }
    }
}

/// Backward-compat: allow `key_type == "secp256k1"` comparisons.
impl PartialEq<&str> for SigningKeyType {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

/// Backward-compat: allow converting to String for callers that need it.
impl From<SigningKeyType> for String {
    fn from(kt: SigningKeyType) -> Self {
        kt.to_string()
    }
}

impl From<SigningKeyType> for crate::block::SignatureType {
    fn from(kt: SigningKeyType) -> Self {
        kt.to_signature_type()
    }
}

/// Signing configuration containing key material for block signing.
pub struct SigningConfig {
    /// Signing key type.
    pub key_type: SigningKeyType,
    /// Raw private key bytes (empty for remote signers like Orbis)
    pub private_key_bytes: Vec<u8>,
    /// Raw public key bytes (for identity in signature header)
    pub public_key_bytes: Vec<u8>,
    /// Public key hex string (for signature header identity, matches Go's pubKey.String())
    pub public_key_hex: String,
    /// Optional remote signer for delegated signing (e.g. Orbis ring)
    pub remote_signer: Option<Arc<dyn RemoteSigner>>,
    /// Optional per-request authorization for delegated signing.
    pub signing_authorization: Option<SigningAuthorization>,
}

impl Clone for SigningConfig {
    fn clone(&self) -> Self {
        Self {
            key_type: self.key_type,
            private_key_bytes: Self::private_key_bytes_from_slice(&self.private_key_bytes),
            public_key_bytes: self.public_key_bytes.clone(),
            public_key_hex: self.public_key_hex.clone(),
            remote_signer: self.remote_signer.clone(),
            signing_authorization: self.signing_authorization.clone(),
        }
    }
}

impl Drop for SigningConfig {
    fn drop(&mut self) {
        self.private_key_bytes.zeroize();
    }
}

impl SigningConfig {
    /// Copy private key bytes into an exactly sized allocation.
    pub fn private_key_bytes_from_slice(bytes: &[u8]) -> Vec<u8> {
        let mut exact = Vec::with_capacity(bytes.len());
        exact.extend_from_slice(bytes);
        exact
    }

    /// Normalize owned private key bytes to an exactly sized allocation.
    pub fn private_key_bytes_from_vec(mut bytes: Vec<u8>) -> Vec<u8> {
        if bytes.len() == bytes.capacity() {
            bytes
        } else {
            let exact = Self::private_key_bytes_from_slice(&bytes);
            // Wipe spare capacity too: oversized buffers may have held key
            // material while being built incrementally.
            bytes.resize(bytes.capacity(), 0);
            bytes.zeroize();
            exact
        }
    }

    /// Returns true if this identity has locally exportable private key bytes.
    pub fn has_local_private_key(&self) -> bool {
        !self.private_key_bytes.is_empty()
    }

    /// Returns true if this identity can sign by delegating to a remote signer.
    pub fn has_remote_signer(&self) -> bool {
        self.remote_signer.is_some()
    }
}

thread_local! {
    static CURRENT_SIGNING_CONFIG: RefCell<Option<SigningConfig>> = const { RefCell::new(None) };
}

/// Set the signing config for the current thread.
pub fn set_signing_config(config: Option<SigningConfig>) {
    CURRENT_SIGNING_CONFIG.with(|c| {
        *c.borrow_mut() = config;
    });
}

/// Get the current signing config for this thread.
pub fn get_signing_config() -> Option<SigningConfig> {
    CURRENT_SIGNING_CONFIG.with(|c| c.borrow().clone())
}

/// Global identity store mapping DID → SigningConfig.
/// When create_identity() generates a keypair, we store it here so that
/// exec_request() can look up the signing key from just a DID string.
static IDENTITY_STORE: std::sync::OnceLock<Mutex<HashMap<String, SigningConfig>>> =
    std::sync::OnceLock::new();

fn identity_store() -> &'static Mutex<HashMap<String, SigningConfig>> {
    IDENTITY_STORE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Store a signing config for a DID.
pub fn store_identity(did: &str, config: SigningConfig) {
    if let Ok(mut store) = identity_store().lock() {
        store.insert(did.to_string(), config);
    }
}

/// Retrieve stored signing config for a DID.
pub fn get_identity(did: &str) -> Option<SigningConfig> {
    identity_store()
        .lock()
        .ok()
        .and_then(|store| store.get(did).cloned())
}

/// Find a registered DID backed by a remote signer.
///
/// This is used by HTTP request handling when the authenticated caller DID has no
/// locally registered signing identity. In Orbis-backed deployments, the node
/// should fall back to the remote signer identity rather than a local `--identity`
/// secp256k1 key, so create mutations still go through the signing gate.
pub fn find_remote_signer_did() -> Option<String> {
    identity_store().lock().ok().and_then(|store| {
        store
            .iter()
            .find_map(|(did, config)| config.has_remote_signer().then(|| did.clone()))
    })
}

/// Resolve signing config for a request identity when signing is enabled.
///
/// When an explicit identity DID is provided, uses that identity's signing config
/// (falling back to node identity for JWT-authenticated users without local keys).
///
/// Callers that need to respect a runtime signing flag should use
/// [`resolve_signing_config_with_flag`].
pub fn resolve_signing_config(
    identity_did: Option<&str>,
    node_identity_did: Option<&str>,
) -> Option<SigningConfig> {
    resolve_signing_config_with_flag(identity_did, node_identity_did, true)
}

/// Resolve signing config for a request identity.
///
/// When signing is enabled and an explicit identity DID is provided, uses that
/// identity's signing config (falling back to node identity for JWT-authenticated
/// users without local keys).
///
/// When no identity is provided (anonymous request):
/// - If signing is enabled on the node, uses the node identity to sign
///   (matching Go's `!signingDisabled` check)
/// - Otherwise, produces unsigned blocks
pub fn resolve_signing_config_with_flag(
    identity_did: Option<&str>,
    node_identity_did: Option<&str>,
    signing_enabled: bool,
) -> Option<SigningConfig> {
    if !signing_enabled {
        return None;
    }

    match identity_did {
        Some(did) if !did.is_empty() => {
            get_identity(did).or_else(|| node_identity_did.and_then(get_identity))
        }
        _ => {
            if signing_enabled {
                node_identity_did.and_then(get_identity)
            } else {
                None
            }
        }
    }
}

/// Clear all stored identities (for node cleanup).
pub fn clear_identity_store() {
    if let Ok(mut store) = identity_store().lock() {
        drop(std::mem::take(&mut *store));
    }
}

// ---------------------------------------------------------------------------
// Request Bearer Token — keyed by DID for ACP registration passthrough
// ---------------------------------------------------------------------------

use std::sync::OnceLock;

/// Global map of DID → JWT for in-flight requests.
///
/// `thread_local!` doesn't work here because tokio can migrate async tasks
/// between OS threads at `.await` points. A global map keyed by DID ensures
/// the token is available regardless of which thread reads it.
fn request_token_store() -> &'static Mutex<HashMap<String, String>> {
    static STORE: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    STORE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Store the raw JWT from the HTTP Authorization header, keyed by the caller's DID.
///
/// When a user authenticates via JWT, the node doesn't have their private key
/// and can't create new bearer tokens for them. Instead, we pass through the
/// original JWT — which IS signed by the user's key — to hub.rs/SourceHub
/// for ACP operations like register_object.
pub fn set_request_bearer_token(did: &str, token: impl Into<String>) {
    if let Ok(mut store) = request_token_store().lock() {
        store.insert(did.to_string(), token.into());
    }
}

/// Get the stored request bearer token for a specific DID.
pub fn get_request_bearer_token(did: &str) -> Option<String> {
    request_token_store().lock().ok()?.get(did).cloned()
}

/// Remove the stored request bearer token for a specific DID.
pub fn clear_request_bearer_token(did: &str) {
    if let Ok(mut store) = request_token_store().lock() {
        store.remove(did);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(label: &str) -> SigningConfig {
        SigningConfig {
            key_type: SigningKeyType::Secp256k1,
            private_key_bytes: vec![],
            public_key_bytes: vec![],
            public_key_hex: label.to_string(),
            remote_signer: None,
            signing_authorization: None,
        }
    }

    /// `IDENTITY_STORE` is process-global, so the tests that clear and refill it
    /// cannot run concurrently: one test's `store_identity` lands between
    /// another's `clear_identity_store` and its assertion. Poisoning is
    /// recovered from rather than propagated, so one failure does not cascade
    /// into the others.
    static IDENTITY_STORE_GUARD: Mutex<()> = Mutex::new(());

    fn lock_identity_store() -> std::sync::MutexGuard<'static, ()> {
        IDENTITY_STORE_GUARD
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn resolve_signing_config_uses_request_identity_when_registered() {
        let _guard = lock_identity_store();
        clear_identity_store();
        store_identity("did:key:request", make_config("request"));
        store_identity("did:key:node", make_config("node"));

        let disabled =
            resolve_signing_config_with_flag(Some("did:key:request"), Some("did:key:node"), false);
        assert!(disabled.is_none());

        let resolved = resolve_signing_config(Some("did:key:request"), Some("did:key:node"))
            .expect("request identity should resolve");
        assert_eq!(resolved.public_key_hex, "request");

        clear_identity_store();
    }

    #[test]
    fn resolve_signing_config_falls_back_to_node_identity_when_request_identity_missing() {
        let _guard = lock_identity_store();
        clear_identity_store();
        store_identity("did:key:node", make_config("node"));

        let resolved = resolve_signing_config(Some("did:key:request"), Some("did:key:node"))
            .expect("node fallback should resolve");
        assert_eq!(resolved.public_key_hex, "node");

        clear_identity_store();
    }

    #[test]
    fn find_remote_signer_did_returns_registered_remote_signer() {
        let _guard = lock_identity_store();
        struct NoopRemoteSigner;

        impl RemoteSigner for NoopRemoteSigner {
            fn sign_sync(
                &self,
                _data: &[u8],
                _authorization: Option<&SigningAuthorization>,
            ) -> Result<Vec<u8>, String> {
                Ok(vec![])
            }
        }

        clear_identity_store();
        store_identity("did:key:local", make_config("local"));
        store_identity(
            "did:key:remote",
            SigningConfig {
                key_type: SigningKeyType::Bls,
                private_key_bytes: vec![],
                public_key_bytes: vec![],
                public_key_hex: "remote".to_string(),
                remote_signer: Some(Arc::new(NoopRemoteSigner)),
                signing_authorization: None,
            },
        );

        let resolved = find_remote_signer_did().expect("remote signer DID should resolve");
        assert_eq!(resolved, "did:key:remote");

        clear_identity_store();
    }

    #[test]
    fn private_key_bytes_from_vec_shrinks_spare_capacity() {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(&[1_u8, 2, 3, 4]);
        assert_eq!(bytes.capacity(), 64);

        let exact = SigningConfig::private_key_bytes_from_vec(bytes);

        assert_eq!(exact, vec![1, 2, 3, 4]);
        assert_eq!(exact.len(), exact.capacity());
    }

    #[test]
    fn private_key_bytes_from_slice_uses_exact_capacity() {
        let bytes = SigningConfig::private_key_bytes_from_slice(&[1_u8, 2, 3, 4]);

        assert_eq!(bytes, vec![1, 2, 3, 4]);
        assert_eq!(bytes.len(), bytes.capacity());
    }

    #[test]
    fn private_key_bytes_from_vec_reuses_exact_capacity() {
        let bytes = vec![1_u8, 2, 3, 4];
        assert_eq!(bytes.len(), bytes.capacity());

        let exact = SigningConfig::private_key_bytes_from_vec(bytes);

        assert_eq!(exact, vec![1, 2, 3, 4]);
        assert_eq!(exact.len(), exact.capacity());
    }

    #[test]
    fn signing_key_type_from_str_rejects_invalid_values() {
        let error = "invalid".parse::<SigningKeyType>().unwrap_err();
        assert_eq!(error, "unsupported signing key type: invalid");
    }
}

// ---------------------------------------------------------------------------
// Broadcast Creator DID — thread-local for P2P ACP propagation
// ---------------------------------------------------------------------------

thread_local! {
    static BROADCAST_CREATOR_DID: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Set the broadcast creator DID for the current thread.
///
/// When set, P2P broadcasts use this DID as the Creator field instead of
/// the node's PeerId. This enables ACP registration on the receiving node:
/// the merge handler registers the document with this DID as owner.
pub fn set_broadcast_creator_did(did: Option<String>) {
    BROADCAST_CREATOR_DID.with(|c| {
        *c.borrow_mut() = did;
    });
}

/// Get the broadcast creator DID for this thread.
///
/// Returns the DID set by `set_broadcast_creator_did`, or None if no
/// identity override is active (broadcasts will use the node PeerId).
pub fn get_broadcast_creator_did() -> Option<String> {
    BROADCAST_CREATOR_DID.with(|c| c.borrow().clone())
}
