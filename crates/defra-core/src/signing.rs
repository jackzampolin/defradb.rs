//! Block signing configuration for CRDT block signing.
//!
//! Provides the SigningConfig type and thread-local storage for passing
//! signing config from the FFI/query layer to the block builder layer.
//! Mirrors the pattern used by encryption.rs for EncryptionConfig.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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

/// Signing configuration containing key material for block signing.
pub struct SigningConfig {
    /// Key type: "ed25519", "secp256k1", or "bls"
    pub key_type: String,
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
            key_type: self.key_type.clone(),
            private_key_bytes: self.private_key_bytes.clone(),
            public_key_bytes: self.public_key_bytes.clone(),
            public_key_hex: self.public_key_hex.clone(),
            remote_signer: self.remote_signer.clone(),
            signing_authorization: self.signing_authorization.clone(),
        }
    }
}

impl SigningConfig {
    /// Returns true if this identity has locally exportable private key bytes.
    pub fn has_local_private_key(&self) -> bool {
        !self.private_key_bytes.is_empty()
    }

    /// Returns true if this identity can sign by delegating to a remote signer.
    pub fn has_remote_signer(&self) -> bool {
        self.remote_signer.is_some()
    }

    /// Map the identity key type to a block signature type.
    pub fn signature_type(&self) -> Result<crate::block::SignatureType, String> {
        match self.key_type.as_str() {
            "ed25519" => Ok(crate::block::SignatureType::EdDSA),
            "secp256k1" => Ok(crate::block::SignatureType::ES256K),
            "secp256r1" => Ok(crate::block::SignatureType::ES256),
            "bls" => Ok(crate::block::SignatureType::BLS),
            other => Err(format!("unsupported signing key type: {}", other)),
        }
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

/// Resolve signing config for a request identity with node-identity fallback.
///
/// - If `identity_did` is `Some(non-empty)`, look up that DID's signing config.
/// - Otherwise, fall back to `node_identity_did` (the node's default identity).
pub fn resolve_signing_config(
    identity_did: Option<&str>,
    node_identity_did: Option<&str>,
) -> Option<SigningConfig> {
    match identity_did {
        Some(did) if !did.is_empty() => {
            get_identity(did).or_else(|| node_identity_did.and_then(get_identity))
        }
        _ => node_identity_did.and_then(get_identity),
    }
}

/// Clear all stored identities (for node cleanup).
pub fn clear_identity_store() {
    if let Ok(mut store) = identity_store().lock() {
        store.clear();
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
pub fn set_request_bearer_token(did: &str, token: String) {
    if let Ok(mut store) = request_token_store().lock() {
        store.insert(did.to_string(), token);
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
            key_type: label.to_string(),
            private_key_bytes: vec![],
            public_key_bytes: vec![],
            public_key_hex: label.to_string(),
            remote_signer: None,
            signing_authorization: None,
        }
    }

    #[test]
    fn resolve_signing_config_uses_request_identity_when_registered() {
        clear_identity_store();
        store_identity("did:key:request", make_config("request"));
        store_identity("did:key:node", make_config("node"));

        let resolved = resolve_signing_config(Some("did:key:request"), Some("did:key:node"))
            .expect("request identity should resolve");
        assert_eq!(resolved.public_key_hex, "request");

        clear_identity_store();
    }

    #[test]
    fn resolve_signing_config_falls_back_to_node_identity_when_request_identity_missing() {
        clear_identity_store();
        store_identity("did:key:node", make_config("node"));

        let resolved = resolve_signing_config(Some("did:key:request"), Some("did:key:node"))
            .expect("node fallback should resolve");
        assert_eq!(resolved.public_key_hex, "node");

        clear_identity_store();
    }

    #[test]
    fn find_remote_signer_did_returns_registered_remote_signer() {
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
                key_type: "bls".to_string(),
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
