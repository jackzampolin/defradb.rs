//! Block signing configuration for CRDT block signing.
//!
//! Provides the SigningConfig type and thread-local storage for passing
//! signing config from the FFI/query layer to the block builder layer.
//! Mirrors the pattern used by encryption.rs for EncryptionConfig.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Remote signing delegate (e.g. Orbis ring threshold signing).
///
/// Implementations call an external service to produce signatures.
/// `sign_sync` must be callable from synchronous contexts (the block builder
/// runs inside `spawn_blocking`).
pub trait RemoteSigner: Send + Sync {
    fn sign_sync(&self, data: &[u8]) -> Result<Vec<u8>, String>;
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
}

impl Clone for SigningConfig {
    fn clone(&self) -> Self {
        Self {
            key_type: self.key_type.clone(),
            private_key_bytes: self.private_key_bytes.clone(),
            public_key_bytes: self.public_key_bytes.clone(),
            public_key_hex: self.public_key_hex.clone(),
            remote_signer: self.remote_signer.clone(),
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

/// Resolve signing config for a request identity with node-identity fallback.
///
/// - If `identity_did` is `Some(non-empty)`, look up that DID's signing config.
/// - Otherwise, fall back to `node_identity_did` (the node's default identity).
pub fn resolve_signing_config(
    identity_did: Option<&str>,
    node_identity_did: Option<&str>,
) -> Option<SigningConfig> {
    match identity_did {
        Some(did) if !did.is_empty() => get_identity(did),
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
