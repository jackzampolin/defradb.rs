//! Block signing configuration for CRDT block signing.
//!
//! Provides the SigningConfig type and thread-local storage for passing
//! signing config from the FFI/query layer to the block builder layer.
//! Mirrors the pattern used by encryption.rs for EncryptionConfig.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Mutex;

/// Signing configuration containing key material for block signing.
#[derive(Clone)]
pub struct SigningConfig {
    /// Key type: "ed25519" or "secp256k1"
    pub key_type: String,
    /// Raw private key bytes
    pub private_key_bytes: Vec<u8>,
    /// Raw public key bytes (for identity in signature header)
    pub public_key_bytes: Vec<u8>,
    /// Public key hex string (for signature header identity, matches Go's pubKey.String())
    pub public_key_hex: String,
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
