use std::ffi::c_char;

use identity::Identity;

use crate::helpers::{get_node_database, get_rt};
use crate::types::{c_str_to_string, FfiResult};
use crate::{ffi_async, try_ffi};

/// Get the node's identity (DID).
///
/// Returns JSON with the node identity:
/// ```json
/// { "did": "did:key:z6Mk..." }
/// ```
///
/// Returns an error if no node identity is configured.
#[no_mangle]
pub extern "C" fn get_node_identity(node_ptr: usize) -> FfiResult {
    let rt = try_ffi!(get_rt());
    let database = try_ffi!(get_node_database(node_ptr));

    ffi_async!(rt, {
        let identity = database
            .node_identity()
            .ok_or_else(|| "node identity not configured".to_string())?;

        let did = identity
            .did()
            .map_err(|e| format!("failed to get DID: {}", e))?;

        let json = serde_json::json!({ "did": did.to_string() }).to_string();
        Ok(json)
    })
}

/// Register an existing identity for block signing.
///
/// This allows Go-created identities to be used for signing blocks in Rust.
/// The identity's signing config is stored in the global identity store.
///
/// # Parameters
/// - `did`: The DID string (e.g., "did:key:z6Mk...")
/// - `private_key_hex`: Hex-encoded private key bytes
/// - `public_key_hex`: Hex-encoded public key bytes
/// - `key_type`: Key type ("secp256k1" or "ed25519")
///
/// # Returns
/// Success with empty JSON object, or error on failure.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn RegisterIdentity(
    did: *const c_char,
    private_key_hex: *const c_char,
    public_key_hex: *const c_char,
    key_type: *const c_char,
) -> FfiResult {
    let result = (|| {
        // SAFETY: These pointers come from Go/C FFI and are valid C strings.
        let did_str = unsafe { c_str_to_string(did) }.ok_or("invalid did parameter")?;
        let priv_hex = unsafe { c_str_to_string(private_key_hex) }
            .ok_or("invalid private_key_hex parameter")?;
        let pub_hex =
            unsafe { c_str_to_string(public_key_hex) }.ok_or("invalid public_key_hex parameter")?;
        let key_type_str =
            unsafe { c_str_to_string(key_type) }.unwrap_or_else(|| "secp256k1".to_string());

        let private_key_bytes =
            hex::decode(&priv_hex).map_err(|e| format!("invalid private key hex: {}", e))?;
        let public_key_bytes =
            hex::decode(&pub_hex).map_err(|e| format!("invalid public key hex: {}", e))?;

        eprintln!(
            "[SIGN-DEBUG] register_identity: did={}, key_type={}",
            did_str, key_type_str
        );

        defra_core::signing::store_identity(
            &did_str,
            defra_core::signing::SigningConfig {
                key_type: key_type_str,
                private_key_bytes,
                public_key_bytes,
                public_key_hex: pub_hex,
            },
        );

        Ok::<String, String>("{}".to_string())
    })();

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Create a new identity (Ed25519 keypair).
///
/// Generates a fresh Ed25519 keypair and returns the DID and private key.
/// This is stateless -- no node is required.
///
/// Returns a JSON object:
/// ```json
/// {
///   "did": "did:key:z6Mk...",
///   "privateKeyHex": "abcd...",
///   "keyType": "ed25519"
/// }
/// ```
#[no_mangle]
pub extern "C" fn create_identity() -> FfiResult {
    let result = (|| {
        let private_key = crypto::generate_ed25519()
            .map_err(|e| format!("failed to generate Ed25519 key: {}", e))?;

        let identity = identity::RawIdentity::from_ed25519(private_key)
            .map_err(|e| format!("failed to create identity: {}", e))?;

        let did = identity
            .did()
            .map_err(|e| format!("failed to derive DID: {}", e))?;

        let private_key_hex = hex::encode(identity.private_key_bytes());
        let public_key_hex = hex::encode(identity.public_key_bytes());

        // Store identity in global store so block signing can look up the
        // private key from just a DID string during mutations.
        defra_core::signing::store_identity(
            did.as_ref(),
            defra_core::signing::SigningConfig {
                key_type: "ed25519".to_string(),
                private_key_bytes: identity.private_key_bytes().to_vec(),
                public_key_bytes: identity.public_key_bytes().to_vec(),
                public_key_hex,
            },
        );

        let json = serde_json::json!({
            "did": did.to_string(),
            "privateKeyHex": private_key_hex,
            "keyType": "ed25519"
        })
        .to_string();

        Ok::<String, String>(json)
    })();

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::types::NodeInitOptions;
    use std::ffi::CStr;

    #[test]
    fn test_get_node_identity_not_configured() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Get node identity (should fail - not configured)
        let result = get_node_identity(node);
        assert_eq!(result.status, 1, "should fail when identity not configured");
        let error = unsafe { CStr::from_ptr(result.error).to_string_lossy() };
        assert!(
            error.contains("not configured"),
            "should indicate not configured"
        );

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_create_identity() {
        let result = create_identity();
        assert_eq!(result.status, 0, "create_identity should succeed");
        assert!(!result.value.is_null());

        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        let parsed: serde_json::Value = serde_json::from_str(&value).unwrap();

        // DID should start with did:key:z6Mk (Ed25519 multicodec prefix)
        let did = parsed["did"].as_str().unwrap();
        assert!(
            did.starts_with("did:key:z6Mk"),
            "DID should start with did:key:z6Mk, got: {}",
            did
        );

        // Private key hex should be non-empty
        let private_key_hex = parsed["privateKeyHex"].as_str().unwrap();
        assert!(
            !private_key_hex.is_empty(),
            "privateKeyHex should be non-empty"
        );

        // Key type should be ed25519
        assert_eq!(parsed["keyType"].as_str().unwrap(), "ed25519");

        unsafe { crate::types::defra_free_string(result.value) };

        // Call twice and verify different DIDs (randomness check)
        let result2 = create_identity();
        assert_eq!(result2.status, 0);
        let value2 = unsafe { CStr::from_ptr(result2.value).to_string_lossy() };
        let parsed2: serde_json::Value = serde_json::from_str(&value2).unwrap();
        let did2 = parsed2["did"].as_str().unwrap();

        assert_ne!(did, did2, "two calls should produce different DIDs");
        unsafe { crate::types::defra_free_string(result2.value) };
    }
}
