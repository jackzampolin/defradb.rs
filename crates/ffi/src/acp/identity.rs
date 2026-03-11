use std::ffi::{c_char, CString};
use std::sync::Arc;

use identity::Identity;

use crate::helpers::{get_node_database, get_rt};
use crate::state::NODES;
use crate::types::{c_str_to_string, DefraRemoteSignCallback, FfiResult};
use crate::{ffi_async, ffi_entry, try_ffi};

const MAX_REMOTE_SIGNATURE_LEN: usize = 512;

struct FfiRemoteSigner {
    did: CString,
    key_type: CString,
    signer_handle: usize,
    callback: DefraRemoteSignCallback,
}

impl defra_core::signing::RemoteSigner for FfiRemoteSigner {
    fn sign_sync(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let mut signature = vec![0u8; MAX_REMOTE_SIGNATURE_LEN];
        let mut signature_len = 0usize;
        let status = unsafe {
            (self.callback)(
                self.signer_handle,
                self.did.as_ptr(),
                self.key_type.as_ptr(),
                data.as_ptr(),
                data.len(),
                signature.as_mut_ptr(),
                signature.len(),
                &mut signature_len,
            )
        };

        if status != 0 {
            return Err(format!(
                "remote signer callback failed with status {}",
                status
            ));
        }
        if signature_len == 0 {
            return Err("remote signer callback returned an empty signature".to_string());
        }
        if signature_len > signature.len() {
            return Err(format!(
                "remote signer callback reported {} bytes, exceeding the {} byte limit",
                signature_len, MAX_REMOTE_SIGNATURE_LEN
            ));
        }

        signature.truncate(signature_len);
        Ok(signature)
    }
}

fn parse_crypto_key_type(key_type: &str) -> Result<crypto::KeyType, String> {
    match key_type {
        "ed25519" => Ok(crypto::KeyType::Ed25519),
        "secp256k1" => Ok(crypto::KeyType::Secp256k1),
        "secp256r1" => Ok(crypto::KeyType::Secp256r1),
        "bls" => Ok(crypto::KeyType::Bls12381),
        other => Err(format!("unsupported key type: {}", other)),
    }
}

fn decode_and_validate_public_key(
    did: &str,
    public_key_hex: &str,
    key_type: &str,
) -> Result<Vec<u8>, String> {
    let public_key_bytes = hex::decode(public_key_hex)
        .map_err(|error| format!("invalid public key hex: {}", error))?;

    validate_public_key_bytes(did, &public_key_bytes, key_type)?;
    Ok(public_key_bytes)
}

fn validate_public_key_bytes(
    did: &str,
    public_key_bytes: &[u8],
    key_type: &str,
) -> Result<(), String> {
    let crypto_key_type = parse_crypto_key_type(key_type)?;

    let public_key = crypto::public_key_from_bytes(crypto_key_type, public_key_bytes)
        .map_err(|error| format!("invalid public key bytes: {}", error))?;
    let derived_did = public_key
        .did()
        .map_err(|error| format!("failed to derive DID from public key: {}", error))?;

    if derived_did != did {
        return Err(format!(
            "public key DID mismatch: expected {}, derived {}",
            did, derived_did
        ));
    }

    Ok(())
}

fn store_remote_identity(
    did: String,
    public_key_bytes: Vec<u8>,
    public_key_hex: String,
    key_type: String,
    signer_handle: usize,
    callback: DefraRemoteSignCallback,
) -> Result<String, String> {
    let signer = Arc::new(FfiRemoteSigner {
        did: CString::new(did.clone())
            .map_err(|_| "did contains an embedded null byte".to_string())?,
        key_type: CString::new(key_type.clone())
            .map_err(|_| "key_type contains an embedded null byte".to_string())?,
        signer_handle,
        callback,
    });

    defra_core::signing::store_identity(
        &did,
        defra_core::signing::SigningConfig {
            key_type,
            private_key_bytes: Vec::new(),
            public_key_bytes,
            public_key_hex,
            remote_signer: Some(signer),
        },
    );

    Ok("{}".to_string())
}

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
    ffi_entry! {
        if let Some(Some(did)) = NODES.get(node_ptr, |state| state.node_identity_did.clone()) {
            return FfiResult::success(serde_json::json!({ "did": did }).to_string());
        }

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
    ffi_entry! {
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

            tracing::debug!(did = %did_str, key_type = %key_type_str, "registering identity");

            defra_core::signing::store_identity(
                &did_str,
                defra_core::signing::SigningConfig {
                    key_type: key_type_str,
                    private_key_bytes,
                    public_key_bytes,
                    public_key_hex: pub_hex,
                    remote_signer: None,
                },
            );

            Ok::<String, String>("{}".to_string())
        })();

        match result {
            Ok(json) => FfiResult::success(json),
            Err(e) => FfiResult::error(e),
        }
    }
}

/// Register an identity whose private key stays outside DefraDB.
///
/// The host provides the DID, public key, key type, and a signing callback.
/// This supports device-bound keys (for example Secure Enclave P-256 keys) and
/// remote identity providers such as Orbis without exporting private key bytes.
///
/// Swift can import this symbol directly from the generated Apple package.
/// The callback is required and uses a plain C function-pointer ABI.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn register_remote_identity(
    did: *const c_char,
    public_key_hex: *const c_char,
    key_type: *const c_char,
    signer_handle: usize,
    callback: DefraRemoteSignCallback,
) -> FfiResult {
    ffi_entry! {
        let result = (|| {
            let did_str = unsafe { c_str_to_string(did) }.ok_or("invalid did parameter")?;
            let pub_hex =
                unsafe { c_str_to_string(public_key_hex) }.ok_or("invalid public_key_hex parameter")?;
            let key_type_str =
                unsafe { c_str_to_string(key_type) }.unwrap_or_else(|| "secp256r1".to_string());

            let public_key_bytes =
                decode_and_validate_public_key(&did_str, &pub_hex, &key_type_str)?;

            store_remote_identity(
                did_str,
                public_key_bytes,
                pub_hex,
                key_type_str,
                signer_handle,
                callback,
            )
        })();

        match result {
            Ok(json) => FfiResult::success(json),
            Err(error) => FfiResult::error(error),
        }
    }
}

/// Register an identity whose public key is already available as raw bytes.
///
/// This is intended for Swift and Objective-C callers that already have the
/// device public key as `Data` or `UnsafePointer<UInt8>`. The callback contract
/// matches [`register_remote_identity`].
///
/// Public key format:
/// - `secp256r1` / `secp256k1`: uncompressed SEC1 / X9.63 bytes
/// - `ed25519`: raw 32-byte public key
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn register_remote_identity_bytes(
    did: *const c_char,
    public_key_ptr: *const u8,
    public_key_len: usize,
    key_type: *const c_char,
    signer_handle: usize,
    callback: DefraRemoteSignCallback,
) -> FfiResult {
    ffi_entry! {
        let result = (|| {
            let did_str = unsafe { c_str_to_string(did) }.ok_or("invalid did parameter")?;
            if public_key_ptr.is_null() || public_key_len == 0 {
                return Err("invalid public_key_bytes parameter".to_string());
            }
            let key_type_str =
                unsafe { c_str_to_string(key_type) }.unwrap_or_else(|| "secp256r1".to_string());
            let public_key_bytes =
                unsafe { std::slice::from_raw_parts(public_key_ptr, public_key_len) }.to_vec();

            validate_public_key_bytes(&did_str, &public_key_bytes, &key_type_str)?;

            store_remote_identity(
                did_str,
                public_key_bytes.clone(),
                hex::encode(&public_key_bytes),
                key_type_str,
                signer_handle,
                callback,
            )
        })();

        match result {
            Ok(json) => FfiResult::success(json),
            Err(error) => FfiResult::error(error),
        }
    }
}

/// Bind a host-provided bearer token to a logical DID for delegated ACP access.
///
/// This is intentionally a passthrough: the token may be signed by a device-
/// bound key that has been authorized to act on behalf of the logical DID.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn bind_identity_bearer_token(
    did: *const c_char,
    bearer_token: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let did_str = match unsafe { c_str_to_string(did) } {
            Some(value) if !value.is_empty() => value,
            _ => return FfiResult::error("invalid did parameter"),
        };
        let token = match unsafe { c_str_to_string(bearer_token) } {
            Some(value) if !value.is_empty() => value,
            _ => return FfiResult::error("invalid bearer_token parameter"),
        };
        if let Err(error) = identity::Did::new(did_str.clone()) {
            return FfiResult::error(format!("invalid DID: {}", error));
        }

        defra_core::signing::set_request_bearer_token(&did_str, token);
        FfiResult::success(serde_json::json!({ "did": did_str }).to_string())
    }
}

/// Set or clear the node's default signing identity.
///
/// When `did` is empty or null the default identity is cleared. Otherwise the
/// DID must already be registered via `RegisterIdentity` or
/// `register_remote_identity` / `register_remote_identity_bytes`.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn node_set_default_identity(node_ptr: usize, did: *const c_char) -> FfiResult {
    ffi_entry! {
        let did_str = unsafe { c_str_to_string(did) }.unwrap_or_default();
        if !did_str.is_empty() && defra_core::signing::get_identity(&did_str).is_none() {
            return FfiResult::error(format!("no signing identity registered for DID: {}", did_str));
        }

        let updated = NODES.get_mut(node_ptr, |state| {
            state.node_identity_did = if did_str.is_empty() {
                None
            } else {
                Some(did_str.clone())
            };
        });

        match updated {
            Some(()) => {
                if did_str.is_empty() {
                    FfiResult::success("{}".to_string())
                } else {
                    FfiResult::success(serde_json::json!({ "did": did_str }).to_string())
                }
            }
            None => FfiResult::error(crate::ERR_INVALID_NODE_HANDLE),
        }
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
    ffi_entry! {
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
                    remote_signer: None,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::types::NodeInitOptions;
    use crypto::{keys::Key, keys::PublicKey, PrivateKey};
    use std::ffi::{CStr, CString};
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    fn remote_key_store() -> &'static Mutex<Vec<u8>> {
        static STORE: OnceLock<Mutex<Vec<u8>>> = OnceLock::new();
        STORE.get_or_init(|| Mutex::new(Vec::new()))
    }

    fn remote_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    unsafe extern "C" fn test_remote_sign_callback(
        _signer_handle: usize,
        _did: *const c_char,
        _key_type: *const c_char,
        payload_ptr: *const u8,
        payload_len: usize,
        out_signature_ptr: *mut u8,
        out_signature_capacity: usize,
        out_signature_len: *mut usize,
    ) -> i32 {
        let key_bytes = remote_key_store().lock().unwrap().clone();
        let private_key = match crypto::Secp256r1PrivateKey::from_bytes(&key_bytes) {
            Ok(key) => key,
            Err(_) => return 1,
        };

        let payload = std::slice::from_raw_parts(payload_ptr, payload_len);
        let signature = match private_key.sign(payload) {
            Ok(signature) => signature,
            Err(_) => return 2,
        };

        if signature.len() > out_signature_capacity {
            return 3;
        }

        std::ptr::copy_nonoverlapping(signature.as_ptr(), out_signature_ptr, signature.len());
        *out_signature_len = signature.len();
        0
    }

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

    #[test]
    fn test_register_remote_identity() {
        let _guard = remote_test_lock().lock().unwrap();

        let private_key = crypto::generate_secp256r1().unwrap();
        *remote_key_store().lock().unwrap() = private_key.raw();

        let raw_identity = identity::RawIdentity::from_secp256r1(private_key).unwrap();
        let did = raw_identity.did().unwrap().to_string();
        let public_key_hex = hex::encode(raw_identity.public_key_bytes());

        let did_c = CString::new(did.clone()).unwrap();
        let pub_c = CString::new(public_key_hex.clone()).unwrap();
        let key_type_c = CString::new("secp256r1").unwrap();

        let result = register_remote_identity(
            did_c.as_ptr(),
            pub_c.as_ptr(),
            key_type_c.as_ptr(),
            42,
            test_remote_sign_callback,
        );
        assert_eq!(result.status, 0, "register_remote_identity should succeed");
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let config = defra_core::signing::get_identity(&did).expect("identity should be stored");
        assert_eq!(config.key_type, "secp256r1");
        assert!(config.private_key_bytes.is_empty());
        assert!(config.remote_signer.is_some());

        let message = b"remote-signature-test";
        let signature = config
            .remote_signer
            .as_ref()
            .unwrap()
            .sign_sync(message)
            .expect("remote signing should succeed");
        let public_key = crypto::Secp256r1PublicKey::from_bytes(&config.public_key_bytes).unwrap();
        let valid = public_key.verify(message, &signature).unwrap();
        assert!(
            valid,
            "signature from registered remote signer should verify"
        );
    }

    #[test]
    fn test_register_remote_identity_bytes() {
        let _guard = remote_test_lock().lock().unwrap();

        let private_key = crypto::generate_secp256r1().unwrap();
        *remote_key_store().lock().unwrap() = private_key.raw();

        let raw_identity = identity::RawIdentity::from_secp256r1(private_key).unwrap();
        let did = raw_identity.did().unwrap().to_string();
        let public_key_bytes = raw_identity.public_key_bytes();

        let did_c = CString::new(did.clone()).unwrap();
        let key_type_c = CString::new("secp256r1").unwrap();

        let result = register_remote_identity_bytes(
            did_c.as_ptr(),
            public_key_bytes.as_ptr(),
            public_key_bytes.len(),
            key_type_c.as_ptr(),
            7,
            test_remote_sign_callback,
        );
        assert_eq!(
            result.status, 0,
            "register_remote_identity_bytes should succeed"
        );
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let config = defra_core::signing::get_identity(&did).expect("identity should be stored");
        assert_eq!(config.key_type, "secp256r1");
        assert_eq!(config.public_key_bytes, public_key_bytes);
        assert_eq!(config.public_key_hex, hex::encode(public_key_bytes));
        assert!(config.remote_signer.is_some());
    }

    #[test]
    fn test_bind_identity_bearer_token_allows_delegated_binding() {
        let device_key = crypto::generate_secp256r1().unwrap();
        let device_identity = identity::RawIdentity::from_secp256r1(device_key).unwrap();
        let token = String::from_utf8(
            identity::new_token(&device_identity, Duration::from_secs(300), None, None).unwrap(),
        )
        .unwrap();

        let logical_identity =
            identity::RawIdentity::from_ed25519(crypto::generate_ed25519().unwrap()).unwrap();
        let logical_did = logical_identity.did().unwrap().to_string();

        let did_c = CString::new(logical_did.clone()).unwrap();
        let token_c = CString::new(token.clone()).unwrap();

        let result = bind_identity_bearer_token(did_c.as_ptr(), token_c.as_ptr());
        assert_eq!(
            result.status, 0,
            "bind_identity_bearer_token should succeed"
        );
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let stored = defra_core::signing::get_request_bearer_token(&logical_did)
            .expect("token should exist");
        assert_eq!(stored, token);
    }

    #[test]
    fn test_node_set_default_identity_uses_registered_remote_identity() {
        let _guard = remote_test_lock().lock().unwrap();

        assert!(crate::runtime::init_runtime());

        let private_key = crypto::generate_secp256r1().unwrap();
        *remote_key_store().lock().unwrap() = private_key.raw();

        let raw_identity = identity::RawIdentity::from_secp256r1(private_key).unwrap();
        let did = raw_identity.did().unwrap().to_string();
        let public_key_hex = hex::encode(raw_identity.public_key_bytes());

        let did_c = CString::new(did.clone()).unwrap();
        let pub_c = CString::new(public_key_hex).unwrap();
        let key_type_c = CString::new("secp256r1").unwrap();

        let register = register_remote_identity(
            did_c.as_ptr(),
            pub_c.as_ptr(),
            key_type_c.as_ptr(),
            1,
            test_remote_sign_callback,
        );
        assert_eq!(register.status, 0);
        if !register.value.is_null() {
            unsafe { crate::types::defra_free_string(register.value) };
        }

        let node_result = new_node(NodeInitOptions::default());
        assert_eq!(node_result.status, 0, "new_node should succeed");
        let node = node_result.node_ptr;
        let result = node_set_default_identity(node, did_c.as_ptr());
        assert_eq!(result.status, 0, "node_set_default_identity should succeed");
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let identity_result = get_node_identity(node);
        assert_eq!(identity_result.status, 0);
        let value = unsafe { CStr::from_ptr(identity_result.value).to_string_lossy() };
        assert!(value.contains(&did));

        unsafe { crate::types::defra_free_string(identity_result.value) };
        node_close(node);
    }
}
