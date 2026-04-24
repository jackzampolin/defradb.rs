//! Signature verification and key generation.
//!
//! Standalone functions for verifying data integrity without a full client.

use wasm_bindgen::prelude::*;

use crypto::{
    generate_ed25519, generate_secp256k1, public_key_from_string, sha256, Key, PrivateKey,
};

use crate::error::{Result, WasmError};

/// Verify an Ed25519 signature.
///
/// # Arguments
/// * `public_key_hex` - Hex-encoded Ed25519 public key (64 hex chars)
/// * `message` - The message bytes that were signed
/// * `signature_hex` - Hex-encoded signature (128 hex chars)
#[wasm_bindgen]
pub fn verify_ed25519_signature(
    public_key_hex: &str,
    message: &[u8],
    signature_hex: &str,
) -> std::result::Result<bool, JsValue> {
    verify_ed25519_impl(public_key_hex, message, signature_hex).map_err(|e| e.into())
}

fn verify_ed25519_impl(public_key_hex: &str, message: &[u8], signature_hex: &str) -> Result<bool> {
    let public_key = public_key_from_string(crypto::KeyType::Ed25519, public_key_hex)
        .map_err(WasmError::from)?;

    let signature = hex::decode(signature_hex)
        .map_err(|e: hex::FromHexError| WasmError::InvalidArgument(e.to_string()))?;

    public_key
        .verify(message, &signature)
        .map_err(WasmError::from)
}

/// Verify a secp256k1 signature.
///
/// # Arguments
/// * `public_key_hex` - Hex-encoded secp256k1 public key (compressed or uncompressed)
/// * `message` - The message bytes that were signed
/// * `signature_hex` - Hex-encoded signature
#[wasm_bindgen]
pub fn verify_secp256k1_signature(
    public_key_hex: &str,
    message: &[u8],
    signature_hex: &str,
) -> std::result::Result<bool, JsValue> {
    verify_secp256k1_impl(public_key_hex, message, signature_hex).map_err(|e| e.into())
}

fn verify_secp256k1_impl(
    public_key_hex: &str,
    message: &[u8],
    signature_hex: &str,
) -> Result<bool> {
    let public_key = public_key_from_string(crypto::KeyType::Secp256k1, public_key_hex)
        .map_err(WasmError::from)?;

    let signature = hex::decode(signature_hex)
        .map_err(|e: hex::FromHexError| WasmError::InvalidArgument(e.to_string()))?;

    public_key
        .verify(message, &signature)
        .map_err(WasmError::from)
}

/// Compute SHA-256 hash of data.
///
/// Returns the hash as a hex string.
#[wasm_bindgen]
pub fn sha256_hash(data: &[u8]) -> String {
    let hash = sha256(data);
    hex::encode(hash)
}

/// Compute the CID for a document.
///
/// The document should be a JSON object. The CID is computed from the
/// DAG-CBOR encoding of the document.
#[wasm_bindgen]
pub fn compute_document_cid(document_json: &str) -> std::result::Result<String, JsValue> {
    compute_cid_impl(document_json).map_err(|e| e.into())
}

fn compute_cid_impl(document_json: &str) -> Result<String> {
    let doc = document::Document::from_json_str(document_json)?;

    // Get the document's computed CID
    if let Some(id) = doc.id() {
        Ok(id.to_string())
    } else {
        Err(WasmError::Document(
            "Document has no ID after creation".to_string(),
        ))
    }
}

/// Generate a new Ed25519 keypair.
///
/// Returns a JSON object with:
/// - `private_key`: Hex-encoded private key
/// - `public_key`: Hex-encoded public key
#[wasm_bindgen]
pub fn generate_ed25519_keypair() -> std::result::Result<JsValue, JsValue> {
    generate_ed25519_impl().map_err(|e| e.into())
}

fn generate_ed25519_impl() -> Result<JsValue> {
    let private_key = generate_ed25519().map_err(WasmError::from)?;
    let public_key = private_key.public_key();

    let result = serde_json::json!({
        "private_key": private_key.to_hex_string(),
        "public_key": public_key.to_hex_string(),
    });

    crate::bindings::to_js(&result)
}

/// Generate a new secp256k1 keypair.
///
/// Returns a JSON object with:
/// - `private_key`: Hex-encoded private key
/// - `public_key`: Hex-encoded public key
#[wasm_bindgen]
pub fn generate_secp256k1_keypair() -> std::result::Result<JsValue, JsValue> {
    generate_secp256k1_impl().map_err(|e| e.into())
}

fn generate_secp256k1_impl() -> Result<JsValue> {
    let private_key = generate_secp256k1().map_err(WasmError::from)?;
    let public_key = private_key.public_key();

    let result = serde_json::json!({
        "private_key": private_key.to_hex_string(),
        "public_key": public_key.to_hex_string(),
    });

    crate::bindings::to_js(&result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_hash() {
        let hash = sha256_hash(b"hello");
        assert_eq!(
            hash,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_sha256_empty_input() {
        let hash = sha256_hash(b"");
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_sha256_deterministic() {
        let h1 = sha256_hash(b"defradb");
        let h2 = sha256_hash(b"defradb");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_verify_ed25519_bad_key_fails() {
        let result = verify_ed25519_impl("not_hex", b"msg", "not_hex");
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_secp256k1_bad_key_fails() {
        let result = verify_secp256k1_impl("not_hex", b"msg", "not_hex");
        assert!(result.is_err());
    }

    #[test]
    fn test_compute_cid_invalid_json() {
        let result = compute_cid_impl("not json");
        assert!(result.is_err());
    }
}
