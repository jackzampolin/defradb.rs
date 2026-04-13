//! Merkle proof and signature verification.
//!
//! Standalone functions for verifying data integrity without a full client.

use wasm_bindgen::prelude::*;

use crypto::{
    generate_ed25519, generate_secp256k1, public_key_from_string, sha256, Key, MerkleProof,
    PrivateKey, SignedMerkleProof,
};

use crate::error::{Result, WasmError};

/// Verify a Merkle proof from JSON.
///
/// The proof should be a JSON object with:
/// - `leaf_cid`: The CID of the leaf block
/// - `root_cid`: The CID of the root block
/// - `path`: Array of proof nodes
///
/// # Behavior
///
/// - **Returns `true`** if the proof verifies successfully.
/// - **Throws** (resolves to `Err` in Rust → exception in JS) for any
///   verification failure: structural input errors (empty path, anchor
///   mismatch), cryptographic failures (content hash mismatch, broken
///   chain link), and decode errors.
///
/// **This function never returns `false`.** Verification failures are
/// always surfaced as exceptions so JS callers cannot accidentally
/// bypass them by using truthiness checks on a `false` result. See
/// issue #733 for the rationale.
#[wasm_bindgen]
pub fn verify_merkle_proof(proof_json: &str) -> std::result::Result<bool, JsValue> {
    verify_merkle_proof_impl(proof_json).map_err(|e| e.into())
}

fn verify_merkle_proof_impl(proof_json: &str) -> Result<bool> {
    // Parse the proof from JSON
    let proof: MerkleProof =
        serde_json::from_str(proof_json).map_err(|e| WasmError::Serialization(e.to_string()))?;

    proof.verify().map_err(WasmError::from)
}

/// Verify a Merkle proof from DAG-CBOR bytes.
///
/// This is the native format for proofs transmitted over the wire.
///
/// Behavior is identical to [`verify_merkle_proof`] — returns `true` on
/// success and throws on any failure (#733). Never returns `false`.
#[wasm_bindgen]
pub fn verify_merkle_proof_cbor(proof_bytes: &[u8]) -> std::result::Result<bool, JsValue> {
    verify_merkle_proof_cbor_impl(proof_bytes).map_err(|e| e.into())
}

fn verify_merkle_proof_cbor_impl(proof_bytes: &[u8]) -> Result<bool> {
    let proof = MerkleProof::from_dag_cbor(proof_bytes)?;
    proof.verify().map_err(WasmError::from)
}

/// Verify a signed Merkle proof using the embedded public key.
///
/// Returns `true` if both the signature and proof verify successfully.
/// Throws on any failure (signature verification failure, hash mismatch,
/// broken chain, structural input error). Never returns `false` — see
/// issue #733 for rationale.
#[wasm_bindgen]
pub fn verify_signed_proof(proof_bytes: &[u8]) -> std::result::Result<bool, JsValue> {
    verify_signed_proof_impl(proof_bytes).map_err(|e| e.into())
}

fn verify_signed_proof_impl(proof_bytes: &[u8]) -> Result<bool> {
    let signed_proof = SignedMerkleProof::from_dag_cbor(proof_bytes)?;
    signed_proof
        .verify_with_embedded_key()
        .map_err(WasmError::from)
}

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
        .map_err(|e| WasmError::Verification(e.to_string()))?;

    let signature = hex::decode(signature_hex)
        .map_err(|e: hex::FromHexError| WasmError::InvalidArgument(e.to_string()))?;

    public_key
        .verify(message, &signature)
        .map_err(|e| WasmError::Verification(e.to_string()))
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
        .map_err(|e| WasmError::Verification(e.to_string()))?;

    let signature = hex::decode(signature_hex)
        .map_err(|e: hex::FromHexError| WasmError::InvalidArgument(e.to_string()))?;

    public_key
        .verify(message, &signature)
        .map_err(|e| WasmError::Verification(e.to_string()))
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
    let private_key = generate_ed25519().map_err(|e| WasmError::Verification(e.to_string()))?;
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
    let private_key = generate_secp256k1().map_err(|e| WasmError::Verification(e.to_string()))?;
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
    fn test_verify_invalid_merkle_proof_json() {
        let result = verify_merkle_proof_impl("not valid json");
        assert!(result.is_err());
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
