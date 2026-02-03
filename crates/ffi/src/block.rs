//! Block operations for FFI.
//!
//! This module exposes block-level operations like signature verification.

use std::ffi::c_char;

use crate::get_runtime;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

/// Verify the signature of a block.
///
/// Loads a block from the blockstore by CID, checks that it has a signature,
/// loads the signature block, and verifies the signature using the provided
/// public key.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `key_type` - Key type string (e.g., "ed25519", "secp256k1")
/// * `public_key` - Hex-encoded public key string
/// * `block_cid` - CID string of the block to verify
/// * `identity_did` - Optional DID of the caller (unused, reserved for future ACP checks)
///
/// # Safety
///
/// All string pointers must be either null or valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn block_verify_signature(
    node_ptr: usize,
    key_type: *const c_char,
    public_key: *const c_char,
    block_cid: *const c_char,
    _identity_did: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let key_type_str = match c_str_to_string(key_type) {
        Some(s) if !s.is_empty() => s,
        _ => "secp256k1".to_string(),
    };

    let pub_key_str = match c_str_to_string(public_key) {
        Some(s) => s,
        None => return FfiResult::error("public_key is null"),
    };

    let cid_str = match c_str_to_string(block_cid) {
        Some(s) => s,
        None => return FfiResult::error("block_cid is null"),
    };

    // Get database from node state
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    // Parse the key type
    let crypto_key_type = match key_type_str.as_str() {
        "ed25519" => crypto::KeyType::Ed25519,
        "secp256k1" => crypto::KeyType::Secp256k1,
        "secp256r1" => crypto::KeyType::Secp256r1,
        other => return FfiResult::error(format!("unsupported key type: {}", other)),
    };

    // Parse the public key from hex string
    let pub_key = match crypto::public_key_from_string(crypto_key_type, &pub_key_str) {
        Ok(k) => k,
        Err(e) => return FfiResult::error(format!("invalid public key: {}", e)),
    };

    // Parse the CID
    let parsed_cid = match cid_str.parse::<cid::Cid>() {
        Ok(c) => c,
        Err(e) => return FfiResult::error(format!("invalid CID: {}", e)),
    };

    let result = rt.block_on(async {
        // Create a read-only transaction to access the blockstore
        let txn = database
            .new_txn(true)
            .await
            .map_err(|e| format!("failed to create transaction: {}", e))?;

        // Load the block from blockstore
        let blockstore = txn
            .blockstore()
            .map_err(|e| format!("failed to get blockstore: {}", e))?;

        let block_bytes = blockstore
            .get(&parsed_cid.to_bytes())
            .await
            .map_err(|e| format!("failed to load block: {}", e))?
            .ok_or_else(|| format!("block not found: {}", cid_str))?;

        let block = defra_core::block::Block::from_dag_cbor(&block_bytes)
            .map_err(|e| format!("failed to decode block: {}", e))?;

        // Check that the block has a signature
        let sig_cid = block
            .signature
            .ok_or("block has no signature")?;

        // Load the signature block from blockstore
        let sig_bytes = blockstore
            .get(&sig_cid.to_bytes())
            .await
            .map_err(|e| format!("failed to load signature block: {}", e))?
            .ok_or_else(|| format!("signature block not found: {}", sig_cid))?;

        let signature = defra_core::block::Signature::from_dag_cbor(&sig_bytes)
            .map_err(|e| format!("failed to decode signature block: {}", e))?;

        // Verify that the identity matches the signature's identity
        let sig_identity = String::from_utf8_lossy(&signature.header.identity);
        if sig_identity.as_ref() != pub_key_str {
            return Err(format!(
                "signature public key mismatch: expected {}, got {}",
                pub_key_str, sig_identity
            ));
        }

        // Get the bytes to verify (block serialized without signature)
        let mut block_to_verify = block.clone();
        block_to_verify.signature = None;
        let signed_bytes = block_to_verify
            .to_dag_cbor()
            .map_err(|e| format!("failed to serialize block for verification: {}", e))?;

        // Verify the signature
        let valid = pub_key
            .verify(&signed_bytes, &signature.value)
            .map_err(|e| format!("signature verification error: {}", e))?;

        if !valid {
            return Err("signature verification failed".to_string());
        }

        // Discard the read-only transaction
        txn.discard()
            .map_err(|e| format!("failed to discard transaction: {}", e))?;

        Ok::<(), String>(())
    });

    match result {
        Ok(()) => FfiResult::success("Block's signature verified."),
        Err(e) => FfiResult::error(e),
    }
}
