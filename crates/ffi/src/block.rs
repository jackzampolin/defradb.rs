//! Block operations for FFI.
//!
//! This module exposes block-level operations like signature verification.

use std::ffi::c_char;

use acp::nac::NodePermission;
use acp::{DocumentPermission, Identity};

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

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
/// * `identity_did` - DID of the caller for NAC permission check
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
    identity_did: *const c_char,
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::SignatureVerify
    ));

    let key_type_str = match c_str_to_string(key_type) {
        Some(s) if !s.is_empty() => s,
        _ => "secp256k1".to_string(),
    };

    let pub_key_str = try_ffi!(require_c_str(public_key, "public_key"));
    let cid_str = try_ffi!(require_c_str(block_cid, "block_cid"));

    // Get database and document_acp from node state
    let (database, document_acp) = match NODES.get(node_ptr, |state| {
        (state.database.clone(), state.document_acp.clone())
    }) {
        Some((db, acp)) => (db, acp),
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    // Parse identity DID for DAC permission check
    let identity_did_str = c_str_to_string(identity_did);

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
        // Create a read-only transaction to access the blockstore.
        // Load block and signature data inside a scope so the blockstore
        // borrow is dropped before we discard the transaction.
        let txn = database
            .new_txn(true)
            .await
            .map_err(|e| format!("failed to create transaction: {}", e))?;

        // Use a scope to ensure blockstore is dropped before we discard the transaction.
        // The blockstore holds an Arc reference to the shared transaction, which must be
        // released before discard() can take ownership via Arc::try_unwrap().
        let (block, signature) = {
            let blockstore = txn
                .blockstore()
                .map_err(|e| format!("failed to get blockstore: {}", e))?;

            let block_bytes = blockstore
                .get(&parsed_cid.to_bytes())
                .await
                .map_err(|e| format!("failed to load block: {}", e))?
                // Match Go error message: "could not find" for block not found
                .ok_or_else(|| format!("could not find block: {}", cid_str))?;

            let block = defra_core::block::Block::from_dag_cbor(&block_bytes)
                .map_err(|e| format!("failed to decode block: {}", e))?;

            // Check that the block has a signature
            let sig_cid = block.signature.ok_or("block has no signature")?;

            // Load the signature block from blockstore
            let sig_bytes = blockstore
                .get(&sig_cid.to_bytes())
                .await
                .map_err(|e| format!("failed to load signature block: {}", e))?
                .ok_or_else(|| format!("signature block not found: {}", sig_cid))?;

            let signature = defra_core::block::Signature::from_dag_cbor(&sig_bytes)
                .map_err(|e| format!("failed to decode signature block: {}", e))?;

            (block, signature)
        }; // blockstore dropped here, releasing its Arc<SharedTxn> reference

        // Check document-level ACP permission (Read) if block has delta with doc_id
        if let (Some(doc_id_bytes), Some(schema_version_id)) =
            (block.delta.doc_id(), block.delta.schema_version_id())
        {
            let doc_id = String::from_utf8_lossy(doc_id_bytes).to_string();

            // Find collection by schema_version_id
            if let Some(collection) = database
                .get_collection_by_version_id(schema_version_id)
                .map_err(|e| format!("failed to get collection: {}", e))?
            {
                // Build identity from caller DID
                let identity: Identity = identity_did_str
                    .as_ref()
                    .and_then(|d| identity::Did::try_from(d.clone()).ok())
                    .into();

                // Check read permission
                let has_permission = db::check_doc_permission(
                    document_acp.as_ref(),
                    &identity,
                    DocumentPermission::Read,
                    collection.schema(),
                    &doc_id,
                )
                .await
                .map_err(|e| format!("ACP check failed: {}", e))?;

                if !has_permission {
                    let _ = txn.discard();
                    return Err("missing permission".to_string());
                }
            }
        }

        // Verify that the identity matches the signature's identity
        let sig_identity = String::from_utf8_lossy(&signature.header.identity);
        if sig_identity.as_ref() != pub_key_str {
            // Discard before returning error
            let _ = txn.discard();
            // Match Go error message: "signature was created by a different key"
            return Err("signature was created by a different key".to_string());
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
            let _ = txn.discard();
            return Err("signature verification failed".to_string());
        }

        Ok::<(), String>(())
    });

    match result {
        Ok(()) => FfiResult::success("Block's signature verified."),
        Err(e) => FfiResult::error(e),
    }
}
