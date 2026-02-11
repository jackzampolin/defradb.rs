//! Block operations for FFI.
//!
//! This module exposes block-level operations like signature verification.

use std::ffi::c_char;

use acp::nac::NodePermission;

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

    let (database, document_acp) = match NODES.get(node_ptr, |state| {
        (state.database.clone(), state.document_acp.clone())
    }) {
        Some((db, acp)) => (db, acp),
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let crypto_key_type = match key_type_str.as_str() {
        "ed25519" => crypto::KeyType::Ed25519,
        "secp256k1" => crypto::KeyType::Secp256k1,
        "secp256r1" => crypto::KeyType::Secp256r1,
        other => return FfiResult::error(format!("unsupported key type: {}", other)),
    };

    let identity_did_str = c_str_to_string(identity_did);
    let caller_identity: acp::Identity = identity_did_str
        .as_ref()
        .and_then(|d| identity::Did::try_from(d.clone()).ok())
        .into();

    let result = rt.block_on(async {
        db::block_verify::verify_block_signature(
            &database,
            document_acp.as_ref(),
            &cid_str,
            &pub_key_str,
            crypto_key_type,
            &caller_identity,
        )
        .await
    });

    match result {
        Ok(()) => FfiResult::success("Block's signature verified."),
        Err(e) => FfiResult::error(e),
    }
}
