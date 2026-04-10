use std::ffi::c_char;

use crate::ffi_entry;
use acp::nac::NodePermission;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

use super::parse_collections_json;

fn replicator_push_options(state: &crate::state::NodeState) -> embedded::ReplicatorPushOptions {
    embedded::ReplicatorPushOptions {
        se_encryption_key: state.se_encryption_key.as_ref().map(|key| key.to_vec()),
        se_identity_pubkey: state
            .node_identity_did
            .as_ref()
            .map(|identity| identity.as_bytes().to_vec()),
    }
}

/// Set (add/update) a replicator for collections.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn p2p_add_replicator(
    node_ptr: usize,
    identity_did: *const c_char,
    peer_addr: *const c_char,
    collections_json: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pReplicatorAdd
        ));

        let addr_str = try_ffi!(require_c_str(peer_addr, "peer_addr"));
        let collections_str = try_ffi!(require_c_str(collections_json, "collections_json"));
        let collections = match parse_collections_json(&collections_str) {
            Ok(collections) => collections,
            Err(error) => return FfiResult::error(error),
        };

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                let addr = addr_str.clone();
                let push_options = replicator_push_options(state);
                let collections = collections.clone();
                rt.block_on(async move {
                    p2p.system
                        .ops()
                        .add_replicator(collections, Some(&addr), push_options)
                        .await
                        .map_err(|error| error.to_string())
                })
            })
            .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
            .and_then(|result| result);

        match result {
            Ok(()) => FfiResult::ok(),
            Err(error) => FfiResult::error(error),
        }
    }
}

/// Delete a replicator.
///
/// # Safety
///
/// `peer_id_str` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn p2p_delete_replicator(
    node_ptr: usize,
    identity_did: *const c_char,
    peer_id_str: *const c_char,
    collections_json: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pReplicatorDelete
        ));

        let peer_str = try_ffi!(require_c_str(peer_id_str, "peer_id_str"));
        let collections = if collections_json.is_null() {
            Vec::new()
        } else {
            match c_str_to_string(collections_json) {
                Some(s) if !s.is_empty() => match parse_collections_json(&s) {
                    Ok(collections) => collections,
                    Err(error) => return FfiResult::error(error),
                },
                _ => Vec::new(),
            }
        };

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                let peer = peer_str.clone();
                let collections = collections.clone();
                rt.block_on(async move {
                    p2p.system
                        .ops()
                        .remove_replicator(collections, Some(&peer))
                        .await
                        .map_err(|error| error.to_string())
                })
            })
            .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
            .and_then(|result| result);

        match result {
            Ok(()) => FfiResult::ok(),
            Err(error) => FfiResult::error(error),
        }
    }
}

/// Get all replicators.
///
/// Returns a JSON array of replicator info objects.
///
/// # Safety
///
/// The caller must free the returned string with `defra_free_string`.
#[no_mangle]
pub unsafe extern "C" fn p2p_list_replicators(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pReplicatorList
        ));

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                rt.block_on(async {
                    let replicators = p2p.system.ops().get_replicators().await
                        .map_err(|error| error.to_string())?;
                    serde_json::to_string(&replicators)
                        .map_err(|error| format!("failed to serialize replicators: {}", error))
                })
            })
            .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
            .and_then(|result| result);

        match result {
            Ok(json) => FfiResult::success(json),
            Err(error) => FfiResult::error(error),
        }
    }
}
