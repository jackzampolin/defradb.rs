use std::ffi::c_char;

use crate::ffi_entry;
use acp::nac::NodePermission;
use p2p::topics::DefraTopic;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

use super::{parse_collections_json, persist_p2p_collections};

/// Add collections to P2P replication.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `collections_json` - JSON array of collection names
///
/// # Safety
///
/// `collections_json` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn p2p_add_collections(
    node_ptr: usize,
    identity_did: *const c_char,
    collections_json: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pCollectionAdd
        ));

        let collections_str = try_ffi!(require_c_str(collections_json, "collections_json"));

        let collections = match parse_collections_json(&collections_str) {
            Ok(c) => c,
            Err(e) => return FfiResult::error(e),
        };

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };
                let db = &state.database;

                rt.block_on(async {
                    let name_to_id = db
                        .resolve_collection_ids(&collections)
                        .map_err(|e| format!("{}", e))?;

                    for (name, collection_id) in &name_to_id {
                        let topic = DefraTopic::collection(collection_id);
                        if let Err(e) = p2p.handle.subscribe(topic).await {
                            tracing::warn!(collection = %name, collection_id = %collection_id, error = %e, "Failed to subscribe to GossipSub topic");
                        }
                        p2p.add_collection(name);
                    }

                    // Persist collection subscriptions so they survive restarts.
                    let all_cols = p2p.get_collections();
                    persist_p2p_collections(db, &all_cols).await;

                    Ok(())
                })
            })
            .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
            .and_then(|r| r);

        match result {
            Ok(()) => FfiResult::ok(),
            Err(e) => FfiResult::error(e),
        }
    }
}

/// Remove collections from P2P replication.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `collections_json` - JSON array of collection names
///
/// # Safety
///
/// `collections_json` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn p2p_delete_collections(
    node_ptr: usize,
    identity_did: *const c_char,
    collections_json: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pCollectionDelete
        ));

        let collections_str = try_ffi!(require_c_str(collections_json, "collections_json"));

        let collections = match parse_collections_json(&collections_str) {
            Ok(c) => c,
            Err(e) => return FfiResult::error(e),
        };

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };
                let db = &state.database;

                rt.block_on(async {
                    let name_to_id = db
                        .resolve_collection_ids(&collections)
                        .map_err(|e| format!("{}", e))?;

                    for (name, collection_id) in &name_to_id {
                        let topic = DefraTopic::collection(collection_id);
                        if let Err(e) = p2p.handle.unsubscribe(topic).await {
                            tracing::warn!(collection = %name, collection_id = %collection_id, error = %e, "Failed to unsubscribe from GossipSub topic");
                        }
                        p2p.remove_collection(name);
                    }
                    Ok(())
                })
            })
            .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
            .and_then(|r| r);

        match result {
            Ok(()) => FfiResult::ok(),
            Err(e) => FfiResult::error(e),
        }
    }
}

/// Get all P2P collections.
///
/// Returns a JSON array of collection names.
///
/// # Safety
///
/// The caller must free the returned string with `defra_free_string`.
#[no_mangle]
pub unsafe extern "C" fn p2p_list_collections(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pCollectionList
        ));

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                let collections = p2p.get_collections();
                serde_json::to_string(&collections)
                    .map_err(|e| format!("failed to serialize collections: {}", e))
            })
            .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
            .and_then(|r| r);

        match result {
            Ok(json) => FfiResult::success(json),
            Err(e) => FfiResult::error(e),
        }
    }
}
