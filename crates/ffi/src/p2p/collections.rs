use std::ffi::c_char;

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
pub unsafe extern "C" fn p2p_create_collections(
    node_ptr: usize,
    identity_did: *const c_char,
    collections_json: *const c_char,
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pCollectionCreate
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
                // Validate all collection names exist and collect their schema root CIDs
                let mut name_to_id = Vec::new();
                for name in &collections {
                    let col = db
                        .get_collection(name)
                        .map_err(|e| format!("failed to get collection: {}", e))?
                        .ok_or_else(|| "collection not found".to_string())?;
                    name_to_id.push((name.clone(), col.collection_id().to_string()));
                }

                for (name, collection_id) in &name_to_id {
                    // Subscribe to the GossipSub topic using the schema root CID
                    // (matches Go behavior which uses col.CollectionID())
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
                // Validate all collection names exist and collect their schema root CIDs
                let mut name_to_id = Vec::new();
                for name in &collections {
                    let col = db
                        .get_collection(name)
                        .map_err(|e| format!("failed to get collection: {}", e))?
                        .ok_or_else(|| "collection not found".to_string())?;
                    name_to_id.push((name.clone(), col.collection_id().to_string()));
                }

                for (name, collection_id) in &name_to_id {
                    // Unsubscribe from the GossipSub topic using the schema root CID
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
