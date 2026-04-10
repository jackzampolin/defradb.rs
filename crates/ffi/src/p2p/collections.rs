use std::ffi::c_char;

use crate::ffi_entry;
use acp::nac::NodePermission;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

use super::parse_collections_json;

/// Add P2P-enabled collections to the node.
///
/// # Safety
///
/// `identity_did` and `collections_json` must be valid null-terminated UTF-8 strings when
/// non-null. `node_ptr` must reference a live node handle created by this library.
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
            Ok(collections) => collections,
            Err(error) => return FfiResult::error(error),
        };

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                rt.block_on(async {
                    p2p.system
                        .ops()
                        .add_collections(collections)
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

/// Remove P2P-enabled collections from the node.
///
/// # Safety
///
/// `identity_did` and `collections_json` must be valid null-terminated UTF-8 strings when
/// non-null. `node_ptr` must reference a live node handle created by this library.
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
            Ok(collections) => collections,
            Err(error) => return FfiResult::error(error),
        };

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                rt.block_on(async {
                    p2p.system
                        .ops()
                        .remove_collections(collections)
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

/// List P2P-enabled collections for the node.
///
/// # Safety
///
/// `identity_did` must be a valid null-terminated UTF-8 string when non-null. `node_ptr` must
/// reference a live node handle created by this library.
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

                rt.block_on(async {
                    let collections = p2p.system.ops().get_collections().await
                        .map_err(|error| error.to_string())?;
                    serde_json::to_string(&collections)
                        .map_err(|error| format!("failed to serialize collections: {}", error))
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
