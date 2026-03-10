use std::ffi::c_char;

use crate::ffi_entry;
use acp::nac::NodePermission;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

use super::parse_doc_ids_json;

/// Sync specific documents from peers.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn p2p_sync_documents(
    node_ptr: usize,
    identity_did: *const c_char,
    collection_name: *const c_char,
    doc_ids_json: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pSyncDocuments
        ));

        let collection_name_str = try_ffi!(require_c_str(collection_name, "collection_name"));
        let doc_ids_str = try_ffi!(require_c_str(doc_ids_json, "doc_ids_json"));
        let doc_ids = match parse_doc_ids_json(&doc_ids_str) {
            Ok(doc_ids) => doc_ids,
            Err(error) => return FfiResult::error(error),
        };

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                let collection_name = collection_name_str.clone();
                let doc_ids = doc_ids.clone();
                rt.block_on(async move {
                    p2p.system
                        .ops()
                        .sync_documents(&collection_name, doc_ids)
                        .await
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

/// Sync a branchable collection from connected peers.
///
/// # Safety
///
/// `identity_did` and `collection_id` must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn p2p_sync_branchable_collection(
    node_ptr: usize,
    identity_did: *const c_char,
    collection_id: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pSyncBranchableCollection
        ));

        let collection_id_str = try_ffi!(require_c_str(collection_id, "collection_id"));

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                let collection_id = collection_id_str.clone();
                rt.block_on(async move {
                    p2p.system
                        .ops()
                        .sync_branchable_collection(&collection_id)
                        .await
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
