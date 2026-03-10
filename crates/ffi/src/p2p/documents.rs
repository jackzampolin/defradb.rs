use std::ffi::c_char;

use crate::ffi_entry;
use acp::nac::NodePermission;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

use super::parse_doc_ids_json;

/// Add tracked P2P documents to the node.
///
/// # Safety
///
/// `identity_did` and `doc_ids_json` must be valid null-terminated UTF-8 strings when non-null.
/// `node_ptr` must reference a live node handle created by this library.
#[no_mangle]
pub unsafe extern "C" fn p2p_add_documents(
    node_ptr: usize,
    identity_did: *const c_char,
    doc_ids_json: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pDocumentAdd
        ));

        let doc_ids_str = try_ffi!(require_c_str(doc_ids_json, "doc_ids_json"));
        let doc_ids = match parse_doc_ids_json(&doc_ids_str) {
            Ok(doc_ids) => doc_ids,
            Err(error) => return FfiResult::error(error),
        };

        let docs = doc_ids
            .into_iter()
            .map(|doc_id| embedded::P2pDocumentRequest {
                collection: String::new(),
                doc_id,
            })
            .collect();

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                rt.block_on(async { p2p.system.ops().add_documents(docs).await })
            })
            .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
            .and_then(|result| result);

        match result {
            Ok(()) => FfiResult::ok(),
            Err(error) => FfiResult::error(error),
        }
    }
}

/// Remove tracked P2P documents from the node.
///
/// # Safety
///
/// `identity_did` and `doc_ids_json` must be valid null-terminated UTF-8 strings when non-null.
/// `node_ptr` must reference a live node handle created by this library.
#[no_mangle]
pub unsafe extern "C" fn p2p_delete_documents(
    node_ptr: usize,
    identity_did: *const c_char,
    doc_ids_json: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pDocumentDelete
        ));

        let doc_ids_str = try_ffi!(require_c_str(doc_ids_json, "doc_ids_json"));
        let doc_ids = match parse_doc_ids_json(&doc_ids_str) {
            Ok(doc_ids) => doc_ids,
            Err(error) => return FfiResult::error(error),
        };

        let docs = doc_ids
            .into_iter()
            .map(|doc_id| embedded::P2pDocumentRequest {
                collection: String::new(),
                doc_id,
            })
            .collect();

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                rt.block_on(async { p2p.system.ops().remove_documents(docs).await })
            })
            .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
            .and_then(|result| result);

        match result {
            Ok(()) => FfiResult::ok(),
            Err(error) => FfiResult::error(error),
        }
    }
}

/// List tracked P2P documents for the node.
///
/// # Safety
///
/// `identity_did` must be a valid null-terminated UTF-8 string when non-null. `node_ptr` must
/// reference a live node handle created by this library.
#[no_mangle]
pub unsafe extern "C" fn p2p_list_documents(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pDocumentList
        ));

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                rt.block_on(async {
                    let documents = p2p.system.ops().get_documents().await?;
                    let mut doc_ids: Vec<String> =
                        documents.into_iter().map(|doc| doc.doc_id).collect();
                    doc_ids.sort();
                    serde_json::to_string(&doc_ids)
                        .map_err(|error| format!("failed to serialize documents: {}", error))
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
