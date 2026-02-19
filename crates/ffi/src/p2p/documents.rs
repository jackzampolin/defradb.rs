use std::ffi::c_char;

use acp::nac::NodePermission;
use p2p::topics::DefraTopic;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

use super::{parse_doc_ids_json, persist_p2p_documents};

/// Add documents to P2P replication by subscribing to their GossipSub topics.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `doc_ids_json` - JSON array of document IDs
///
/// # Safety
///
/// `doc_ids_json` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn p2p_add_documents(
    node_ptr: usize,
    identity_did: *const c_char,
    doc_ids_json: *const c_char,
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pDocumentAdd
    ));

    let doc_ids_str = try_ffi!(require_c_str(doc_ids_json, "doc_ids_json"));

    let doc_ids = match parse_doc_ids_json(&doc_ids_str) {
        Ok(d) => d,
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
                // Validate all document IDs have valid format (atomic: all or nothing)
                document::validate_doc_ids(&doc_ids)
                    .map_err(|_| "malformed document ID, missing either version or cid".to_string())?;

                for doc_id in &doc_ids {
                    let topic = DefraTopic::document(doc_id);
                    if let Err(e) = p2p.handle.subscribe(topic).await {
                        tracing::warn!(doc_id = %doc_id, error = %e, "Failed to subscribe to GossipSub topic for document");
                    }
                    p2p.add_document(doc_id);
                }

                // Persist document subscriptions so they survive restarts.
                let all_docs = p2p.get_documents();
                persist_p2p_documents(db, &all_docs).await;

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

/// Remove documents from P2P replication by unsubscribing from their GossipSub topics.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `doc_ids_json` - JSON array of document IDs
///
/// # Safety
///
/// `doc_ids_json` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn p2p_delete_documents(
    node_ptr: usize,
    identity_did: *const c_char,
    doc_ids_json: *const c_char,
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pDocumentDelete
    ));

    let doc_ids_str = try_ffi!(require_c_str(doc_ids_json, "doc_ids_json"));

    let doc_ids = match parse_doc_ids_json(&doc_ids_str) {
        Ok(d) => d,
        Err(e) => return FfiResult::error(e),
    };

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("no p2p system configured".to_string()),
            };

            rt.block_on(async {
                // Validate all document IDs have valid format (atomic: all or nothing)
                document::validate_doc_ids(&doc_ids)
                    .map_err(|_| "malformed document ID, missing either version or cid".to_string())?;

                for doc_id in &doc_ids {
                    let topic = DefraTopic::document(doc_id);
                    if let Err(e) = p2p.handle.unsubscribe(topic).await {
                        tracing::warn!(doc_id = %doc_id, error = %e, "Failed to unsubscribe from GossipSub topic for document");
                    }
                    p2p.remove_document(doc_id);
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

/// Get all P2P documents.
///
/// Returns a JSON array of document IDs.
///
/// # Safety
///
/// The caller must free the returned string with `defra_free_string`.
#[no_mangle]
pub unsafe extern "C" fn p2p_list_documents(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
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

            let mut documents = p2p.get_documents();
            documents.sort();
            serde_json::to_string(&documents)
                .map_err(|e| format!("failed to serialize documents: {}", e))
        })
        .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
        .and_then(|r| r);

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}
