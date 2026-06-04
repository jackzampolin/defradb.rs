use std::ffi::c_char;

use crate::ffi_entry;
use acp::nac::NodePermission;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::try_ffi;
use crate::types::FfiResult;
use defra_http::router::P2pDocumentRequest;

use super::{into_ffi_ok, into_ffi_result, parse_doc_ids_json, FfiP2PError};

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

        // Bind the caller's identity so the adapter's inner NAC check resolves the
        // actual caller instead of the wildcard. The body runs on this thread via
        // `block_on`, so the thread-local is visible throughout; the guard restores
        // on drop so it never leaks into the next request on this pooled thread.
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            crate::types::c_str_to_string(identity_did).filter(|s| !s.is_empty()),
        );

        let doc_ids_str = try_ffi!(require_c_str(doc_ids_json, "doc_ids_json"));
        let doc_ids = match parse_doc_ids_json(&doc_ids_str) {
            Ok(doc_ids) => doc_ids,
            Err(error) => return FfiResult::error(error.message),
        };

        let docs = doc_ids
            .into_iter()
            .map(|doc_id| P2pDocumentRequest {
                collection: String::new(),
                doc_id,
            })
            .collect();

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err(FfiP2PError::no_p2p_system()),
                };

                rt.block_on(async {
                    p2p.system
                        .ops()
                        .add_documents(docs)
                        .await
                        .map_err(FfiP2PError::from)
                })
            })
            .ok_or_else(FfiP2PError::invalid_node_handle)
            .and_then(|result| result);

        into_ffi_ok(result)
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

        // Bind the caller's identity so the adapter's inner NAC check resolves the
        // actual caller instead of the wildcard. The body runs on this thread via
        // `block_on`, so the thread-local is visible throughout; the guard restores
        // on drop so it never leaks into the next request on this pooled thread.
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            crate::types::c_str_to_string(identity_did).filter(|s| !s.is_empty()),
        );

        let doc_ids_str = try_ffi!(require_c_str(doc_ids_json, "doc_ids_json"));
        let doc_ids = match parse_doc_ids_json(&doc_ids_str) {
            Ok(doc_ids) => doc_ids,
            Err(error) => return FfiResult::error(error.message),
        };

        let docs = doc_ids
            .into_iter()
            .map(|doc_id| P2pDocumentRequest {
                collection: String::new(),
                doc_id,
            })
            .collect();

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err(FfiP2PError::no_p2p_system()),
                };

                rt.block_on(async {
                    p2p.system
                        .ops()
                        .remove_documents(docs)
                        .await
                        .map_err(FfiP2PError::from)
                })
            })
            .ok_or_else(FfiP2PError::invalid_node_handle)
            .and_then(|result| result);

        into_ffi_ok(result)
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

        // Bind the caller's identity so the adapter's inner NAC check resolves the
        // actual caller instead of the wildcard. The body runs on this thread via
        // `block_on`, so the thread-local is visible throughout; the guard restores
        // on drop so it never leaks into the next request on this pooled thread.
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            crate::types::c_str_to_string(identity_did).filter(|s| !s.is_empty()),
        );

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err(FfiP2PError::no_p2p_system()),
                };

                rt.block_on(async {
                    let documents = p2p
                        .system
                        .ops()
                        .get_documents()
                        .await
                        .map_err(FfiP2PError::from)?;
                    let mut doc_ids: Vec<String> =
                        documents.into_iter().map(|doc| doc.doc_id).collect();
                    doc_ids.sort();
                    serde_json::to_string(&doc_ids)
                        .map_err(|error| {
                            FfiP2PError::internal(format!(
                                "failed to serialize documents: {}",
                                error
                            ))
                        })
                })
            })
            .ok_or_else(FfiP2PError::invalid_node_handle)
            .and_then(|result| result);

        into_ffi_result(result)
    }
}
