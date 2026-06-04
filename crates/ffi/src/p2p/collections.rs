use std::ffi::c_char;

use crate::ffi_entry;
use acp::nac::NodePermission;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::try_ffi;
use crate::types::FfiResult;

use super::{into_ffi_ok, into_ffi_result, parse_collections_json, FfiP2PError};

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

        // Bind the caller's identity so the adapter's inner NAC check resolves the
        // actual caller instead of the wildcard. The body runs on this thread via
        // `block_on`, so the thread-local is visible throughout; the guard restores
        // on drop so it never leaks into the next request on this pooled thread.
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            crate::types::c_str_to_string(identity_did).filter(|s| !s.is_empty()),
        );

        let collections_str = try_ffi!(require_c_str(collections_json, "collections_json"));
        let collections = match parse_collections_json(&collections_str) {
            Ok(collections) => collections,
            Err(error) => return FfiResult::error(error.message),
        };

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err(FfiP2PError::no_p2p_system()),
                };

                rt.block_on(async {
                    p2p.system
                        .ops()
                        .add_collections(collections)
                        .await
                        .map_err(FfiP2PError::from)
                })
            })
            .ok_or_else(FfiP2PError::invalid_node_handle)
            .and_then(|result| result);

        into_ffi_ok(result)
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

        // Bind the caller's identity so the adapter's inner NAC check resolves the
        // actual caller instead of the wildcard. The body runs on this thread via
        // `block_on`, so the thread-local is visible throughout; the guard restores
        // on drop so it never leaks into the next request on this pooled thread.
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            crate::types::c_str_to_string(identity_did).filter(|s| !s.is_empty()),
        );

        let collections_str = try_ffi!(require_c_str(collections_json, "collections_json"));
        let collections = match parse_collections_json(&collections_str) {
            Ok(collections) => collections,
            Err(error) => return FfiResult::error(error.message),
        };

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err(FfiP2PError::no_p2p_system()),
                };

                rt.block_on(async {
                    p2p.system
                        .ops()
                        .remove_collections(collections)
                        .await
                        .map_err(FfiP2PError::from)
                })
            })
            .ok_or_else(FfiP2PError::invalid_node_handle)
            .and_then(|result| result);

        into_ffi_ok(result)
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
                    let collections = p2p
                        .system
                        .ops()
                        .get_collections()
                        .await
                        .map_err(FfiP2PError::from)?;
                    serde_json::to_string(&collections)
                        .map_err(|error| {
                            FfiP2PError::internal(format!(
                                "failed to serialize collections: {}",
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
