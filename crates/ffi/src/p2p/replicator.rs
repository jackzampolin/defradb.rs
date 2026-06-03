use std::ffi::c_char;

use crate::ffi_entry;
use acp::nac::NodePermission;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::try_ffi;
use crate::types::{c_str_to_string, FfiResult};

use super::{into_ffi_ok, into_ffi_result, parse_collections_json, FfiP2PError};

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

        // Bind the caller's identity so the adapter's inner NAC check resolves the
        // actual caller instead of the wildcard. The body runs on this thread via
        // `block_on`, so the thread-local is visible throughout; the guard restores
        // on drop so it never leaks into the next request on this pooled thread.
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            c_str_to_string(identity_did).filter(|s| !s.is_empty()),
        );

        let addr_str = try_ffi!(require_c_str(peer_addr, "peer_addr"));
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
                state
                    .sync_replicator_push_options()
                    .map_err(FfiP2PError::internal)?;

                let addr = addr_str.clone();
                let collections = collections.clone();
                rt.block_on(async move {
                    p2p.system
                        .ops()
                        .add_replicator(collections, Some(&addr), Vec::new(), None)
                        .await
                        .map_err(FfiP2PError::from)
                })
            })
            .ok_or_else(FfiP2PError::invalid_node_handle)
            .and_then(|result| result);

        into_ffi_ok(result)
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

        // Bind the caller's identity so the adapter's inner NAC check resolves the
        // actual caller instead of the wildcard. The body runs on this thread via
        // `block_on`, so the thread-local is visible throughout; the guard restores
        // on drop so it never leaks into the next request on this pooled thread.
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            c_str_to_string(identity_did).filter(|s| !s.is_empty()),
        );

        let peer_str = try_ffi!(require_c_str(peer_id_str, "peer_id_str"));
        let collections = if collections_json.is_null() {
            Vec::new()
        } else {
            match c_str_to_string(collections_json) {
                Some(s) if !s.is_empty() => match parse_collections_json(&s) {
                    Ok(collections) => collections,
                    Err(error) => return FfiResult::error(error.message),
                },
                _ => Vec::new(),
            }
        };

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err(FfiP2PError::no_p2p_system()),
                };

                let peer = peer_str.clone();
                let collections = collections.clone();
                rt.block_on(async move {
                    p2p.system
                        .ops()
                        .remove_replicator(collections, Some(&peer))
                        .await
                        .map_err(FfiP2PError::from)
                })
            })
            .ok_or_else(FfiP2PError::invalid_node_handle)
            .and_then(|result| result);

        into_ffi_ok(result)
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

        // Bind the caller's identity so the adapter's inner NAC check resolves the
        // actual caller instead of the wildcard. The body runs on this thread via
        // `block_on`, so the thread-local is visible throughout; the guard restores
        // on drop so it never leaks into the next request on this pooled thread.
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            c_str_to_string(identity_did).filter(|s| !s.is_empty()),
        );

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err(FfiP2PError::no_p2p_system()),
                };

                rt.block_on(async {
                    let replicators = p2p
                        .system
                        .ops()
                        .get_replicators()
                        .await
                        .map_err(FfiP2PError::from)?;
                    serde_json::to_string(&replicators)
                        .map_err(|error| {
                            FfiP2PError::internal(format!(
                                "failed to serialize replicators: {}",
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
