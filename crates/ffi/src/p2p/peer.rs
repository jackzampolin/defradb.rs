use std::ffi::c_char;

use crate::ffi_entry;
use acp::nac::NodePermission;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::try_ffi;
use crate::types::FfiResult;

use super::{into_ffi_ok, into_ffi_result, FfiP2PError};

/// Get local P2P peer info.
///
/// # Safety
///
/// `identity_did` must be a valid null-terminated UTF-8 string when non-null. `node_ptr` must
/// reference a live node handle created by this library.
#[no_mangle]
pub unsafe extern "C" fn p2p_peer_info(node_ptr: usize, identity_did: *const c_char) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pPeerInfo
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
                    None => return Ok("[]".to_string()),
                };

                rt.block_on(async {
                    let peer_id = p2p
                        .system
                        .ops()
                        .local_peer_id()
                        .await
                        .map_err(FfiP2PError::from)?;
                    let addresses = p2p
                        .system
                        .ops()
                        .listen_addresses()
                        .await
                        .map_err(FfiP2PError::from)?;

                    let full_addrs = match p2p.system.kind() {
                        embedded::TransportKind::Libp2p => addresses
                            .into_iter()
                            .map(|addr| format!("{}/p2p/{}", addr, peer_id))
                            .collect::<Vec<_>>(),
                        #[cfg(feature = "iroh")]
                        embedded::TransportKind::Iroh => addresses,
                        _ => addresses,
                    };

                    serde_json::to_string(&full_addrs)
                        .map_err(|error| {
                            FfiP2PError::internal(format!(
                                "failed to serialize peer info: {}",
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

/// Notify the active P2P transport that the network may have changed.
///
/// # Safety
///
/// `identity_did` must be a valid null-terminated UTF-8 string when non-null.
/// `node_ptr` must reference a live node handle created by this library.
#[no_mangle]
pub unsafe extern "C" fn p2p_notify_network_change(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pPeerConnect
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
                    p2p.system
                        .ops()
                        .notify_network_change()
                        .await
                        .map_err(FfiP2PError::from)
                })
            })
            .ok_or_else(FfiP2PError::invalid_node_handle)
            .and_then(|result| result);

        into_ffi_ok(result)
    }
}

/// Get connected peers.
///
/// # Safety
///
/// `identity_did` must be a valid null-terminated UTF-8 string when non-null.
/// `node_ptr` must reference a live node handle created by this library.
#[no_mangle]
pub unsafe extern "C" fn p2p_active_peers(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pPeerActive
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
                    let peers = p2p
                        .system
                        .ops()
                        .connected_peers()
                        .await
                        .map_err(FfiP2PError::from)?;
                    serde_json::to_string(&peers)
                        .map_err(|error| {
                            FfiP2PError::internal(format!(
                                "failed to serialize peer list: {}",
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

/// Connect to a peer address.
///
/// # Safety
///
/// `identity_did` and `addr` must be valid null-terminated UTF-8 strings when non-null.
/// `node_ptr` must reference a live node handle created by this library.
#[no_mangle]
pub unsafe extern "C" fn p2p_connect(
    node_ptr: usize,
    identity_did: *const c_char,
    addr: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pPeerConnect
        ));

        let addr_str = try_ffi!(require_c_str(addr, "addr"));

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err(FfiP2PError::no_p2p_system()),
                };

                rt.block_on(async {
                    p2p.system
                        .ops()
                        .connect_peer(&addr_str)
                        .await
                        .map_err(FfiP2PError::from)
                })
            })
            .ok_or_else(FfiP2PError::invalid_node_handle)
            .and_then(|result| result);

        into_ffi_ok(result)
    }
}
