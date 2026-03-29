use std::ffi::c_char;

use crate::ffi_entry;
use acp::nac::NodePermission;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

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
                        .await?;
                    let addresses = p2p
                        .system
                        .ops()
                        .listen_addresses()
                        .await?;

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
                        .map_err(|error| format!("failed to serialize peer info: {}", error))
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

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                rt.block_on(async { p2p.system.ops().notify_network_change().await })
            })
            .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
            .and_then(|result| result);

        match result {
            Ok(()) => FfiResult::ok(),
            Err(error) => FfiResult::error(error),
        }
    }
}

/// Get connected peers.
///
/// # Safety
///
/// `identity_did` must be a valid null-terminated UTF-8 string when non-null.
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

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                rt.block_on(async {
                    let peers = p2p.system.ops().connected_peers().await?;
                    serde_json::to_string(&peers)
                        .map_err(|error| format!("failed to serialize peer list: {}", error))
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
                    None => return Err("no p2p system configured".to_string()),
                };

                rt.block_on(async { p2p.system.ops().connect_peer(&addr_str).await })
            })
            .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
            .and_then(|result| result);

        match result {
            Ok(()) => FfiResult::ok(),
            Err(error) => FfiResult::error(error),
        }
    }
}
