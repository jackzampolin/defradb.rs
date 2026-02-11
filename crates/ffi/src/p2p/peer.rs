use std::ffi::c_char;

use acp::nac::NodePermission;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

use super::parse_multiaddr_with_peer_id;

/// Get P2P peer info (local peer ID and listening addresses).
///
/// Returns a JSON array of full multiaddrs with peer ID embedded:
/// `["/ip4/127.0.0.1/tcp/9171/p2p/12D3KooW..."]`
///
/// # Safety
///
/// The caller must free the returned string with `defra_free_string`.
#[no_mangle]
pub unsafe extern "C" fn p2p_peer_info(node_ptr: usize, identity_did: *const c_char) -> FfiResult {
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
                    .handle
                    .local_peer_id()
                    .await
                    .map_err(|e| format!("failed to get peer ID: {}", e))?;
                let addresses = p2p
                    .handle
                    .listen_addresses()
                    .await
                    .map_err(|e| format!("failed to get addresses: {}", e))?;
                let full_addrs: Vec<String> = addresses
                    .into_iter()
                    .map(|addr| format!("{}/p2p/{}", addr, peer_id))
                    .collect();
                serde_json::to_string(&full_addrs)
                    .map_err(|e| format!("failed to serialize peer info: {}", e))
            })
        })
        .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
        .and_then(|r| r);

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Get list of connected peers with full multiaddrs.
///
/// Returns a JSON array of multiaddr strings (Go-compatible format).
///
/// # Safety
///
/// The caller must free the returned string with `defra_free_string`.
#[no_mangle]
pub extern "C" fn p2p_active_peers(node_ptr: usize) -> FfiResult {
    let rt = try_ffi!(get_rt());

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("no p2p system configured".to_string()),
            };

            rt.block_on(async {
                let local_pid = p2p
                    .handle
                    .local_peer_id()
                    .await
                    .map(|p| p.to_string())
                    .unwrap_or_else(|_| "unknown".to_string());
                let connected = p2p
                    .handle
                    .connected_peers()
                    .await
                    .map_err(|e| format!("failed to get connected peers: {}", e))?;
                eprintln!(
                    "[FFI-ACTIVE-PEERS] node={} node_ptr={} connected={}",
                    &local_pid[local_pid.len().saturating_sub(8)..],
                    node_ptr,
                    connected.len()
                );

                let mut host_addrs = Vec::new();
                let mut covered: std::collections::HashSet<String> =
                    std::collections::HashSet::new();

                for attempt in 0..5 {
                    host_addrs = p2p
                        .handle
                        .peer_addresses()
                        .await
                        .map_err(|e| format!("failed to get peer addresses: {}", e))?;
                    covered.clear();
                    for addr_str in &host_addrs {
                        if let Some(pid) = addr_str.rsplit("/p2p/").next() {
                            covered.insert(pid.to_string());
                        }
                    }
                    let all_resolved = connected.iter().all(|pid| {
                        let pid_str = pid.to_string();
                        covered.contains(&pid_str) || p2p.get_peer_address(&pid_str).is_some()
                    });
                    if all_resolved || attempt == 4 {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }

                let mut all_addrs = host_addrs;
                for pid in &connected {
                    let pid_str = pid.to_string();
                    if !covered.contains(&pid_str) {
                        if let Some(ffi_addr) = p2p.get_peer_address(&pid_str) {
                            all_addrs.push(ffi_addr.to_string());
                        }
                    }
                }

                eprintln!(
                    "[FFI-ACTIVE-PEERS] connected={} host_addrs={} all_addrs={}",
                    connected.len(),
                    covered.len(),
                    all_addrs.len()
                );
                for a in &all_addrs {
                    eprintln!("[FFI-ACTIVE-PEERS]   addr={}", a);
                }

                serde_json::to_string(&all_addrs)
                    .map_err(|e| format!("failed to serialize peer list: {}", e))
            })
        })
        .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
        .and_then(|r| r);

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Connect to a peer at the given multiaddr.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `addr` - Full multiaddr including peer ID (e.g., "/ip4/127.0.0.1/tcp/9171/p2p/12D3KooW...")
///
/// # Safety
///
/// `addr` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn p2p_connect(
    node_ptr: usize,
    identity_did: *const c_char,
    addr: *const c_char,
) -> FfiResult {
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

            rt.block_on(async {
                let parsed = parse_multiaddr_with_peer_id(&addr_str)?;
                p2p.handle
                    .dial(parsed.peer_id, vec![parsed.transport_addr])
                    .await
                    .map_err(|e| format!("failed to connect: {}", e))?;

                let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
                loop {
                    if let Ok(connected) = p2p.handle.connected_peers().await {
                        if connected.contains(&parsed.peer_id) {
                            break;
                        }
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err("connection timed out waiting for peer".to_string());
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }

                p2p.set_peer_address(&parsed.peer_id.to_string(), &addr_str);
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
