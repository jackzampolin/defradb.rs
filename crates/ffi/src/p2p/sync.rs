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
/// This implements the DocSync pull-based protocol: sends requests to connected peers
/// asking for the heads of specific documents, then fetches the missing DAG blocks
/// via Bitswap and merges them.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `identity_did` - Identity DID for NAC permission check
/// * `collection_name` - Name of the collection containing the documents
/// * `doc_ids_json` - JSON array of document IDs to sync
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
            Ok(d) => d,
            Err(e) => return FfiResult::error(e),
        };

        tracing::debug!(collection = %collection_name_str, doc_ids = ?doc_ids, "p2p_sync_documents called");

        let result = NODES
            .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("no p2p system configured".to_string()),
            };
            let db = &state.database;
            let event_bus = &state.event_bus;

            rt.block_on(async {
                // Verify the collection exists
                let _collection = db
                    .get_collection(&collection_name_str)
                    .map_err(|e| format!("failed to get collection: {}", e))?
                    .ok_or_else(|| format!("collection '{}' not found", collection_name_str))?;

                // Get connected peers
                let connected_peers = p2p
                    .handle
                    .connected_peers()
                    .await
                    .map_err(|e| format!("failed to get connected peers: {}", e))?;

                tracing::debug!(count = connected_peers.len(), "connected peers");

                if connected_peers.is_empty() {
                    tracing::debug!("no connected peers for DocSync");
                    return Ok(());
                }

                tracing::debug!(doc_count = doc_ids.len(), peer_count = connected_peers.len(), "starting DocSync");

                // Subscribe to merge_complete events BEFORE sending requests
                // so we don't miss events that arrive quickly
                let mut sub = event_bus.subscribe(&[events::EventName::MergeComplete]);

                let total_expected = connected_peers.len() * doc_ids.len();
                let mut total_received = 0;
                let overall_timeout = std::time::Duration::from_secs(30);
                let idle_timeout = std::time::Duration::from_secs(3);
                let start = std::time::Instant::now();
                let doc_set: std::collections::HashSet<String> = doc_ids.iter().cloned().collect();

                // Retry DocSync up to 3 times to handle "connection is closed" errors
                // where a peer fails to send the response back.
                for attempt in 0..3 {
                    if total_received >= total_expected || start.elapsed() >= overall_timeout {
                        break;
                    }

                    // Create a fresh request per attempt (unique message_id)
                    let mut request = p2p::message::DocSyncRequest::new(doc_ids.clone());
                    if let Err(e) = p2p::signing::sign_message(p2p.handle.keypair(), &mut request) {
                        return Err(format!("failed to sign DocSync request: {}", e));
                    }

                    tracing::debug!(attempt = attempt + 1, peer_count = connected_peers.len(), received = total_received, expected = total_expected, "DocSync attempt");

                    for peer_id in &connected_peers {
                        tracing::debug!(peer_id = %peer_id, "sending DocSync request");
                        match p2p
                            .handle
                            .send_doc_sync_request(*peer_id, request.clone())
                            .await
                        {
                            Ok(()) => {
                                tracing::debug!(peer_id = %peer_id, "DocSync request sent")
                            }
                            Err(e) => tracing::warn!(peer_id = %peer_id, error = %e, "failed to send DocSync request"),
                        }
                    }

                    // Wait for merges with idle timeout
                    let mut last_merge = std::time::Instant::now();
                    while total_received < total_expected && start.elapsed() < overall_timeout {
                        if total_received >= doc_ids.len() && last_merge.elapsed() > idle_timeout {
                            break;
                        }

                        match tokio::time::timeout(
                            std::time::Duration::from_millis(100),
                            sub.recv(),
                        )
                        .await
                        {
                            Ok(Some(msg)) => {
                                if let Some(data) = msg.as_merge_complete() {
                                    if doc_set.contains(&data.doc_id) {
                                        total_received += 1;
                                        last_merge = std::time::Instant::now();
                                        tracing::debug!(doc_id = %data.doc_id, received = total_received, expected = total_expected, "document merged");
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(_) => {}
                        }
                    }
                }

                event_bus.unsubscribe(sub.id());

                tracing::debug!(received = total_received, expected = total_expected, "DocSync complete");

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
}

/// Sync a branchable collection from connected peers.
///
/// Looks up the collection, verifies it is branchable, then sends a
/// BranchableSyncRequest to each connected peer via the two-stream protocol.
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

        tracing::debug!(collection_id = %collection_id_str, "p2p_sync_branchable_collection called");

        let result = NODES
            .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("no p2p system configured".to_string()),
            };
            let db = &state.database;

            rt.block_on(async {
                // Look up collection by its collection_id
                let collection = match db.find_collection_by_id(&collection_id_str) {
                    Ok(Some(c)) => c,
                    Ok(None) => {
                        tracing::warn!(collection_id = %collection_id_str, "collection not found");
                        return Err(format!(
                            "collection with ID '{}' not found",
                            collection_id_str
                        ));
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "find_collection_by_id error");
                        return Err(format!("failed to find collection: {}", e));
                    }
                };

                tracing::debug!(name = %collection.name(), branchable = collection.schema().is_branchable, "found collection");

                // Check if the collection is branchable
                if !collection.schema().is_branchable {
                    return Err("collection is not branchable".to_string());
                }

                // Get connected peers
                let connected_peers = p2p
                    .handle
                    .connected_peers()
                    .await
                    .map_err(|e| format!("failed to get connected peers: {}", e))?;

                tracing::debug!(count = connected_peers.len(), "connected peers");

                if connected_peers.is_empty() {
                    tracing::debug!("no connected peers, returning early");
                    return Ok(());
                }

                // Create BranchableSync request
                let mut request =
                    p2p::message::BranchableSyncRequest::new(collection_id_str.clone());

                // Sign the request
                if let Err(e) = p2p::signing::sign_message(p2p.handle.keypair(), &mut request) {
                    return Err(format!("failed to sign BranchableSync request: {}", e));
                }

                // Send to each connected peer (fire-and-forget)
                for peer_id in &connected_peers {
                    tracing::debug!(peer_id = %peer_id, "sending BranchableSyncRequest");
                    let request_clone = request.clone();
                    let handle = p2p.handle.clone();
                    let peer_id = *peer_id;

                    tokio::spawn(async move {
                        if let Err(e) = handle
                            .send_branchable_sync_request(peer_id, request_clone)
                            .await
                        {
                            tracing::warn!(peer_id = %peer_id, error = %e, "failed to send BranchableSyncRequest");
                        } else {
                            tracing::debug!(peer_id = %peer_id, "BranchableSyncRequest sent");
                        }
                    });
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
}
