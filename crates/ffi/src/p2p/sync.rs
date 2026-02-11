use std::ffi::c_char;

use acp::nac::NodePermission;

use crate::get_runtime;
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

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
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pDocumentCreate,
    ) {
        return e;
    }

    let collection_name_str = match c_str_to_string(collection_name) {
        Some(s) => s,
        None => return FfiResult::error("collection_name is null"),
    };

    let doc_ids_str = match c_str_to_string(doc_ids_json) {
        Some(s) => s,
        None => return FfiResult::error("doc_ids_json is null"),
    };

    let doc_ids = match parse_doc_ids_json(&doc_ids_str) {
        Ok(d) => d,
        Err(e) => return FfiResult::error(e),
    };

    eprintln!(
        "[DOCSYNC] p2p_sync_documents called: collection={} doc_ids={:?}",
        collection_name_str, doc_ids
    );

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

                eprintln!("[DOCSYNC] connected_peers count={}", connected_peers.len());

                if connected_peers.is_empty() {
                    eprintln!("[DOCSYNC] No connected peers for DocSync");
                    return Ok(());
                }

                eprintln!(
                    "[DOCSYNC] Starting DocSync for {} documents to {} peers",
                    doc_ids.len(),
                    connected_peers.len()
                );

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

                    eprintln!(
                        "[DOCSYNC] Attempt {} - sending to {} peers, have {}/{} merges",
                        attempt + 1,
                        connected_peers.len(),
                        total_received,
                        total_expected
                    );

                    for peer_id in &connected_peers {
                        eprintln!("[DOCSYNC] Sending DocSync request to peer={}", peer_id);
                        match p2p
                            .handle
                            .send_doc_sync_request(*peer_id, request.clone())
                            .await
                        {
                            Ok(()) => {
                                eprintln!("[DOCSYNC] Sent DocSync request to peer={}", peer_id)
                            }
                            Err(e) => eprintln!(
                                "[DOCSYNC] Failed to send DocSync request to peer={}: {}",
                                peer_id, e
                            ),
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
                                        eprintln!(
                                            "[DOCSYNC] Doc merged: doc_id={} ({}/{})",
                                            data.doc_id, total_received, total_expected
                                        );
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(_) => {}
                        }
                    }
                }

                event_bus.unsubscribe(sub.id());

                eprintln!(
                    "[DOCSYNC] Done: {}/{} merges received",
                    total_received, total_expected
                );

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
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pCollectionCreate,
    ) {
        return e;
    }

    let collection_id_str = match c_str_to_string(collection_id) {
        Some(s) => s,
        None => return FfiResult::error("collection_id is null"),
    };

    eprintln!(
        "[FFI-BRANCHABLE] p2p_sync_branchable_collection called with collection_id={}",
        collection_id_str
    );

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
                        eprintln!(
                            "[FFI-BRANCHABLE] collection '{}' not found",
                            collection_id_str
                        );
                        return Err(format!(
                            "collection with ID '{}' not found",
                            collection_id_str
                        ));
                    }
                    Err(e) => {
                        eprintln!("[FFI-BRANCHABLE] find_collection_by_id error: {}", e);
                        return Err(format!("failed to find collection: {}", e));
                    }
                };

                eprintln!(
                    "[FFI-BRANCHABLE] Found collection name={} branchable={}",
                    collection.name(),
                    collection.schema().is_branchable
                );

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

                eprintln!(
                    "[FFI-BRANCHABLE] Connected peers: {}",
                    connected_peers.len()
                );

                if connected_peers.is_empty() {
                    eprintln!("[FFI-BRANCHABLE] No connected peers, returning early");
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
                    eprintln!(
                        "[FFI-BRANCHABLE] Sending BranchableSyncRequest to peer={}",
                        peer_id
                    );
                    let request_clone = request.clone();
                    let handle = p2p.handle.clone();
                    let peer_id = *peer_id;

                    tokio::spawn(async move {
                        if let Err(e) = handle
                            .send_branchable_sync_request(peer_id, request_clone)
                            .await
                        {
                            eprintln!(
                                "[FFI-BRANCHABLE] Failed to send request to peer={}: {}",
                                peer_id, e
                            );
                        } else {
                            eprintln!(
                                "[FFI-BRANCHABLE] Successfully sent request to peer={}",
                                peer_id
                            );
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
