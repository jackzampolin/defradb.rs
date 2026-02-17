use std::ffi::c_char;
use std::sync::Arc;

use acp::nac::NodePermission;
use p2p::topics::DefraTopic;
use p2p::ReplicatorInfo;
use storage::stores::Peerstore;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

use super::{parse_collections_json, parse_multiaddr_with_peer_id};

/// Set (add/update) a replicator for collections.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `peer_addr` - Full multiaddr of the peer including peer ID
/// * `collections_json` - JSON array of collection names
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn p2p_create_replicator(
    node_ptr: usize,
    identity_did: *const c_char,
    peer_addr: *const c_char,
    collections_json: *const c_char,
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pReplicatorCreate
    ));

    let addr_str = try_ffi!(require_c_str(peer_addr, "peer_addr"));
    let collections_str = try_ffi!(require_c_str(collections_json, "collections_json"));
    let collections = match parse_collections_json(&collections_str) {
        Ok(c) => c,
        Err(e) => return FfiResult::error(e),
    };

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p { Some(p2p) => p2p, None => return Err("no p2p system configured".to_string()) };
            let db = &state.database;

            rt.block_on(async {
                let parsed = parse_multiaddr_with_peer_id(&addr_str)?;
                let effective_collections = if collections.is_empty() {
                    db.list_collections().map_err(|e| format!("failed to list collections: {}", e))?
                } else { collections };

                p2p.handle.dial(parsed.peer_id, vec![parsed.transport_addr]).await
                    .map_err(|e| format!("failed to connect to replicator peer: {}", e))?;
                p2p.set_peer_address(&parsed.peer_id.to_string(), &addr_str);

                let collection_cids: Vec<String> = db
                    .resolve_collection_ids(&effective_collections)
                    .map_err(|e| format!("{}", e))?
                    .into_iter()
                    .map(|(_, id)| id)
                    .collect();

                p2p.handle.create_replicator(parsed.peer_id, collection_cids.clone()).await
                    .map_err(|e| format!("failed to set replicator: {}", e))?;

                let info = ReplicatorInfo::new(parsed.peer_id, collection_cids);
                if let Ok(bytes) = info.to_bytes() {
                    let peerstore = Peerstore::new(db.store().clone());
                    match peerstore.create_replicator(&parsed.peer_id.to_string(), &bytes).await {
                        Ok(()) => { tracing::debug!(peer_id = %parsed.peer_id, bytes = bytes.len(), "replicator persisted"); }
                        Err(e) => { tracing::warn!(error = %e, "failed to persist replicator"); }
                    }
                }

                for name in &effective_collections {
                    if let Ok(Some(col)) = db.get_collection(name) {
                        let collection_id = col.collection_id().to_string();
                        let topic = DefraTopic::collection(&collection_id);
                        if let Err(e) = p2p.handle.subscribe(topic).await {
                            tracing::warn!(collection = %name, collection_id = %collection_id, error = %e, "Failed to subscribe to GossipSub topic for replicator");
                        }
                    }
                    p2p.add_collection(name);
                }

                let push_handle = p2p.handle.clone();
                let push_db = Arc::clone(db);
                let push_peer_id = parsed.peer_id;
                let push_collections = effective_collections;
                let push_event_bus = state.event_bus.clone();
                let push_se_key = state.se_encryption_key.clone();

                tokio::spawn(async move {
                    if let Err(e) = super::push::push_existing_docs(&push_handle, &push_db, push_peer_id, &push_collections, push_se_key.as_deref()).await {
                        tracing::error!(error = %e, "Failed to push existing docs to replicator");
                    }
                    tracing::debug!("publishing ReplicatorCompleted event");
                    push_event_bus.publish(events::Message::replicator_completed());
                    tracing::debug!("ReplicatorCompleted event published");
                });

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
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pReplicatorDelete
    ));

    let peer_str = try_ffi!(require_c_str(peer_id_str, "peer_id_str"));
    let collections: Vec<String> = if !collections_json.is_null() {
        match c_str_to_string(collections_json) {
            Some(s) if !s.is_empty() && s != "[]" => serde_json::from_str(&s).unwrap_or_default(),
            _ => Vec::new(),
        }
    } else {
        Vec::new()
    };

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p { Some(p2p) => p2p, None => return Err("no p2p system configured".to_string()) };
            let db = &state.database;

            rt.block_on(async {
                let peer_id: libp2p::PeerId = peer_str.parse()
                    .map_err(|e| format!("invalid peer ID '{}': {}", peer_str, e))?;

                let removed_collections = p2p.handle.get_replicator(peer_id).await
                    .ok().flatten().map(|info| info.collections).unwrap_or_default();

                if collections.is_empty() {
                    p2p.handle.delete_replicator(peer_id).await
                        .map_err(|e| format!("failed to delete replicator: {}", e))?;
                } else {
                    p2p.handle.remove_replicator_collections(peer_id, collections).await
                        .map_err(|e| format!("failed to delete replicator: {}", e))?;
                }

                let peerstore = Peerstore::new(db.store().clone());
                if let Err(e) = peerstore.delete_replicator(&peer_id.to_string()).await {
                    tracing::warn!(peer_id = %peer_id, error = %e, "Failed to delete replicator from storage");
                }

                let remaining_replicators = p2p.handle.list_replicators().await.unwrap_or_default();
                for collection_id in &removed_collections {
                    let still_needed = remaining_replicators.iter().any(|r| r.collections.contains(collection_id));
                    if !still_needed {
                        let topic = DefraTopic::collection(collection_id);
                        let _ = p2p.handle.unsubscribe(topic).await;
                    }
                }

                state.event_bus.publish(events::Message::replicator_completed());
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
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pReplicatorList
    ));

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p { Some(p2p) => p2p, None => return Err("no p2p system configured".to_string()) };

            rt.block_on(async {
                let replicators = p2p.handle.list_replicators().await
                    .map_err(|e| format!("failed to get replicators: {}", e))?;

                let response: Vec<serde_json::Value> = replicators.into_iter().map(|r| {
                    let peer_id_str = r.peer_id_str();
                    let addresses: Vec<String> = r.addresses().into_iter()
                        .map(|a| format!("{}/p2p/{}", a, peer_id_str)).collect();
                    serde_json::json!({ "ID": peer_id_str, "Addresses": addresses, "CollectionIDs": r.collections })
                }).collect();

                serde_json::to_string(&response)
                    .map_err(|e| format!("failed to serialize replicators: {}", e))
            })
        })
        .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
        .and_then(|r| r);

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}
