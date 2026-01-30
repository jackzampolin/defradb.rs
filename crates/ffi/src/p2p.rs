//! P2P FFI functions for DefraDB.
//!
//! This module provides FFI functions for P2P networking operations:
//! - Peer info and connection
//! - Replicator management
//! - P2P collection management

use std::ffi::c_char;
use std::sync::Arc;

use acp::nac::NodePermission;

use crate::get_runtime;
use crate::nac_check::check_nac_for_node;
use crate::state::{NodeState, P2PState, PolicyStore, NODES};
use crate::types::{c_str_to_string, FfiResult, NewNodeResult, NodeInitOptions};
use crate::ERR_INVALID_NODE_HANDLE;

use blockstore::DefraBlockstore;
use p2p::bitswap::BitswapStoreAdapter;
use p2p::sync::{ReplicationConfig, ReplicationLoop, ReplicationResult, SyncConfig, SyncCoordinator};
use p2p::topics::DefraTopic;
use p2p::P2PHost;

/// Parsed multiaddr containing peer ID and transport address.
struct ParsedMultiaddr {
    /// The peer ID extracted from the multiaddr.
    peer_id: libp2p::PeerId,
    /// The transport address (multiaddr without the /p2p component).
    transport_addr: libp2p::Multiaddr,
}

/// Parse a full multiaddr string that includes a peer ID.
///
/// Expects format like: `/ip4/127.0.0.1/tcp/9171/p2p/12D3KooW...`
///
/// Returns the peer ID and the transport address (without /p2p component).
fn parse_multiaddr_with_peer_id(addr_str: &str) -> Result<ParsedMultiaddr, String> {
    // Parse the multiaddr
    let full_addr: libp2p::Multiaddr = addr_str
        .parse()
        .map_err(|e| format!("invalid multiaddr '{}': {}", addr_str, e))?;

    // Extract peer ID from the multiaddr
    let peer_id = full_addr
        .iter()
        .find_map(|p| {
            if let libp2p::multiaddr::Protocol::P2p(peer_id) = p {
                Some(peer_id)
            } else {
                None
            }
        })
        .ok_or_else(|| format!("multiaddr '{}' does not contain peer ID", addr_str))?;

    // Remove the p2p component to get the transport address
    let transport_addr: libp2p::Multiaddr = full_addr
        .iter()
        .filter(|p| !matches!(p, libp2p::multiaddr::Protocol::P2p(_)))
        .collect();

    Ok(ParsedMultiaddr {
        peer_id,
        transport_addr,
    })
}

/// Parse a JSON array of collection names.
///
/// Expects format like: `["collection1", "collection2"]`
fn parse_collections_json(json_str: &str) -> Result<Vec<String>, String> {
    serde_json::from_str(json_str).map_err(|e| format!("invalid collections JSON: {}", e))
}

fn parse_doc_ids_json(json_str: &str) -> Result<Vec<String>, String> {
    serde_json::from_str(json_str).map_err(|e| format!("invalid doc_ids JSON: {}", e))
}

/// Create a new DefraDB node with P2P enabled.
///
/// This creates an in-memory database instance with P2P networking.
/// The node will listen on the specified address for peer connections.
///
/// # Arguments
///
/// * `options` - Node initialization options
/// * `listen_addr` - P2P multiaddr to listen on (e.g., "/ip4/127.0.0.1/tcp/9171")
///
/// # Safety
///
/// * `listen_addr` must be a valid null-terminated UTF-8 string
/// * The returned `node_ptr` must be freed by calling `node_close`
#[no_mangle]
pub unsafe extern "C" fn new_node_with_p2p(
    _options: NodeInitOptions,
    listen_addr: *const c_char,
) -> NewNodeResult {
    let rt = get_runtime!(NewNodeResult);

    let listen_addr_str = match c_str_to_string(listen_addr) {
        Some(s) => s,
        None => return NewNodeResult::error("listen_addr is null"),
    };

    let result = rt.block_on(async {
        // Create in-memory storage
        let store = Arc::new(storage::MemoryStore::new());

        // Create event bus for subscriptions
        let event_bus: Arc<dyn events::Bus> = Arc::new(events::ChannelBus::default());

        // Open database and load collections
        let mut database = db::DB::open_from_arc(store.clone())
            .await
            .map_err(|e| format!("failed to open database: {}", e))?;

        // Wire event bus to database
        database.set_event_bus(event_bus.clone());
        let database = Arc::new(database);

        // Create blockstore for P2P/Bitswap
        let blockstore = Arc::new(DefraBlockstore::new(store.clone(), true));
        let bitswap_store = BitswapStoreAdapter::new(blockstore.clone());

        // Create P2P host
        let (host, handle, event_rx, _replicator_registry) = P2PHost::new(bitswap_store.clone())
            .await
            .map_err(|e| format!("failed to create P2P host: {}", e))?;

        // Spawn the P2P host event loop BEFORE sending any commands.
        // The host must be running to process commands like Listen/Dial.
        // Without this, handle.listen() below would deadlock because the
        // command channel has no consumer.
        tokio::spawn(async move {
            host.run().await;
        });

        // Parse and start listening on the address
        let addr: libp2p::Multiaddr = listen_addr_str
            .parse()
            .map_err(|e| format!("invalid multiaddr '{}': {}", listen_addr_str, e))?;

        handle
            .listen(addr)
            .await
            .map_err(|e| format!("failed to start listening: {}", e))?;

        // Create SyncCoordinator for processing incoming P2P sync messages
        let (coordinator, sync_events_rx) =
            SyncCoordinator::new(handle.clone(), blockstore.clone(), SyncConfig::default())
                .await
                .map_err(|e| format!("failed to create sync coordinator: {}", e))?;
        let coordinator = Arc::new(coordinator);

        // Create DbMergeHandler for merging CRDT blocks into the database
        let merge_handler = Arc::new(db::DbMergeHandler::new(
            database.clone(),
            blockstore.clone(),
        ));

        // Task 1: Host event loop - reads HostEvents and feeds them to the coordinator
        let coord_for_events = coordinator.clone();
        let host_event_task = tokio::spawn(async move {
            let mut rx = event_rx;
            while let Some(event) = rx.recv().await {
                if let Err(e) = coord_for_events.handle_host_event(event).await {
                    tracing::error!(error = %e, "Error handling host event");
                }
            }
        });

        // Task 2: Replication loop - processes sync events and publishes MergeComplete
        let coord_for_repl = coordinator.clone();
        let handler_for_repl = merge_handler.clone();
        let event_bus_for_repl = event_bus.clone();
        let replication_task = tokio::spawn(async move {
            let config = ReplicationConfig::default();
            let mut events = sync_events_rx;
            loop {
                let result = ReplicationLoop::process_next(
                    &coord_for_repl,
                    &mut events,
                    handler_for_repl.as_ref(),
                    &config,
                )
                .await;
                match &result {
                    ReplicationResult::Merged {
                        cid,
                        doc_id,
                        collection_id,
                    } => {
                        tracing::info!(
                            cid = %cid,
                            doc_id = %doc_id,
                            collection_id = %collection_id,
                            "Block merged - publishing MergeComplete event"
                        );
                        let mc = events::MergeCompleteData {
                            doc_id: doc_id.clone(),
                            cid: *cid,
                            collection_id: collection_id.clone(),
                            by_peer: coord_for_repl.local_peer_id().to_string(),
                        };
                        event_bus_for_repl.publish(events::Message::merge_complete(mc));
                    }
                    ReplicationResult::MergedButBroadcastFailed {
                        cid,
                        doc_id,
                        collection_id,
                        ..
                    } => {
                        // Merge succeeded, still publish MergeComplete
                        tracing::info!(
                            cid = %cid,
                            doc_id = %doc_id,
                            "Block merged (broadcast failed) - publishing MergeComplete event"
                        );
                        let mc = events::MergeCompleteData {
                            doc_id: doc_id.clone(),
                            cid: *cid,
                            collection_id: collection_id.clone(),
                            by_peer: coord_for_repl.local_peer_id().to_string(),
                        };
                        event_bus_for_repl.publish(events::Message::merge_complete(mc));
                    }
                    ReplicationResult::ChannelClosed => {
                        tracing::info!("Sync event channel closed, stopping replication loop");
                        break;
                    }
                    ReplicationResult::Failed { cid, error } => {
                        tracing::error!(cid = %cid, error = %error, "Block merge failed");
                    }
                    _ => {}
                }
            }
            tracing::info!("FFI replication loop stopped");
        });

        // Task 3: Local update broadcaster - broadcasts local mutations to P2P network
        // Retries with backoff when GossipSub mesh hasn't formed yet (InsufficientPeers).
        let coord_for_broadcast = coordinator.clone();
        let event_bus_for_broadcast = event_bus.clone();
        let broadcast_task = tokio::spawn(async move {
            let mut sub = event_bus_for_broadcast.subscribe(&[events::EventName::Update]);
            while let Some(msg) = sub.recv().await {
                if let Some(update) = msg.as_update() {
                    // Skip relay updates (already from P2P)
                    if update.is_relay {
                        continue;
                    }
                    let cid = update.cid;
                    let block = update.block.clone();
                    let doc_id = update.doc_id.clone();
                    let collection_id = update.collection_id.clone();

                    // Retry loop for GossipSub mesh formation.
                    // After peers connect, GossipSub needs ~1 heartbeat interval
                    // to form the mesh. Retry with backoff up to ~5 seconds total.
                    let max_retries = 10u32;
                    let mut attempt = 0u32;
                    loop {
                        attempt += 1;
                        match coord_for_broadcast
                            .broadcast_local_update(&cid, &block, &doc_id, &collection_id)
                            .await
                        {
                            Ok(_) => break,
                            Err(e) => {
                                let err_str = e.to_string();
                                if err_str.contains("InsufficientPeers") && attempt <= max_retries {
                                    let delay_ms = 100 * (1u64 << attempt.min(5));
                                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                                    continue;
                                }
                                tracing::debug!(
                                    doc_id = %doc_id,
                                    error = %e,
                                    "Failed to broadcast local update"
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Create P2P state with sync pipeline abort handles
        let p2p_state = Arc::new(P2PState::with_sync_pipeline(
            handle.clone(),
            host_event_task.abort_handle(),
            replication_task.abort_handle(),
            broadcast_task.abort_handle(),
        ));

        // Create lensed auto-committing fetcher
        let fetcher = db::LensedAutoCommitFetcher::new(database.clone());

        // Create collection provider
        let collection_provider: Arc<dyn query::CollectionProvider> =
            db::DbCollectionProvider::new_arc(database.clone());

        // Create transaction registry
        let registry = db::DbTransactionRegistry::new(database.clone());

        // Create mutator
        let mutator: Arc<dyn query::DocMutator> =
            Arc::new(db::AutoCommitMutator::new(database.clone()));

        // Create ACP store
        let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());
        let document_acp: Arc<dyn acp::DocumentACP> =
            Arc::new(acp::LocalDocumentACP::new(acp_store));

        // Create NAC manager
        let nac_store = Arc::new(acp::MemoryZanzibarStore::new());
        let nac_config = db::NacConfig::new().with_dev_mode();
        let nac_manager = Arc::new(db::NacManager::new(nac_store, nac_config));

        // Create query runner with lens support
        let query_runner =
            query::QueryRunner::with_registry_and_provider(fetcher, collection_provider, registry)
                .with_mutator(mutator)
                .with_acp(document_acp.clone())
                .with_lens_store(database.lens_store().clone());

        let runner: Arc<dyn query::QueryExecutor> = Arc::new(query_runner);

        // Create policy store
        let policy_store = Arc::new(PolicyStore::new());

        // Create node state with P2P
        let state = NodeState {
            database,
            query_runner: runner,
            nac_manager,
            document_acp,
            event_bus,
            policy_store,
            p2p: Some(p2p_state),
        };

        // Register and get handle
        let handle = NODES.insert(state);
        Ok::<usize, String>(handle)
    });

    match result {
        Ok(handle) => NewNodeResult::success(handle),
        Err(e) => NewNodeResult::error(e),
    }
}

/// Get P2P peer info (local peer ID and listening addresses).
///
/// Returns a JSON array of full multiaddrs with peer ID embedded:
/// `["/ip4/127.0.0.1/tcp/9171/p2p/12D3KooW..."]`
///
/// # Safety
///
/// The caller must free the returned string with `defra_free_string`.
#[no_mangle]
pub extern "C" fn p2p_peer_info(node_ptr: usize) -> FfiResult {
    let rt = get_runtime!(FfiResult);

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

                // Build full multiaddrs with peer ID embedded (Go-compatible format)
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

/// Get list of connected peers.
///
/// Returns a JSON array of peer IDs.
///
/// # Safety
///
/// The caller must free the returned string with `defra_free_string`.
#[no_mangle]
pub extern "C" fn p2p_active_peers(node_ptr: usize) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("P2P not enabled for this node".to_string()),
            };

            rt.block_on(async {
                let peers = p2p
                    .handle
                    .connected_peers()
                    .await
                    .map_err(|e| format!("failed to get peers: {}", e))?;

                let peer_ids: Vec<String> = peers.into_iter().map(|p| p.to_string()).collect();

                serde_json::to_string(&peer_ids)
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
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::P2pPeerConnect) {
        return e;
    }

    let addr_str = match c_str_to_string(addr) {
        Some(s) => s,
        None => return FfiResult::error("addr is null"),
    };

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("P2P not enabled for this node".to_string()),
            };

            rt.block_on(async {
                // Parse the multiaddr and extract peer ID + transport address
                let parsed = parse_multiaddr_with_peer_id(&addr_str)?;

                // Dial the peer
                p2p.handle
                    .dial(parsed.peer_id, vec![parsed.transport_addr])
                    .await
                    .map_err(|e| format!("failed to connect: {}", e))?;

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
pub unsafe extern "C" fn p2p_set_replicator(
    node_ptr: usize,
    identity_did: *const c_char,
    peer_addr: *const c_char,
    collections_json: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::P2pReplicatorCreate) {
        return e;
    }

    let addr_str = match c_str_to_string(peer_addr) {
        Some(s) => s,
        None => return FfiResult::error("peer_addr is null"),
    };

    let collections_str = match c_str_to_string(collections_json) {
        Some(s) => s,
        None => return FfiResult::error("collections_json is null"),
    };

    let collections = match parse_collections_json(&collections_str) {
        Ok(c) => c,
        Err(e) => return FfiResult::error(e),
    };

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("P2P not enabled for this node".to_string()),
            };

            rt.block_on(async {
                // Parse the multiaddr and extract peer ID
                let parsed = parse_multiaddr_with_peer_id(&addr_str)?;

                // Set the replicator
                p2p.handle
                    .set_replicator(parsed.peer_id, collections)
                    .await
                    .map_err(|e| format!("failed to set replicator: {}", e))?;

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
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `peer_id_str` - Peer ID string (e.g., "12D3KooW...")
///
/// # Safety
///
/// `peer_id_str` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn p2p_delete_replicator(
    node_ptr: usize,
    identity_did: *const c_char,
    peer_id_str: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::P2pReplicatorDelete) {
        return e;
    }

    let peer_str = match c_str_to_string(peer_id_str) {
        Some(s) => s,
        None => return FfiResult::error("peer_id_str is null"),
    };

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("P2P not enabled for this node".to_string()),
            };

            rt.block_on(async {
                let peer_id: libp2p::PeerId = peer_str
                    .parse()
                    .map_err(|e| format!("invalid peer ID '{}': {}", peer_str, e))?;

                p2p.handle
                    .delete_replicator(peer_id)
                    .await
                    .map_err(|e| format!("failed to delete replicator: {}", e))?;

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
/// Returns a JSON array of replicator info objects:
/// ```json
/// [
///   {
///     "ID": "12D3KooW...",
///     "Addresses": ["/ip4/127.0.0.1/tcp/9171/p2p/12D3KooW..."],
///     "CollectionIDs": ["users", "posts"]
///   }
/// ]
/// ```
///
/// # Safety
///
/// The caller must free the returned string with `defra_free_string`.
#[no_mangle]
pub unsafe extern "C" fn p2p_get_all_replicators(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::P2pReplicatorList) {
        return e;
    }

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("P2P not enabled for this node".to_string()),
            };

            rt.block_on(async {
                let replicators = p2p
                    .handle
                    .get_all_replicators()
                    .await
                    .map_err(|e| format!("failed to get replicators: {}", e))?;

                // Convert to Go-compatible format
                let response: Vec<serde_json::Value> = replicators
                    .into_iter()
                    .map(|r| {
                        let peer_id_str = r.peer_id_str();
                        let addresses: Vec<String> = r
                            .addresses()
                            .into_iter()
                            .map(|a| format!("{}/p2p/{}", a, peer_id_str))
                            .collect();

                        serde_json::json!({
                            "ID": peer_id_str,
                            "Addresses": addresses,
                            "CollectionIDs": r.collections,
                        })
                    })
                    .collect();

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

/// Add collections to P2P replication.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `collections_json` - JSON array of collection names
///
/// # Safety
///
/// `collections_json` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn p2p_add_collections(
    node_ptr: usize,
    identity_did: *const c_char,
    collections_json: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::P2pCollectionCreate) {
        return e;
    }

    let collections_str = match c_str_to_string(collections_json) {
        Some(s) => s,
        None => return FfiResult::error("collections_json is null"),
    };

    let collections = match parse_collections_json(&collections_str) {
        Ok(c) => c,
        Err(e) => return FfiResult::error(e),
    };

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("P2P not enabled for this node".to_string()),
            };

            rt.block_on(async {
                for name in &collections {
                    // Subscribe to the GossipSub topic for this collection
                    let topic = DefraTopic::collection(name);
                    if let Err(e) = p2p.handle.subscribe(topic).await {
                        tracing::warn!(collection = %name, error = %e, "Failed to subscribe to GossipSub topic");
                    }
                    p2p.add_collection(name);
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

/// Remove collections from P2P replication.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `collections_json` - JSON array of collection names
///
/// # Safety
///
/// `collections_json` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn p2p_remove_collections(
    node_ptr: usize,
    identity_did: *const c_char,
    collections_json: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::P2pCollectionDelete) {
        return e;
    }

    let collections_str = match c_str_to_string(collections_json) {
        Some(s) => s,
        None => return FfiResult::error("collections_json is null"),
    };

    let collections = match parse_collections_json(&collections_str) {
        Ok(c) => c,
        Err(e) => return FfiResult::error(e),
    };

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("P2P not enabled for this node".to_string()),
            };

            rt.block_on(async {
                for name in &collections {
                    // Unsubscribe from the GossipSub topic for this collection
                    let topic = DefraTopic::collection(name);
                    if let Err(e) = p2p.handle.unsubscribe(topic).await {
                        tracing::warn!(collection = %name, error = %e, "Failed to unsubscribe from GossipSub topic");
                    }
                    p2p.remove_collection(name);
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

/// Get all P2P collections.
///
/// Returns a JSON array of collection names.
///
/// # Safety
///
/// The caller must free the returned string with `defra_free_string`.
#[no_mangle]
pub unsafe extern "C" fn p2p_get_all_collections(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::P2pCollectionList) {
        return e;
    }

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("P2P not enabled for this node".to_string()),
            };

            let collections = p2p.get_collections();
            serde_json::to_string(&collections)
                .map_err(|e| format!("failed to serialize collections: {}", e))
        })
        .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
        .and_then(|r| r);

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

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
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::P2pDocumentCreate) {
        return e;
    }

    let doc_ids_str = match c_str_to_string(doc_ids_json) {
        Some(s) => s,
        None => return FfiResult::error("doc_ids_json is null"),
    };

    let doc_ids = match parse_doc_ids_json(&doc_ids_str) {
        Ok(d) => d,
        Err(e) => return FfiResult::error(e),
    };

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("P2P not enabled for this node".to_string()),
            };

            rt.block_on(async {
                for doc_id in &doc_ids {
                    let topic = DefraTopic::document(doc_id);
                    if let Err(e) = p2p.handle.subscribe(topic).await {
                        tracing::warn!(doc_id = %doc_id, error = %e, "Failed to subscribe to GossipSub topic for document");
                    }
                    p2p.add_document(doc_id);
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
pub unsafe extern "C" fn p2p_remove_documents(
    node_ptr: usize,
    identity_did: *const c_char,
    doc_ids_json: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::P2pDocumentDelete) {
        return e;
    }

    let doc_ids_str = match c_str_to_string(doc_ids_json) {
        Some(s) => s,
        None => return FfiResult::error("doc_ids_json is null"),
    };

    let doc_ids = match parse_doc_ids_json(&doc_ids_str) {
        Ok(d) => d,
        Err(e) => return FfiResult::error(e),
    };

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("P2P not enabled for this node".to_string()),
            };

            rt.block_on(async {
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
pub unsafe extern "C" fn p2p_get_all_documents(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::P2pDocumentList) {
        return e;
    }

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("P2P not enabled for this node".to_string()),
            };

            let documents = p2p.get_documents();
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
