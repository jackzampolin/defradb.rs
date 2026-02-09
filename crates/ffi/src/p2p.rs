//! P2P FFI functions for DefraDB.
//!
//! This module provides FFI functions for P2P networking operations:
//! - Peer info and connection
//! - Replicator management
//! - P2P collection management

use std::ffi::c_char;
use std::str::FromStr;
use std::sync::Arc;

use acp::nac::NodePermission;
use identity::Identity;

use crate::get_runtime;
use crate::nac_check::check_nac_for_node;
use crate::state::{FfiStore, NodeState, P2PState, PolicyStore, NODES};
use crate::types::{c_str_to_string, FfiResult, NewNodeResult, NodeInitOptions};
use crate::ERR_INVALID_NODE_HANDLE;

use blockstore::{Blockstore, DefraBlockstore};
use defra_core::Block;
use p2p::bitswap::BitswapStoreAdapter;
use p2p::message::PushLogRequest;
use p2p::sync::{
    ReplicationConfig, ReplicationLoop, ReplicationResult, SyncConfig, SyncCoordinator,
};
use p2p::topics::DefraTopic;
use p2p::P2PHost;
use storage::corekv::IterOptions;

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
/// Also handles JSON `null` (treated as empty array).
fn parse_collections_json(json_str: &str) -> Result<Vec<String>, String> {
    let opt: Option<Vec<String>> =
        serde_json::from_str(json_str).map_err(|e| format!("invalid collections JSON: {}", e))?;
    Ok(opt.unwrap_or_default())
}

fn parse_doc_ids_json(json_str: &str) -> Result<Vec<String>, String> {
    let opt: Option<Vec<String>> =
        serde_json::from_str(json_str).map_err(|e| format!("invalid doc_ids JSON: {}", e))?;
    Ok(opt.unwrap_or_default())
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
    options: NodeInitOptions,
    listen_addr: *const c_char,
) -> NewNodeResult {
    let rt = get_runtime!(NewNodeResult);

    let listen_addr_str = match c_str_to_string(listen_addr) {
        Some(s) => s,
        None => return NewNodeResult::error("listen_addr is null"),
    };

    let enable_signing = options.enable_signing != 0;

    let result = rt.block_on(async {
        // Create storage backend based on options
        let store: Arc<FfiStore> = if options.in_memory == 0 && !options.db_path.is_null() {
            let path = c_str_to_string(options.db_path)
                .ok_or_else(|| "db_path is not valid UTF-8".to_string())?;
            let redb = storage::RedbStore::open(&path)
                .map_err(|e| format!("failed to open redb store at '{}': {}", path, e))?;
            Arc::new(FfiStore::Redb(redb))
        } else {
            Arc::new(FfiStore::Memory(storage::MemoryStore::new()))
        };

        // Create event bus for subscriptions
        let event_bus: Arc<dyn events::Bus> = Arc::new(events::ChannelBus::default());

        // Generate or load node identity BEFORE opening database so it can be passed via options.
        let (raw_identity_opt, node_identity_did) = if enable_signing {
            // Check if a signing key was provided by the caller
            let raw_identity = if !options.signing_private_key.is_null()
                && options.signing_private_key_len > 0
            {
                let key_bytes = unsafe {
                    std::slice::from_raw_parts(
                        options.signing_private_key,
                        options.signing_private_key_len,
                    )
                };
                let key_type = unsafe { crate::types::c_str_to_string(options.signing_key_type) }
                    .unwrap_or_else(|| "secp256k1".to_string());

                match key_type.as_str() {
                    "secp256k1" => {
                        let private_key = crypto::Secp256k1PrivateKey::from_bytes(key_bytes)
                            .map_err(|e| format!("failed to load secp256k1 key: {}", e))?;
                        identity::RawIdentity::from_secp256k1(private_key)
                            .map_err(|e| format!("failed to create node identity: {}", e))?
                    }
                    "ed25519" => {
                        let private_key = crypto::Ed25519PrivateKey::from_bytes(key_bytes)
                            .map_err(|e| format!("failed to load ed25519 key: {}", e))?;
                        identity::RawIdentity::from_ed25519(private_key)
                            .map_err(|e| format!("failed to create node identity: {}", e))?
                    }
                    other => {
                        return Err(format!("unsupported signing key type: {}", other));
                    }
                }
            } else {
                // Auto-generate secp256k1 key
                let private_key = crypto::generate_secp256k1()
                    .map_err(|e| format!("failed to generate node signing key: {}", e))?;
                identity::RawIdentity::from_secp256k1(private_key)
                    .map_err(|e| format!("failed to create node identity: {}", e))?
            };

            let did = raw_identity
                .did()
                .map_err(|e| format!("failed to derive node DID: {}", e))?;
            let did_str = did.to_string();

            let key_type = if !options.signing_private_key.is_null() {
                unsafe { crate::types::c_str_to_string(options.signing_key_type) }
                    .unwrap_or_else(|| "secp256k1".to_string())
            } else {
                "secp256k1".to_string()
            };

            // Store in global identity store so exec_request can look up the signing config
            defra_core::signing::store_identity(
                &did_str,
                defra_core::signing::SigningConfig {
                    key_type,
                    private_key_bytes: raw_identity.private_key_bytes().to_vec(),
                    public_key_bytes: raw_identity.public_key_bytes().to_vec(),
                    public_key_hex: hex::encode(raw_identity.public_key_bytes()),
                },
            );

            eprintln!(
                "[SIGN-DEBUG] new_node_with_p2p: node identity DID={}",
                did_str
            );
            (Some(raw_identity), Some(did_str))
        } else {
            (None, None)
        };

        // Open database with node identity (if signing enabled)
        let mut db_options = db::DbOptions::default();
        if let Some(raw_id) = raw_identity_opt {
            db_options = db_options.with_node_identity(raw_id);
        }

        let mut database = db::DB::open_from_arc_with_options(store.clone(), db_options)
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

        // Subscribe to default GossipSub topics (matches Go's p2p.New behavior)
        for topic in &[
            DefraTopic::DocSync,
            DefraTopic::Encryption,
            DefraTopic::Custom("sync-branchable".to_string()),
        ] {
            if let Err(e) = handle.subscribe(topic.clone()).await {
                tracing::warn!(topic = %topic, error = %e, "Failed to subscribe to default topic");
            }
        }

        // Create head provider for DocSync responses
        let head_provider: Arc<dyn p2p::sync::DocumentHeadProvider> =
            Arc::new(db::DbHeadProvider::new(database.clone()));

        // Create SyncCoordinator for processing incoming P2P sync messages
        let (coordinator, sync_events_rx) = SyncCoordinator::with_head_provider(
            handle.clone(),
            blockstore.clone(),
            SyncConfig::default(),
            p2p::bitswap::AccessMode::Open,
            Arc::new(p2p::bitswap::ReplicatorRegistry::new()),
            Arc::new(p2p::sync::NoOpCollectionStorage),
            head_provider,
        )
        .await
        .map_err(|e| format!("failed to create sync coordinator: {}", e))?;
        let coordinator = Arc::new(coordinator);

        // Create DbMergeHandler for merging CRDT blocks into the database
        let merge_handler = Arc::new(db::DbMergeHandler::new(
            database.clone(),
            blockstore.clone(),
        ));

        // Task 1: Host event loop - reads HostEvents and feeds them to the coordinator.
        // Also publishes TopicPeerEvent to the FFI event bus for GossipSub peer joins/leaves.
        let coord_for_events = coordinator.clone();
        let event_bus_for_host = event_bus.clone();
        let host_event_task = tokio::spawn(async move {
            let mut rx = event_rx;
            while let Some(event) = rx.recv().await {
                // Publish TopicPeerEvent for GossipSub peer subscription changes
                match &event {
                    p2p::HostEvent::PeerSubscribed { peer_id, topic } => {
                        event_bus_for_host.publish(events::Message::topic_peer_event(
                            events::TopicPeerEventData {
                                peer_id: peer_id.to_string(),
                                topic: topic.clone(),
                                event_type: "JOINED".to_string(),
                            },
                        ));
                    }
                    p2p::HostEvent::PeerUnsubscribed { peer_id, topic } => {
                        event_bus_for_host.publish(events::Message::topic_peer_event(
                            events::TopicPeerEventData {
                                peer_id: peer_id.to_string(),
                                topic: topic.clone(),
                                event_type: "LEFT".to_string(),
                            },
                        ));
                    }
                    _ => {}
                }
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
                        let mc = events::MergeCompleteData {
                            doc_id: doc_id.clone(),
                            cid: *cid,
                            collection_id: collection_id.clone(),
                            by_peer: coord_for_repl.local_peer_id().to_string(),
                        };
                        event_bus_for_repl.publish(events::Message::merge_complete(mc));

                        // Publish SE artifact received event so the Go SE coordinator
                        // bridge picks it up. The Go test framework uses this to know
                        // when encrypted index data is available after replication.
                        if !doc_id.is_empty() {
                            event_bus_for_repl.publish(events::Message::se_artifact_received(
                                events::SEArtifactReceivedData {
                                    doc_id: doc_id.clone(),
                                },
                            ));
                        }
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

                        if !doc_id.is_empty() {
                            event_bus_for_repl.publish(events::Message::se_artifact_received(
                                events::SEArtifactReceivedData {
                                    doc_id: doc_id.clone(),
                                },
                            ));
                        }
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

        // Create P2P state with sync pipeline components and abort handles
        // Note: Local update broadcast is now handled by BroadcastMutator directly
        // when mutations are executed, so no separate broadcast task is needed.
        let p2p_state = Arc::new(P2PState::new(
            handle.clone(),
            blockstore.clone(),
            merge_handler.clone(),
            host_event_task.abort_handle(),
            replication_task.abort_handle(),
        ));

        // Create lensed auto-committing fetcher
        let fetcher = db::LensedAutoCommitFetcher::new(database.clone());

        // Create collection provider
        let collection_provider: Arc<dyn query::CollectionProvider> =
            db::DbCollectionProvider::new_arc(database.clone());

        // Create transaction registry
        let registry = Arc::new(db::DbTransactionRegistry::new(database.clone()));

        // Create mutator with P2P broadcast support
        // BroadcastMutator handles push_to_replicators + GossipSub broadcast with retry
        let mutator: Arc<dyn query::DocMutator> = Arc::new(db::BroadcastMutator::new(
            database.clone(),
            coordinator.clone(),
        ));

        // Create ACP store
        let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());
        let document_acp: Arc<dyn acp::DocumentACP> =
            Arc::new(acp::LocalDocumentACP::new(acp_store));

        // Create NAC manager
        let nac_store = Arc::new(acp::MemoryZanzibarStore::new());
        let nac_config = db::NacConfig::new().with_dev_mode();
        let nac_manager = Arc::new(db::NacManager::new(nac_store, nac_config));

        // Encryption key for CRDT delta encryption (test key matching Go DefraDB)
        let encryption_key = b"examplekey1234567890examplekey12".to_vec();

        // Create query runner with lens and encryption support
        let query_runner = query::QueryRunner::with_arc_registry_and_provider(
            fetcher,
            collection_provider,
            registry.clone(),
        )
        .with_mutator(mutator)
        .with_acp(document_acp.clone())
        .with_encryption_key(encryption_key)
        .with_lens_store(database.lens_store().clone());

        let runner: Arc<dyn query::QueryExecutor> = Arc::new(query_runner);

        // Create policy store
        let policy_store = Arc::new(PolicyStore::new());

        // Create node state with P2P
        let state = NodeState {
            database,
            txn_registry: registry,
            query_runner: runner,
            nac_manager,
            document_acp,
            event_bus,
            policy_store,
            p2p: Some(p2p_state),
            node_identity_did,
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

/// Get list of connected peers with full multiaddrs.
///
/// Returns a JSON array of multiaddr strings (Go-compatible format).
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
                None => return Err("no p2p system configured".to_string()),
            };

            rt.block_on(async {
                // Get connected peers from the host first (authoritative list).
                let connected = p2p
                    .handle
                    .connected_peers()
                    .await
                    .map_err(|e| format!("failed to get connected peers: {}", e))?;

                // Get host-resolved addresses (populated by ConnectionEstablished events).
                let mut host_addrs = p2p
                    .handle
                    .peer_addresses()
                    .await
                    .map_err(|e| format!("failed to get peer addresses: {}", e))?;

                // Build a set of peer IDs already covered by host_addrs
                let mut covered: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                for addr_str in &host_addrs {
                    if let Some(pid) = addr_str.rsplit("/p2p/").next() {
                        covered.insert(pid.to_string());
                    }
                }

                // Check if any connected peers are missing from host_addrs.
                // This can happen when incoming ConnectionEstablished events
                // haven't been processed yet by the host event loop.
                let has_missing = connected.iter().any(|pid| {
                    let pid_str = pid.to_string();
                    !covered.contains(&pid_str) && p2p.get_peer_address(&pid_str).is_none()
                });

                if has_missing {
                    // Brief yield to let the host process pending events
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    // Retry peer_addresses
                    if let Ok(retry) = p2p.handle.peer_addresses().await {
                        covered.clear();
                        for addr_str in &retry {
                            if let Some(pid) = addr_str.rsplit("/p2p/").next() {
                                covered.insert(pid.to_string());
                            }
                        }
                        host_addrs = retry;
                    }
                }

                // Merge FFI-stored addresses for connected peers not in host map
                let mut all_addrs = host_addrs;
                for pid in &connected {
                    let pid_str = pid.to_string();
                    if !covered.contains(&pid_str) {
                        if let Some(ffi_addr) = p2p.get_peer_address(&pid_str) {
                            all_addrs.push(ffi_addr.to_string());
                        }
                    }
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
                None => return Err("no p2p system configured".to_string()),
            };

            rt.block_on(async {
                // Parse the multiaddr and extract peer ID + transport address
                let parsed = parse_multiaddr_with_peer_id(&addr_str)?;

                // Dial the peer
                p2p.handle
                    .dial(parsed.peer_id, vec![parsed.transport_addr])
                    .await
                    .map_err(|e| format!("failed to connect: {}", e))?;

                // Wait until the connection is established so that
                // p2p_active_peers returns the peer immediately after connect.
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

                // Store the peer's full multiaddr for ActivePeers lookups
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

    if let Err(e) = check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pReplicatorCreate,
    ) {
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
                None => return Err("no p2p system configured".to_string()),
            };
            let db = &state.database;

            rt.block_on(async {
                // Parse the multiaddr and extract peer ID
                let parsed = parse_multiaddr_with_peer_id(&addr_str)?;

                // Empty collections means "all collections" (Go behavior)
                let effective_collections = if collections.is_empty() {
                    db.list_collections()
                        .map_err(|e| format!("failed to list collections: {}", e))?
                } else {
                    collections
                };

                // Dial the peer first (Go's SetReplicator connects automatically)
                p2p.handle
                    .dial(parsed.peer_id, vec![parsed.transport_addr])
                    .await
                    .map_err(|e| format!("failed to connect to replicator peer: {}", e))?;

                // Store the peer's full multiaddr for ActivePeers lookups
                p2p.set_peer_address(&parsed.peer_id.to_string(), &addr_str);

                // Map collection names → CIDs for the replicator registry.
                // The replicator registry compares against Update event collection IDs
                // (which are CIDs), so we must store CIDs, not names.
                let mut collection_cids = Vec::new();
                for name in &effective_collections {
                    if let Ok(Some(col)) = db.get_collection(name) {
                        collection_cids.push(col.collection_id().to_string());
                    } else {
                        return Err(format!("collection '{}' not found", name));
                    }
                }

                // Set the replicator with collection CIDs (not names)
                p2p.handle
                    .set_replicator(parsed.peer_id, collection_cids)
                    .await
                    .map_err(|e| format!("failed to set replicator: {}", e))?;

                // Auto-subscribe to collection topics using schema root CIDs
                // (Go does this implicitly via SetReplicator → subscribe_collection)
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

                // Push existing documents to the replicator peer (Go's pushHeadsForAllDocs).
                // Documents created before the replicator was set up won't trigger
                // Update events, so we push them directly here.
                // Like Go, pushes run in background tasks (goroutines) and don't
                // block SetReplicator from returning.
                let push_handle = p2p.handle.clone();
                let push_db = Arc::clone(db);
                let push_peer_id = parsed.peer_id;
                let push_collections = effective_collections;
                let push_event_bus = state.event_bus.clone();

                tokio::spawn(async move {
                    if let Err(e) = push_existing_docs(
                        &push_handle,
                        &push_db,
                        push_peer_id,
                        &push_collections,
                    )
                    .await
                    {
                        tracing::error!(error = %e, "Failed to push existing docs to replicator");
                    }
                    // Signal that the replicator configuration is complete
                    // (all existing docs have been pushed). The Go test framework
                    // waits for this event before proceeding.
                    eprintln!("[PUSH-EXISTING] Publishing ReplicatorCompleted event");
                    push_event_bus.publish(events::Message::replicator_completed());
                    eprintln!("[PUSH-EXISTING] ReplicatorCompleted event published");
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

/// Push existing documents to a replicator peer.
///
/// Matches Go's `pushHeadsForAllDocs`: for each collection, iterate all docs,
/// get composite heads from headstore, load blocks, send PushLog to peer.
async fn push_existing_docs(
    handle: &p2p::P2PHostHandle,
    db: &crate::state::FfiDatabase,
    peer_id: libp2p::PeerId,
    collections: &[String],
) -> Result<(), String> {
    // Wait for the connection to be fully established (dial is non-blocking).
    let conn_timeout = std::time::Duration::from_secs(5);
    let conn_start = std::time::Instant::now();
    loop {
        let peers = handle.connected_peers().await.unwrap_or_default();
        if peers.contains(&peer_id) {
            break;
        }
        if conn_start.elapsed() > conn_timeout {
            return Err("timeout waiting for peer connection before push".to_string());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let local_peer_id = handle
        .local_peer_id()
        .await
        .map_err(|e| format!("failed to get local peer ID: {}", e))?;

    let txn = db
        .new_txn(true)
        .await
        .map_err(|e| format!("failed to create transaction: {}", e))?;

    let headstore = txn
        .headstore()
        .map_err(|e| format!("failed to get headstore: {}", e))?;
    let blockstore_view = txn
        .blockstore()
        .map_err(|e| format!("failed to get blockstore: {}", e))?;
    let datastore = txn
        .datastore()
        .map_err(|e| format!("failed to get datastore: {}", e))?;

    // Collect JoinHandles so we can await all pushes before signaling completion.
    let mut push_handles = Vec::new();

    for col_name in collections {
        let collection = match db
            .get_collection(col_name)
            .map_err(|e| format!("failed to get collection: {}", e))?
        {
            Some(c) => c,
            None => continue,
        };

        // Iterate datastore keys-only to get doc IDs.
        // Key format: /d/{collection_id}/{doc_id}
        // Sub-keys like /d/{collection_id}/{doc_id}/v are filtered out.
        let col_prefix = format!("/d/{}/", collection.collection_id()).into_bytes();
        let opts = IterOptions::new()
            .with_prefix(col_prefix)
            .with_keys_only(true);
        let mut doc_iter = datastore
            .iterator(opts)
            .await
            .map_err(|e| format!("failed to iterate datastore: {}", e))?;

        let mut doc_ids = Vec::new();
        while let Some(pair) = doc_iter
            .next()
            .await
            .map_err(|e| format!("datastore iteration error: {}", e))?
        {
            let key_str = String::from_utf8_lossy(&pair.key);
            let parts: Vec<&str> = key_str.split('/').collect();
            // Exact doc key: ["", "d", collection_id, doc_id] = 4 parts
            if parts.len() == 4 {
                doc_ids.push(parts[3].to_string());
            }
        }
        doc_iter
            .close()
            .await
            .map_err(|e| format!("datastore close error: {}", e))?;

        // For each document, push composite head blocks to the replicator.
        for doc_id in &doc_ids {
            let prefix = storage::keys::headstore::HeadstoreDocKey::field_prefix(doc_id, "C");
            let opts = IterOptions::new().with_prefix(prefix);
            let mut iter = headstore
                .iterator(opts)
                .await
                .map_err(|e| format!("failed to iterate headstore: {}", e))?;

            while let Some(pair) = iter
                .next()
                .await
                .map_err(|e| format!("headstore iteration error: {}", e))?
            {
                // Parse CID from key: /d/{doc_id}/C/{cid}
                let key_str = String::from_utf8_lossy(&pair.key);
                let parts: Vec<&str> = key_str.split('/').collect();
                if parts.len() < 5 {
                    continue;
                }
                let head_cid = match cid::Cid::from_str(parts[4]) {
                    Ok(c) => c,
                    Err(_) => continue,
                };

                // Read block data from blockstore
                let block_key = head_cid.to_bytes();
                let block_data = match blockstore_view.get(&block_key).await {
                    Ok(Some(data)) => data,
                    _ => continue,
                };

                let mut request = PushLogRequest::new(
                    doc_id.clone(),
                    head_cid.to_bytes(),
                    collection.collection_id().to_string(),
                    local_peer_id.to_string(),
                    block_data,
                );

                if let Err(e) = p2p::signing::sign_message(handle.keypair(), &mut request) {
                    tracing::warn!(error = %e, "Failed to sign PushLog request");
                    continue;
                }

                // Spawn each push concurrently but track the handle so we can
                // await completion before emitting ReplicatorCompleted.
                let push_h = handle.clone();
                push_handles.push(tokio::spawn(async move {
                    let _ = push_h.send_two_stream_request(peer_id, request).await;
                }));
            }

            iter.close()
                .await
                .map_err(|e| format!("headstore close error: {}", e))?;
        }
    }

    // Await all push tasks so ReplicatorCompleted isn't emitted prematurely.
    // The Go test framework copies expected heads on ReplicatorCompleted, then
    // waits for merge events — if pushes haven't landed yet, we get timeouts.
    eprintln!(
        "[PUSH-EXISTING] Awaiting {} push tasks to complete",
        push_handles.len()
    );
    for handle in push_handles {
        let _ = handle.await;
    }
    eprintln!("[PUSH-EXISTING] All push tasks completed");

    Ok(())
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
    collections_json: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pReplicatorDelete,
    ) {
        return e;
    }

    let peer_str = match c_str_to_string(peer_id_str) {
        Some(s) => s,
        None => return FfiResult::error("peer_id_str is null"),
    };

    // Parse optional collections filter
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
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("no p2p system configured".to_string()),
            };

            rt.block_on(async {
                let peer_id: libp2p::PeerId = peer_str
                    .parse()
                    .map_err(|e| format!("invalid peer ID '{}': {}", peer_str, e))?;

                p2p.handle
                    .remove_replicator_collections(peer_id, collections)
                    .await
                    .map_err(|e| format!("failed to delete replicator: {}", e))?;

                // Signal that the replicator deletion is complete.
                // The Go test framework waits for this event before proceeding.
                state
                    .event_bus
                    .publish(events::Message::replicator_completed());

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

    if let Err(e) = check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pReplicatorList,
    ) {
        return e;
    }

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("no p2p system configured".to_string()),
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

    if let Err(e) = check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pCollectionCreate,
    ) {
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
                None => return Err("no p2p system configured".to_string()),
            };
            let db = &state.database;

            rt.block_on(async {
                // Validate all collection names exist and collect their schema root CIDs
                let mut name_to_id = Vec::new();
                for name in &collections {
                    let col = db
                        .get_collection(name)
                        .map_err(|e| format!("failed to get collection: {}", e))?
                        .ok_or_else(|| "collection not found".to_string())?;
                    name_to_id.push((name.clone(), col.collection_id().to_string()));
                }

                for (name, collection_id) in &name_to_id {
                    // Subscribe to the GossipSub topic using the schema root CID
                    // (matches Go behavior which uses col.CollectionID())
                    let topic = DefraTopic::collection(collection_id);
                    if let Err(e) = p2p.handle.subscribe(topic).await {
                        tracing::warn!(collection = %name, collection_id = %collection_id, error = %e, "Failed to subscribe to GossipSub topic");
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

    if let Err(e) = check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pCollectionDelete,
    ) {
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
                None => return Err("no p2p system configured".to_string()),
            };
            let db = &state.database;

            rt.block_on(async {
                // Validate all collection names exist and collect their schema root CIDs
                let mut name_to_id = Vec::new();
                for name in &collections {
                    let col = db
                        .get_collection(name)
                        .map_err(|e| format!("failed to get collection: {}", e))?
                        .ok_or_else(|| "collection not found".to_string())?;
                    name_to_id.push((name.clone(), col.collection_id().to_string()));
                }

                for (name, collection_id) in &name_to_id {
                    // Unsubscribe from the GossipSub topic using the schema root CID
                    let topic = DefraTopic::collection(collection_id);
                    if let Err(e) = p2p.handle.unsubscribe(topic).await {
                        tracing::warn!(collection = %name, collection_id = %collection_id, error = %e, "Failed to unsubscribe from GossipSub topic");
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

    if let Err(e) = check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pCollectionList,
    ) {
        return e;
    }

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("no p2p system configured".to_string()),
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

    if let Err(e) = check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pDocumentCreate,
    ) {
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
                None => return Err("no p2p system configured".to_string()),
            };

            rt.block_on(async {
                // Validate all document IDs have valid format (atomic: all or nothing)
                for doc_id in &doc_ids {
                    if document::DocID::from_string(doc_id).is_err() {
                        return Err(
                            "malformed document ID, missing either version or cid".to_string(),
                        );
                    }
                }

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

    if let Err(e) = check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pDocumentDelete,
    ) {
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
                None => return Err("no p2p system configured".to_string()),
            };

            rt.block_on(async {
                // Validate all document IDs have valid format (atomic: all or nothing)
                for doc_id in &doc_ids {
                    if document::DocID::from_string(doc_id).is_err() {
                        return Err(
                            "malformed document ID, missing either version or cid".to_string()
                        );
                    }
                }

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

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::P2pDocumentList)
    {
        return e;
    }

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("no p2p system configured".to_string()),
            };

            let mut documents = p2p.get_documents();
            documents.sort();
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

                // Create DocSync request
                let mut request = p2p::message::DocSyncRequest::new(doc_ids.clone());

                // Sign the request
                if let Err(e) = p2p::signing::sign_message(p2p.handle.keypair(), &mut request) {
                    return Err(format!("failed to sign DocSync request: {}", e));
                }

                // Send DocSync request to each connected peer
                // The response handling happens asynchronously via the coordinator:
                // 1. Request is sent via two-stream protocol
                // 2. Peer responds with DocSyncReply containing head CIDs
                // 3. Coordinator receives DocSyncReply and initiates Bitswap fetch
                // 4. Blocks are stored and merged via the replication loop
                for peer_id in &connected_peers {
                    eprintln!("[DOCSYNC] Sending DocSync request to peer={}", peer_id);
                    let request_clone = request.clone();
                    let handle = p2p.handle.clone();
                    let peer_id = *peer_id;

                    tokio::spawn(async move {
                        eprintln!("[DOCSYNC] Spawned task sending to peer={}", peer_id);
                        match handle.send_doc_sync_request(peer_id, request_clone).await {
                            Ok(()) => {
                                eprintln!("[DOCSYNC] Sent DocSync request to peer={}", peer_id)
                            }
                            Err(e) => eprintln!(
                                "[DOCSYNC] Failed to send DocSync request to peer={}: {}",
                                peer_id, e
                            ),
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

/// Sync collection versions (schema definitions) from connected peers via Bitswap.
///
/// This fetches collection definition blocks by their CIDs (version IDs), recursively
/// fetches previous versions and field definition blocks, then saves them to the
/// database as inactive collection versions.
///
/// Unlike DocSync and BranchableSync (which use PubSub request/reply), this uses
/// Bitswap directly to fetch blocks by CID.
///
/// # Safety
///
/// `identity_did` and `version_ids_json` must be valid null-terminated UTF-8 strings.
/// `version_ids_json` should be a JSON array of CID strings: `["bafyrei...", "bafyrei..."]`
#[no_mangle]
pub unsafe extern "C" fn p2p_sync_collection_versions(
    node_ptr: usize,
    identity_did: *const c_char,
    version_ids_json: *const c_char,
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

    let version_ids_str = match c_str_to_string(version_ids_json) {
        Some(s) => s,
        None => return FfiResult::error("version_ids_json is null"),
    };

    eprintln!(
        "[FFI-COLLECTION-VERSION] p2p_sync_collection_versions called with version_ids={}",
        version_ids_str
    );

    // Parse the JSON array of version IDs
    let version_ids: Vec<String> = match serde_json::from_str(&version_ids_str) {
        Ok(ids) => ids,
        Err(e) => return FfiResult::error(format!("failed to parse version_ids_json: {}", e)),
    };

    if version_ids.is_empty() {
        eprintln!("[FFI-COLLECTION-VERSION] No version IDs provided, returning early");
        return FfiResult::ok();
    }

    eprintln!(
        "[FFI-COLLECTION-VERSION] Parsed {} version IDs to sync",
        version_ids.len()
    );

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("no p2p system configured".to_string()),
            };
            let db = &state.database;

            rt.block_on(async {
                // Get connected peers to use as providers
                let connected_peers = p2p
                    .handle
                    .connected_peers()
                    .await
                    .map_err(|e| format!("failed to get connected peers: {}", e))?;

                eprintln!(
                    "[FFI-COLLECTION-VERSION] Connected peers: {}",
                    connected_peers.len()
                );

                if connected_peers.is_empty() {
                    eprintln!("[FFI-COLLECTION-VERSION] No connected peers, returning early");
                    return Ok(());
                }

                // Process each version ID
                for version_id_str in &version_ids {
                    eprintln!(
                        "[FFI-COLLECTION-VERSION] Processing version_id={}",
                        version_id_str
                    );

                    // Parse CID from version ID string
                    let version_cid = match cid::Cid::try_from(version_id_str.as_str()) {
                        Ok(cid) => cid,
                        Err(e) => {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Invalid CID '{}': {}",
                                version_id_str, e
                            );
                            continue;
                        }
                    };

                    // Start Bitswap sync for the version CID
                    eprintln!(
                        "[FFI-COLLECTION-VERSION] Starting Bitswap sync for cid={}",
                        version_cid
                    );

                    if let Err(e) = p2p.handle
                        .bitswap_sync(version_cid, connected_peers.clone(), vec![version_cid])
                        .await
                    {
                        eprintln!(
                            "[FFI-COLLECTION-VERSION] Bitswap sync failed for {}: {}",
                            version_cid, e
                        );
                        continue;
                    }

                    // Wait for block to be fetched by polling the blockstore via transaction
                    let timeout = std::time::Duration::from_secs(30);
                    let start = std::time::Instant::now();
                    let mut block_found = false;

                    while start.elapsed() < timeout {
                        // Create a read-only transaction to check blockstore
                        let txn = match db.new_txn(true).await {
                            Ok(t) => t,
                            Err(e) => {
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] Failed to create txn: {}",
                                    e
                                );
                                break;
                            }
                        };

                        let blockstore = match txn.blockstore() {
                            Ok(b) => b,
                            Err(e) => {
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] Failed to get blockstore: {}",
                                    e
                                );
                                break;
                            }
                        };

                        // Check if block exists
                        let cid_bytes = version_cid.to_bytes();
                        match blockstore.has(&cid_bytes).await {
                            Ok(true) => {
                                block_found = true;
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] Block {} fetched successfully",
                                    version_cid
                                );
                                break;
                            }
                            Ok(false) => {
                                // Not yet, wait and retry
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            }
                            Err(e) => {
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] Blockstore check failed: {}",
                                    e
                                );
                                break;
                            }
                        }
                    }

                    if !block_found {
                        eprintln!(
                            "[FFI-COLLECTION-VERSION] Timeout waiting for block {}",
                            version_cid
                        );
                        continue;
                    }

                    eprintln!(
                        "[FFI-COLLECTION-VERSION] Block fetched, extracting linked field blocks"
                    );

                    // Read block data from blockstore
                    let block_data = match p2p.blockstore.get(&version_cid).await {
                        Ok(Some(data)) => data,
                        Ok(None) => {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Block {} not found in blockstore after fetch",
                                version_cid
                            );
                            continue;
                        }
                        Err(e) => {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Failed to read block {}: {}",
                                version_cid, e
                            );
                            continue;
                        }
                    };

                    // Decode block to extract linked field CIDs
                    let linked_cids = match Block::from_dag_cbor(&block_data) {
                        Ok(block) => {
                            let links = block.all_links();
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Collection block has {} linked CIDs",
                                links.len()
                            );
                            links
                        }
                        Err(e) => {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Failed to decode block {}: {}",
                                version_cid, e
                            );
                            vec![]
                        }
                    };

                    // Fetch all linked field blocks
                    for link_cid in &linked_cids {
                        // Check if we already have this block
                        match p2p.blockstore.get(link_cid).await {
                            Ok(Some(_)) => {
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] Link {} already present",
                                    link_cid
                                );
                                continue;
                            }
                            Ok(None) => {
                                // Need to fetch
                            }
                            Err(e) => {
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] Error checking link {}: {}",
                                    link_cid, e
                                );
                                continue;
                            }
                        }

                        eprintln!(
                            "[FFI-COLLECTION-VERSION] Fetching linked block {}",
                            link_cid
                        );

                        // Start Bitswap sync for linked block
                        if let Err(e) = p2p.handle
                            .bitswap_sync(*link_cid, connected_peers.clone(), vec![*link_cid])
                            .await
                        {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Bitswap sync failed for link {}: {}",
                                link_cid, e
                            );
                            continue;
                        }

                        // Wait for linked block
                        let link_timeout = std::time::Duration::from_secs(10);
                        let link_start = std::time::Instant::now();
                        let mut link_found = false;

                        while link_start.elapsed() < link_timeout {
                            match p2p.blockstore.get(link_cid).await {
                                Ok(Some(_)) => {
                                    link_found = true;
                                    eprintln!(
                                        "[FFI-COLLECTION-VERSION] Linked block {} fetched",
                                        link_cid
                                    );
                                    break;
                                }
                                Ok(None) => {
                                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                }
                                Err(e) => {
                                    eprintln!(
                                        "[FFI-COLLECTION-VERSION] Error waiting for link {}: {}",
                                        link_cid, e
                                    );
                                    break;
                                }
                            }
                        }

                        if !link_found {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Timeout waiting for linked block {}",
                                link_cid
                            );
                        }
                    }

                    eprintln!(
                        "[FFI-COLLECTION-VERSION] All linked blocks fetched, processing through merge handler"
                    );

                    // Process through merge handler with recovery metadata
                    // (collection definitions don't have doc_id/collection_id in the traditional sense)
                    use p2p::sync::{BlockMetadata, MergeHandler};
                    let metadata = BlockMetadata::recovery();

                    match p2p.merge_handler.handle_block(&version_cid, &block_data, metadata).await {
                        Ok(outcome) => {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Merge handler result for {}: {:?}",
                                version_cid, outcome
                            );
                        }
                        Err(e) => {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Merge handler error for {}: {}",
                                version_cid, e
                            );
                        }
                    }

                    eprintln!(
                        "[FFI-COLLECTION-VERSION] Successfully synced version {}",
                        version_id_str
                    );
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
