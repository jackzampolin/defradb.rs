//! P2P FFI functions for DefraDB.
//!
//! This module provides FFI functions for P2P networking operations:
//! - Peer info and connection
//! - Replicator management
//! - P2P collection management
//! - Document sync operations

use std::ffi::c_char;

use crate::get_runtime;
use crate::state::{P2PState, NODES};
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

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
    let full_addr: libp2p::Multiaddr = addr_str
        .parse()
        .map_err(|e| format!("invalid multiaddr '{}': {}", addr_str, e))?;

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

/// Helper to extract P2P state from a node or return an error.
fn get_p2p_state(node_ptr: usize) -> Result<std::sync::Arc<P2PState>, String> {
    NODES
        .get(node_ptr, |state| {
            state
                .p2p
                .clone()
                .ok_or_else(|| "P2P not enabled for this node".to_string())
        })
        .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
        .and_then(|r| r)
}

/// Get P2P peer info (local peer ID and listening addresses).
///
/// Returns a JSON array of full multiaddrs with peer ID embedded.
///
/// # Safety
///
/// The caller must free the returned string with `defra_free_string`.
#[export_name = "PeerInfo"]
pub extern "C" fn p2p_peer_info(node_ptr: usize) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let p2p = match get_p2p_state(node_ptr) {
        Ok(p) => p,
        Err(e) => return FfiResult::error(e),
    };

    let result = rt.block_on(async {
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
    });

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
#[export_name = "ActivePeers"]
pub extern "C" fn p2p_active_peers(node_ptr: usize, _identity_ptr: usize) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let p2p = match get_p2p_state(node_ptr) {
        Ok(p) => p,
        Err(e) => return FfiResult::error(e),
    };

    let result = rt.block_on(async {
        let peers = p2p
            .handle
            .connected_peers()
            .await
            .map_err(|e| format!("failed to get peers: {}", e))?;

        let peer_ids: Vec<String> = peers.into_iter().map(|p| p.to_string()).collect();

        serde_json::to_string(&peer_ids)
            .map_err(|e| format!("failed to serialize peer list: {}", e))
    });

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
/// * `addr` - Full multiaddr including peer ID
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// `addr` must be a valid null-terminated UTF-8 string.
#[export_name = "Connect"]
pub unsafe extern "C" fn p2p_connect(
    node_ptr: usize,
    addr: *const c_char,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let addr_str = match c_str_to_string(addr) {
        Some(s) => s,
        None => return FfiResult::error("addr is null"),
    };

    let p2p = match get_p2p_state(node_ptr) {
        Ok(p) => p,
        Err(e) => return FfiResult::error(e),
    };

    let result = rt.block_on(async {
        let parsed = parse_multiaddr_with_peer_id(&addr_str)?;

        p2p.handle
            .dial(parsed.peer_id, vec![parsed.transport_addr])
            .await
            .map_err(|e| format!("failed to connect: {}", e))?;

        Ok::<(), String>(())
    });

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
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[export_name = "SetReplicator"]
pub unsafe extern "C" fn p2p_set_replicator(
    node_ptr: usize,
    peer_addr: *const c_char,
    collections_json: *const c_char,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

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

    let p2p = match get_p2p_state(node_ptr) {
        Ok(p) => p,
        Err(e) => return FfiResult::error(e),
    };

    let result = rt.block_on(async {
        let parsed = parse_multiaddr_with_peer_id(&addr_str)?;

        p2p.handle
            .set_replicator(parsed.peer_id, collections)
            .await
            .map_err(|e| format!("failed to set replicator: {}", e))?;

        Ok::<(), String>(())
    });

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
/// * `peer_id_str` - Peer ID string
/// * `collections_json` - JSON array of collection names to remove
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[export_name = "DeleteReplicator"]
pub unsafe extern "C" fn p2p_delete_replicator(
    node_ptr: usize,
    peer_id_str: *const c_char,
    _collections_json: *const c_char,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let peer_str = match c_str_to_string(peer_id_str) {
        Some(s) => s,
        None => return FfiResult::error("peer_id_str is null"),
    };

    let p2p = match get_p2p_state(node_ptr) {
        Ok(p) => p,
        Err(e) => return FfiResult::error(e),
    };

    let result = rt.block_on(async {
        let peer_id: libp2p::PeerId = peer_str
            .parse()
            .map_err(|e| format!("invalid peer ID '{}': {}", peer_str, e))?;

        p2p.handle
            .delete_replicator(peer_id)
            .await
            .map_err(|e| format!("failed to delete replicator: {}", e))?;

        Ok::<(), String>(())
    });

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
#[export_name = "GetAllReplicators"]
pub extern "C" fn p2p_get_all_replicators(node_ptr: usize, _identity_ptr: usize) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let p2p = match get_p2p_state(node_ptr) {
        Ok(p) => p,
        Err(e) => return FfiResult::error(e),
    };

    let result = rt.block_on(async {
        let replicators = p2p
            .handle
            .get_all_replicators()
            .await
            .map_err(|e| format!("failed to get replicators: {}", e))?;

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
    });

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
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// `collections_json` must be a valid null-terminated UTF-8 string.
#[export_name = "AddP2PCollections"]
pub unsafe extern "C" fn p2p_add_collections(
    node_ptr: usize,
    collections_json: *const c_char,
    _identity_ptr: usize,
) -> FfiResult {
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

            for name in collections {
                p2p.add_collection(&name);
            }

            Ok(())
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
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// `collections_json` must be a valid null-terminated UTF-8 string.
#[export_name = "RemoveP2PCollections"]
pub unsafe extern "C" fn p2p_remove_collections(
    node_ptr: usize,
    collections_json: *const c_char,
    _identity_ptr: usize,
) -> FfiResult {
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

            for name in collections {
                p2p.remove_collection(&name);
            }

            Ok(())
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
#[export_name = "GetAllP2PCollections"]
pub extern "C" fn p2p_get_all_collections(node_ptr: usize, _identity_ptr: usize) -> FfiResult {
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

// =============================================================================
// P2P Document Sync Operations
// =============================================================================

/// Add collections to P2P document sync.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[export_name = "P2PDocumentAdd"]
pub unsafe extern "C" fn p2p_document_add(
    node_ptr: usize,
    collections: *const c_char,
    _identity_ptr: usize,
) -> FfiResult {
    let collections_str = match c_str_to_string(collections) {
        Some(s) => s,
        None => return FfiResult::error("collections is null"),
    };

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("P2P not enabled for this node".to_string()),
            };

            for name in collections_str.split(',') {
                let trimmed = name.trim();
                if !trimmed.is_empty() {
                    p2p.add_collection(trimmed);
                }
            }
            Ok(())
        })
        .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
        .and_then(|r| r);

    match result {
        Ok(()) => FfiResult::ok(),
        Err(e) => FfiResult::error(e),
    }
}

/// Remove collections from P2P document sync.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[export_name = "P2PDocumentRemove"]
pub unsafe extern "C" fn p2p_document_remove(
    node_ptr: usize,
    collections: *const c_char,
    _identity_ptr: usize,
) -> FfiResult {
    let collections_str = match c_str_to_string(collections) {
        Some(s) => s,
        None => return FfiResult::error("collections is null"),
    };

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("P2P not enabled for this node".to_string()),
            };

            for name in collections_str.split(',') {
                let trimmed = name.trim();
                if !trimmed.is_empty() {
                    p2p.remove_collection(trimmed);
                }
            }
            Ok(())
        })
        .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
        .and_then(|r| r);

    match result {
        Ok(()) => FfiResult::ok(),
        Err(e) => FfiResult::error(e),
    }
}

/// Get all P2P document collections.
///
/// Returns a JSON array of collection names.
#[export_name = "P2PDocumentGetAll"]
pub extern "C" fn p2p_document_get_all(node_ptr: usize, _identity_ptr: usize) -> FfiResult {
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

/// Sync specific documents in a collection with P2P peers.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings or null.
#[export_name = "P2PDocumentSync"]
pub unsafe extern "C" fn p2p_document_sync(
    node_ptr: usize,
    _collection: *const c_char,
    _doc_ids: *const c_char,
    _timeout_str: *const c_char,
    _identity_ptr: usize,
) -> FfiResult {
    if NODES.get(node_ptr, |_| ()).is_none() {
        return FfiResult::error(ERR_INVALID_NODE_HANDLE);
    }
    FfiResult::error("P2P document sync not yet implemented in Rust")
}

/// Sync collection versions with P2P peers.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings or null.
#[export_name = "P2PCollectionSyncVersions"]
pub unsafe extern "C" fn p2p_collection_sync_versions(
    node_ptr: usize,
    _version_ids: *const c_char,
    _timeout_str: *const c_char,
    _identity_ptr: usize,
) -> FfiResult {
    if NODES.get(node_ptr, |_| ()).is_none() {
        return FfiResult::error(ERR_INVALID_NODE_HANDLE);
    }
    FfiResult::error("P2P collection sync versions not yet implemented in Rust")
}

/// Sync a branchable collection with P2P peers.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings or null.
#[export_name = "P2PBranchableCollectionSync"]
pub unsafe extern "C" fn p2p_branchable_collection_sync(
    node_ptr: usize,
    _collection_id: *const c_char,
    _timeout_str: *const c_char,
    _identity_ptr: usize,
) -> FfiResult {
    if NODES.get(node_ptr, |_| ()).is_none() {
        return FfiResult::error(ERR_INVALID_NODE_HANDLE);
    }
    FfiResult::error("P2P branchable collection sync not yet implemented in Rust")
}

/// Create a new DefraDB node with P2P enabled (internal, not exported to Go).
///
/// Go handles P2P via NodeInitOptions, so this is not part of the Go ABI.
/// Kept for Rust-only tests.
///
/// # Safety
///
/// `listen_addr` must be a valid null-terminated UTF-8 string.
pub unsafe fn new_node_with_p2p_internal(
    _options: crate::types::NodeInitOptions,
    listen_addr: *const c_char,
) -> crate::types::NewNodeResult {
    use std::sync::Arc;

    use blockstore::DefraBlockstore;
    use p2p::bitswap::BitswapStoreAdapter;
    use p2p::P2PHost;

    use crate::state::{NodeState, PolicyStore, NODES};
    use crate::types::NewNodeResult;

    let rt = get_runtime!(NewNodeResult);

    let listen_addr_str = match c_str_to_string(listen_addr) {
        Some(s) => s,
        None => return NewNodeResult::error("listen_addr is null"),
    };

    let result = rt.block_on(async {
        let store = Arc::new(storage::MemoryStore::new());
        let event_bus: Arc<dyn events::Bus> = Arc::new(events::ChannelBus::default());

        let mut database = db::DB::open_from_arc(store.clone())
            .await
            .map_err(|e| format!("failed to open database: {}", e))?;

        database.set_event_bus(event_bus.clone());
        let database = Arc::new(database);

        let blockstore = Arc::new(DefraBlockstore::new(store.clone(), true));
        let bitswap_store = BitswapStoreAdapter::new(blockstore);

        let (host, handle, _event_rx, _replicator_registry) = P2PHost::new(bitswap_store)
            .await
            .map_err(|e| format!("failed to create P2P host: {}", e))?;

        let addr: libp2p::Multiaddr = listen_addr_str
            .parse()
            .map_err(|e| format!("invalid multiaddr '{}': {}", listen_addr_str, e))?;

        handle
            .listen(addr)
            .await
            .map_err(|e| format!("failed to start listening: {}", e))?;

        let host_handle = handle.clone();
        tokio::spawn(async move {
            host.run().await;
        });

        let p2p_state = Arc::new(P2PState::new(host_handle));
        let fetcher = db::LensedAutoCommitFetcher::new(database.clone());
        let collection_provider: Arc<dyn query::CollectionProvider> =
            db::DbCollectionProvider::new_arc(database.clone());
        let registry = db::DbTransactionRegistry::new(database.clone());
        let mutator: Arc<dyn query::DocMutator> =
            Arc::new(db::AutoCommitMutator::new(database.clone()));
        let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());
        let document_acp: Arc<dyn acp::DocumentACP> =
            Arc::new(acp::LocalDocumentACP::new(acp_store));
        let nac_store = Arc::new(acp::MemoryZanzibarStore::new());
        let nac_config = db::NacConfig::new().with_dev_mode();
        let nac_manager = Arc::new(db::NacManager::new(nac_store, nac_config));

        let query_runner =
            query::QueryRunner::with_registry_and_provider(fetcher, collection_provider, registry)
                .with_mutator(mutator)
                .with_acp(document_acp.clone());
        let runner: Arc<dyn query::QueryExecutor> = Arc::new(query_runner);
        let policy_store = Arc::new(PolicyStore::new());

        let state = NodeState {
            database,
            query_runner: runner,
            nac_manager,
            document_acp,
            event_bus,
            policy_store,
            p2p: Some(p2p_state),
        };

        let handle = NODES.insert(state);
        Ok::<usize, String>(handle)
    });

    match result {
        Ok(handle) => NewNodeResult::success(handle),
        Err(e) => NewNodeResult::error(e),
    }
}
