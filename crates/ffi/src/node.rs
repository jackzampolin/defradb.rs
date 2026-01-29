//! Node lifecycle management for FFI.
//!
//! This module provides the core node creation and management functions
//! exposed via FFI to match Go's cbindings/node.go behavior.

use std::sync::Arc;

use std::ffi::c_char;

use crate::get_runtime;
use crate::state::{NodeState, PolicyStore, NODES};
use crate::types::{FfiResult, NewNodeResult, NodeInitOptions};
use crate::ERR_INVALID_NODE_HANDLE;

/// Create a new DefraDB node.
///
/// This creates an in-memory database instance with a query runner.
/// The returned handle must be passed to `node_close` when done.
///
/// # Safety
///
/// The returned `node_ptr` must be freed by calling `node_close`.
#[no_mangle]
pub extern "C" fn new_node(_options: NodeInitOptions) -> NewNodeResult {
    let rt = get_runtime!(NewNodeResult);

    let result = rt.block_on(async {
        // Create in-memory storage (for MVP)
        let store = Arc::new(storage::MemoryStore::new());

        // Create event bus for subscriptions (created early so it can be wired to database)
        let event_bus: Arc<dyn events::Bus> = Arc::new(events::ChannelBus::default());

        // Open database and load collections
        let mut database = db::DB::open_from_arc(store.clone())
            .await
            .map_err(|e| format!("failed to open database: {}", e))?;

        // Wire event bus to database so mutations publish events
        database.set_event_bus(event_bus.clone());

        let database = Arc::new(database);

        // Create lensed auto-committing fetcher for non-transactional queries
        // This applies schema migrations during document fetch
        let fetcher = db::LensedAutoCommitFetcher::new(database.clone());

        // Create collection provider for on-demand schema resolution
        let collection_provider: Arc<dyn query::CollectionProvider> =
            db::DbCollectionProvider::new_arc(database.clone());

        // Create transaction registry for explicit transaction support
        let registry = db::DbTransactionRegistry::new(database.clone());

        // Create mutator for mutations
        let mutator: Arc<dyn query::DocMutator> =
            Arc::new(db::AutoCommitMutator::new(database.clone()));

        // Create in-memory ACP store for document-level access control
        let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());
        let document_acp: Arc<dyn acp::DocumentACP> =
            Arc::new(acp::LocalDocumentACP::new(acp_store));

        // Create NAC manager for node-level access control
        let nac_store = Arc::new(acp::MemoryZanzibarStore::new());
        let nac_config = db::NacConfig::new().with_dev_mode();
        let nac_manager = Arc::new(db::NacManager::new(nac_store, nac_config));

        // Create query runner with transaction, mutation, and ACP support
        let query_runner =
            query::QueryRunner::with_registry_and_provider(fetcher, collection_provider, registry)
                .with_mutator(mutator)
                .with_acp(document_acp.clone());

        let runner: Arc<dyn query::QueryExecutor> = Arc::new(query_runner);

        // Create policy store for DAC policies
        let policy_store = Arc::new(PolicyStore::new());

        // Create node state
        // P2P is disabled by default in FFI - use new_node_with_p2p for P2P-enabled nodes
        let state = NodeState {
            database,
            query_runner: runner,
            nac_manager,
            document_acp,
            event_bus,
            policy_store,
            p2p: None,
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

/// Close a DefraDB node and release resources.
///
/// # Safety
///
/// The `node_ptr` must be a valid handle returned by `new_node`.
/// After this call, the handle is no longer valid.
/// All subscriptions associated with this node will be closed.
#[no_mangle]
pub extern "C" fn node_close(node_ptr: usize) -> FfiResult {
    use crate::state::SUBSCRIPTIONS;

    let rt = get_runtime!(FfiResult);

    // Remove all subscriptions for this node
    let removed_subs = SUBSCRIPTIONS.remove_for_node(node_ptr);
    for sub_state in removed_subs {
        // Unsubscribe from the event bus
        NODES.get(node_ptr, |state| {
            state.event_bus.unsubscribe(sub_state.subscription.id());
        });
    }

    // Remove from registry
    let state = match NODES.remove(node_ptr) {
        Some(state) => state,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    // Close the event bus
    state.event_bus.close();

    // Close the database
    let result = rt.block_on(async { state.database.close().await });

    match result {
        Ok(()) => FfiResult::ok(),
        Err(e) => FfiResult::error(format!("failed to close database: {}", e)),
    }
}

/// Export the database to a JSON file.
///
/// # Safety
///
/// `node_ptr` must be a valid handle from `new_node`.
/// `config_json` must be a valid null-terminated C string.
#[no_mangle]
pub extern "C" fn basic_export(node_ptr: usize, _config_json: *const c_char) -> FfiResult {
    let _rt = get_runtime!(FfiResult);

    if NODES.get(node_ptr, |_| ()).is_none() {
        return FfiResult::error(ERR_INVALID_NODE_HANDLE);
    }

    FfiResult::error("basic_export is not yet implemented")
}

/// Import documents from a JSON backup file.
///
/// # Safety
///
/// `node_ptr` must be a valid handle from `new_node`.
/// `filepath` must be a valid null-terminated C string.
#[no_mangle]
pub extern "C" fn basic_import(node_ptr: usize, _filepath: *const c_char) -> FfiResult {
    let _rt = get_runtime!(FfiResult);

    if NODES.get(node_ptr, |_| ()).is_none() {
        return FfiResult::error(ERR_INVALID_NODE_HANDLE);
    }

    FfiResult::error("basic_import is not yet implemented")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_lifecycle() {
        // Initialize runtime
        assert!(crate::runtime::init_runtime());

        // Create node
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0, "new_node should succeed");
        assert!(result.node_ptr > 0, "should have valid handle");

        let handle = result.node_ptr;

        // Close node
        let result = node_close(handle);
        assert_eq!(result.status, 0, "node_close should succeed");

        // Double close should fail
        let result = node_close(handle);
        assert_eq!(result.status, 1, "double close should fail");
    }

    // Edge case tests (H2)

    #[test]
    fn test_node_close_invalid_handle() {
        assert!(crate::runtime::init_runtime());

        // Closing an invalid handle should return error, not panic
        let result = node_close(0);
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());

        let error = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
        assert!(error.contains("invalid"), "should indicate invalid handle");

        unsafe { crate::types::defra_free_string(result.error) };
    }

    #[test]
    fn test_node_close_nonexistent_handle() {
        assert!(crate::runtime::init_runtime());

        // Closing a random handle should return error
        let result = node_close(999999);
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());

        unsafe { crate::types::defra_free_string(result.error) };
    }

    #[test]
    fn test_multiple_nodes() {
        assert!(crate::runtime::init_runtime());

        // Create multiple nodes
        let options = NodeInitOptions::default();
        let result1 = new_node(options);
        assert_eq!(result1.status, 0);

        let options = NodeInitOptions::default();
        let result2 = new_node(options);
        assert_eq!(result2.status, 0);

        // Handles should be different
        assert_ne!(result1.node_ptr, result2.node_ptr);

        // Both should be closeable
        let close1 = node_close(result1.node_ptr);
        assert_eq!(close1.status, 0);

        let close2 = node_close(result2.node_ptr);
        assert_eq!(close2.status, 0);
    }
}
