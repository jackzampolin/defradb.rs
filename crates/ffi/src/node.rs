//! Node lifecycle management for FFI.
//!
//! This module provides the core node creation and management functions
//! exposed via FFI to match Go's cbindings/node.go behavior.

use std::sync::Arc;

use crate::runtime::RUNTIME;
use crate::state::{NodeState, NODES};
use crate::types::{FfiResult, NewNodeResult, NodeInitOptions};

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
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return NewNodeResult::error("runtime not initialized - call defra_init() first"),
    };

    let result = rt.block_on(async {
        // Create in-memory storage (for MVP)
        let store = Arc::new(storage::MemoryStore::new());

        // Open database and load collections
        let database = db::DB::open_from_arc(store.clone())
            .await
            .map_err(|e| format!("failed to open database: {}", e))?;
        let database = Arc::new(database);

        // Create auto-committing fetcher for non-transactional queries
        let fetcher = db::AutoCommitFetcher::new(database.clone());

        // Create collection provider for on-demand schema resolution
        let collection_provider: Arc<dyn query::CollectionProvider> =
            db::DbCollectionProvider::new_arc(database.clone());

        // Create transaction registry for explicit transaction support
        let registry = db::DbTransactionRegistry::new(database.clone());

        // Create mutator for mutations
        let mutator: Arc<dyn query::DocMutator> =
            Arc::new(db::AutoCommitMutator::new(database.clone()));

        // Create in-memory ACP store
        let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());
        let document_acp: Arc<dyn acp::DocumentACP> =
            Arc::new(acp::LocalDocumentACP::new(acp_store));

        // Create query runner with transaction, mutation, and ACP support
        let query_runner =
            query::QueryRunner::with_registry_and_provider(fetcher, collection_provider, registry)
                .with_mutator(mutator)
                .with_acp(document_acp);

        let runner: Arc<dyn query::QueryExecutor> = Arc::new(query_runner);

        // Create node state
        let state = NodeState {
            database,
            query_runner: runner,
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
#[no_mangle]
pub extern "C" fn node_close(node_ptr: usize) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized"),
    };

    // Remove from registry
    let state = match NODES.remove(node_ptr) {
        Some(state) => state,
        None => return FfiResult::error("invalid node handle"),
    };

    // Close the database
    let result = rt.block_on(async { state.database.close().await });

    match result {
        Ok(()) => FfiResult::ok(),
        Err(e) => FfiResult::error(format!("failed to close database: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_lifecycle() {
        // Initialize runtime
        crate::runtime::init_runtime();

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
}
