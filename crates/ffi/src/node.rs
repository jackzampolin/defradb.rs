//! Node lifecycle management for FFI.
//!
//! This module provides the core node creation and management functions
//! exposed via FFI to match Go's cbindings/node.go behavior.

use std::sync::Arc;

use identity::Identity;

use crate::get_runtime;
use crate::state::{FfiStore, NodeState, PolicyStore, NODES};
use crate::types::{c_str_to_string, FfiResult, NewNodeResult, NodeInitOptions};
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
pub extern "C" fn new_node(options: NodeInitOptions) -> NewNodeResult {
    let rt = get_runtime!(NewNodeResult);

    let enable_signing = options.enable_signing != 0;

    let result = rt.block_on(async {
        // Create storage backend based on options
        let store: Arc<FfiStore> = if options.in_memory == 0 && !options.db_path.is_null() {
            let path = unsafe { c_str_to_string(options.db_path) }
                .ok_or_else(|| "db_path is not valid UTF-8".to_string())?;
            let redb = storage::RedbStore::open(&path)
                .map_err(|e| format!("failed to open redb store at '{}': {}", path, e))?;
            Arc::new(FfiStore::Redb(redb))
        } else {
            Arc::new(FfiStore::Memory(storage::MemoryStore::new()))
        };

        // Create event bus for subscriptions (created early so it can be wired to database)
        let event_bus: Arc<dyn events::Bus> = Arc::new(events::ChannelBus::default());

        // Generate or load node identity BEFORE opening database so it can be passed via options.
        // This enables `get_node_identity()` to return the correct DID.
        eprintln!("[SIGN-DEBUG] new_node: enable_signing={}", enable_signing);
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

            eprintln!("[SIGN-DEBUG] new_node: node identity DID={}", did_str);
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
        let registry = Arc::new(db::DbTransactionRegistry::new(database.clone()));

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

        // Encryption key for CRDT delta encryption (test key matching Go DefraDB)
        let encryption_key = b"examplekey1234567890examplekey12".to_vec();

        // Create query runner with transaction, mutation, ACP, lens, and encryption support
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

        // Create policy store for DAC policies
        let policy_store = Arc::new(PolicyStore::new());

        // Create node state
        // P2P is disabled by default in FFI - use new_node_with_p2p for P2P-enabled nodes
        let state = NodeState {
            database,
            txn_registry: registry,
            query_runner: runner,
            nac_manager,
            document_acp,
            event_bus,
            policy_store,
            p2p: None,
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

    // Shutdown P2P host if enabled
    if let Some(ref p2p) = state.p2p {
        // Abort all background sync pipeline tasks
        p2p.abort_all_tasks();
        // Send shutdown command to P2P host
        let _ = rt.block_on(async { p2p.handle.shutdown().await });
    }

    // Close the event bus
    state.event_bus.close();

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
