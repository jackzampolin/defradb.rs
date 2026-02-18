//! Node lifecycle management for FFI.
//!
//! This module provides the core node creation and management functions
//! exposed via FFI to match Go's cbindings/node.go behavior.

use std::sync::Arc;

use identity::Identity;

use crate::state::{FfiStore, NodeState, PolicyStore, NODES};
use crate::try_ffi;
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
    let rt = match crate::runtime::RUNTIME.get() {
        Some(rt) => rt,
        None => return NewNodeResult::error("runtime not initialized - call defra_init() first"),
    };

    let enable_signing = options.enable_signing != 0;

    let result = rt.block_on(async {
        // Create storage backend based on options
        let db_path_opt: Option<String>;
        let backend_name = unsafe { c_str_to_string(options.datastore_backend) }
            .unwrap_or_default()
            .to_lowercase();

        let store: Arc<FfiStore> = if options.in_memory != 0
            || backend_name == "memory"
            || options.db_path.is_null()
        {
            db_path_opt = None;
            Arc::new(FfiStore::Memory(storage::MemoryStore::new()))
        } else {
            let path = unsafe { c_str_to_string(options.db_path) }
                .ok_or_else(|| "db_path is not valid UTF-8".to_string())?;
            db_path_opt = Some(path.clone());

            // Pick backend: explicit choice > compile-time default
            let effective_backend = if backend_name.is_empty() {
                "redb"
            } else {
                &backend_name
            };

            match effective_backend {
                "redb" | "badger" => {
                    let redb = storage::RedbStore::open(&path)
                        .map_err(|e| format!("failed to open redb store at '{}': {}", path, e))?;
                    Arc::new(FfiStore::Redb(redb))
                }
                #[cfg(feature = "fjall")]
                "fjall" => {
                    let fjall = storage::FjallStore::open(&path)
                        .map_err(|e| format!("failed to open fjall store at '{}': {}", path, e))?;
                    Arc::new(FfiStore::Fjall(fjall))
                }
                #[cfg(not(feature = "fjall"))]
                "fjall" => {
                    return Err("fjall backend not enabled. Rebuild with --features fjall".into());
                }
                #[cfg(feature = "rocksdb")]
                "rocksdb" => {
                    let rocks = storage::RocksDbStore::open(&path).map_err(|e| {
                        format!("failed to open rocksdb store at '{}': {}", path, e)
                    })?;
                    Arc::new(FfiStore::RocksDb(rocks))
                }
                #[cfg(not(feature = "rocksdb"))]
                "rocksdb" => {
                    return Err(
                        "rocksdb backend not enabled. Rebuild with --features rocksdb".into(),
                    );
                }
                other => {
                    return Err(format!(
                        "unknown datastore backend '{}'. Supported: redb, fjall, rocksdb, memory",
                        other
                    ));
                }
            }
        };

        // Create event bus for subscriptions (created early so it can be wired to database)
        let event_bus: Arc<dyn events::Bus> = Arc::new(events::ChannelBus::default());

        // Generate or load node identity BEFORE opening database so it can be passed via options.
        // This enables `get_node_identity()` to return the correct DID.
        tracing::debug!(enable_signing, "new_node: signing configuration");
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
                    remote_signer: None,
                },
            );

            tracing::debug!(did = %did_str, "node identity created");
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

        // Create document-level access control: SourceHub (on-chain) or Local (in-memory)
        let (document_acp, sourcehub_acp): (
            Arc<dyn acp::DocumentACP>,
            Option<Arc<sourcehub::SourceHubDocumentACP>>,
        ) = if !options.sourcehub_grpc_address.is_null() {
            let grpc_addr = unsafe { c_str_to_string(options.sourcehub_grpc_address) }
                .ok_or_else(|| "sourcehub_grpc_address is not valid UTF-8".to_string())?;
            let comet_addr = unsafe { c_str_to_string(options.sourcehub_comet_rpc_address) }
                .ok_or_else(|| "sourcehub_comet_rpc_address is not valid UTF-8".to_string())?;
            let chain_id = unsafe { c_str_to_string(options.sourcehub_chain_id) }
                .ok_or_else(|| "sourcehub_chain_id is not valid UTF-8".to_string())?;
            let signer_key = if !options.sourcehub_signer_key.is_null()
                && options.sourcehub_signer_key_len > 0
            {
                unsafe {
                    std::slice::from_raw_parts(
                        options.sourcehub_signer_key,
                        options.sourcehub_signer_key_len,
                    )
                }
            } else {
                return Err(
                    "sourcehub_signer_key is required when SourceHub is configured".to_string(),
                );
            };
            let sh_client = sourcehub::SourceHubClient::new(grpc_addr, comet_addr);
            let sh_signer = sourcehub::TxSigner::from_secp256k1_bytes(signer_key, &chain_id)
                .map_err(|e| format!("failed to create SourceHub signer: {}", e))?;
            tracing::debug!(validator = %sh_signer.address(), "SourceHub ACP configured");
            let sh_acp = Arc::new(sourcehub::SourceHubDocumentACP::new(sh_client, sh_signer));
            (sh_acp.clone() as Arc<dyn acp::DocumentACP>, Some(sh_acp))
        } else if db_path_opt.is_some() {
            // File-based storage: use persistent ACP store (namespace isolated in main DB)
            let acp_store: Arc<dyn acp::AcpStore> =
                Arc::new(acp::PersistentAcpStore::from_store(store.clone()));
            (
                Arc::new(acp::LocalDocumentACP::new(acp_store)) as Arc<dyn acp::DocumentACP>,
                None,
            )
        } else {
            let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());
            (
                Arc::new(acp::LocalDocumentACP::new(acp_store)) as Arc<dyn acp::DocumentACP>,
                None,
            )
        };

        // Create NAC manager for node-level access control
        // Use persistent store when file-based storage is configured
        let nac_manager: Arc<dyn db::NacManagerApi> = if db_path_opt.is_some() {
            // File-based storage: use persistent NAC store (namespace isolated in main DB)
            let nac_store = Arc::new(acp::PersistentZanzibarStore::from_store(store.clone()));
            let nac_config = db::NacConfig::new().with_dev_mode();
            let mgr = Arc::new(db::NacManager::new(nac_store, nac_config));
            mgr.initialize(None)
                .await
                .map_err(|e| format!("failed to initialize NAC from persistent store: {}", e))?;
            mgr as Arc<dyn db::NacManagerApi>
        } else {
            let nac_store = Arc::new(acp::MemoryZanzibarStore::new());
            let nac_config = db::NacConfig::new().with_dev_mode();
            Arc::new(db::NacManager::new(nac_store, nac_config))
        };

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
            sourcehub_acp,
            se_encryption_key: None,
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
    use crate::state::{GRAPHQL_SUBSCRIPTIONS, SUBSCRIPTIONS};

    let rt = try_ffi!(crate::helpers::get_rt());

    // Remove all raw subscriptions for this node
    let removed_subs = SUBSCRIPTIONS.remove_for_node(node_ptr);
    for sub_state in removed_subs {
        NODES.get(node_ptr, |state| {
            state.event_bus.unsubscribe(sub_state.subscription.id());
        });
    }

    // Remove all GraphQL subscriptions for this node
    let removed_gql_subs = GRAPHQL_SUBSCRIPTIONS.remove_for_node(node_ptr);
    for sub_state in removed_gql_subs {
        sub_state.task_abort.abort();
        NODES.get(node_ptr, |state| {
            state.event_bus.unsubscribe(sub_state.event_sub_id);
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
