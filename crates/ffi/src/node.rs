//! Node lifecycle management for FFI.

use std::sync::Arc;

use crate::ffi_entry;
use crate::state::{FfiStore, NodeState, P2PState, PolicyStore, NODES};
use crate::try_ffi;
use crate::types::{c_str_to_string, FfiResult, NewNodeResult, NodeInitOptions};
use crate::ERR_INVALID_NODE_HANDLE;

/// Maximum length for private key byte slices passed via FFI.
pub(crate) const MAX_PRIVATE_KEY_LEN: usize = 128;

/// Create a new DefraDB node without P2P.
#[no_mangle]
pub extern "C" fn new_node(options: NodeInitOptions) -> NewNodeResult {
    ffi_entry! {
        let rt = match crate::runtime::RUNTIME.get() {
            Some(rt) => rt,
            None => return NewNodeResult::error("runtime not initialized - call defra_init() first"),
        };

        let result = rt
            .block_on(async { build_node_state(options, embedded::TransportConfig::None).await })
            .map(|state| NODES.insert(state));

        match result {
            Ok(handle) => NewNodeResult::success(handle),
            Err(error) => NewNodeResult::error(error),
        }
    }
}

pub(crate) async fn build_node_state(
    options: NodeInitOptions,
    transport: embedded::TransportConfig,
) -> Result<NodeState, String> {
    let (store, persistence, db_path_opt) = resolve_store(&options)?;
    let mut config = resolve_embedded_config(&options, persistence)?;
    config.transport = with_derived_iroh_key_path(transport, &db_path_opt);

    let node = embedded::build_with_store(store, config)
        .await
        .map_err(|error| error.to_string())?;

    Ok(NodeState {
        database: node.database.clone(),
        background_tasks: node.background_tasks(),
        txn_registry: node.txn_registry.clone(),
        query_runner: node.query_runner.clone(),
        nac_manager: node.nac_manager.clone(),
        document_acp: node.document_acp.clone(),
        event_bus: node.event_bus.clone(),
        policy_store: Arc::new(PolicyStore::new()),
        local_zanzibar_store: node.local_zanzibar_store.clone(),
        p2p: node
            .p2p
            .clone()
            .map(|system| Arc::new(P2PState::new(system))),
        node_identity_did: node.node_identity_did.clone(),
        signing_enabled: node.node_identity_did.is_some(),
        sourcehub_acp: node.sourcehub_acp.clone(),
        se_encryption_key: None,
    })
}

fn resolve_store(
    options: &NodeInitOptions,
) -> Result<(Arc<FfiStore>, embedded::Persistence, Option<String>), String> {
    let backend_name = unsafe { c_str_to_string(options.datastore_backend) }
        .unwrap_or_default()
        .to_lowercase();

    if options.in_memory != 0 || backend_name == "memory" || options.db_path.is_null() {
        return Ok((
            Arc::new(FfiStore::Memory(storage::MemoryStore::new())),
            embedded::Persistence::Memory,
            None,
        ));
    }

    let path = unsafe { c_str_to_string(options.db_path) }
        .ok_or_else(|| "db_path is not valid UTF-8".to_string())?;
    let effective_backend = if backend_name.is_empty() {
        "lark"
    } else {
        &backend_name
    };

    let store = match effective_backend {
        "redb" => Arc::new(FfiStore::Redb(storage::RedbStore::open(&path).map_err(
            |error| format!("failed to open redb store at '{}': {}", path, error),
        )?)),
        #[cfg(feature = "fjall")]
        "fjall" => Arc::new(FfiStore::Fjall(storage::FjallStore::open(&path).map_err(
            |error| format!("failed to open fjall store at '{}': {}", path, error),
        )?)),
        #[cfg(not(feature = "fjall"))]
        "fjall" => {
            return Err("fjall backend not enabled. Rebuild with --features fjall".to_string());
        }
        #[cfg(feature = "rocksdb")]
        "rocksdb" => {
            let opts = storage::RocksDbStoreOptions::from_env();
            Arc::new(FfiStore::RocksDb(
                storage::RocksDbStore::open_with_options(&path, opts).map_err(|error| {
                    format!("failed to open rocksdb store at '{}': {}", path, error)
                })?,
            ))
        }
        #[cfg(not(feature = "rocksdb"))]
        "rocksdb" => {
            return Err("rocksdb backend not enabled. Rebuild with --features rocksdb".to_string());
        }
        #[cfg(feature = "lark")]
        "lark" => {
            let opts = storage::LarkStoreOptions::from_env();
            Arc::new(FfiStore::Lark(
                storage::LarkStore::open_with_options(&path, opts).map_err(|error| {
                    format!("failed to open lark store at '{}': {}", path, error)
                })?,
            ))
        }
        #[cfg(not(feature = "lark"))]
        "lark" => {
            return Err("lark backend not enabled. Rebuild with --features lark".to_string());
        }
        other => {
            return Err(format!(
                "unknown datastore backend '{}'. Supported: lark, redb, fjall, rocksdb, memory",
                other
            ));
        }
    };

    Ok((store, embedded::Persistence::Persistent, Some(path)))
}

fn resolve_embedded_config(
    options: &NodeInitOptions,
    persistence: embedded::Persistence,
) -> Result<embedded::EmbeddedNodeConfig, String> {
    let signing = if options.enable_signing != 0 {
        let key = if !options.signing_private_key.is_null() && options.signing_private_key_len > 0 {
            if options.signing_private_key_len > MAX_PRIVATE_KEY_LEN {
                return Err(format!(
                    "signing_private_key_len {} exceeds maximum {}",
                    options.signing_private_key_len, MAX_PRIVATE_KEY_LEN
                ));
            }
            // SAFETY: `signing_private_key` is non-null (checked above) and
            // `signing_private_key_len` is bounded by MAX_PRIVATE_KEY_LEN.
            // The caller guarantees the pointer is valid for the given length.
            let key_bytes = unsafe {
                std::slice::from_raw_parts(
                    options.signing_private_key,
                    options.signing_private_key_len,
                )
                .to_vec()
            };
            let key_type = unsafe { c_str_to_string(options.signing_key_type) }
                .unwrap_or_else(|| "secp256k1".to_string());
            let signing_key_type: defra_core::signing::SigningKeyType = key_type.parse()?;

            Some(match signing_key_type {
                defra_core::signing::SigningKeyType::Secp256k1 => {
                    embedded::SigningKey::Secp256k1(key_bytes)
                }
                defra_core::signing::SigningKeyType::Secp256r1 => {
                    embedded::SigningKey::Secp256r1(key_bytes)
                }
                defra_core::signing::SigningKeyType::Ed25519 => {
                    embedded::SigningKey::Ed25519(key_bytes)
                }
                defra_core::signing::SigningKeyType::Bls => {
                    return Err("unsupported signing key type: bls".to_string())
                }
                other => return Err(format!("unsupported signing key type: {}", other)),
            })
        } else {
            None
        };

        embedded::SigningConfig::Enabled { key }
    } else {
        embedded::SigningConfig::Disabled
    };

    let document_acp = if !options.sourcehub_grpc_address.is_null() {
        let grpc_address = unsafe { c_str_to_string(options.sourcehub_grpc_address) }
            .ok_or_else(|| "sourcehub_grpc_address is not valid UTF-8".to_string())?;
        let comet_rpc_address = unsafe { c_str_to_string(options.sourcehub_comet_rpc_address) }
            .ok_or_else(|| "sourcehub_comet_rpc_address is not valid UTF-8".to_string())?;
        let chain_id = unsafe { c_str_to_string(options.sourcehub_chain_id) }
            .ok_or_else(|| "sourcehub_chain_id is not valid UTF-8".to_string())?;

        if options.sourcehub_signer_key.is_null() || options.sourcehub_signer_key_len == 0 {
            return Err(
                "sourcehub_signer_key is required when SourceHub is configured".to_string(),
            );
        }
        if options.sourcehub_signer_key_len > MAX_PRIVATE_KEY_LEN {
            return Err(format!(
                "sourcehub_signer_key_len {} exceeds maximum {}",
                options.sourcehub_signer_key_len, MAX_PRIVATE_KEY_LEN
            ));
        }

        // SAFETY: `sourcehub_signer_key` is non-null (checked above) and
        // `sourcehub_signer_key_len` is bounded by MAX_PRIVATE_KEY_LEN.
        // The caller guarantees the pointer is valid for the given length.
        let signer_key = unsafe {
            std::slice::from_raw_parts(
                options.sourcehub_signer_key,
                options.sourcehub_signer_key_len,
            )
            .to_vec()
        };

        embedded::DocumentAcpConfig::SourceHub(embedded::SourceHubConfig {
            grpc_address,
            comet_rpc_address,
            chain_id,
            signer_key,
        })
    } else {
        embedded::DocumentAcpConfig::Local
    };

    Ok(embedded::EmbeddedNodeConfig {
        persistence,
        transport: embedded::TransportConfig::None,
        signing,
        document_acp,
        max_concurrent_dag_fetches: if options.max_concurrent_dag_fetches > 0 {
            Some(options.max_concurrent_dag_fetches)
        } else {
            None
        },
        max_concurrent_push_tasks: if options.max_concurrent_push_tasks > 0 {
            Some(options.max_concurrent_push_tasks)
        } else {
            None
        },
        rate_limit_burst: if options.rate_limit_burst > 0 {
            Some(options.rate_limit_burst)
        } else {
            None
        },
        rate_limit_rate: if options.rate_limit_rate > 0.0 {
            Some(options.rate_limit_rate)
        } else {
            None
        },
    })
}

fn with_derived_iroh_key_path(
    transport: embedded::TransportConfig,
    _db_path: &Option<String>,
) -> embedded::TransportConfig {
    #[cfg(feature = "iroh")]
    if let embedded::TransportConfig::Iroh(mut config) = transport {
        if config.secret_key_path.is_none() {
            if let Some(path) = _db_path {
                config.secret_key_path = Some(std::path::PathBuf::from(format!("{path}.iroh.key")));
            }
        }
        return embedded::TransportConfig::Iroh(config);
    }

    transport
}

/// Close a DefraDB node and release resources.
#[no_mangle]
pub extern "C" fn node_close(node_ptr: usize) -> FfiResult {
    ffi_entry! {
        use crate::state::{GRAPHQL_SUBSCRIPTIONS, SUBSCRIPTIONS};

        let rt = try_ffi!(crate::helpers::get_rt());

        let removed_subs = SUBSCRIPTIONS.remove_for_node(node_ptr);
        for sub_state in removed_subs {
            NODES.get(node_ptr, |state| {
                state.event_bus.unsubscribe(sub_state.subscription.id());
            });
        }

        let removed_gql_subs = GRAPHQL_SUBSCRIPTIONS.remove_for_node(node_ptr);
        for sub_state in removed_gql_subs {
            sub_state.task_abort.abort();
            NODES.get(node_ptr, |state| {
                state.event_bus.unsubscribe(sub_state.event_sub_id);
            });
        }

        let state = match NODES.remove(node_ptr) {
            Some(state) => state,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        if let Some(ref p2p) = state.p2p {
            rt.block_on(async { p2p.system.shutdown().await });
        }

        state.event_bus.close();
        let result = rt.block_on(async { state.database.close().await });

        match result {
            Ok(()) => FfiResult::ok(),
            Err(error) => FfiResult::error(format!("failed to close database: {}", error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_lifecycle() {
        assert!(crate::runtime::init_runtime());

        let result = new_node(NodeInitOptions::default());
        assert_eq!(result.status, 0);
        assert!(result.node_ptr > 0);

        let handle = result.node_ptr;
        assert_eq!(node_close(handle).status, 0);
        assert_eq!(node_close(handle).status, 1);
    }

    #[test]
    fn test_node_close_invalid_handle() {
        assert!(crate::runtime::init_runtime());

        let result = node_close(0);
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());

        let error = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
        assert!(error.contains("invalid"));

        unsafe { crate::types::defra_free_string(result.error) };
    }

    #[test]
    fn test_multiple_nodes() {
        assert!(crate::runtime::init_runtime());

        let result1 = new_node(NodeInitOptions::default());
        let result2 = new_node(NodeInitOptions::default());

        assert_eq!(result1.status, 0);
        assert_eq!(result2.status, 0);
        assert_ne!(result1.node_ptr, result2.node_ptr);

        assert_eq!(node_close(result1.node_ptr).status, 0);
        assert_eq!(node_close(result2.node_ptr).status, 0);
    }
}
