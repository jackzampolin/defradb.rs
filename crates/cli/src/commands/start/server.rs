//! Store and server initialization

use std::net::SocketAddr;
use std::sync::Arc;

use tracing::{error, info, warn};

use super::node::{Node, P2PTasks};
use crate::config::{AcpDocumentType, Config};
use crate::error::{Error, Result};
use identity::Identity;

impl Node {
    /// Initialize store, database, P2P, and HTTP server.
    ///
    /// This function creates the database, loads collections, sets up the query
    /// runner with proper transaction support, and returns the HTTP server.
    ///
    /// Returns a tuple of (P2PHostHandle, P2PTasks, HTTP Server) where the tasks
    /// are tracked for graceful shutdown.
    pub(super) async fn init_store_and_server<S>(
        store: Arc<S>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
        user_identity: Option<std::sync::Arc<identity::RawIdentity>>,
        acp_store: Arc<dyn acp::AcpStore>,
        zanzibar_store: Arc<dyn acp::ZanzibarStore>,
        node_identity_did: Option<String>,
    ) -> Result<(
        Option<p2p::P2PHostHandle>,
        Option<P2PTasks>,
        defra_http::Server,
    )>
    where
        S: storage::corekv::Store + 'static,
    {
        // Extract DID from user identity for query runner (before consuming it)
        // SECURITY: If user explicitly provides --identity, DID derivation MUST succeed.
        // Failing silently to anonymous would violate user's security expectations.
        let user_did = match &user_identity {
            Some(identity) => Some(identity.did().map_err(|e| {
                Error::InvalidIdentity(format!(
                    "failed to derive DID from --identity flag: {}. \
                     Verify your key is valid and matches --identity-key-type. \
                     Remove --identity flag to run without authentication.",
                    e
                ))
            })?),
            None => None,
        };

        // Extract private key bytes before consuming user_identity (needed for SourceHub signer)
        let identity_key_bytes = user_identity.as_ref().map(|id| id.private_key_bytes());

        // Build database options with optional user identity
        let mut db_options = db::DbOptions::new();
        if let Some(identity) = user_identity {
            db_options = db_options.with_node_identity_arc(identity);
            info!("Database configured with user identity");
        }

        // Open database and load collections from storage
        let mut database = db::DB::open_from_arc_with_options(store.clone(), db_options)
            .await
            .map_err(|e| Error::Storage(storage::Error::Other(e.to_string())))?;

        // Create and configure event bus for GraphQL subscriptions
        let event_bus: Arc<dyn events::Bus> = Arc::new(events::ChannelBus::new());
        database.set_event_bus(event_bus.clone());
        info!("Event bus configured for subscriptions");

        // Now wrap database in Arc
        let database = Arc::new(database);

        let collection_count = database
            .list_collections()
            .map_err(|e| Error::Storage(storage::Error::Other(e.to_string())))?
            .len();
        info!("Loaded {} collection schema(s)", collection_count);

        // Set up P2P if enabled
        // Clone store before potential move for sync coordinator blockstore
        let store_for_sync = store.clone();
        let (p2p, mut p2p_events, p2p_host_task) = if config.net.p2p_disabled {
            (None, None, None)
        } else {
            info!("Initializing P2P network");
            let blockstore = Arc::new(blockstore::DefraBlockstore::new(store, false));
            let bitswap_store = p2p::BitswapStoreAdapter::new(blockstore);
            let (handle, events, host_task) = Self::start_p2p(
                config,
                bitswap_store,
                peer_keypair,
                config.net.pubsub_enabled,
            )
            .await?;
            (Some(handle), Some(events), Some(host_task))
        };

        // Create HTTP server with database-backed query runner
        let (http_server, p2p_tasks) = {
            let api_address: SocketAddr =
                config
                    .api
                    .address
                    .parse()
                    .map_err(|e: std::net::AddrParseError| {
                        Error::InvalidApiAddress(config.api.address.clone(), e.to_string())
                    })?;

            let server_config = defra_http::ServerConfig {
                address: api_address,
                allowed_origins: config.api.allowed_origins.clone(),
            };

            // Create auto-committing fetcher for non-transactional queries
            let fetcher = db::LensedAutoCommitFetcher::new(database.clone());

            // Create sync coordinator if P2P is enabled (shared between mutator and P2P adapter)
            // Also captures task handles for graceful shutdown
            let (sync_coordinator, replication_task, event_handler_task) =
                if let Some(ref p2p_handle) = p2p {
                    let sync_blockstore = Arc::new(blockstore::DefraBlockstore::new(
                        store_for_sync.clone(),
                        false,
                    ));

                    // Clone blockstore for merge handler (before moving into coordinator)
                    let merge_blockstore = sync_blockstore.clone();

                    // Create persistent collection store for P2P subscriptions
                    let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
                        Arc::new(p2p::sync::P2PCollectionStore::new(store_for_sync));

                    let (coordinator, sync_events) =
                        p2p::sync::SyncCoordinator::with_collection_store(
                            p2p_handle.clone(),
                            sync_blockstore,
                            p2p::sync::SyncConfig::default(),
                            collection_store,
                        )
                        .await
                        .map_err(Error::P2P)?;

                    let coordinator = Arc::new(coordinator);

                    // Load persisted P2P collection subscriptions
                    match coordinator.load_p2p_collections().await {
                        Ok(count) => {
                            if count > 0 {
                                info!("Loaded {} persisted P2P collection subscription(s)", count);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to load persisted P2P collections: {}", e);
                        }
                    }

                    // Create merge handler for CRDT merging
                    let merge_handler =
                        Arc::new(db::DbMergeHandler::new(database.clone(), merge_blockstore));

                    // Spawn replication loop to process incoming blocks
                    // Track the task handle for graceful shutdown
                    let coordinator_for_replication = coordinator.clone();
                    let replication_config = p2p::sync::ReplicationConfig {
                        continue_on_error: true,
                        rebroadcast_on_merge: false, // Don't re-broadcast during initial sync
                    };
                    let replication_task = tokio::spawn(async move {
                        info!("Starting replication loop for P2P sync");
                        p2p::sync::ReplicationLoop::run(
                            coordinator_for_replication,
                            sync_events,
                            merge_handler,
                            replication_config,
                        )
                        .await;
                        info!("Replication loop stopped");
                    });

                    // Spawn host event handler to process incoming P2P events through coordinator
                    // Track the task handle for graceful shutdown
                    let event_handler_task = if let Some(mut events) = p2p_events.take() {
                        let coordinator_for_events = coordinator.clone();
                        Some(tokio::spawn(async move {
                            while let Some(event) = events.recv().await {
                                // Log events for visibility
                                match &event {
                                    p2p::HostEvent::PeerConnected(peer) => {
                                        info!("Peer connected: {}", peer);
                                    }
                                    p2p::HostEvent::PeerDisconnected(peer) => {
                                        info!("Peer disconnected: {}", peer);
                                    }
                                    p2p::HostEvent::Listening(addr) => {
                                        info!("Now listening on: {}", addr);
                                    }
                                    p2p::HostEvent::GossipMessage {
                                        propagation_source,
                                        topic,
                                        ..
                                    } => {
                                        info!(
                                            "Received gossip message on {} from {}",
                                            topic, propagation_source
                                        );
                                    }
                                    p2p::HostEvent::TwoStreamRequest { peer_id, request } => {
                                        info!(
                                            peer_id = %peer_id,
                                            message_id = %request.metadata.message_id,
                                            doc_id = %request.doc_id,
                                            "Processing TwoStreamRequest through coordinator"
                                        );
                                    }
                                    _ => {}
                                }

                                // Process event through coordinator for response handling
                                if let Err(e) =
                                    coordinator_for_events.handle_host_event(event).await
                                {
                                    error!("Failed to handle host event: {}", e);
                                }
                            }
                        }))
                    } else {
                        None
                    };

                    info!("P2P sync coordinator initialized");
                    (
                        Some(coordinator),
                        Some(replication_task),
                        event_handler_task,
                    )
                } else {
                    (None, None, None)
                };

            // Create mutator - use BroadcastMutator if P2P is enabled for network propagation
            let mutator: Arc<dyn query::mutator::DocMutator> =
                if let Some(ref coordinator) = sync_coordinator {
                    Arc::new(db::BroadcastMutator::new(
                        database.clone(),
                        coordinator.clone(),
                    ))
                } else {
                    Arc::new(db::AutoCommitMutator::new(database.clone()))
                };

            // Create transaction registry for explicit transaction support (Arc-shared)
            let registry = Arc::new(db::DbTransactionRegistry::new(database.clone()));

            // Create collection provider for on-demand schema resolution
            // This ensures newly added schemas are immediately available for queries
            let collection_provider: Arc<dyn query::CollectionProvider> =
                db::DbCollectionProvider::new_arc(database.clone());
            info!(
                "Collection provider configured ({} collection(s) available)",
                database.list_collections().map(|c| c.len()).unwrap_or(0)
            );

            // Create DocumentACP: SourceHub (on-chain) or Local
            let (document_acp, sourcehub_acp_adapter): (
                Arc<dyn acp::DocumentACP>,
                Option<Arc<dyn defra_http::router::AcpOperations>>,
            ) = if config.acp.document_type == AcpDocumentType::SourceHub {
                if config.acp.sourcehub_address.is_empty() {
                    return Err(Error::InvalidConfig(
                        "sourcehub_address required when document_type is source-hub".into(),
                    ));
                }

                let signer_key_bytes = identity_key_bytes.as_ref().ok_or_else(|| {
                    Error::InvalidConfig(
                        "node identity required for SourceHub ACP (use --identity)".into(),
                    )
                })?;

                let client = sourcehub::SourceHubClient::new(
                    config.acp.sourcehub_address.clone(),
                    config.acp.sourcehub_comet_address.clone(),
                );
                let signer = sourcehub::TxSigner::from_secp256k1_bytes(
                    signer_key_bytes,
                    &config.acp.sourcehub_chain_id,
                )
                .map_err(|e| Error::InvalidConfig(format!("SourceHub signer: {}", e)))?;

                let sh_acp = Arc::new(sourcehub::SourceHubDocumentACP::new(client, signer));
                let sh_adapter = crate::sourcehub_acp_adapter::SourceHubAcpAdapter::new_arc(
                    sh_acp.clone(),
                    zanzibar_store.clone(),
                );

                info!("Document ACP configured (SourceHub)");
                (sh_acp as Arc<dyn acp::DocumentACP>, Some(sh_adapter))
            } else {
                info!("Document ACP configured (local)");
                (
                    Arc::new(acp::LocalDocumentACP::new(acp_store)) as Arc<dyn acp::DocumentACP>,
                    None,
                )
            };
            let document_acp_for_block = document_acp.clone();

            // Create query runner with transaction, mutation, and ACP support
            // Use Arc-shared registry so it can also be used by TxnRegistryAdapter
            let document_acp_for_http = document_acp.clone();
            let mut query_runner = query::QueryRunner::with_arc_registry_and_provider(
                fetcher,
                collection_provider,
                registry.clone(),
            )
            .with_mutator(mutator)
            .with_acp(document_acp)
            .with_lens_store(database.lens_store().clone());

            // Wire CRDT delta encryption key (matches FFI behavior)
            if !config.datastore.no_encryption {
                let encryption_key = b"examplekey1234567890examplekey12".to_vec();
                query_runner = query_runner.with_encryption_key(encryption_key);
                info!("CRDT delta encryption enabled");
            }

            // Wire default identity for ACP permission checks (from --identity CLI flag)
            if let Some(did) = user_did {
                info!("Query runner configured with default identity for ACP");
                query_runner = query_runner.with_default_identity(did);
            }

            let runner = Arc::new(query_runner);
            let runner_for_backup: Arc<dyn query::executor::QueryExecutor> = runner.clone();

            // Create REST operations that wrap the query runner
            let rest_ops = query::rest::RestOperationsImpl::new(Arc::clone(&runner));

            // Create HTTP server with REST endpoints enabled
            // Cast the Arc<QueryRunner> to Arc<dyn QueryExecutor> for the server
            let executor: Arc<dyn query::executor::QueryExecutor> = runner;
            let mut server = defra_http::Server::from_arc_with_config(executor, server_config)
                .with_rest(rest_ops);

            // Wire node identity DID for signing config fallback in HTTP handlers
            if let Some(did) = node_identity_did {
                server = server.with_node_identity_did(did);
            }

            // Wire P2P to HTTP server if enabled
            if let Some(ref p2p_handle) = p2p {
                let p2p_adapter = if let Some(ref coordinator) = sync_coordinator {
                    let doc_pusher = crate::p2p_adapter::DbDocPusher::new_arc(database.clone());
                    crate::p2p_adapter::P2PAdapter::with_full_context_arc(
                        p2p_handle.clone(),
                        coordinator.clone(),
                        doc_pusher,
                        event_bus.clone(),
                    )
                } else {
                    crate::p2p_adapter::P2PAdapter::new_arc(p2p_handle.clone())
                };
                server = server.with_p2p_arc(p2p_adapter);
                info!("P2P HTTP endpoints enabled");
            }

            // Wire schema operations to HTTP server
            let schema_adapter = crate::schema_adapter::SchemaAdapter::new_arc(database.clone());
            server = server.with_schema_arc(schema_adapter);
            info!("Schema HTTP endpoint enabled");

            // Wire lens operations to HTTP server (backed by persistent database lens store)
            let lens_adapter = crate::lens_adapter::LensAdapter::new_arc(database.clone());
            server = server.with_lens_arc(lens_adapter);
            info!("Lens HTTP endpoint enabled");

            // Wire NAC (Node Access Control) to HTTP server only when enabled
            if config.acp.node_enable {
                let nac_config = db::NacConfig::new().with_enabled();
                let nac_manager: std::sync::Arc<dyn db::NacManagerApi> =
                    std::sync::Arc::new(db::create_memory_nac_manager(nac_config));
                let nac_adapter = crate::nac_adapter::NacAdapter::new_arc(nac_manager);
                server = server.with_nac_arc(nac_adapter);
                info!("NAC HTTP endpoints enabled");
            } else {
                info!("NAC disabled (use --acp-node-enable to enable)");
            }

            // Wire ACP adapters only when document ACP is enabled
            if config.acp.document_type != AcpDocumentType::None {
                let zanzibar_store_for_doc_acp = zanzibar_store.clone();

                // Use SourceHub adapter for policy CRUD when configured, otherwise local
                let acp_adapter: Arc<dyn defra_http::router::AcpOperations> =
                    if let Some(sh_adapter) = sourcehub_acp_adapter {
                        sh_adapter
                    } else {
                        crate::acp_adapter::AcpAdapter::new_arc(zanzibar_store)
                    };
                server = server.with_acp_arc(acp_adapter);
                info!(
                    "ACP policy HTTP endpoints enabled (type: {})",
                    config.acp.document_type
                );

                // Use the already-created document_acp (SourceHub or local) for doc operations
                let doc_acp_adapter = crate::doc_acp_adapter::DocumentAcpAdapter::new_arc(
                    database.clone(),
                    document_acp_for_http,
                    zanzibar_store_for_doc_acp,
                );
                server = server.with_doc_acp_arc(doc_acp_adapter);
                info!("Document ACP HTTP endpoints enabled");
            } else {
                info!("Document ACP disabled (use --document-acp-type to enable)");
            }

            // Wire view operations to HTTP server
            let view_adapter = crate::view_adapter::ViewAdapter::new_arc(database.clone());
            server = server.with_view_arc(view_adapter);
            info!("View HTTP endpoints enabled");

            // Wire collection management operations to HTTP server
            let collection_mgmt_adapter =
                crate::collection_mgmt_adapter::CollectionManagementAdapter::new_arc(
                    database.clone(),
                );
            server = server.with_collection_mgmt_arc(collection_mgmt_adapter);
            info!("Collection management HTTP endpoints enabled");

            // Wire transaction-scoped operations to HTTP server
            let txn_adapter = crate::txn_adapter::TxnRegistryAdapter::new_arc(registry);
            server = server.with_txn_ops_arc(txn_adapter);
            info!("Transaction-scoped HTTP endpoints enabled");

            // Wire index operations to HTTP server
            let index_adapter = crate::index_adapter::IndexAdapter::new_arc(database.clone());
            server = server.with_index_arc(index_adapter);
            info!("Index HTTP endpoints enabled");

            // Wire encrypted index operations to HTTP server
            let encrypted_index_adapter =
                crate::encrypted_index_adapter::EncryptedIndexAdapter::new_arc(database.clone());
            server = server.with_encrypted_index_arc(encrypted_index_adapter);
            info!("Encrypted index HTTP endpoints enabled");

            // Wire backup operations to HTTP server
            let backup_adapter =
                crate::backup_adapter::BackupAdapter::new_arc(database.clone(), runner_for_backup);
            server = server.with_backup_arc(backup_adapter);
            info!("Backup HTTP endpoints enabled");

            // Wire block operations to HTTP server
            let block_adapter = crate::block_adapter::BlockAdapter::new_arc(
                database.clone(),
                document_acp_for_block,
            );
            server = server.with_block_arc(block_adapter);
            info!("Block HTTP endpoints enabled");

            // Wire event bus to HTTP server for GraphQL subscriptions
            server = server.with_event_bus_arc(event_bus);
            info!("Subscription event bus enabled");

            info!(
                "HTTP server configured on {} with REST endpoints enabled",
                api_address
            );

            // Build P2PTasks if P2P is enabled with all required task handles
            let p2p_tasks = match (p2p_host_task, replication_task) {
                (Some(host_task), Some(replication_task)) => Some(P2PTasks {
                    host_task,
                    replication_task,
                    event_handler_task,
                }),
                _ => None,
            };

            (server, p2p_tasks)
        };

        Ok((p2p, p2p_tasks, http_server))
    }
}
