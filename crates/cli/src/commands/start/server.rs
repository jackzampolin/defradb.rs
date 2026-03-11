//! Store and server initialization

use std::net::SocketAddr;
use std::sync::Arc;

use tracing::{error, info, warn};

use super::node::{Node, P2PTasks};
use crate::config::{AcpDocumentType, Config, TransportType};
use crate::error::{Error, Result};
use identity::Identity;
#[cfg(feature = "iroh")]
use p2p::P2PTransport;

/// Callback to wire DocumentACP into the merge handler after ACP initialization.
type SetMergeAcp = Option<Box<dyn FnOnce(Arc<dyn acp::DocumentACP>)>>;

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
        Option<pg_compat::PgServer>,
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

        // Store identity in global identity store so HTTP handlers can look up
        // signing config from DID (mirrors FFI path in crates/ffi/src/node.rs)
        if let (Some(did), Some(identity)) = (&user_did, &user_identity) {
            defra_core::signing::store_identity(
                did.as_ref(),
                defra_core::signing::SigningConfig {
                    key_type: identity.identity_key_type().to_string(),
                    private_key_bytes: identity.private_key_bytes(),
                    public_key_bytes: identity.public_key_bytes(),
                    public_key_hex: hex::encode(identity.public_key_bytes()),
                    remote_signer: None,
                    signing_authorization: None,
                },
            );
            info!("Stored node identity signing config for DID {}", did);
        }

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
        // Clone peer_keypair before potential move (iroh branch needs it later)
        #[cfg(feature = "iroh")]
        let peer_keypair_for_iroh = peer_keypair.clone();

        // Only create libp2p P2PHost for libp2p transport; iroh creates its own transport
        #[allow(unused_mut)]
        let (p2p, mut p2p_events, mut p2p_host_task) =
            if config.net.p2p_disabled || config.net.transport == TransportType::Iroh {
                (None, None, None)
            } else {
                info!("Initializing P2P network (libp2p)");
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
        let (http_server, p2p_tasks, pg_server) = {
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
                max_body_size: config.api.max_body_size,
                max_schema_size: config.api.max_schema_size,
                max_backup_size: config.api.max_backup_size,
                request_timeout: config.api.request_timeout,
                max_concurrent_requests: config.api.max_concurrent_requests,
            };

            // Create auto-committing fetcher for non-transactional queries
            let fetcher = db::LensedAutoCommitFetcher::new(database.clone());

            // Create sync coordinator if P2P is enabled (shared between mutator and P2P adapter)
            // Also captures task handles for graceful shutdown
            #[allow(unused_mut)]
            let mut iroh_p2p_adapter: Option<
                Arc<dyn defra_http::router::P2POperations>,
            > = None;
            let (
                sync_coordinator,
                mutator,
                replication_task,
                event_handler_task,
                version_syncer,
                doc_pusher,
                failure_recorder_task,
                retry_loop_task,
                restored_doc_ids,
                mut set_merge_handler_acp,
            ) = if config.net.p2p_disabled {
                let mutator: Arc<dyn query::mutator::DocMutator> =
                    Arc::new(db::AutoCommitMutator::new(database.clone()));
                let no_acp: SetMergeAcp = None;
                (
                    None,
                    mutator,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    std::collections::HashSet::new(),
                    no_acp,
                )
            } else if config.net.transport == TransportType::Iroh {
                #[cfg(feature = "iroh")]
                {
                    info!("Initializing P2P network (iroh)");
                    let sync_blockstore = Arc::new(blockstore::DefraBlockstore::new(
                        store_for_sync.clone(),
                        false,
                    ));
                    let merge_blockstore = sync_blockstore.clone();
                    let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
                        Arc::new(p2p::sync::P2PCollectionStore::new(store_for_sync.clone()));

                    // Create iroh secret key from peer keypair seed (first 32 bytes)
                    let iroh_secret_key = if let Some(ref kp) = peer_keypair_for_iroh {
                        let seed = kp.derive_secret(b"iroh-transport").ok_or_else(|| {
                            Error::InvalidConfig("iroh transport requires Ed25519 key".into())
                        })?;
                        iroh_net::SecretKey::from_bytes(&seed)
                    } else {
                        iroh_net::SecretKey::generate(&mut rand::rng())
                    };

                    let iroh_config = p2p::iroh::IrohEndpointConfig {
                        secret_key: iroh_secret_key.clone(),
                        relay_url: config.net.iroh_relay_url.clone(),
                        discovery: config.net.iroh_discovery,
                        bind_port: config.net.iroh_bind_port,
                        bind_addr: config.net.iroh_bind_addr,
                    };
                    let (command_tx, mut iroh_events, iroh_task) =
                        p2p::iroh::spawn_endpoint(iroh_config)
                            .await
                            .map_err(Error::P2P)?;

                    // Store iroh background task for graceful shutdown via P2PTasks
                    p2p_host_task = Some(iroh_task);

                    let iroh_transport = p2p::iroh::IrohTransport::new(command_tx, iroh_secret_key);
                    let iroh_transport_for_adapter = iroh_transport.clone();

                    info!(
                        "Iroh transport initialized, peer ID: {}",
                        iroh_transport.local_peer_id()
                    );

                    let access_mode = if config.acp.document_type != AcpDocumentType::None {
                        p2p::bitswap::AccessMode::Controlled
                    } else {
                        p2p::bitswap::AccessMode::Open
                    };

                    let head_provider: Arc<dyn p2p::sync::DocumentHeadProvider> =
                        Arc::new(db::DbHeadProvider::new(database.clone()));

                    let (mut coordinator, sync_events) =
                        p2p::sync::SyncCoordinator::with_head_provider(
                            iroh_transport,
                            sync_blockstore,
                            p2p::sync::SyncConfig::default(),
                            access_mode,
                            Arc::new(p2p::ReplicatorRegistry::new()),
                            collection_store,
                            head_provider,
                        )
                        .await
                        .map_err(Error::P2P)?;

                    let (failure_tx, failure_rx) =
                        tokio::sync::mpsc::unbounded_channel::<p2p::sync::PushFailure>();
                    coordinator.set_failure_channel(failure_tx);
                    let coordinator = Arc::new(coordinator);

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

                    let merge_blockstore_for_syncer = merge_blockstore.clone();
                    let merge_handler =
                        Arc::new(db::DbMergeHandler::new(database.clone(), merge_blockstore));
                    let merge_handler_for_syncer = merge_handler.clone();
                    let merge_handler_for_acp: Arc<db::DbMergeHandler<_, _>> =
                        merge_handler.clone();

                    // Spawn replication loop
                    let coordinator_for_replication = coordinator.clone();
                    let replication_config = p2p::sync::ReplicationConfig {
                        continue_on_error: true,
                        rebroadcast_on_merge: false,
                        batch_size: 50,
                        max_workers: 32,
                    };
                    let replication_task = tokio::spawn(async move {
                        info!("Starting replication loop for P2P sync (iroh)");
                        p2p::sync::ReplicationLoop::run(
                            coordinator_for_replication,
                            sync_events,
                            merge_handler,
                            replication_config,
                        )
                        .await;
                        info!("Replication loop stopped (iroh)");
                    });

                    // Spawn iroh event handler (reads TransportEvents directly)
                    let coordinator_for_events = coordinator.clone();
                    let event_bus_for_handler = event_bus.clone();
                    let event_handler_task = Some(tokio::spawn(async move {
                        let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
                        while let Some(event) = iroh_events.recv().await {
                            match &event {
                                p2p::TransportEvent::PeerConnected(peer) => {
                                    info!("Peer connected (iroh): {}", peer);
                                }
                                p2p::TransportEvent::PeerDisconnected(peer) => {
                                    info!("Peer disconnected (iroh): {}", peer);
                                }
                                p2p::TransportEvent::Listening(addr) => {
                                    info!("Now listening (iroh): {}", addr);
                                }
                                p2p::TransportEvent::GossipMessage { topic, .. } => {
                                    info!("Received gossip message (iroh) on {}", topic);
                                }
                                p2p::TransportEvent::PeerSubscribed { peer_id, topic } => {
                                    info!("Peer subscribed (iroh): {} on {}", peer_id, topic);
                                    event_bus_for_handler.publish(
                                        events::Message::topic_peer_event(
                                            events::TopicPeerEventData {
                                                peer_id: peer_id.to_string(),
                                                topic: topic.clone(),
                                                event_type: "JOINED".to_string(),
                                            },
                                        ),
                                    );
                                }
                                p2p::TransportEvent::PeerUnsubscribed { peer_id, topic } => {
                                    info!("Peer unsubscribed (iroh): {} on {}", peer_id, topic);
                                    event_bus_for_handler.publish(
                                        events::Message::topic_peer_event(
                                            events::TopicPeerEventData {
                                                peer_id: peer_id.to_string(),
                                                topic: topic.clone(),
                                                event_type: "LEFT".to_string(),
                                            },
                                        ),
                                    );
                                }
                                _ => {}
                            }
                            let permit = semaphore.clone().acquire_owned().await.unwrap();
                            let coord = coordinator_for_events.clone();
                            tokio::spawn(async move {
                                if let Err(e) = coord.handle_transport_event(event).await {
                                    error!("Failed to handle iroh event: {}", e);
                                }
                                drop(permit);
                            });
                        }
                    }));

                    // Create version syncer for schema sync
                    let iroh_version_syncer: Option<
                        Arc<dyn crate::transport_version_syncer::TransportVersionSyncer>,
                    > = Some(
                        crate::transport_version_syncer::DbTransportVersionSyncer::new_arc(
                            merge_blockstore_for_syncer,
                            merge_handler_for_syncer,
                            database.clone(),
                            iroh_transport_for_adapter.clone(),
                        ),
                    );

                    // Create transport doc pusher
                    let iroh_doc_pusher: Arc<dyn crate::transport_doc_pusher::TransportDocPusher> =
                        crate::transport_doc_pusher::DbTransportDocPusher::new_arc(
                            database.clone(),
                            iroh_transport_for_adapter.clone(),
                        );

                    // Clone store for background tasks
                    let store_for_iroh_bg = store_for_sync.clone();

                    // Spawn failure recorder task
                    let recorder_store = store_for_iroh_bg.clone();
                    let failure_recorder_task = tokio::spawn(async move {
                        let mut rx = failure_rx;
                        while let Some(failure) = rx.recv().await {
                            let peerstore = storage::stores::Peerstore::new(recorder_store.clone());
                            let retry_info = storage::stores::RetryInfo::new_initial();
                            let info_bytes = match retry_info.to_bytes() {
                                Ok(b) => b,
                                Err(e) => {
                                    warn!(error = %e, "Failed to serialize RetryInfo");
                                    continue;
                                }
                            };
                            if let Err(e) = peerstore
                                .record_push_failure(
                                    &failure.peer_id,
                                    &failure.doc_id,
                                    &failure.collection_id,
                                    &info_bytes,
                                )
                                .await
                            {
                                warn!(error = %e, "Failed to record push failure");
                            }
                        }
                    });

                    // Spawn retry loop task
                    let retry_store = store_for_iroh_bg.clone();
                    let retry_pusher = iroh_doc_pusher.clone();
                    let retry_transport = iroh_transport_for_adapter.clone();
                    let retry_loop_task = tokio::spawn(async move {
                        loop {
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                            let peerstore = storage::stores::Peerstore::new(retry_store.clone());
                            let peers = match peerstore.get_all_retry_peers().await {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            for (peer_id_str, info_bytes) in peers {
                                let mut retry_info =
                                    match storage::stores::RetryInfo::from_bytes(&info_bytes) {
                                        Ok(i) => i,
                                        Err(_) => continue,
                                    };
                                if !retry_info.is_due() {
                                    continue;
                                }
                                let peer_id = p2p::transport::PeerId::new(peer_id_str.clone());
                                let connected =
                                    retry_transport.connected_peers().await.unwrap_or_default();
                                if !connected.iter().any(|p| p.as_str() == peer_id.as_str()) {
                                    continue;
                                }
                                let docs = match peerstore.get_retry_doc_ids(&peer_id_str).await {
                                    Ok(d) => d,
                                    Err(_) => continue,
                                };
                                if docs.is_empty() {
                                    let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                                    continue;
                                }
                                let mut all_succeeded = true;
                                for (doc_id, collection_id) in &docs {
                                    match retry_pusher
                                        .retry_doc(&peer_id, doc_id, collection_id)
                                        .await
                                    {
                                        Ok(()) => {
                                            let _ = peerstore
                                                .remove_retry_doc(&peer_id_str, doc_id)
                                                .await;
                                        }
                                        Err(_) => {
                                            all_succeeded = false;
                                        }
                                    }
                                }
                                if all_succeeded {
                                    let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                                } else {
                                    retry_info.bump();
                                    if let Ok(bytes) = retry_info.to_bytes() {
                                        let _ =
                                            peerstore.update_retry_info(&peer_id_str, &bytes).await;
                                    }
                                }
                            }
                        }
                    });

                    // Restore replicators from peerstore
                    let restore_peerstore =
                        storage::stores::Peerstore::new(store_for_iroh_bg.clone());
                    match restore_peerstore.list_replicators().await {
                        Ok(entries) => {
                            for (_peer_id_str, data) in entries {
                                if let Ok(rep_info) = p2p::ReplicatorInfo::from_bytes(&data) {
                                    let pid = p2p::transport::PeerId::new(
                                        rep_info.peer_id_str().to_string(),
                                    );
                                    let _ = coordinator
                                        .create_replicator(
                                            &pid,
                                            rep_info.collections.clone(),
                                            false,
                                        )
                                        .await;
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to load replicators from storage");
                        }
                    }

                    // Restore document subscriptions from peerstore
                    let mut restored_doc_ids = std::collections::HashSet::new();
                    if let Ok(doc_ids) = restore_peerstore.load_documents().await {
                        for doc_id in &doc_ids {
                            let _ = iroh_transport_for_adapter
                                .subscribe(p2p::topics::DefraTopic::document(doc_id))
                                .await;
                            restored_doc_ids.insert(doc_id.clone());
                        }
                    }

                    // Create mutator with iroh coordinator
                    let mutator: Arc<dyn query::mutator::DocMutator> = Arc::new(
                        db::BroadcastMutator::new(database.clone(), coordinator.clone()),
                    );

                    // Create IrohP2PAdapter for HTTP endpoints
                    let adapter = crate::iroh_p2p_adapter::IrohP2PAdapter::with_full_context(
                        iroh_transport_for_adapter,
                        coordinator.clone(),
                        iroh_doc_pusher,
                        event_bus.clone(),
                        iroh_version_syncer,
                    );
                    adapter.set_initial_tracked_documents(restored_doc_ids.clone());
                    iroh_p2p_adapter =
                        Some(Arc::new(adapter) as Arc<dyn defra_http::router::P2POperations>);

                    info!("P2P sync coordinator initialized (iroh)");
                    let set_acp: SetMergeAcp = Some(Box::new(move |acp| {
                        merge_handler_for_acp.set_document_acp(acp);
                    }));
                    (
                        None,
                        mutator,
                        Some(replication_task),
                        event_handler_task,
                        None,
                        None,
                        Some(failure_recorder_task),
                        Some(retry_loop_task),
                        restored_doc_ids,
                        set_acp,
                    )
                }
                #[cfg(not(feature = "iroh"))]
                {
                    return Err(Error::InvalidTransport(
                        "iroh transport not enabled. Rebuild with --features iroh".into(),
                    ));
                }
            } else if let Some(ref p2p_handle) = p2p {
                let sync_blockstore = Arc::new(blockstore::DefraBlockstore::new(
                    store_for_sync.clone(),
                    false,
                ));

                // Clone blockstore for merge handler (before moving into coordinator)
                let merge_blockstore = sync_blockstore.clone();

                // Clone store for background tasks before it's consumed
                let store_for_background = store_for_sync.clone();

                // Create persistent collection store for P2P subscriptions
                let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
                    Arc::new(p2p::sync::P2PCollectionStore::new(store_for_sync));

                let access_mode = if config.acp.document_type != AcpDocumentType::None {
                    p2p::bitswap::AccessMode::Controlled
                } else {
                    p2p::bitswap::AccessMode::Open
                };

                let libp2p_transport = p2p::Libp2pTransport::new(p2p_handle.clone());

                let (mut coordinator, sync_events) =
                    p2p::sync::SyncCoordinator::with_collection_store(
                        libp2p_transport,
                        sync_blockstore,
                        p2p::sync::SyncConfig::default(),
                        access_mode,
                        collection_store,
                    )
                    .await
                    .map_err(Error::P2P)?;

                // Set up failure channel for push failure recording
                let (failure_tx, failure_rx) =
                    tokio::sync::mpsc::unbounded_channel::<p2p::sync::PushFailure>();
                coordinator.set_failure_channel(failure_tx);

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
                let merge_blockstore_for_syncer = merge_blockstore.clone();
                let merge_handler =
                    Arc::new(db::DbMergeHandler::new(database.clone(), merge_blockstore));

                // Clone merge handler for VersionSyncer and ACP wiring
                // (before moving into replication loop)
                let merge_handler_for_syncer = merge_handler.clone();
                let merge_handler_for_acp: Arc<db::DbMergeHandler<_, _>> = merge_handler.clone();

                // Spawn replication loop to process incoming blocks
                // Track the task handle for graceful shutdown
                let coordinator_for_replication = coordinator.clone();
                let replication_config = p2p::sync::ReplicationConfig {
                    continue_on_error: true,
                    rebroadcast_on_merge: false, // Don't re-broadcast during initial sync
                    batch_size: 50,
                    max_workers: 32,
                };
                let replication_task = tokio::spawn(async move {
                    info!("Starting parallel replication loop for P2P sync");
                    p2p::sync::ReplicationLoop::run_parallel(
                        coordinator_for_replication,
                        sync_events,
                        merge_handler,
                        replication_config,
                        |result| {
                            match &result {
                                p2p::sync::ReplicationResult::Merged { cid, doc_id, .. } => {
                                    info!(cid = %cid, doc_id = %doc_id, "Block merged successfully");
                                }
                                p2p::sync::ReplicationResult::MergedButBroadcastFailed {
                                    cid, doc_id, broadcast_error, ..
                                } => {
                                    error!(
                                        cid = %cid, doc_id = %doc_id, error = %broadcast_error,
                                        "Block merged but re-broadcast failed"
                                    );
                                }
                                p2p::sync::ReplicationResult::Failed { cid, error } => {
                                    error!(cid = %cid, error = %error, "Block merge failed");
                                }
                                p2p::sync::ReplicationResult::Skipped { cid, reason } => {
                                    tracing::debug!(cid = %cid, reason = %reason, "Block skipped");
                                }
                                p2p::sync::ReplicationResult::MergedButNotMarked { cid, error } => {
                                    error!(cid = %cid, error = %error, "Block merged but failed to mark");
                                }
                                _ => {}
                            }
                        },
                    )
                    .await;
                    info!("Replication loop stopped");
                });

                // Spawn host event handler to process incoming P2P events through coordinator
                // Track the task handle for graceful shutdown
                let event_handler_task = if let Some(mut events) = p2p_events.take() {
                    let coordinator_for_events = coordinator.clone();
                    Some(tokio::spawn(async move {
                        let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
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

                            // Spawn concurrent processing
                            let permit = semaphore.clone().acquire_owned().await.unwrap();
                            let coord = coordinator_for_events.clone();
                            tokio::spawn(async move {
                                let transport_event = p2p::convert_host_event(event);
                                if let Err(e) = coord.handle_transport_event(transport_event).await
                                {
                                    error!("Failed to handle host event: {}", e);
                                }
                                drop(permit);
                            });
                        }
                    }))
                } else {
                    None
                };

                // Create version syncer for schema sync via Bitswap
                let version_syncer: Option<Arc<dyn crate::p2p_adapter::VersionSyncer>> =
                    Some(crate::version_syncer::DbVersionSyncer::new_arc(
                        merge_blockstore_for_syncer,
                        merge_handler_for_syncer,
                        database.clone(),
                    ));

                // Create doc pusher for retry loop and P2PAdapter
                let doc_pusher = crate::p2p_adapter::DbDocPusher::new_arc(database.clone());

                // Spawn failure recorder task
                let recorder_store = store_for_background.clone();
                let failure_recorder_task = tokio::spawn(async move {
                    let mut rx = failure_rx;
                    while let Some(failure) = rx.recv().await {
                        let peerstore = storage::stores::Peerstore::new(recorder_store.clone());
                        let retry_info = storage::stores::RetryInfo::new_initial();
                        let info_bytes = match retry_info.to_bytes() {
                            Ok(b) => b,
                            Err(e) => {
                                warn!(error = %e, "Failed to serialize RetryInfo");
                                continue;
                            }
                        };
                        if let Err(e) = peerstore
                            .record_push_failure(
                                &failure.peer_id.to_string(),
                                &failure.doc_id,
                                &failure.collection_id,
                                &info_bytes,
                            )
                            .await
                        {
                            warn!(error = %e, "Failed to record push failure");
                        }
                    }
                });

                // Spawn retry loop task
                let retry_store = store_for_background.clone();
                let retry_handle = p2p_handle.clone();
                let retry_pusher = doc_pusher.clone();
                let retry_loop_task = tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        let peerstore = storage::stores::Peerstore::new(retry_store.clone());
                        let peers = match peerstore.get_all_retry_peers().await {
                            Ok(p) => p,
                            Err(_) => continue,
                        };
                        for (peer_id_str, info_bytes) in peers {
                            let mut retry_info =
                                match storage::stores::RetryInfo::from_bytes(&info_bytes) {
                                    Ok(i) => i,
                                    Err(_) => continue,
                                };
                            if !retry_info.is_due() {
                                continue;
                            }
                            let peer_id = match peer_id_str.parse::<libp2p::PeerId>() {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            let connected =
                                retry_handle.connected_peers().await.unwrap_or_default();
                            if !connected.contains(&peer_id) {
                                continue;
                            }
                            let docs = match peerstore.get_retry_doc_ids(&peer_id_str).await {
                                Ok(d) => d,
                                Err(_) => continue,
                            };
                            if docs.is_empty() {
                                let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                                continue;
                            }
                            let mut all_succeeded = true;
                            for (doc_id, collection_id) in &docs {
                                match retry_pusher
                                    .retry_doc(&retry_handle, peer_id, doc_id, collection_id)
                                    .await
                                {
                                    Ok(()) => {
                                        let _ =
                                            peerstore.remove_retry_doc(&peer_id_str, doc_id).await;
                                    }
                                    Err(_) => {
                                        all_succeeded = false;
                                    }
                                }
                            }
                            if all_succeeded {
                                let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                            } else {
                                retry_info.bump();
                                if let Ok(bytes) = retry_info.to_bytes() {
                                    let _ = peerstore.update_retry_info(&peer_id_str, &bytes).await;
                                }
                            }
                        }
                    }
                });

                // Restore replicators from peerstore
                let restore_peerstore =
                    storage::stores::Peerstore::new(store_for_background.clone());
                match restore_peerstore.list_replicators().await {
                    Ok(entries) => {
                        for (_peer_id_str, data) in entries {
                            if let Ok(rep_info) = p2p::ReplicatorInfo::from_bytes(&data) {
                                if let Some(pid) = rep_info.peer_id() {
                                    let _ = p2p_handle
                                        .create_replicator(pid, rep_info.collections.clone())
                                        .await;
                                    for cid in &rep_info.collections {
                                        let _ = p2p_handle
                                            .subscribe(p2p::topics::DefraTopic::collection(cid))
                                            .await;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to load replicators from storage");
                    }
                }

                // Restore document subscriptions from peerstore
                let mut restored_doc_ids = std::collections::HashSet::new();
                if let Ok(doc_ids) = restore_peerstore.load_documents().await {
                    for doc_id in &doc_ids {
                        let _ = p2p_handle
                            .subscribe(p2p::topics::DefraTopic::document(doc_id))
                            .await;
                        restored_doc_ids.insert(doc_id.clone());
                    }
                }

                // Create mutator with libp2p coordinator
                let mutator: Arc<dyn query::mutator::DocMutator> = Arc::new(
                    db::BroadcastMutator::new(database.clone(), coordinator.clone()),
                );

                info!("P2P sync coordinator initialized");
                let set_acp: SetMergeAcp = Some(Box::new(move |acp| {
                    merge_handler_for_acp.set_document_acp(acp);
                }));
                (
                    Some(coordinator),
                    mutator,
                    Some(replication_task),
                    event_handler_task,
                    version_syncer,
                    Some(doc_pusher),
                    Some(failure_recorder_task),
                    Some(retry_loop_task),
                    restored_doc_ids,
                    set_acp,
                )
            } else {
                let mutator: Arc<dyn query::mutator::DocMutator> =
                    Arc::new(db::AutoCommitMutator::new(database.clone()));
                let no_acp: SetMergeAcp = None;
                (
                    None,
                    mutator,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    std::collections::HashSet::new(),
                    no_acp,
                )
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

            // Create DocumentACP: SourceHub, hub.rs, or Local
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

                let tuning = sourcehub::AcpTuning {
                    request_timeout: std::time::Duration::from_secs(config.acp.request_timeout),
                    circuit_breaker_threshold: config.acp.circuit_breaker_threshold,
                    circuit_breaker_reset_timeout: std::time::Duration::from_secs(
                        config.acp.circuit_breaker_reset_timeout,
                    ),
                    cache_ttl: std::time::Duration::from_secs(config.acp.cache_ttl),
                    receipt_timeout: std::time::Duration::from_secs(config.acp.receipt_timeout),
                };

                info!(
                    request_timeout_s = config.acp.request_timeout,
                    circuit_breaker_threshold = config.acp.circuit_breaker_threshold,
                    circuit_breaker_reset_timeout_s = config.acp.circuit_breaker_reset_timeout,
                    cache_ttl_s = config.acp.cache_ttl,
                    receipt_timeout_s = config.acp.receipt_timeout,
                    "Resolved ACP tuning (SourceHub)"
                );

                let provider = Arc::new(
                    sourcehub::CosmosProvider::new(
                        config.acp.sourcehub_address.clone(),
                        config.acp.sourcehub_comet_address.clone(),
                        signer_key_bytes,
                        &config.acp.sourcehub_chain_id,
                        &tuning,
                    )
                    .map_err(|e| Error::InvalidConfig(format!("SourceHub provider: {}", e)))?,
                );

                let sh_acp = Arc::new(sourcehub::SourceHubDocumentACP::new(
                    provider,
                    tuning.cache_ttl,
                ));
                let sh_adapter = crate::sourcehub_acp_adapter::SourceHubAcpAdapter::new_arc(
                    sh_acp.clone(),
                    zanzibar_store.clone(),
                );

                info!("Document ACP configured (SourceHub)");
                (sh_acp as Arc<dyn acp::DocumentACP>, Some(sh_adapter))
            } else if config.acp.document_type == AcpDocumentType::HubRs {
                if config.acp.hub_rs_address.is_empty() {
                    return Err(Error::InvalidConfig(
                        "hub_rs_address required when document_type is hub-rs".into(),
                    ));
                }

                let signer_key_bytes = identity_key_bytes.as_ref().ok_or_else(|| {
                    Error::InvalidConfig(
                        "node identity required for hub.rs ACP (use --identity)".into(),
                    )
                })?;

                let tuning = sourcehub::AcpTuning {
                    request_timeout: std::time::Duration::from_secs(config.acp.request_timeout),
                    circuit_breaker_threshold: config.acp.circuit_breaker_threshold,
                    circuit_breaker_reset_timeout: std::time::Duration::from_secs(
                        config.acp.circuit_breaker_reset_timeout,
                    ),
                    cache_ttl: std::time::Duration::from_secs(config.acp.cache_ttl),
                    receipt_timeout: std::time::Duration::from_secs(config.acp.receipt_timeout),
                };

                info!(
                    request_timeout_s = config.acp.request_timeout,
                    circuit_breaker_threshold = config.acp.circuit_breaker_threshold,
                    circuit_breaker_reset_timeout_s = config.acp.circuit_breaker_reset_timeout,
                    receipt_timeout_s = config.acp.receipt_timeout,
                    "Resolved ACP tuning (hub.rs; access decision cache disabled)"
                );

                let provider = Arc::new(
                    sourcehub::HubRsProvider::new(
                        config.acp.hub_rs_address.clone(),
                        signer_key_bytes,
                        &tuning,
                        Some(event_bus.clone()),
                    )
                    .await
                    .map_err(|e| Error::InvalidConfig(format!("hub.rs provider: {}", e)))?,
                );

                let sh_acp = Arc::new(sourcehub::SourceHubDocumentACP::without_access_cache(
                    provider,
                ));
                let sh_adapter = crate::sourcehub_acp_adapter::SourceHubAcpAdapter::new_arc(
                    sh_acp.clone(),
                    zanzibar_store.clone(),
                );

                info!("Document ACP configured (hub.rs)");
                (sh_acp as Arc<dyn acp::DocumentACP>, Some(sh_adapter))
            } else {
                info!("Document ACP configured (local)");
                (
                    Arc::new(acp::LocalDocumentACP::new(acp_store)) as Arc<dyn acp::DocumentACP>,
                    None,
                )
            };
            let document_acp_for_block = document_acp.clone();

            // Wire DocumentACP into the P2P merge handler so it can register
            // replicated documents with the original owner DID (merge-denial).
            if let Some(set_acp) = set_merge_handler_acp.take() {
                set_acp(document_acp.clone());
            }

            // Clone collection provider for PG server before it's moved into QueryRunner
            let collection_provider_for_pg = collection_provider.clone();

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
            .with_lens_store(database.lens_store().clone())
            .with_query_timeout(config.api.query_timeout);

            // Wire CRDT delta encryption key (matches FFI behavior)
            if !config.datastore.no_encryption {
                let encryption_key = b"examplekey1234567890examplekey12".to_vec();
                query_runner = query_runner.with_encryption_key(encryption_key);
                info!("CRDT delta encryption enabled");
            }

            // Create NAC adapter early so it can serve both QueryRunner and HTTP server.
            // Must be before user_did is consumed by with_default_identity().
            let nac_adapter: Option<Arc<crate::nac_adapter::NacAdapter>> = if config.acp.node_enable
            {
                let nac_config = db::NacConfig::new().with_enabled();
                let nac_manager: Arc<dyn db::NacManagerApi> =
                    Arc::new(db::create_memory_nac_manager(nac_config));
                // Initialize NAC: transitions from NotConfigured to Enabled with owner
                nac_manager
                    .initialize(user_did.as_ref())
                    .await
                    .map_err(|e| {
                        Error::InvalidConfig(format!("failed to initialize NAC: {}", e))
                    })?;
                Some(Arc::new(crate::nac_adapter::NacAdapter::new(nac_manager)))
            } else {
                None
            };

            // Wire default identity for ACP permission checks (from --identity CLI flag).
            // Skip for SourceHub ACP: identity must come from bearer tokens, not defaults.
            // Anonymous requests should be truly anonymous for on-chain policy evaluation.
            if let Some(ref did) = user_did {
                if config.acp.document_type != AcpDocumentType::SourceHub
                    && config.acp.document_type != AcpDocumentType::HubRs
                {
                    info!("Query runner configured with default identity for ACP");
                    query_runner = query_runner.with_default_identity(did.clone());
                }
            }

            // Wire NAC into QueryRunner for query-level enforcement
            if let Some(ref adapter) = nac_adapter {
                query_runner = query_runner.with_nac(adapter.clone() as Arc<dyn query::NacChecker>);
            }

            let runner = Arc::new(query_runner);
            let runner_for_backup: Arc<dyn query::executor::QueryExecutor> = runner.clone();
            let runner_for_pg: Arc<dyn query::executor::QueryExecutor> = runner.clone();

            // Create REST operations that wrap the query runner
            let rest_ops = query::rest::RestOperationsImpl::new(Arc::clone(&runner));

            // Create HTTP server with REST endpoints enabled
            // Cast the Arc<QueryRunner> to Arc<dyn QueryExecutor> for the server
            let executor: Arc<dyn query::executor::QueryExecutor> = runner;
            let mut server = defra_http::Server::from_arc_with_config(executor, server_config)
                .with_rest(rest_ops)
                .with_dev_mode(config.development);

            // Wire signing identity DID for anonymous-request fallback in HTTP handlers.
            // Prefer user_did (from --identity) because store_identity() stores the
            // signing config under that DID. Fall back to node_identity_did (P2P peer
            // key) which shares a DID only when no explicit --identity is given.
            let signing_did = user_did
                .as_ref()
                .map(|d| d.to_string())
                .or(node_identity_did);
            if let Some(did) = signing_did {
                server = server.with_node_identity_did(did);
            }

            // Wire P2P to HTTP server if enabled
            if let Some(ref p2p_handle) = p2p {
                let p2p_adapter: Arc<dyn defra_http::router::P2POperations> =
                    if let Some(ref coordinator) = sync_coordinator {
                        let pusher = doc_pusher.clone().unwrap_or_else(|| {
                            crate::p2p_adapter::DbDocPusher::new_arc(database.clone())
                        });
                        let adapter = crate::p2p_adapter::P2PAdapter::with_full_context(
                            p2p_handle.clone(),
                            coordinator.clone(),
                            pusher,
                            event_bus.clone(),
                            version_syncer,
                        );
                        adapter.set_initial_tracked_documents(restored_doc_ids);
                        Arc::new(adapter)
                    } else {
                        crate::p2p_adapter::P2PAdapter::new_arc(p2p_handle.clone())
                    };
                server = server.with_p2p_arc(p2p_adapter);
                info!("P2P HTTP endpoints enabled");
            } else if let Some(adapter) = iroh_p2p_adapter {
                server = server.with_p2p_arc(adapter);
                info!("P2P HTTP endpoints enabled (iroh)");
            }

            // Wire schema operations to HTTP server
            let schema_adapter = crate::schema_adapter::SchemaAdapter::new_arc(database.clone());
            server = server.with_schema_arc(schema_adapter);
            info!("Schema HTTP endpoint enabled");

            // Wire lens operations to HTTP server (backed by persistent database lens store)
            let lens_adapter = crate::lens_adapter::LensAdapter::new_arc(database.clone());
            server = server.with_lens_arc(lens_adapter);
            info!("Lens HTTP endpoint enabled");

            // Wire NAC (Node Access Control) to HTTP server (adapter already created above)
            if let Some(ref adapter) = nac_adapter {
                server =
                    server.with_nac_arc(
                        adapter.clone() as Arc<dyn defra_http::router::NodeAcpOperations>
                    );
                info!("NAC HTTP endpoints enabled");
            } else {
                info!("NAC disabled (use --node-acp-enable to enable)");
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

            // Wire dump operations to HTTP server
            let dump_adapter = crate::dump_adapter::DumpAdapter::new_arc(database.clone());
            server = server.with_dump_arc(dump_adapter);
            info!("Dump HTTP endpoint enabled");

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
            let p2p_tasks = match (
                p2p_host_task,
                replication_task,
                failure_recorder_task,
                retry_loop_task,
            ) {
                (
                    Some(host_task),
                    Some(replication_task),
                    Some(failure_recorder_task),
                    Some(retry_loop_task),
                ) => Some(P2PTasks {
                    host_task,
                    replication_task,
                    event_handler_task,
                    failure_recorder_task,
                    retry_loop_task,
                }),
                _ => None,
            };

            // Create PG wire protocol server if configured
            let pg_server = if !config.api.pg_address.is_empty() {
                let pg_addr: SocketAddr =
                    config
                        .api
                        .pg_address
                        .parse()
                        .map_err(|e: std::net::AddrParseError| {
                            Error::InvalidApiAddress(config.api.pg_address.clone(), e.to_string())
                        })?;
                let pg_executor: std::sync::Arc<dyn query::executor::QueryExecutor> =
                    runner_for_pg.clone();
                let pg_collections: std::sync::Arc<dyn query::CollectionProvider> =
                    collection_provider_for_pg.clone();
                let pg_schema_manager =
                    crate::schema_adapter::SchemaAdapter::new_pg_arc(database.clone());
                Some(pg_compat::PgServer::new(
                    pg_addr,
                    pg_executor,
                    pg_collections,
                    Some(pg_schema_manager),
                ))
            } else {
                None
            };

            (server, p2p_tasks, pg_server)
        };

        Ok((p2p, p2p_tasks, http_server, pg_server))
    }
}
