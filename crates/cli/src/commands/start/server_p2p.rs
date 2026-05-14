//! P2P initialization helpers for Node startup.

use std::collections::HashSet;
use std::sync::Arc;

use tracing::{error, info, warn};

use super::node::{Node, P2PTasks};
use crate::config::{AcpDocumentType, Config, TransportType};
use crate::error::{Error, Result};
#[cfg(feature = "iroh")]
use p2p::P2PTransport;

type WireDocumentAcp = Option<Box<dyn FnOnce(Arc<dyn acp::DocumentACP>)>>;

async fn set_persisted_replicator_status<S: storage::corekv::Store>(
    peerstore: &storage::stores::Peerstore<S>,
    peer_id: &str,
    status: p2p::ReplicatorStatus,
) -> Result<bool> {
    let Some(bytes) = peerstore
        .get_replicator(peer_id)
        .await
        .map_err(|e| Error::Server(format!("failed to load replicator: {e}")))?
    else {
        return Ok(false);
    };

    let mut info = p2p::ReplicatorInfo::from_bytes(&bytes)
        .map_err(|e| Error::Server(format!("failed to decode replicator: {e}")))?;
    if !info.set_status_if_changed_now(status) {
        return Ok(false);
    }

    let bytes = info
        .to_bytes()
        .map_err(|e| Error::Server(format!("failed to encode replicator: {e}")))?;
    peerstore
        .create_replicator(peer_id, &bytes)
        .await
        .map_err(|e| Error::Server(format!("failed to persist replicator: {e}")))?;
    Ok(true)
}

pub(super) struct P2PSetup {
    pub(super) host_handle: Option<p2p::P2PHostHandle>,
    pub(super) p2p_tasks: Option<P2PTasks>,
    pub(super) mutator: Arc<dyn query::mutator::DocMutator>,
    pub(super) http_adapter: Option<Arc<dyn defra_http::router::P2POperations>>,
    pub(super) wire_merge_acp: WireDocumentAcp,
    pub(super) wire_doc_pusher_acp: WireDocumentAcp,
}

impl Node {
    pub(super) async fn setup_p2p<S>(
        store: Arc<S>,
        database: Arc<db::DB<S>>,
        event_bus: Arc<dyn events::Bus>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
    ) -> Result<P2PSetup>
    where
        S: storage::corekv::Store + 'static,
    {
        if config.net.p2p_disabled {
            return Ok(Self::p2p_disabled(database));
        }

        if config.net.transport == TransportType::Iroh {
            #[cfg(feature = "iroh")]
            {
                return Self::setup_iroh_p2p(store, database, event_bus, config, peer_keypair)
                    .await;
            }
            #[cfg(not(feature = "iroh"))]
            {
                let _ = (store, database, event_bus, peer_keypair);
                return Err(Error::InvalidTransport(
                    "iroh transport not enabled. Rebuild with --features iroh".into(),
                ));
            }
        }

        Self::setup_libp2p_p2p(store, database, event_bus, config, peer_keypair).await
    }

    fn p2p_disabled<S>(database: Arc<db::DB<S>>) -> P2PSetup
    where
        S: storage::corekv::Store + 'static,
    {
        P2PSetup {
            host_handle: None,
            p2p_tasks: None,
            mutator: Arc::new(db::AutoCommitMutator::new(database)),
            http_adapter: None,
            wire_merge_acp: None,
            wire_doc_pusher_acp: None,
        }
    }

    async fn setup_libp2p_p2p<S>(
        store: Arc<S>,
        database: Arc<db::DB<S>>,
        event_bus: Arc<dyn events::Bus>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
    ) -> Result<P2PSetup>
    where
        S: storage::corekv::Store + 'static,
    {
        info!("Initializing P2P network (libp2p)");

        let blockstore = Arc::new(blockstore::DefraBlockstore::new(store.clone(), true));
        let bitswap_store = p2p::BitswapStoreAdapter::new(blockstore);
        let (handle, mut events, replicator_registry, host_task) = Self::start_p2p(
            config,
            bitswap_store,
            peer_keypair,
            config.net.pubsub_enabled,
        )
        .await?;

        let sync_blockstore = Arc::new(blockstore::DefraBlockstore::new(store.clone(), true));
        let merge_blockstore = sync_blockstore.clone();
        let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
            Arc::new(p2p::sync::P2PCollectionStore::new(store.clone()));
        let head_provider: Arc<dyn p2p::sync::DocumentHeadProvider> =
            Arc::new(db_merge::create_head_provider(database.clone()));

        let (mut coordinator, sync_events) = p2p::sync::SyncCoordinator::with_head_provider(
            p2p::Libp2pTransport::new(handle.clone()),
            sync_blockstore,
            Self::sync_config(config),
            Self::access_mode(config),
            replicator_registry,
            collection_store,
            head_provider,
        )
        .await
        .map_err(Error::P2P)?;

        let failure_rx = db_merge::attach_failure_channel(&mut coordinator, 1024);
        let coordinator = Arc::new(coordinator);
        let coordinator_for_acp = coordinator.clone();

        match db_merge::load_persisted_collections(&coordinator).await {
            Ok(count) => {
                if count > 0 {
                    info!("Loaded {} persisted P2P collection subscription(s)", count);
                }
            }
            Err(e) => {
                warn!("Failed to load persisted P2P collections: {}", e);
            }
        }

        // Start pubsub_rpc doc-sync / sync-branchable services (#828) so
        // this node can interoperate with Go DefraDB peers over gossipsub.
        if let Err(e) = coordinator.start_pubsub_services().await {
            warn!("Failed to start pubsub_rpc services: {}", e);
        }

        let merge_blockstore_for_syncer = merge_blockstore.clone();
        let replication = db_merge::create_replication_stack(
            database.clone(),
            merge_blockstore,
            coordinator.clone(),
        );
        let merge_handler_for_loop = replication.merge_handler.clone();
        let merge_handler_inner_for_syncer = replication.merge_handler_inner.clone();
        let broadcast_mutator = replication.broadcast_mutator.clone();
        let merge_handler_for_acp = replication.merge_handler.clone();

        let coordinator_for_replication = coordinator.clone();
        let replication_task = tokio::spawn(async move {
            info!("Starting parallel replication loop for P2P sync");
            p2p::sync::ReplicationLoop::run_parallel(
                coordinator_for_replication,
                sync_events,
                merge_handler_for_loop,
                p2p::sync::ReplicationConfig {
                    continue_on_error: true,
                    rebroadcast_on_merge: false,
                    batch_size: 50,
                    max_workers: 32,
                },
                |result| match &result {
                    p2p::sync::ReplicationResult::Merged {
                        cid,
                        doc_id,
                        collection_id,
                    } => {
                        info!(
                            cid = %cid,
                            doc_id = %doc_id,
                            collection_id = %collection_id,
                            "Block merged successfully"
                        );
                    }
                    p2p::sync::ReplicationResult::MergedButBroadcastFailed {
                        cid,
                        doc_id,
                        broadcast_error,
                        ..
                    } => {
                        error!(
                            cid = %cid,
                            doc_id = %doc_id,
                            error = %broadcast_error,
                            "Block merged but re-broadcast failed"
                        );
                    }
                    p2p::sync::ReplicationResult::Failed { cid, error } => {
                        error!(cid = %cid, error = %error, "Block merge failed");
                    }
                    p2p::sync::ReplicationResult::Skipped { cid, reason, .. } => {
                        tracing::debug!(cid = %cid, reason = %reason, "Block skipped");
                    }
                    p2p::sync::ReplicationResult::MergedButNotMarked { cid, error } => {
                        error!(cid = %cid, error = %error, "Block merged but failed to mark");
                    }
                    _ => {}
                },
            )
            .await;
            info!("Replication loop stopped");
        });

        let coordinator_for_events = coordinator.clone();
        let event_handler_task = Some(tokio::spawn(async move {
            let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
            while let Some(event) = events.recv().await {
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
                    p2p::HostEvent::TwoStreamRequest {
                        peer_id, request, ..
                    } => {
                        info!(
                            peer_id = %peer_id,
                            message_id = %request.message_id,
                            doc_id = %request.doc_id,
                            "Processing TwoStreamRequest through coordinator"
                        );
                    }
                    _ => {}
                }

                let transport_event = p2p::convert_host_event(event);
                if transport_event.requires_inline_ordering() {
                    if let Err(e) = coordinator_for_events
                        .handle_transport_event(transport_event)
                        .await
                    {
                        error!("Failed to handle host event: {}", e);
                    }
                    continue;
                }

                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let coord = coordinator_for_events.clone();
                tokio::spawn(async move {
                    if let Err(e) = coord.handle_transport_event(transport_event).await {
                        error!("Failed to handle host event: {}", e);
                    }
                    drop(permit);
                });
            }
        }));

        let version_syncer: Arc<dyn crate::p2p_adapter::VersionSyncer> =
            crate::version_syncer::DbVersionSyncer::new_arc(
                merge_blockstore_for_syncer,
                merge_handler_inner_for_syncer,
                database.clone(),
            );

        let doc_pusher_impl = Arc::new(crate::p2p_adapter::DbDocPusher::new(database.clone()));
        let doc_pusher_for_acp = doc_pusher_impl.clone();
        let doc_pusher: Arc<dyn crate::p2p_adapter::DocPusher> = doc_pusher_impl;

        let recorder_store = store.clone();
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
                    continue;
                }
                if let Err(e) = set_persisted_replicator_status(
                    &peerstore,
                    &failure.peer_id.to_string(),
                    p2p::ReplicatorStatus::Inactive,
                )
                .await
                {
                    warn!(error = %e, "Failed to mark replicator inactive");
                }
            }
        });

        let retry_store = store.clone();
        let retry_handle = handle.clone();
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
                    let mut retry_info = match storage::stores::RetryInfo::from_bytes(&info_bytes) {
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
                    let connected = retry_handle.connected_peers().await.unwrap_or_default();
                    if !connected.contains(&peer_id) {
                        continue;
                    }
                    let docs = match peerstore.get_retry_doc_ids(&peer_id_str).await {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    if docs.is_empty() {
                        let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                        let _ = set_persisted_replicator_status(
                            &peerstore,
                            &peer_id_str,
                            p2p::ReplicatorStatus::Active,
                        )
                        .await;
                        continue;
                    }
                    let mut all_succeeded = true;
                    for (doc_id, collection_id) in &docs {
                        match retry_pusher
                            .retry_doc(&retry_handle, peer_id, doc_id, collection_id)
                            .await
                        {
                            Ok(()) => {
                                let _ = peerstore.remove_retry_doc(&peer_id_str, doc_id).await;
                            }
                            Err(_) => {
                                all_succeeded = false;
                            }
                        }
                    }
                    if all_succeeded {
                        let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                        let _ = set_persisted_replicator_status(
                            &peerstore,
                            &peer_id_str,
                            p2p::ReplicatorStatus::Active,
                        )
                        .await;
                    } else {
                        let _ = set_persisted_replicator_status(
                            &peerstore,
                            &peer_id_str,
                            p2p::ReplicatorStatus::Inactive,
                        )
                        .await;
                        retry_info.bump();
                        if let Ok(bytes) = retry_info.to_bytes() {
                            let _ = peerstore.update_retry_info(&peer_id_str, &bytes).await;
                        }
                    }
                }
            }
        });

        let restore_peerstore = storage::stores::Peerstore::new(store);
        match restore_peerstore.list_replicators().await {
            Ok(entries) => {
                for (_peer_id_str, data) in entries {
                    if let Ok(rep_info) = p2p::ReplicatorInfo::from_bytes(&data) {
                        if let Some(pid) = rep_info.peer_id() {
                            let _ = handle
                                .create_replicator(pid, rep_info.collections.clone())
                                .await;
                            for cid in &rep_info.collections {
                                let _ = handle
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

        let mut restored_doc_ids = HashSet::new();
        if let Ok(doc_ids) = restore_peerstore.load_documents().await {
            for doc_id in &doc_ids {
                let _ = handle
                    .subscribe(p2p::topics::DefraTopic::document(doc_id))
                    .await;
                restored_doc_ids.insert(doc_id.clone());
            }
        }

        let adapter = crate::p2p_adapter::P2PAdapter::with_full_context(
            handle.clone(),
            coordinator.clone(),
            doc_pusher,
            event_bus,
            Some(version_syncer),
        );
        adapter.set_initial_tracked_documents(restored_doc_ids);

        info!("P2P sync coordinator initialized");

        Ok(P2PSetup {
            host_handle: Some(handle),
            p2p_tasks: Some(P2PTasks {
                coordinator: coordinator.shutdown_handle(),
                host_task,
                replication_task,
                event_handler_task,
                failure_recorder_task,
                retry_loop_task,
            }),
            mutator: broadcast_mutator,
            http_adapter: Some(Arc::new(adapter)),
            wire_merge_acp: Some(Box::new(move |acp| {
                coordinator_for_acp.set_document_acp(acp.clone());
                merge_handler_for_acp.set_document_acp(acp);
            })),
            wire_doc_pusher_acp: Some(Box::new(move |acp| {
                doc_pusher_for_acp.set_document_acp(acp);
            })),
        })
    }

    #[cfg(feature = "iroh")]
    async fn setup_iroh_p2p<S>(
        store: Arc<S>,
        database: Arc<db::DB<S>>,
        event_bus: Arc<dyn events::Bus>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
    ) -> Result<P2PSetup>
    where
        S: storage::corekv::Store + 'static,
    {
        info!("Initializing P2P network (iroh)");

        let sync_blockstore = Arc::new(blockstore::DefraBlockstore::new(store.clone(), true));
        let merge_blockstore = sync_blockstore.clone();
        let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
            Arc::new(p2p::sync::P2PCollectionStore::new(store.clone()));
        let head_provider: Arc<dyn p2p::sync::DocumentHeadProvider> =
            Arc::new(db_merge::create_head_provider(database.clone()));

        let iroh_secret_key = Self::iroh_secret_key(peer_keypair.as_ref())?;
        let (command_tx, mut iroh_events, replicator_registry, host_task) =
            p2p::iroh::spawn_endpoint(p2p::iroh::IrohEndpointConfig {
                secret_key: iroh_secret_key.clone(),
                relay_mode: Self::iroh_relay_mode(config)?,
                discovery: Self::iroh_discovery(config)?,
                bind_port: config.net.iroh_bind_port,
                bind_addr: config.net.iroh_bind_addr,
            })
            .await
            .map_err(Error::P2P)?;

        let transport = p2p::iroh::IrohTransport::new(command_tx, iroh_secret_key);
        info!(
            "Iroh transport initialized, peer ID: {}",
            transport.local_peer_id()
        );

        let (mut coordinator, sync_events) = p2p::sync::SyncCoordinator::with_head_provider(
            transport.clone(),
            sync_blockstore,
            Self::sync_config(config),
            Self::access_mode(config),
            replicator_registry,
            collection_store,
            head_provider,
        )
        .await
        .map_err(Error::P2P)?;

        let failure_rx = db_merge::attach_failure_channel(&mut coordinator, 1024);
        let coordinator = Arc::new(coordinator);
        let coordinator_for_acp = coordinator.clone();

        match db_merge::load_persisted_collections(&coordinator).await {
            Ok(count) => {
                if count > 0 {
                    info!("Loaded {} persisted P2P collection subscription(s)", count);
                }
            }
            Err(e) => {
                warn!("Failed to load persisted P2P collections: {}", e);
            }
        }

        // Start pubsub_rpc doc-sync / sync-branchable services (#828) so
        // this node can interoperate with Go DefraDB peers over gossipsub.
        if let Err(e) = coordinator.start_pubsub_services().await {
            warn!("Failed to start pubsub_rpc services: {}", e);
        }

        let merge_blockstore_for_syncer = merge_blockstore.clone();
        let replication = db_merge::create_replication_stack(
            database.clone(),
            merge_blockstore,
            coordinator.clone(),
        );
        let merge_handler_for_loop = replication.merge_handler.clone();
        let merge_handler_inner_for_syncer = replication.merge_handler_inner.clone();
        let broadcast_mutator = replication.broadcast_mutator.clone();
        let merge_handler_for_acp = replication.merge_handler.clone();

        let coordinator_for_replication = coordinator.clone();
        let replication_task = tokio::spawn(async move {
            info!("Starting replication loop for P2P sync (iroh)");
            p2p::sync::ReplicationLoop::run(
                coordinator_for_replication,
                sync_events,
                merge_handler_for_loop,
                p2p::sync::ReplicationConfig {
                    continue_on_error: true,
                    rebroadcast_on_merge: false,
                    batch_size: 50,
                    max_workers: 32,
                },
            )
            .await;
            info!("Replication loop stopped (iroh)");
        });

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
                        event_bus_for_handler.publish(events::Message::topic_peer_event(
                            events::TopicPeerEventData {
                                peer_id: peer_id.to_string(),
                                topic: topic.clone(),
                                event_type: "JOINED".to_string(),
                            },
                        ));
                    }
                    p2p::TransportEvent::PeerUnsubscribed { peer_id, topic } => {
                        info!("Peer unsubscribed (iroh): {} on {}", peer_id, topic);
                        event_bus_for_handler.publish(events::Message::topic_peer_event(
                            events::TopicPeerEventData {
                                peer_id: peer_id.to_string(),
                                topic: topic.clone(),
                                event_type: "LEFT".to_string(),
                            },
                        ));
                    }
                    _ => {}
                }

                if event.requires_inline_ordering() {
                    if let Err(e) = coordinator_for_events.handle_transport_event(event).await {
                        error!("Failed to handle iroh event: {}", e);
                    }
                    continue;
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

        let version_syncer: Arc<dyn crate::transport_version_syncer::TransportVersionSyncer> =
            crate::transport_version_syncer::DbTransportVersionSyncer::new_arc(
                merge_blockstore_for_syncer,
                merge_handler_inner_for_syncer,
                database.clone(),
                transport.clone(),
            );

        let doc_pusher_impl = Arc::new(crate::transport_doc_pusher::DbTransportDocPusher::new(
            database.clone(),
            transport.clone(),
        ));
        let doc_pusher_for_acp = doc_pusher_impl.clone();
        let doc_pusher: Arc<dyn crate::transport_doc_pusher::TransportDocPusher> = doc_pusher_impl;

        let recorder_store = store.clone();
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
                    continue;
                }
                if let Err(e) = set_persisted_replicator_status(
                    &peerstore,
                    &failure.peer_id,
                    p2p::ReplicatorStatus::Inactive,
                )
                .await
                {
                    warn!(error = %e, "Failed to mark replicator inactive");
                }
            }
        });

        let retry_store = store.clone();
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
                    let mut retry_info = match storage::stores::RetryInfo::from_bytes(&info_bytes) {
                        Ok(i) => i,
                        Err(_) => continue,
                    };
                    if !retry_info.is_due() {
                        continue;
                    }
                    let peer_id = p2p::transport::PeerId::new(peer_id_str.clone());
                    // Iroh request-response can reconnect on demand, so don't
                    // gate retries on the peer-map snapshot.
                    let docs = match peerstore.get_retry_doc_ids(&peer_id_str).await {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    if docs.is_empty() {
                        let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                        let _ = set_persisted_replicator_status(
                            &peerstore,
                            &peer_id_str,
                            p2p::ReplicatorStatus::Active,
                        )
                        .await;
                        continue;
                    }
                    let mut all_succeeded = true;
                    for (doc_id, collection_id) in &docs {
                        match retry_pusher
                            .retry_doc(&peer_id, doc_id, collection_id)
                            .await
                        {
                            Ok(()) => {
                                let _ = peerstore.remove_retry_doc(&peer_id_str, doc_id).await;
                            }
                            Err(_) => {
                                all_succeeded = false;
                            }
                        }
                    }
                    if all_succeeded {
                        let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                        let _ = set_persisted_replicator_status(
                            &peerstore,
                            &peer_id_str,
                            p2p::ReplicatorStatus::Active,
                        )
                        .await;
                    } else {
                        let _ = set_persisted_replicator_status(
                            &peerstore,
                            &peer_id_str,
                            p2p::ReplicatorStatus::Inactive,
                        )
                        .await;
                        retry_info.bump();
                        if let Ok(bytes) = retry_info.to_bytes() {
                            let _ = peerstore.update_retry_info(&peer_id_str, &bytes).await;
                        }
                    }
                }
            }
        });

        let restore_peerstore = storage::stores::Peerstore::new(store);
        match restore_peerstore.list_replicators().await {
            Ok(entries) => {
                for (_peer_id_str, data) in entries {
                    if let Ok(rep_info) = p2p::ReplicatorInfo::from_bytes(&data) {
                        let pid = p2p::transport::PeerId::new(rep_info.peer_id_str().to_string());
                        let _ = coordinator
                            .create_replicator(&pid, rep_info.collections.clone(), false)
                            .await;
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to load replicators from storage");
            }
        }

        let mut restored_doc_ids = HashSet::new();
        if let Ok(doc_ids) = restore_peerstore.load_documents().await {
            for doc_id in &doc_ids {
                let _ = transport
                    .subscribe(p2p::topics::DefraTopic::document(doc_id))
                    .await;
                restored_doc_ids.insert(doc_id.clone());
            }
        }

        let adapter = crate::iroh_p2p_adapter::IrohP2PAdapter::with_full_context(
            transport.clone(),
            coordinator.clone(),
            doc_pusher,
            event_bus,
            Some(version_syncer),
        );
        adapter.set_initial_tracked_documents(restored_doc_ids);

        info!("P2P sync coordinator initialized (iroh)");

        Ok(P2PSetup {
            host_handle: None,
            p2p_tasks: Some(P2PTasks {
                coordinator: coordinator.shutdown_handle(),
                host_task,
                replication_task,
                event_handler_task,
                failure_recorder_task,
                retry_loop_task,
            }),
            mutator: broadcast_mutator,
            http_adapter: Some(Arc::new(adapter)),
            wire_merge_acp: Some(Box::new(move |acp| {
                coordinator_for_acp.set_document_acp(acp.clone());
                merge_handler_for_acp.set_document_acp(acp);
            })),
            wire_doc_pusher_acp: Some(Box::new(move |acp| {
                doc_pusher_for_acp.set_document_acp(acp);
            })),
        })
    }

    fn access_mode(config: &Config) -> p2p::bitswap::AccessMode {
        if config.acp.document_type != AcpDocumentType::None {
            p2p::bitswap::AccessMode::Controlled
        } else {
            p2p::bitswap::AccessMode::Open
        }
    }

    fn sync_config(config: &Config) -> p2p::sync::SyncConfig {
        p2p::sync::SyncConfig {
            rate_limit_burst: config.net.p2p_rate_limit_burst,
            rate_limit_rate: config.net.p2p_rate_limit_rate,
            ..Default::default()
        }
    }

    #[cfg(feature = "iroh")]
    fn iroh_secret_key(peer_keypair: Option<&p2p::Keypair>) -> Result<iroh_net::SecretKey> {
        if let Some(kp) = peer_keypair {
            let seed = kp.derive_secret(b"iroh-transport").ok_or_else(|| {
                Error::InvalidConfig("iroh transport requires Ed25519 key".into())
            })?;
            Ok(iroh_net::SecretKey::from_bytes(&seed))
        } else {
            Ok(iroh_net::SecretKey::generate(&mut rand::rng()))
        }
    }

    #[cfg(feature = "iroh")]
    fn iroh_relay_mode(config: &Config) -> Result<p2p::iroh::IrohRelayModeConfig> {
        match config.net.iroh_relay_mode.as_deref() {
            Some("disabled") => Ok(p2p::iroh::IrohRelayModeConfig::Disabled),
            Some("default") => Ok(p2p::iroh::IrohRelayModeConfig::Default),
            Some("custom") => {
                let urls = Self::iroh_relay_urls(config);
                if urls.is_empty() {
                    Err(Error::InvalidConfig(
                        "iroh_relay_mode=custom requires at least one relay URL".into(),
                    ))
                } else {
                    Ok(p2p::iroh::IrohRelayModeConfig::Custom(urls))
                }
            }
            Some(other) => Err(Error::InvalidConfig(format!(
                "unsupported iroh_relay_mode '{}'",
                other
            ))),
            None => {
                let urls = Self::iroh_relay_urls(config);
                if urls.is_empty() {
                    Ok(p2p::iroh::IrohRelayModeConfig::Default)
                } else {
                    Ok(p2p::iroh::IrohRelayModeConfig::Custom(urls))
                }
            }
        }
    }

    #[cfg(feature = "iroh")]
    fn iroh_relay_urls(config: &Config) -> Vec<String> {
        let mut urls = config.net.iroh_relay_urls.clone();
        if let Some(url) = &config.net.iroh_relay_url {
            urls.push(url.clone());
        }
        urls
    }

    #[cfg(feature = "iroh")]
    fn iroh_discovery(config: &Config) -> Result<p2p::iroh::IrohDiscoveryConfig> {
        match (
            config.net.iroh_discovery,
            config.net.iroh_discovery_origin_domain.clone(),
            config.net.iroh_pkarr_relay_url.clone(),
        ) {
            (_, Some(origin_domain), Some(pkarr_relay_url)) => {
                Ok(p2p::iroh::IrohDiscoveryConfig::CustomDns {
                    origin_domain,
                    pkarr_relay_url,
                })
            }
            (_, Some(_), None) | (_, None, Some(_)) => Err(Error::InvalidConfig(
                "custom iroh discovery requires both iroh_discovery_origin_domain and iroh_pkarr_relay_url"
                    .into(),
            )),
            (false, None, None) => Ok(p2p::iroh::IrohDiscoveryConfig::Disabled),
            (true, None, None) => Ok(p2p::iroh::IrohDiscoveryConfig::N0),
        }
    }
}
