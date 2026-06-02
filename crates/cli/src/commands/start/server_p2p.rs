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
type WireKms = Option<Box<dyn FnOnce(Arc<dyn kms::KmsService>) + Send>>;

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
    /// Hook for forwarding committed `/tx` writes to P2P peers. `Some` when the
    /// P2P stack is up; `None` for the non-P2P fallback path.
    pub(super) txn_broadcaster: Option<Arc<dyn db::event_emission::TxnBroadcaster>>,
    /// Type-erased KMS transport for this node's P2P system. server.rs adds it
    /// to the DefraKms transports list and installs the serve handler. `None`
    /// on the non-P2P fallback path.
    pub(super) kms_transport: Option<Arc<dyn kms::KeyTransport>>,
    /// Defers wiring the late-built KMS into the inner merge handler (mirrors
    /// `wire_merge_acp`). NAC/document_acp aren't available when the P2P system
    /// is created, so the KMS is built later in server.rs.
    pub(super) wire_kms: WireKms,
    /// This node's transport-level peer id (stringified). server.rs binds it
    /// into the KMS so served ECIES replies carry the correct AAD peer id.
    pub(super) local_peer_id: String,
    /// SE remote query transport (owner-queries-replicator, #976). `Some` on the
    /// libp2p path when an SE key is present; `None` for iroh (the SE-query
    /// send path is libp2p-only) and the non-P2P fallback.
    pub(super) se_transport: Option<Arc<dyn query::SeQueryTransport>>,
    /// Inbound management-channel serve deps, read lazily by the event loop and
    /// populated by server.rs once the controller (`P2POperations`) and NAC
    /// manager are built. `None` on the non-P2P fallback path; the event loop
    /// drops manage requests until populated.
    pub(super) manage_hooks: Option<defra_p2p_adapter::manage::hooks::ManageHooksCell>,
    /// The `P2POperations` controller (the `http_adapter`) bound into
    /// `manage_hooks` after the NAC manager exists. `None` on the fallback path.
    pub(super) manage_controller: Option<Arc<dyn defra_http::router::P2POperations>>,
    /// Requester-side manage correlators (mutating + query). Event-loop clones
    /// deliver inbound replies; these clones feed the requester API (Task 6.3)
    /// and are bound into `manage_hooks` so both agree on message_id
    /// correlation. `None` on the fallback path.
    pub(super) manage_correlator: Option<p2p::ManageCorrelator>,
    pub(super) manage_query_correlator: Option<p2p::ManageQueryCorrelator>,
}

impl Node {
    pub(super) async fn setup_p2p<S>(
        store: Arc<S>,
        database: Arc<db::DB<S>>,
        event_bus: Arc<dyn events::Bus>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
        se_key: Option<[u8; 32]>,
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
                return Self::setup_iroh_p2p(
                    store,
                    database,
                    event_bus,
                    config,
                    peer_keypair,
                    se_key,
                )
                .await;
            }
            #[cfg(not(feature = "iroh"))]
            {
                let _ = (store, database, event_bus, peer_keypair, se_key);
                return Err(Error::InvalidTransport(
                    "iroh transport not enabled. Rebuild with --features iroh".into(),
                ));
            }
        }

        Self::setup_libp2p_p2p(store, database, event_bus, config, peer_keypair, se_key).await
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
            txn_broadcaster: None,
            kms_transport: None,
            wire_kms: None,
            local_peer_id: String::new(),
            se_transport: None,
            manage_hooks: None,
            manage_controller: None,
            manage_correlator: None,
            manage_query_correlator: None,
        }
    }

    async fn setup_libp2p_p2p<S>(
        store: Arc<S>,
        database: Arc<db::DB<S>>,
        event_bus: Arc<dyn events::Bus>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
        se_key: Option<[u8; 32]>,
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

        // SE query correlator + replicator registry handle for the
        // owner-queries-replicator loop (#976). The registry returned by
        // start_p2p is the same Arc the host updates via create_replicator, so
        // it reflects live `p2p replicator set` calls.
        let se_correlator = p2p::SeQueryCorrelator::new();
        let se_replicator_registry = replicator_registry.clone();

        // Manage channel: correlators shared between the event loop (delivers
        // inbound replies) and the requester API (Task 6.3); a deferred hooks
        // cell server.rs populates once the controller + NAC manager exist.
        let manage_correlator = p2p::ManageCorrelator::new();
        let manage_query_correlator = p2p::ManageQueryCorrelator::new();
        let manage_hooks = defra_p2p_adapter::manage::hooks::new_manage_hooks_cell();
        let manage_hooks_for_events = manage_hooks.clone();

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

        // Build the KMS pubsub transport and install it on the coordinator so
        // raw gossip on the encryption topic is routed to it (mirrors
        // crates/embedded/src/node_p2p.rs::setup_libp2p).
        let kms_transport =
            p2p::kms::PubsubKeyTransport::new(p2p::Libp2pTransport::new(handle.clone()))
                .await
                .map_err(|e| Error::Server(format!("failed to create KMS transport: {e}")))?;
        coordinator.install_kms_transport(kms_transport.clone());
        let local_peer_id = {
            use p2p::transport::P2PTransport;
            p2p::Libp2pTransport::new(handle.clone())
                .local_peer_id()
                .to_string()
        };

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
        let merge_handler_inner_for_kms = replication.merge_handler_inner.clone();
        let broadcast_mutator = replication.broadcast_mutator.clone();
        let broadcast_mutator_for_acp = replication.broadcast_mutator.clone();
        let merge_handler_for_acp = replication.merge_handler.clone();
        let txn_broadcaster = replication.txn_broadcaster.clone();

        // Feed the keyring-loaded searchable-encryption key into the same SE
        // path the FFI uses (BroadcastMutator::set_se_options for live writes,
        // DbMergeHandler::set_se_enc_key for merged/replicated docs). This lets
        // a `defra start` node produce and verify SE artifacts.
        if let Some(key) = se_key {
            if let Err(e) =
                replication
                    .broadcast_mutator
                    .set_se_options(db_merge::BroadcastSeOptions {
                        encryption_key: Some(zeroize::Zeroizing::new(key.to_vec())),
                        identity_pubkey: None,
                    })
            {
                warn!(error = %e, "failed to set searchable encryption options on broadcast mutator");
            }
            replication.merge_handler_inner.set_se_enc_key(key.to_vec());
        }

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
        let se_store = store.clone();
        let se_handle = handle.clone();
        let se_transport_serve = p2p::Libp2pTransport::new(handle.clone());
        let se_correlator_for_events = se_correlator.clone();
        let se_event_bus = event_bus.clone();
        let event_handler_task = Some(tokio::spawn(async move {
            use p2p::P2PTransport as _;
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

                // Intercept SE events: the CLI must store inbound artifacts and
                // serve/route SE queries itself (the coordinator does not). #976.
                let transport_event = match p2p::convert_host_event(event) {
                    p2p::TransportEvent::SEArtifactsReceived { peer_id, data } => {
                        let doc_ids = match peer_id.as_str().parse::<libp2p::PeerId>() {
                            Ok(pid) => {
                                db_merge::se::serve::handle_artifacts_push(
                                    se_store.as_ref(),
                                    &se_handle,
                                    pid,
                                    &data,
                                )
                                .await
                            }
                            Err(_) => {
                                db_merge::se::serve::handle_artifacts_received(
                                    se_store.as_ref(),
                                    &peer_id.to_string(),
                                    &data,
                                )
                                .await
                            }
                        };
                        for doc_id in doc_ids {
                            se_event_bus.publish(events::Message::se_artifact_received(
                                events::SEArtifactReceivedData { doc_id },
                            ));
                        }
                        continue;
                    }
                    p2p::TransportEvent::SEQueryRequest { peer_id, request } => {
                        db_merge::se::serve::handle_query_request(
                            se_store.as_ref(),
                            &se_transport_serve,
                            peer_id,
                            request,
                        )
                        .await;
                        continue;
                    }
                    p2p::TransportEvent::SEQueryReply { reply, .. } => {
                        se_correlator_for_events.deliver(reply);
                        continue;
                    }
                    p2p::TransportEvent::ManageRequest { peer_id, request } => {
                        if let Some(hooks) = manage_hooks_for_events.get() {
                            let mut reply = defra_p2p_adapter::manage::serve::build_manage_reply(
                                hooks.ops.as_ref(),
                                hooks.nac.as_ref(),
                                request,
                            )
                            .await;
                            if p2p::signing::sign_with_transport(&se_transport_serve, &mut reply)
                                .is_ok()
                            {
                                if let Err(e) = se_transport_serve
                                    .send_manage_response(&peer_id, reply)
                                    .await
                                {
                                    warn!(error = %e, "failed to send manage response");
                                }
                            }
                        } else {
                            tracing::debug!(%peer_id, "manage request before hooks ready; dropping");
                        }
                        continue;
                    }
                    p2p::TransportEvent::ManageQueryRequest { peer_id, request } => {
                        if let Some(hooks) = manage_hooks_for_events.get() {
                            let mut reply =
                                defra_p2p_adapter::manage::serve::build_manage_query_reply(
                                    hooks.ops.as_ref(),
                                    hooks.nac.as_ref(),
                                    request,
                                )
                                .await;
                            if p2p::signing::sign_with_transport(&se_transport_serve, &mut reply)
                                .is_ok()
                            {
                                if let Err(e) = se_transport_serve
                                    .send_manage_query_response(&peer_id, reply)
                                    .await
                                {
                                    warn!(error = %e, "failed to send manage query response");
                                }
                            }
                        } else {
                            tracing::debug!(%peer_id, "manage query request before hooks ready; dropping");
                        }
                        continue;
                    }
                    p2p::TransportEvent::ManageReply { reply, .. } => {
                        if let Some(hooks) = manage_hooks_for_events.get() {
                            hooks.correlator.deliver(reply);
                        }
                        continue;
                    }
                    p2p::TransportEvent::ManageQueryReply { reply, .. } => {
                        if let Some(hooks) = manage_hooks_for_events.get() {
                            hooks.query_correlator.deliver(reply);
                        }
                        continue;
                    }
                    other => other,
                };
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

        // Build the SE remote query transport (owner-queries-replicator, #976).
        // Identity is None to match the write side (server_p2p SE options use
        // identity_pubkey: None), so write-tags and query-tags agree.
        let se_transport: Option<Arc<dyn query::SeQueryTransport>> = se_key.map(|key| {
            Arc::new(db_merge::DbMergeSeQueryTransport::new(
                p2p::Libp2pTransport::new(handle.clone()),
                se_correlator,
                se_replicator_registry,
                db_merge::filled_se_key_handle(key, None),
            )) as Arc<dyn query::SeQueryTransport>
        });

        info!("P2P sync coordinator initialized");

        let manage_controller: Arc<dyn defra_http::router::P2POperations> = Arc::new(adapter);

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
            http_adapter: Some(manage_controller.clone()),
            wire_merge_acp: Some(Box::new(move |acp| {
                coordinator_for_acp.set_document_acp(acp.clone());
                // Wire the document ACP into the broadcast mutator so newly
                // created ACP-protected docs are registered *before* their
                // detached P2P broadcast fires (#976). Without this, the
                // mutator's pre-broadcast registration is skipped (ACP handle
                // absent) and an encrypted doc's DEK can leak during the
                // ~4.5s SourceHub registration window.
                broadcast_mutator_for_acp.set_document_acp(acp.clone());
                merge_handler_for_acp.set_document_acp(acp);
            })),
            wire_doc_pusher_acp: Some(Box::new(move |acp| {
                doc_pusher_for_acp.set_document_acp(acp);
            })),
            txn_broadcaster: Some(txn_broadcaster),
            kms_transport: Some(kms_transport as Arc<dyn kms::KeyTransport>),
            wire_kms: Some(Box::new(move |kms| {
                merge_handler_inner_for_kms.set_kms(kms);
            })),
            local_peer_id,
            se_transport,
            manage_hooks: Some(manage_hooks),
            manage_controller: Some(manage_controller),
            manage_correlator: Some(manage_correlator),
            manage_query_correlator: Some(manage_query_correlator),
        })
    }

    #[cfg(feature = "iroh")]
    async fn setup_iroh_p2p<S>(
        store: Arc<S>,
        database: Arc<db::DB<S>>,
        event_bus: Arc<dyn events::Bus>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
        se_key: Option<[u8; 32]>,
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

        let se_correlator = p2p::SeQueryCorrelator::new();
        let se_replicator_registry = replicator_registry.clone();

        // Manage channel (iroh): correlators shared between the event loop and
        // the requester API (Task 6.3); deferred hooks cell server.rs populates
        // once the controller + NAC manager exist.
        let manage_correlator = p2p::ManageCorrelator::new();
        let manage_query_correlator = p2p::ManageQueryCorrelator::new();
        let manage_hooks = defra_p2p_adapter::manage::hooks::new_manage_hooks_cell();
        let manage_hooks_for_events = manage_hooks.clone();

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

        // Build the KMS pubsub transport and install it on the coordinator so
        // raw gossip on the encryption topic is routed to it (mirrors
        // crates/embedded/src/node_p2p.rs::setup_iroh).
        let local_peer_id = transport.local_peer_id().to_string();
        let kms_transport = p2p::kms::PubsubKeyTransport::new(transport.clone())
            .await
            .map_err(|e| Error::Server(format!("failed to create KMS transport: {e}")))?;
        coordinator.install_kms_transport(kms_transport.clone());

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
        let merge_handler_inner_for_kms = replication.merge_handler_inner.clone();
        let broadcast_mutator = replication.broadcast_mutator.clone();
        let broadcast_mutator_for_acp = replication.broadcast_mutator.clone();
        let merge_handler_for_acp = replication.merge_handler.clone();
        let txn_broadcaster = replication.txn_broadcaster.clone();

        // Mirror the libp2p SE wiring (keyring-loaded SE key into the FFI's SE
        // path) so an iroh `defra start` node produces/verifies SE artifacts.
        if let Some(key) = se_key {
            if let Err(e) =
                replication
                    .broadcast_mutator
                    .set_se_options(db_merge::BroadcastSeOptions {
                        encryption_key: Some(zeroize::Zeroizing::new(key.to_vec())),
                        identity_pubkey: None,
                    })
            {
                warn!(error = %e, "failed to set searchable encryption options on broadcast mutator");
            }
            replication.merge_handler_inner.set_se_enc_key(key.to_vec());
        }

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
        let se_store = store.clone();
        let se_transport_serve = transport.clone();
        let se_correlator_for_events = se_correlator.clone();
        let se_event_bus = event_bus.clone();
        let event_handler_task = Some(tokio::spawn(async move {
            let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
            while let Some(event) = iroh_events.recv().await {
                // SE events: store inbound artifacts and serve/route SE queries
                // over the iroh transport (mirrors the libp2p loop, #976). Rust
                // -> Rust artifact push is fire-and-forget, so use the no-ack
                // `handle_artifacts_received` (Go -> Rust over iroh, which
                // expects a PushSEArtifactsReply ack, is a follow-up).
                let event = match event {
                    p2p::TransportEvent::SEArtifactsReceived { peer_id, data } => {
                        let doc_ids = db_merge::se::serve::handle_artifacts_received(
                            se_store.as_ref(),
                            &peer_id.to_string(),
                            &data,
                        )
                        .await;
                        for doc_id in doc_ids {
                            se_event_bus.publish(events::Message::se_artifact_received(
                                events::SEArtifactReceivedData { doc_id },
                            ));
                        }
                        continue;
                    }
                    p2p::TransportEvent::SEQueryRequest { peer_id, request } => {
                        db_merge::se::serve::handle_query_request(
                            se_store.as_ref(),
                            &se_transport_serve,
                            peer_id,
                            request,
                        )
                        .await;
                        continue;
                    }
                    p2p::TransportEvent::SEQueryReply { reply, .. } => {
                        se_correlator_for_events.deliver(reply);
                        continue;
                    }
                    p2p::TransportEvent::ManageRequest { peer_id, request } => {
                        if let Some(hooks) = manage_hooks_for_events.get() {
                            let mut reply = defra_p2p_adapter::manage::serve::build_manage_reply(
                                hooks.ops.as_ref(),
                                hooks.nac.as_ref(),
                                request,
                            )
                            .await;
                            if p2p::signing::sign_with_transport(&se_transport_serve, &mut reply)
                                .is_ok()
                            {
                                if let Err(e) = se_transport_serve
                                    .send_manage_response(&peer_id, reply)
                                    .await
                                {
                                    warn!(error = %e, "failed to send manage response (iroh)");
                                }
                            }
                        } else {
                            tracing::debug!(%peer_id, "manage request before hooks ready; dropping");
                        }
                        continue;
                    }
                    p2p::TransportEvent::ManageQueryRequest { peer_id, request } => {
                        if let Some(hooks) = manage_hooks_for_events.get() {
                            let mut reply =
                                defra_p2p_adapter::manage::serve::build_manage_query_reply(
                                    hooks.ops.as_ref(),
                                    hooks.nac.as_ref(),
                                    request,
                                )
                                .await;
                            if p2p::signing::sign_with_transport(&se_transport_serve, &mut reply)
                                .is_ok()
                            {
                                if let Err(e) = se_transport_serve
                                    .send_manage_query_response(&peer_id, reply)
                                    .await
                                {
                                    warn!(error = %e, "failed to send manage query response (iroh)");
                                }
                            }
                        } else {
                            tracing::debug!(%peer_id, "manage query request before hooks ready; dropping");
                        }
                        continue;
                    }
                    p2p::TransportEvent::ManageReply { reply, .. } => {
                        if let Some(hooks) = manage_hooks_for_events.get() {
                            hooks.correlator.deliver(reply);
                        }
                        continue;
                    }
                    p2p::TransportEvent::ManageQueryReply { reply, .. } => {
                        if let Some(hooks) = manage_hooks_for_events.get() {
                            hooks.query_correlator.deliver(reply);
                        }
                        continue;
                    }
                    other => other,
                };
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

        let manage_controller: Arc<dyn defra_http::router::P2POperations> = Arc::new(adapter);

        // Build the SE remote query transport over iroh so encrypted queries
        // fan out to replicators (owner-queries-replicator, #976). Identity is
        // None to match the write side (iroh SE options use identity_pubkey:
        // None), so write-tags and query-tags agree.
        let se_transport: Option<Arc<dyn query::SeQueryTransport>> = se_key.map(|key| {
            Arc::new(db_merge::DbMergeSeQueryTransport::new(
                transport.clone(),
                se_correlator,
                se_replicator_registry,
                db_merge::filled_se_key_handle(key, None),
            )) as Arc<dyn query::SeQueryTransport>
        });

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
            http_adapter: Some(manage_controller.clone()),
            txn_broadcaster: Some(txn_broadcaster),
            wire_merge_acp: Some(Box::new(move |acp| {
                coordinator_for_acp.set_document_acp(acp.clone());
                // Wire the document ACP into the broadcast mutator so newly
                // created ACP-protected docs are registered *before* their
                // detached P2P broadcast fires (#976). Without this, the
                // mutator's pre-broadcast registration is skipped (ACP handle
                // absent) and an encrypted doc's DEK can leak during the
                // ~4.5s SourceHub registration window.
                broadcast_mutator_for_acp.set_document_acp(acp.clone());
                merge_handler_for_acp.set_document_acp(acp);
            })),
            wire_doc_pusher_acp: Some(Box::new(move |acp| {
                doc_pusher_for_acp.set_document_acp(acp);
            })),
            kms_transport: Some(kms_transport as Arc<dyn kms::KeyTransport>),
            wire_kms: Some(Box::new(move |kms| {
                merge_handler_inner_for_kms.set_kms(kms);
            })),
            local_peer_id,
            se_transport,
            manage_hooks: Some(manage_hooks),
            manage_controller: Some(manage_controller),
            manage_correlator: Some(manage_correlator),
            manage_query_correlator: Some(manage_query_correlator),
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
            Ok(iroh_net::SecretKey::generate())
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
