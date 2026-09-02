use std::sync::Arc;

use anyhow::{anyhow, Result};
use p2p::sync::SyncConfig;
use p2p::topics::DefraTopic;

use crate::node::{
    EmbeddedBlockstore, EmbeddedMergeHandler, WireDocumentAcpCallback, WireKmsCallback,
};
use crate::node_recovery::{restore_libp2p_documents, restore_libp2p_replicators};
use crate::node_tasks::{spawn_libp2p_event_handler, spawn_replication_loop};
use crate::{Libp2pConfig, ManagedP2PSystem, TransportKind};
use defra_p2p_adapter::{
    DbTransportDocPusher, DbVersionSyncer, P2PAdapter, ReplicatorPushOptions,
    ReplicatorPushOptionsState, TransportDocPusher,
};

pub(crate) struct P2PSetup<S: storage::corekv::Store + 'static> {
    pub system: Arc<ManagedP2PSystem>,
    pub mutator: Arc<dyn query::DocMutator>,
    pub merge_handler: Arc<EmbeddedMergeHandler<S>>,
    pub wire_document_acp: Option<WireDocumentAcpCallback>,
    /// Forwards committed `/tx` writes to P2P peers; mirrors what the CLI
    /// `P2PSetup` exposes. Without this, transactional writes commit locally
    /// but never replicate.
    pub txn_broadcaster: Arc<dyn db::event::emission::TxnBroadcaster>,
    /// Type-erased KMS transport for this node's P2P system. node.rs adds it
    /// to the DefraKms transports list and installs the serve handler.
    pub kms_transport: Arc<dyn kms::KeyTransport>,
    /// This node's transport-level peer id (stringified). node.rs binds it
    /// into the KMS so served ECIES replies carry the correct AAD peer id.
    pub local_peer_id: String,
    /// Defers wiring the late-built KMS into the inner merge handler
    /// (mirrors `wire_document_acp`). NAC/document_acp aren't available when
    /// the P2P system is created, so the KMS is built later in node.rs.
    pub wire_kms: Option<WireKmsCallback>,
    /// SE remote query transport (owner-queries-replicator, #976). Lets this
    /// embedded node act as an SE query OWNER, fanning `encrypted_<Collection>`
    /// queries to replicators. The SE key is read lazily because it's
    /// provisioned at runtime via `set_se_options`.
    pub se_transport: Option<Arc<dyn query::SeQueryTransport>>,
    /// Inbound management-channel serve deps, read lazily by the event loop and
    /// populated by node.rs once the controller (`P2POperations`) and NAC
    /// manager are built. The event loop drops manage requests until then.
    pub manage_hooks: defra_p2p_adapter::manage::hooks::ManageHooksCell,
    /// The `P2POperations` controller (the `adapter`) bound into `manage_hooks`
    /// after the NAC manager exists. node.rs uses it as `hooks.ops`.
    pub manage_controller: Arc<dyn defra_http::P2POperations>,
    /// Requester-side manage correlators (mutating + query). The event-loop
    /// clones deliver inbound replies; these clones are for the requester API
    /// (Task 6.3) and are bound into `manage_hooks` so requester and event loop
    /// agree on message_id correlation.
    pub manage_correlator: p2p::ManageCorrelator,
    pub manage_query_correlator: p2p::ManageQueryCorrelator,
}

async fn shutdown_libp2p_host(handle: &p2p::P2PHostHandle, host_task: tokio::task::JoinHandle<()>) {
    if let Err(error) = handle.shutdown().await {
        tracing::debug!(%error, "failed to signal P2P host during setup rollback");
    }
    if let Err(error) = host_task.await {
        tracing::debug!(%error, "P2P host task failed during setup rollback");
    }
}

#[cfg(feature = "iroh")]
async fn shutdown_iroh_endpoint(
    transport: &p2p::iroh::IrohTransport,
    endpoint_task: tokio::task::JoinHandle<()>,
) {
    use p2p::transport::P2PTransport;

    if let Err(error) = transport.shutdown().await {
        tracing::debug!(%error, "failed to signal Iroh endpoint during setup rollback");
    }
    if let Err(error) = endpoint_task.await {
        tracing::debug!(%error, "Iroh endpoint task failed during setup rollback");
    }
}

pub(crate) async fn setup_libp2p<S>(
    store: Arc<S>,
    database: Arc<db::DB<S>>,
    event_bus: Arc<dyn events::Bus>,
    config: &Libp2pConfig,
    sync_config: SyncConfig,
) -> Result<P2PSetup<S>>
where
    S: storage::corekv::Store + 'static,
{
    use p2p::bitswap::BitswapStoreAdapter;
    use p2p::sync::DocumentHeadProvider;
    use storage::stores::Peerstore;

    let listen_addr = config
        .listen_addr
        .parse()
        .map_err(|error| anyhow!("invalid multiaddr '{}': {error}", config.listen_addr))?;
    let blockstore = Arc::new(EmbeddedBlockstore::new(store.clone(), true));
    let bitswap_store = BitswapStoreAdapter::new(blockstore.clone());

    let p2p_keypair = {
        let peerstore = Peerstore::new(store.clone());
        let key_id = "__local_p2p_identity__";
        match peerstore.get_replicator(key_id).await {
            Ok(Some(bytes)) => match libp2p::identity::Keypair::from_protobuf_encoding(&bytes) {
                Ok(keypair) => keypair,
                Err(_) => {
                    let keypair = libp2p::identity::Keypair::generate_ed25519();
                    if let Ok(encoded) = keypair.to_protobuf_encoding() {
                        let _ = peerstore.create_replicator(key_id, &encoded).await;
                    }
                    keypair
                }
            },
            _ => {
                let keypair = libp2p::identity::Keypair::generate_ed25519();
                if let Ok(encoded) = keypair.to_protobuf_encoding() {
                    let _ = peerstore.create_replicator(key_id, &encoded).await;
                }
                keypair
            }
        }
    };

    let classifier = defra_p2p_adapter::DbBlockClassifier::new_arc(database.clone());
    let serve_acp = Arc::new(p2p::bitswap::LateBoundServeAcp::new());
    let (host, handle, event_rx, replicator_registry) =
        p2p::P2PHost::with_keypair_and_config_and_identity_and_serve_gate(
            p2p_keypair,
            bitswap_store,
            p2p::P2PHostConfig::default(),
            database.node_identity(),
            classifier.clone(),
            serve_acp.clone(),
        )
        .await
        .map_err(|error| anyhow!("failed to create P2P host: {error}"))?;
    let host_task = tokio::spawn(async move {
        host.run().await;
    });

    if let Err(error) = handle.listen(listen_addr).await {
        shutdown_libp2p_host(&handle, host_task).await;
        return Err(anyhow!("failed to start listening: {error}"));
    }

    for topic in [
        DefraTopic::DocSync,
        DefraTopic::Encryption,
        DefraTopic::Custom("sync-branchable".to_string()),
    ] {
        if let Err(error) = handle.subscribe(topic.clone()).await {
            tracing::warn!(topic = %topic, error = %error, "failed to subscribe to default topic");
        }
    }

    let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
        Arc::new(p2p::sync::P2PCollectionStore::new(store.clone()));
    let head_provider: Arc<dyn DocumentHeadProvider> =
        Arc::new(db::merge::create_head_provider(database.clone()));
    let (mut coordinator, sync_events_rx) =
        match p2p::sync::SyncCoordinator::with_head_provider_and_serve_gate(
            p2p::Libp2pTransport::new(handle.clone()),
            blockstore.clone(),
            sync_config,
            p2p::bitswap::AccessMode::Controlled,
            replicator_registry,
            collection_store,
            head_provider,
            std::sync::Arc::new(replication_filter::QueryReplicationFilterMatcher::new()),
            classifier,
            serve_acp.clone(),
        )
        .await
        {
            Ok(coordinator) => coordinator,
            Err(error) => {
                shutdown_libp2p_host(&handle, host_task).await;
                return Err(anyhow!("failed to create sync coordinator: {error}"));
            }
        };

    let failure_rx = db::merge::attach_failure_channel(&mut coordinator, 1024);
    let coordinator = Arc::new(coordinator);
    coordinator
        .install_pending_dag_store(Arc::new(p2p::sync::PendingDagStore::new(store.clone())))
        .await;
    let replication = db::merge::create_replication_stack(
        database.clone(),
        blockstore.clone(),
        coordinator.clone(),
    );

    let kms_libp2p_transport = p2p::Libp2pTransport::new(handle.clone());
    let local_peer_id = {
        use p2p::transport::P2PTransport;
        kms_libp2p_transport.local_peer_id().to_string()
    };
    let kms_transport = match p2p::kms::PubsubKeyTransport::new(
        kms_libp2p_transport,
        Arc::new(p2p::HandlePeerIdentityResolver::new(handle.clone())),
    )
    .await
    {
        Ok(transport) => transport,
        Err(error) => {
            coordinator.shutdown().await;
            shutdown_libp2p_host(&handle, host_task).await;
            return Err(anyhow!("failed to create KMS transport: {error}"));
        }
    };
    coordinator.install_kms_transport(kms_transport.clone());
    let merge_handler_inner_for_kms = replication.merge_handler_inner.clone();

    let coordinator_for_restore = coordinator.clone();
    let pending_dag_resync_task = tokio::spawn(async move {
        coordinator_for_restore
            .run_pending_dag_resync(std::time::Duration::from_secs(60))
            .await;
    });

    // Receiver's re-arm loop (#1116 stage 2): dispatches due pending roots
    // at a tight cadence. Sibling of the resync sweep above.
    let coordinator_for_retry_clock = coordinator.clone();
    let pending_dag_retry_task = tokio::spawn(async move {
        coordinator_for_retry_clock
            .run_pending_dag_retry_clock(std::time::Duration::from_secs(2))
            .await;
    });

    match db::merge::load_persisted_collections(&coordinator).await {
        Ok(count) if count > 0 => tracing::debug!(count, "loaded persisted P2P collections"),
        Ok(_) => {}
        Err(error) => tracing::warn!(error = %error, "failed to load persisted P2P collections"),
    }

    // Start pubsub_rpc doc-sync / sync-branchable services (#828) so this
    // node can interoperate with Go DefraDB peers over gossipsub.
    if let Err(error) = coordinator.start_pubsub_services().await {
        tracing::warn!(error = %error, "failed to start pubsub_rpc services");
    }

    // SE query correlator: lets this node serve as an SE replicator and route
    // any inbound replies. Cloned so the SAME correlator is shared between the
    // event handler (which delivers replies) and the owner/querier transport
    // (which awaits them) — they must agree on message_id correlation (#976).
    let se_correlator = p2p::SeQueryCorrelator::new();
    let se_correlator_for_transport = se_correlator.clone();
    // Manage channel: correlators shared between the event loop (which delivers
    // inbound replies) and the requester API (Task 6.3), and a deferred hooks
    // cell node.rs populates once the controller + NAC manager exist.
    let manage_correlator = p2p::ManageCorrelator::new();
    let manage_query_correlator = p2p::ManageQueryCorrelator::new();
    let manage_hooks = defra_p2p_adapter::manage::hooks::new_manage_hooks_cell();
    let host_event_task = spawn_libp2p_event_handler(
        event_rx,
        coordinator.clone(),
        store.clone(),
        event_bus.clone(),
        handle.clone(),
        se_correlator,
        manage_hooks.clone(),
    );
    let replication_task = spawn_replication_loop(
        coordinator.clone(),
        sync_events_rx,
        replication.merge_handler.clone(),
        event_bus.clone(),
    );
    let failure_recorder_task =
        defra_p2p_adapter::spawn_failure_recorder(store.clone(), failure_rx);

    let doc_pusher_impl = Arc::new(DbTransportDocPusher::new(
        database.clone(),
        p2p::Libp2pTransport::new(handle.clone()),
        coordinator.head_hint_car_authority(),
    ));
    let doc_pusher_for_acp = doc_pusher_impl.clone();
    let doc_pusher: Arc<dyn TransportDocPusher> = doc_pusher_impl;
    let version_syncer = Some(DbVersionSyncer::new_arc(
        blockstore.clone(),
        replication.merge_handler_inner.clone(),
        database.clone(),
    ));
    let se_repusher: Arc<dyn db::merge::SeArtifactRepusher> = replication.broadcast_mutator.clone();
    let retry_store = store.clone();
    let retry_transport = p2p::Libp2pTransport::new(handle.clone());
    let retry_doc_pusher = doc_pusher.clone();
    let retry_se_repusher = se_repusher.clone();
    let retry_loop_task = defra_p2p_adapter::spawn_retry_loop(
        store.clone(),
        p2p::Libp2pTransport::new(handle.clone()),
        doc_pusher.clone(),
        Some(se_repusher),
    );

    let restore_peerstore = storage::stores::Peerstore::new(store.clone());
    restore_libp2p_replicators(&handle, &restore_peerstore).await;
    let restored_doc_ids = restore_libp2p_documents(&handle, &restore_peerstore).await;

    let replicator_push_options = ReplicatorPushOptionsState::default();
    let adapter = P2PAdapter::with_full_context(
        handle.clone(),
        coordinator.clone(),
        doc_pusher,
        event_bus,
        version_syncer,
        db::node_access_checker(database.clone()),
    )
    .with_replicator_push_options_state(replicator_push_options.clone());
    adapter.set_initial_tracked_documents(restored_doc_ids);
    let coordinator_for_acp = coordinator.clone();
    let serve_acp_for_acp = serve_acp.clone();
    let handle_for_acp = handle.clone();
    let broadcast_mutator_for_acp = replication.broadcast_mutator.clone();
    let broadcast_mutator_for_se = replication.broadcast_mutator.clone();
    // Lazy SE-key handle: teed by the callback below (runtime provisioning),
    // read by the owner/querier transport at query time (#976).
    let se_key_handle = db::merge::empty_se_key_handle();
    let se_key_handle_for_callback = se_key_handle.clone();
    let se_options_callback = Arc::new(move |options: ReplicatorPushOptions| {
        tee_se_key(&se_key_handle_for_callback, &options);
        broadcast_mutator_for_se.set_se_options(db::merge::BroadcastSeOptions {
            encryption_key: options.se_encryption_key,
            identity_pubkey: options.se_identity_pubkey,
        })
    });
    let se_transport: Option<Arc<dyn query::SeQueryTransport>> =
        Some(Arc::new(db::merge::DbMergeSeQueryTransport::new(
            p2p::Libp2pTransport::new(handle.clone()),
            se_correlator_for_transport,
            coordinator.replicators().clone(),
            se_key_handle,
        )) as Arc<dyn query::SeQueryTransport>);
    let manage_controller: Arc<dyn defra_http::P2POperations> = Arc::new(adapter);
    let system = Arc::new(ManagedP2PSystem::with_replicator_push_options_callback(
        TransportKind::Libp2p,
        manage_controller.clone(),
        crate::node::ShutdownHandle::libp2p(
            handle.clone(),
            coordinator.shutdown_handle(),
            vec![
                host_task,
                host_event_task,
                replication_task,
                failure_recorder_task,
                retry_loop_task,
                pending_dag_resync_task,
                pending_dag_retry_task,
            ],
        ),
        replicator_push_options,
        Some(se_options_callback),
    ));
    system.set_retry_replicators(Arc::new(move || {
        let store = retry_store.clone();
        let transport = retry_transport.clone();
        let doc_pusher = retry_doc_pusher.clone();
        let se_repusher = retry_se_repusher.clone();
        Box::pin(async move {
            defra_p2p_adapter::run_retry_pass(
                &store,
                &transport,
                &doc_pusher,
                Some(&se_repusher),
                true,
            )
            .await;
        })
    }));

    // Outbound management requester over the same libp2p transport, sharing the
    // requester-side manage correlators (Task 7a). Installed on the system so an
    // HTTP consumer can wire it into `AppState` via `with_manage`.
    system.set_manage_requester(Arc::new(
        defra_p2p_adapter::manage::client::ManageClient::new(
            p2p::Libp2pTransport::new(handle.clone()),
            manage_correlator.clone(),
            manage_query_correlator.clone(),
        ),
    ));

    Ok(P2PSetup {
        system,
        mutator: replication.broadcast_mutator,
        merge_handler: replication.merge_handler,
        txn_broadcaster: replication.txn_broadcaster,
        kms_transport: kms_transport as Arc<dyn kms::KeyTransport>,
        local_peer_id,
        wire_kms: Some(Box::new(move |kms| {
            merge_handler_inner_for_kms.set_kms(kms);
        })),
        wire_document_acp: Some(Box::new(move |acp| {
            serve_acp_for_acp.set(p2p::bitswap::ServeAcp {
                resolver: Arc::new(p2p::HandlePeerIdentityResolver::new(handle_for_acp)),
                gate: defra_p2p_adapter::DbBlockReadGate::new_arc(acp.clone()),
            });
            coordinator_for_acp.set_document_acp(acp.clone());
            doc_pusher_for_acp.set_document_acp(acp.clone());
            broadcast_mutator_for_acp.set_document_acp(acp);
        })),
        se_transport,
        manage_hooks,
        manage_controller,
        manage_correlator,
        manage_query_correlator,
    })
}

/// Tee the SE key material from runtime `set_se_options` into the lazy handle
/// read by the owner/querier transport. Skips non-32-byte keys (#976).
fn tee_se_key(handle: &db::merge::SeKeyHandle, options: &ReplicatorPushOptions) {
    match &options.se_encryption_key {
        Some(key_bytes) => match <[u8; 32]>::try_from(key_bytes.as_slice()) {
            Ok(key) => db::merge::store_se_key(
                handle,
                Some(db::merge::SeKeyMaterial::new(
                    key,
                    options.se_identity_pubkey.clone(),
                )),
            ),
            Err(_) => {
                tracing::warn!(
                    len = key_bytes.len(),
                    "SE key from set_se_options is not 32 bytes; skipping owner-transport tee"
                );
            }
        },
        None => db::merge::store_se_key(handle, None),
    }
}

#[cfg(feature = "iroh")]
pub(crate) async fn setup_iroh<S>(
    store: Arc<S>,
    database: Arc<db::DB<S>>,
    event_bus: Arc<dyn events::Bus>,
    config: &crate::IrohConfig,
    sync_config: SyncConfig,
    node_identity: Option<Arc<identity::RawIdentity>>,
) -> Result<P2PSetup<S>>
where
    S: storage::corekv::Store + 'static,
{
    use defra_p2p_adapter::{
        DbTransportDocPusher, DbTransportVersionSyncer, IrohP2PAdapter, TransportDocPusher,
    };
    use storage::stores::Peerstore;

    use crate::node_recovery::{restore_iroh_documents, restore_iroh_replicators};
    use crate::node_tasks::spawn_iroh_event_handler;

    let secret_key =
        p2p::iroh::load_or_generate_secret_key(config.secret_key_path.as_deref()).await?;
    let iroh_config = p2p::iroh::IrohEndpointConfig {
        secret_key: secret_key.clone(),
        node_identity,
        relay_mode: config.relay_mode.clone(),
        discovery: config.discovery.clone(),
        bind_port: config.bind_port,
        bind_addr: config.bind_addr,
        max_concurrent_multipath_paths: config.max_concurrent_multipath_paths,
        gossip_heal: p2p::iroh::GossipHealConfig::from_env(),
    };
    let (command_tx, event_rx, replicator_registry, endpoint_task) =
        p2p::iroh::spawn_endpoint(iroh_config)
            .await
            .map_err(|error| anyhow!("failed to spawn iroh endpoint: {error}"))?;

    let transport = p2p::iroh::IrohTransport::new(command_tx, secret_key);
    let blockstore = Arc::new(EmbeddedBlockstore::new(store.clone(), true));
    let classifier = defra_p2p_adapter::DbBlockClassifier::new_arc(database.clone());
    let serve_acp = Arc::new(p2p::bitswap::LateBoundServeAcp::new());
    let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
        Arc::new(p2p::sync::P2PCollectionStore::new(store.clone()));
    let head_provider: Arc<dyn p2p::sync::DocumentHeadProvider> =
        Arc::new(db::merge::create_head_provider(database.clone()));
    let (mut coordinator, sync_events_rx) =
        match p2p::sync::SyncCoordinator::with_head_provider_and_serve_gate(
            transport.clone(),
            blockstore.clone(),
            sync_config,
            p2p::bitswap::AccessMode::Controlled,
            replicator_registry,
            collection_store,
            head_provider,
            std::sync::Arc::new(replication_filter::QueryReplicationFilterMatcher::new()),
            classifier,
            serve_acp.clone(),
        )
        .await
        {
            Ok(coordinator) => coordinator,
            Err(error) => {
                shutdown_iroh_endpoint(&transport, endpoint_task).await;
                return Err(anyhow!("failed to create iroh sync coordinator: {error}"));
            }
        };

    let failure_rx = db::merge::attach_failure_channel(&mut coordinator, 1024);
    let coordinator = Arc::new(coordinator);
    coordinator
        .install_pending_dag_store(Arc::new(p2p::sync::PendingDagStore::new(store.clone())))
        .await;
    let local_peer_id = {
        use p2p::transport::P2PTransport;
        transport.local_peer_id().to_string()
    };
    let kms_transport = match p2p::kms::PubsubKeyTransport::new(
        transport.clone(),
        Arc::new(p2p::AnonymousResolver),
    )
    .await
    {
        Ok(transport) => transport,
        Err(error) => {
            coordinator.shutdown().await;
            shutdown_iroh_endpoint(&transport, endpoint_task).await;
            return Err(anyhow!("failed to create KMS transport: {error}"));
        }
    };
    coordinator.install_kms_transport(kms_transport.clone());
    let replication = db::merge::create_replication_stack(
        database.clone(),
        blockstore.clone(),
        coordinator.clone(),
    );
    let merge_handler_inner_for_kms = replication.merge_handler_inner.clone();

    let coordinator_for_restore = coordinator.clone();
    let pending_dag_resync_task = tokio::spawn(async move {
        coordinator_for_restore
            .run_pending_dag_resync(std::time::Duration::from_secs(60))
            .await;
    });

    // Receiver's re-arm loop (#1116 stage 2): dispatches due pending roots
    // at a tight cadence. Sibling of the resync sweep above.
    let coordinator_for_retry_clock = coordinator.clone();
    let pending_dag_retry_task = tokio::spawn(async move {
        coordinator_for_retry_clock
            .run_pending_dag_retry_clock(std::time::Duration::from_secs(2))
            .await;
    });

    match db::merge::load_persisted_collections(&coordinator).await {
        Ok(count) if count > 0 => tracing::debug!(count, "loaded persisted P2P collections"),
        Ok(_) => {}
        Err(error) => tracing::warn!(error = %error, "failed to load persisted P2P collections"),
    }

    // Shared correlator between event handler (delivers replies) and the
    // owner/querier transport (awaits them); see libp2p setup (#976).
    let se_correlator = p2p::SeQueryCorrelator::new();
    let se_correlator_for_transport = se_correlator.clone();
    let manage_correlator = p2p::ManageCorrelator::new();
    let manage_query_correlator = p2p::ManageQueryCorrelator::new();
    let manage_hooks = defra_p2p_adapter::manage::hooks::new_manage_hooks_cell();
    let event_handler_task = spawn_iroh_event_handler(
        event_rx,
        coordinator.clone(),
        store.clone(),
        event_bus.clone(),
        se_correlator,
        transport.clone(),
        manage_hooks.clone(),
    );
    let replication_task = spawn_replication_loop(
        coordinator.clone(),
        sync_events_rx,
        replication.merge_handler.clone(),
        event_bus.clone(),
    );
    let failure_recorder_task =
        defra_p2p_adapter::spawn_failure_recorder(store.clone(), failure_rx);

    let doc_pusher_impl = Arc::new(DbTransportDocPusher::new(
        database.clone(),
        transport.clone(),
        coordinator.head_hint_car_authority(),
    ));
    let doc_pusher_for_acp = doc_pusher_impl.clone();
    let doc_pusher: Arc<dyn TransportDocPusher> = doc_pusher_impl;
    let version_syncer = Some(DbTransportVersionSyncer::new_arc(
        blockstore.clone(),
        replication.merge_handler_inner.clone(),
        database.clone(),
        transport.clone(),
    ));
    let se_repusher: Arc<dyn db::merge::SeArtifactRepusher> = replication.broadcast_mutator.clone();
    let retry_store = store.clone();
    let retry_transport = transport.clone();
    let retry_doc_pusher = doc_pusher.clone();
    let retry_se_repusher = se_repusher.clone();
    let retry_loop_task = defra_p2p_adapter::spawn_retry_loop(
        store.clone(),
        transport.clone(),
        doc_pusher.clone(),
        Some(se_repusher),
    );

    let restore_peerstore = Peerstore::new(store.clone());
    restore_iroh_replicators(&coordinator, &restore_peerstore).await;
    let restored_doc_ids = restore_iroh_documents(&transport, &restore_peerstore).await;

    let replicator_push_options = ReplicatorPushOptionsState::default();
    let adapter = IrohP2PAdapter::with_full_context(
        transport.clone(),
        coordinator.clone(),
        doc_pusher,
        event_bus,
        version_syncer,
        db::node_access_checker(database.clone()),
    )
    .with_replicator_push_options_state(replicator_push_options.clone());
    adapter.set_initial_tracked_documents(restored_doc_ids);
    let coordinator_for_acp = coordinator.clone();
    let serve_acp_for_acp = serve_acp.clone();
    let broadcast_mutator_for_acp = replication.broadcast_mutator.clone();
    let broadcast_mutator_for_se = replication.broadcast_mutator.clone();
    let se_key_handle = db::merge::empty_se_key_handle();
    let se_key_handle_for_callback = se_key_handle.clone();
    let se_options_callback = Arc::new(move |options: ReplicatorPushOptions| {
        tee_se_key(&se_key_handle_for_callback, &options);
        broadcast_mutator_for_se.set_se_options(db::merge::BroadcastSeOptions {
            encryption_key: options.se_encryption_key,
            identity_pubkey: options.se_identity_pubkey,
        })
    });
    let se_transport: Option<Arc<dyn query::SeQueryTransport>> =
        Some(Arc::new(db::merge::DbMergeSeQueryTransport::new(
            transport.clone(),
            se_correlator_for_transport,
            coordinator.replicators().clone(),
            se_key_handle,
        )) as Arc<dyn query::SeQueryTransport>);
    let manage_controller: Arc<dyn defra_http::P2POperations> = Arc::new(adapter);
    let system = Arc::new(ManagedP2PSystem::with_replicator_push_options_callback(
        TransportKind::Iroh,
        manage_controller.clone(),
        crate::node::ShutdownHandle::iroh(
            transport.clone(),
            coordinator.shutdown_handle(),
            vec![
                endpoint_task,
                event_handler_task,
                replication_task,
                failure_recorder_task,
                retry_loop_task,
                pending_dag_resync_task,
                pending_dag_retry_task,
            ],
        ),
        replicator_push_options,
        Some(se_options_callback),
    ));
    system.set_retry_replicators(Arc::new(move || {
        let store = retry_store.clone();
        let transport = retry_transport.clone();
        let doc_pusher = retry_doc_pusher.clone();
        let se_repusher = retry_se_repusher.clone();
        Box::pin(async move {
            defra_p2p_adapter::run_retry_pass(
                &store,
                &transport,
                &doc_pusher,
                Some(&se_repusher),
                true,
            )
            .await;
        })
    }));

    // Outbound management requester over the same iroh transport, sharing the
    // requester-side manage correlators (Task 7a). Installed on the system so an
    // HTTP consumer can wire it into `AppState` via `with_manage`.
    system.set_manage_requester(Arc::new(
        defra_p2p_adapter::manage::client::ManageClient::new(
            transport.clone(),
            manage_correlator.clone(),
            manage_query_correlator.clone(),
        ),
    ));

    Ok(P2PSetup {
        system,
        mutator: replication.broadcast_mutator,
        merge_handler: replication.merge_handler,
        txn_broadcaster: replication.txn_broadcaster,
        kms_transport: kms_transport as Arc<dyn kms::KeyTransport>,
        local_peer_id,
        wire_kms: Some(Box::new(move |kms| {
            merge_handler_inner_for_kms.set_kms(kms);
        })),
        wire_document_acp: Some(Box::new(move |acp| {
            serve_acp_for_acp.set(p2p::bitswap::ServeAcp {
                resolver: Arc::new(p2p::IrohPeerIdentityResolver::new(transport.clone())),
                gate: defra_p2p_adapter::DbBlockReadGate::new_arc(acp.clone()),
            });
            coordinator_for_acp.set_document_acp(acp.clone());
            doc_pusher_for_acp.set_document_acp(acp.clone());
            broadcast_mutator_for_acp.set_document_acp(acp);
        })),
        se_transport,
        manage_hooks,
        manage_controller,
        manage_correlator,
        manage_query_correlator,
    })
}
