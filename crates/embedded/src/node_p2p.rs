use std::sync::Arc;

use anyhow::{anyhow, Result};
use p2p::sync::SyncConfig;
use p2p::topics::DefraTopic;

use crate::libp2p_adapter::P2PAdapter;
use crate::libp2p_doc_pusher::DbDocPusher;
use crate::node::{EmbeddedBlockstore, EmbeddedMergeHandler, WireDocumentAcpCallback};
use crate::node_recovery::{restore_libp2p_documents, restore_libp2p_replicators};
use crate::node_tasks::{
    spawn_failure_recorder, spawn_libp2p_event_handler, spawn_libp2p_retry_loop,
    spawn_replication_loop,
};
use crate::version_syncer::DbVersionSyncer;
use crate::{Libp2pConfig, ManagedP2PSystem, P2POperations, TransportKind};

pub(crate) struct P2PSetup<S: storage::corekv::Store + 'static> {
    pub system: Arc<ManagedP2PSystem>,
    pub mutator: Arc<dyn query::DocMutator>,
    pub merge_handler: Arc<EmbeddedMergeHandler<S>>,
    pub wire_document_acp: Option<WireDocumentAcpCallback>,
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

    let (host, handle, event_rx, _replicator_registry) =
        p2p::P2PHost::with_keypair_and_config_and_identity(
            p2p_keypair,
            bitswap_store,
            p2p::P2PHostConfig::default(),
            database.node_identity(),
        )
        .await
        .map_err(|error| anyhow!("failed to create P2P host: {error}"))?;
    tokio::spawn(async move {
        host.run().await;
    });

    let listen_addr = config
        .listen_addr
        .parse()
        .map_err(|error| anyhow!("invalid multiaddr '{}': {error}", config.listen_addr))?;
    handle
        .listen(listen_addr)
        .await
        .map_err(|error| anyhow!("failed to start listening: {error}"))?;

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
        Arc::new(db_merge::create_head_provider(database.clone()));
    let (mut coordinator, sync_events_rx) = p2p::sync::SyncCoordinator::with_head_provider(
        p2p::Libp2pTransport::new(handle.clone()),
        blockstore.clone(),
        sync_config,
        p2p::bitswap::AccessMode::Controlled,
        Arc::new(p2p::ReplicatorRegistry::new()),
        collection_store,
        head_provider,
    )
    .await
    .map_err(|error| anyhow!("failed to create sync coordinator: {error}"))?;

    let failure_rx = db_merge::attach_failure_channel(&mut coordinator, 1024);
    let coordinator = Arc::new(coordinator);
    let replication = db_merge::create_replication_stack(
        database.clone(),
        blockstore.clone(),
        coordinator.clone(),
    );

    match db_merge::load_persisted_collections(&coordinator).await {
        Ok(count) if count > 0 => tracing::debug!(count, "loaded persisted P2P collections"),
        Ok(_) => {}
        Err(error) => tracing::warn!(error = %error, "failed to load persisted P2P collections"),
    }

    let host_event_task =
        spawn_libp2p_event_handler(event_rx, coordinator.clone(), event_bus.clone());
    let replication_task = spawn_replication_loop(
        coordinator.clone(),
        sync_events_rx,
        replication.merge_handler.clone(),
        event_bus.clone(),
    );
    let failure_recorder_task = spawn_failure_recorder(store.clone(), failure_rx);

    let doc_pusher_impl = Arc::new(DbDocPusher::new(database.clone()));
    let doc_pusher_for_acp = doc_pusher_impl.clone();
    let doc_pusher: Arc<dyn crate::DocPusher> = doc_pusher_impl;
    let version_syncer = Some(DbVersionSyncer::new_arc(
        blockstore.clone(),
        replication.merge_handler_inner.clone(),
        database.clone(),
    ));
    let retry_loop_task =
        spawn_libp2p_retry_loop(store.clone(), handle.clone(), doc_pusher.clone());

    let restore_peerstore = storage::stores::Peerstore::new(store.clone());
    restore_libp2p_replicators(&handle, &restore_peerstore).await;
    let restored_doc_ids = restore_libp2p_documents(&handle, &restore_peerstore).await;

    let adapter = P2PAdapter::with_full_context(
        handle.clone(),
        coordinator.clone(),
        doc_pusher,
        event_bus,
        version_syncer,
    );
    adapter.set_initial_tracked_documents(restored_doc_ids);
    let coordinator_for_acp = coordinator.clone();
    let broadcast_mutator_for_acp = replication.broadcast_mutator.clone();
    let system = Arc::new(ManagedP2PSystem::new(
        TransportKind::Libp2p,
        Arc::new(adapter) as Arc<dyn P2POperations>,
        crate::node::ShutdownHandle::libp2p(
            handle.clone(),
            vec![
                host_event_task.abort_handle(),
                replication_task.abort_handle(),
                failure_recorder_task.abort_handle(),
                retry_loop_task.abort_handle(),
            ],
        ),
    ));

    Ok(P2PSetup {
        system,
        mutator: replication.broadcast_mutator,
        merge_handler: replication.merge_handler,
        wire_document_acp: Some(Box::new(move |acp| {
            coordinator_for_acp.set_document_acp(acp.clone());
            doc_pusher_for_acp.set_document_acp(acp.clone());
            broadcast_mutator_for_acp.set_document_acp(acp);
        })),
    })
}

#[cfg(feature = "iroh")]
pub(crate) async fn setup_iroh<S>(
    store: Arc<S>,
    database: Arc<db::DB<S>>,
    event_bus: Arc<dyn events::Bus>,
    config: &crate::IrohConfig,
    sync_config: SyncConfig,
) -> Result<P2PSetup<S>>
where
    S: storage::corekv::Store + 'static,
{
    use crate::transport_doc_pusher::DbTransportDocPusher;
    use crate::transport_version_syncer::DbTransportVersionSyncer;
    use crate::IrohP2PAdapter;
    use p2p::sync::PushFailure;
    use storage::stores::Peerstore;

    use crate::node_recovery::{restore_iroh_documents, restore_iroh_replicators};
    use crate::node_tasks::{spawn_iroh_event_handler, spawn_iroh_retry_loop};

    let secret_key = load_or_generate_iroh_secret_key(config.secret_key_path.as_deref()).await?;
    let iroh_config = p2p::iroh::IrohEndpointConfig {
        secret_key: secret_key.clone(),
        relay_mode: config.relay_mode.clone(),
        discovery: config.discovery.clone(),
        bind_port: config.bind_port,
        bind_addr: config.bind_addr,
    };
    let (command_tx, event_rx, endpoint_task) = p2p::iroh::spawn_endpoint(iroh_config)
        .await
        .map_err(|error| anyhow!("failed to spawn iroh endpoint: {error}"))?;

    let transport = p2p::iroh::IrohTransport::new(command_tx, secret_key);
    let blockstore = Arc::new(EmbeddedBlockstore::new(store.clone(), true));
    let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
        Arc::new(p2p::sync::P2PCollectionStore::new(store.clone()));
    let head_provider: Arc<dyn p2p::sync::DocumentHeadProvider> =
        Arc::new(db_merge::create_head_provider(database.clone()));
    let (mut coordinator, sync_events_rx) = p2p::sync::SyncCoordinator::with_head_provider(
        transport.clone(),
        blockstore.clone(),
        sync_config,
        p2p::bitswap::AccessMode::Controlled,
        Arc::new(p2p::ReplicatorRegistry::new()),
        collection_store,
        head_provider,
    )
    .await
    .map_err(|error| anyhow!("failed to create iroh sync coordinator: {error}"))?;

    let failure_rx = db_merge::attach_failure_channel(&mut coordinator, 1024);
    let coordinator = Arc::new(coordinator);
    let replication = db_merge::create_replication_stack(
        database.clone(),
        blockstore.clone(),
        coordinator.clone(),
    );

    match db_merge::load_persisted_collections(&coordinator).await {
        Ok(count) if count > 0 => tracing::debug!(count, "loaded persisted P2P collections"),
        Ok(_) => {}
        Err(error) => tracing::warn!(error = %error, "failed to load persisted P2P collections"),
    }

    let event_handler_task =
        spawn_iroh_event_handler(event_rx, coordinator.clone(), event_bus.clone());
    let replication_task = spawn_replication_loop(
        coordinator.clone(),
        sync_events_rx,
        replication.merge_handler.clone(),
        event_bus.clone(),
    );
    let failure_recorder_task = spawn_failure_recorder(store.clone(), failure_rx);

    let doc_pusher_impl = Arc::new(DbTransportDocPusher::new(
        database.clone(),
        transport.clone(),
    ));
    let doc_pusher_for_acp = doc_pusher_impl.clone();
    let doc_pusher: Arc<dyn crate::TransportDocPusher> = doc_pusher_impl;
    let version_syncer = Some(DbTransportVersionSyncer::new_arc(
        blockstore.clone(),
        replication.merge_handler_inner.clone(),
        database.clone(),
        transport.clone(),
    ));
    let retry_loop_task =
        spawn_iroh_retry_loop(store.clone(), transport.clone(), doc_pusher.clone());

    let restore_peerstore = Peerstore::new(store.clone());
    restore_iroh_replicators(&coordinator, &restore_peerstore).await;
    let restored_doc_ids = restore_iroh_documents(&transport, &restore_peerstore).await;

    let adapter = IrohP2PAdapter::with_full_context(
        transport.clone(),
        coordinator.clone(),
        doc_pusher,
        event_bus,
        version_syncer,
    );
    adapter.set_initial_tracked_documents(restored_doc_ids);
    let coordinator_for_acp = coordinator.clone();
    let broadcast_mutator_for_acp = replication.broadcast_mutator.clone();
    let system = Arc::new(ManagedP2PSystem::new(
        TransportKind::Iroh,
        Arc::new(adapter) as Arc<dyn P2POperations>,
        crate::node::ShutdownHandle::iroh(
            transport.clone(),
            vec![
                endpoint_task.abort_handle(),
                event_handler_task.abort_handle(),
                replication_task.abort_handle(),
                failure_recorder_task.abort_handle(),
                retry_loop_task.abort_handle(),
            ],
        ),
    ));

    Ok(P2PSetup {
        system,
        mutator: replication.broadcast_mutator,
        merge_handler: replication.merge_handler,
        wire_document_acp: Some(Box::new(move |acp| {
            coordinator_for_acp.set_document_acp(acp.clone());
            doc_pusher_for_acp.set_document_acp(acp.clone());
            broadcast_mutator_for_acp.set_document_acp(acp);
        })),
    })
}

#[cfg(feature = "iroh")]
pub(crate) async fn load_or_generate_iroh_secret_key(
    path: Option<&std::path::Path>,
) -> Result<iroh_net::SecretKey> {
    use anyhow::Context;

    match path {
        Some(path) if path.exists() => {
            let bytes = tokio::fs::read(path)
                .await
                .with_context(|| format!("failed to read iroh secret key '{}'", path.display()))?;
            let array: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow!("iroh secret key file must contain exactly 32 bytes"))?;
            Ok(iroh_net::SecretKey::from_bytes(&array))
        }
        Some(path) => {
            let key = iroh_net::SecretKey::generate(&mut rand::rng());
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await.with_context(|| {
                    format!("failed to create iroh key directory '{}'", parent.display())
                })?;
            }
            tokio::fs::write(path, key.to_bytes())
                .await
                .with_context(|| format!("failed to write iroh secret key '{}'", path.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                    .await
                    .with_context(|| {
                        format!("failed to set permissions on '{}'", path.display())
                    })?;
            }
            Ok(key)
        }
        None => Ok(iroh_net::SecretKey::generate(&mut rand::rng())),
    }
}
