//! Private P2P lifecycle, setup, restoration, retry, and background-task runtime.
//!
//! Gated behind the `p2p` feature. Not part of the public `defra_node` API.

use std::sync::{Arc, Mutex};

use p2p::P2PTransport;

use crate::P2PConfig;

type WireDocumentAcpCallback = Box<dyn FnOnce(Arc<dyn acp::DocumentACP>, bool)>;

/// Owned Iroh/P2P background tasks and handles; shut down via [`P2PLifecycle::shutdown`].
pub(super) struct P2PLifecycle {
    inner: Mutex<Option<P2PLifecycleInner>>,
}

struct P2PLifecycleInner {
    transport: p2p::iroh::IrohTransport,
    coordinator: p2p::sync::SyncShutdownHandle,
    endpoint_task: tokio::task::JoinHandle<()>,
    replication_task: tokio::task::JoinHandle<()>,
    event_handler_task: tokio::task::JoinHandle<()>,
    failure_recorder_task: tokio::task::JoinHandle<()>,
    retry_loop_task: tokio::task::JoinHandle<()>,
}

impl P2PLifecycle {
    fn new(inner: P2PLifecycleInner) -> Self {
        Self {
            inner: Mutex::new(Some(inner)),
        }
    }

    pub(super) async fn shutdown(&self) {
        let inner = match self.inner.lock() {
            Ok(mut guard) => guard.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };

        if let Some(inner) = inner {
            inner.shutdown().await;
        }
    }
}

impl P2PLifecycleInner {
    async fn shutdown(self) {
        let shutdown_started = std::time::Instant::now();
        let Self {
            transport,
            coordinator,
            endpoint_task,
            replication_task,
            event_handler_task,
            failure_recorder_task,
            retry_loop_task,
        } = self;

        let retry_started = std::time::Instant::now();
        abort_background_task("iroh retry loop", retry_loop_task).await;
        tracing::warn!(target: "defra_node",
            elapsed_ms = retry_started.elapsed().as_millis(),
            "P2P shutdown: retry loop stopped"
        );

        let coordinator_started = std::time::Instant::now();
        coordinator.shutdown().await;
        tracing::warn!(target: "defra_node",
            elapsed_ms = coordinator_started.elapsed().as_millis(),
            "P2P shutdown: coordinator stopped"
        );

        let transport_started = std::time::Instant::now();
        if let Err(error) = transport.shutdown().await {
            tracing::debug!(target: "defra_node", %error, "Iroh transport shutdown returned an error");
        }
        tracing::warn!(target: "defra_node",
            elapsed_ms = transport_started.elapsed().as_millis(),
            "P2P shutdown: transport stop requested"
        );

        drop(transport);
        drop(coordinator);

        let event_handler_started = std::time::Instant::now();
        abort_background_task("iroh event handler", event_handler_task).await;
        tracing::warn!(target: "defra_node",
            elapsed_ms = event_handler_started.elapsed().as_millis(),
            "P2P shutdown: event handler stopped"
        );

        let replication_started = std::time::Instant::now();
        abort_background_task("iroh replication loop", replication_task).await;
        tracing::warn!(target: "defra_node",
            elapsed_ms = replication_started.elapsed().as_millis(),
            "P2P shutdown: replication loop stopped"
        );

        let failure_started = std::time::Instant::now();
        abort_background_task("iroh failure recorder", failure_recorder_task).await;
        tracing::warn!(target: "defra_node",
            elapsed_ms = failure_started.elapsed().as_millis(),
            "P2P shutdown: failure recorder stopped"
        );

        let endpoint_started = std::time::Instant::now();
        await_endpoint_task(endpoint_task).await;
        tracing::warn!(target: "defra_node",
            elapsed_ms = endpoint_started.elapsed().as_millis(),
            total_elapsed_ms = shutdown_started.elapsed().as_millis(),
            "P2P shutdown: endpoint task stopped"
        );
    }
}

async fn await_endpoint_task(mut task: tokio::task::JoinHandle<()>) {
    match tokio::time::timeout(std::time::Duration::from_secs(5), &mut task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) if error.is_cancelled() => {
            tracing::debug!(target: "defra_node", "Iroh endpoint task was already cancelled");
        }
        Ok(Err(error)) => {
            tracing::warn!(target: "defra_node", %error, "Iroh endpoint task failed during shutdown");
        }
        Err(_) => {
            tracing::warn!(target: "defra_node", "Iroh endpoint task did not stop after graceful shutdown; aborting");
            task.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), task).await;
        }
    }
}

async fn abort_background_task(task_name: &'static str, task: tokio::task::JoinHandle<()>) {
    task.abort();
    match tokio::time::timeout(std::time::Duration::from_secs(1), task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) if error.is_cancelled() => {}
        Ok(Err(error)) => {
            tracing::debug!(target: "defra_node", task = task_name, %error, "P2P background task failed during shutdown");
        }
        Err(_) => {
            tracing::debug!(target: "defra_node",
                task = task_name,
                "P2P background task did not stop after abort"
            );
        }
    }
}

/// Internal result from P2P setup, carrying the type-erased ops and mutator.
pub(super) struct P2PSetupResult {
    pub(super) ops: Arc<dyn defra_http::P2POperations>,
    pub(super) lifecycle: Option<P2PLifecycle>,
    pub(super) mutator: Arc<dyn query::DocMutator>,
    pub(super) wire_document_acp: Option<WireDocumentAcpCallback>,
    pub(super) txn_broadcaster: Arc<dyn db::event::emission::TxnBroadcaster>,
}

pub(super) async fn setup_p2p<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    database: Arc<db::DB<S>>,
    event_bus: Arc<dyn events::Bus>,
    config: &P2PConfig,
    node_identity: Option<Arc<identity::RawIdentity>>,
) -> anyhow::Result<P2PSetupResult> {
    // 1. Load or generate secret key for stable node identity
    let secret_key =
        p2p::iroh::load_or_generate_secret_key(config.secret_key_path.as_deref()).await?;

    // 2. Configure and spawn IROH endpoint with pinned port + optional bind address
    let iroh_config = p2p::iroh::IrohEndpointConfig {
        secret_key: secret_key.clone(),
        node_identity: node_identity.clone(),
        relay_mode: config.relay_mode.clone(),
        discovery: config.discovery.clone(),
        bind_port: Some(config.port),
        bind_addr: config.bind_addr,
        max_concurrent_multipath_paths: config.max_concurrent_multipath_paths,
        gossip_heal: p2p::iroh::GossipHealConfig::from_env(),
    };
    let (command_tx, iroh_events, replicator_registry, endpoint_task) =
        p2p::iroh::spawn_endpoint(iroh_config)
            .await
            .map_err(|e| anyhow::anyhow!("IROH endpoint spawn failed: {}", e))?;

    // 3. Create IROH transport facade
    let transport = p2p::iroh::IrohTransport::new(command_tx, secret_key);

    // 5. Blockstore for sync coordinator + merge handler
    let sync_blockstore = Arc::new(blockstore::DefraBlockstore::new(store.clone(), true));
    let classifier = defra_p2p_adapter::DbBlockClassifier::new_arc(database.clone());
    let serve_acp = Arc::new(p2p::bitswap::LateBoundServeAcp::new());

    // 6. Collection store (persists subscriptions)
    let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
        Arc::new(p2p::sync::P2PCollectionStore::new(store.clone()));

    let rebroadcast_on_merge = config.rebroadcast_on_merge;

    // 7. SyncCoordinator (transport-generic -- same constructor, different type param)
    let sync_config = p2p::sync::SyncConfig {
        max_concurrent_dag_fetches: config.max_concurrent_dag_fetches,
        max_concurrent_push_tasks: config.max_concurrent_push_tasks,
        max_doc_sync_request_doc_ids: config.max_doc_sync_request_doc_ids,
        rate_limit_burst: config.rate_limit_burst,
        rate_limit_rate: config.rate_limit_rate,
        max_pending_dags: config.max_pending_dags,
        ..Default::default()
    };
    // The head provider serves DocSync and BranchableSync requests from this
    // node's headstore. The access-control constructor installs a
    // NoOpHeadProvider, which silently answers every head lookup with an
    // empty list — wire the real one, as the CLI runtime does.
    let head_provider: Arc<dyn p2p::sync::DocumentHeadProvider> =
        Arc::new(db::merge::create_head_provider(database.clone()));
    let (mut coordinator, sync_events) =
        p2p::sync::SyncCoordinator::with_head_provider_and_serve_gate(
            transport.clone(),
            sync_blockstore.clone(),
            sync_config,
            p2p::AccessMode::Controlled,
            replicator_registry,
            collection_store,
            head_provider,
            Arc::new(replication_filter::QueryReplicationFilterMatcher::new()),
            classifier,
            serve_acp.clone(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("SyncCoordinator creation failed: {}", e))?;

    // Failure channel (required by replication loop)
    let failure_rx = db::merge::attach_failure_channel(&mut coordinator, 1024);
    let failure_recorder_task =
        defra_p2p_adapter::spawn_failure_recorder(store.clone(), failure_rx);

    let coordinator = Arc::new(coordinator);
    coordinator
        .install_pending_dag_store(Arc::new(p2p::sync::PendingDagStore::new(store.clone())))
        .await;

    if config.load_persisted_collections {
        db::merge::load_persisted_collections(&coordinator)
            .await
            .ok();
    } else {
        tracing::info!(target: "defra_node", "skipping persisted P2P collection subscriptions");
    }

    // 8. Merge handler
    let replication = db::merge::create_replication_stack(
        database.clone(),
        sync_blockstore.clone(),
        coordinator.clone(),
    );
    let merge_handler_for_loop = replication.merge_handler.clone();
    let broadcast_mutator = replication.broadcast_mutator.clone();
    let merge_handler_for_acp = replication.merge_handler.clone();
    let serve_acp_for_acp = serve_acp.clone();
    let database_for_acp = database.clone();

    // 9. Replication loop (transport-generic)
    let coord_for_repl = coordinator.clone();
    let replication_task = tokio::spawn(async move {
        p2p::sync::ReplicationLoop::run(
            coord_for_repl,
            sync_events,
            merge_handler_for_loop,
            p2p::sync::ReplicationConfig {
                rebroadcast_on_merge,
                ..p2p::sync::ReplicationConfig::default()
            },
            |_| {},
        )
        .await;
    });

    // Restore pending-DAG registrations persisted before the last
    // shutdown/crash as due receiver-clock work, then keep sweeping
    // periodically for records skipped at capacity or TTL-evicted (#1099).
    // Spawned through the coordinator so shutdown drains them before the
    // coordinator (and through it the store) is dropped; a bare tokio::spawn
    // here outlives shutdown and keeps the data path locked (#1309).
    let coord_for_restore = coordinator.clone();
    coordinator.spawn_background_task("pending_dag_resync", async move {
        coord_for_restore
            .run_pending_dag_resync(std::time::Duration::from_secs(60))
            .await;
    });

    // Receiver's re-arm loop (#1116 stage 2): dispatches due pending
    // roots at a tight cadence. Sibling of the resync sweep above.
    let coord_for_retry_clock = coordinator.clone();
    coordinator.spawn_background_task("pending_dag_retry_clock", async move {
        coord_for_retry_clock
            .run_pending_dag_retry_clock(std::time::Duration::from_secs(2))
            .await;
    });

    // 10. IROH event handler (events are already TransportEvent -- no conversion needed)
    let coord_for_events = coordinator.clone();
    let store_for_events = store.clone();
    let event_handler_task = tokio::spawn(async move {
        run_event_handler(iroh_events, coord_for_events, store_for_events).await;
    });
    let doc_pusher_impl = Arc::new(defra_p2p_adapter::DbTransportDocPusher::new(
        database.clone(),
        transport.clone(),
        coordinator.head_hint_car_authority(),
    ));
    let doc_pusher_for_acp = doc_pusher_impl.clone();
    let doc_pusher: Arc<dyn defra_p2p_adapter::TransportDocPusher> = doc_pusher_impl;
    let retry_loop_task = defra_p2p_adapter::spawn_retry_loop(
        store.clone(),
        transport.clone(),
        doc_pusher.clone(),
        None,
    );
    let version_syncer = Some(defra_p2p_adapter::DbTransportVersionSyncer::new_arc(
        sync_blockstore,
        replication.merge_handler_inner.clone(),
        database.clone(),
        transport.clone(),
    ));

    // 11. BroadcastMutator (replaces AutoCommitMutator)
    let broadcast_mutator_for_acp = broadcast_mutator.clone();
    let mutator: Arc<dyn query::DocMutator> = broadcast_mutator;

    let restored_doc_ids = restore_iroh_p2p_state(store.clone(), &transport, &coordinator).await;

    let peer_id = transport.local_peer_id().to_string();
    tracing::info!(target: "defra_node", peer_id = %peer_id, "P2P started (IROH/QUIC)");
    let adapter = defra_p2p_adapter::IrohP2PAdapter::with_full_context(
        transport.clone(),
        coordinator.clone(),
        doc_pusher,
        event_bus,
        version_syncer,
        db::node_access_checker(database.clone()),
    );
    adapter.set_initial_tracked_documents(restored_doc_ids);
    let ops: Arc<dyn defra_http::P2POperations> = Arc::new(adapter);
    let peer_identity_resolver = p2p::IrohPeerIdentityResolver::new(transport.clone());

    Ok(P2PSetupResult {
        ops,
        lifecycle: Some(P2PLifecycle::new(P2PLifecycleInner {
            transport,
            coordinator: coordinator.shutdown_handle(),
            endpoint_task,
            replication_task,
            event_handler_task,
            failure_recorder_task,
            retry_loop_task,
        })),
        mutator,
        wire_document_acp: Some(Box::new(move |acp, strict| {
            serve_acp_for_acp.set(p2p::bitswap::ServeAcp {
                resolver: Arc::new(peer_identity_resolver),
                gate: defra_p2p_adapter::DbBlockReadGate::new_arc(
                    acp.clone(),
                    database_for_acp.node_did(),
                ),
            });
            merge_handler_for_acp.set_document_acp(acp.clone());
            merge_handler_for_acp.set_strict_replicated_doc_access(strict);
            doc_pusher_for_acp.set_document_acp(acp.clone());
            broadcast_mutator_for_acp.set_document_acp(acp);
        })),
        txn_broadcaster: replication.txn_broadcaster,
    })
}

async fn run_event_handler<B: blockstore::Blockstore + Send + Sync + 'static>(
    events: tokio::sync::mpsc::Receiver<
        p2p::TransportEvent<<p2p::iroh::IrohTransport as P2PTransport>::ResponseToken>,
    >,
    coordinator: Arc<p2p::sync::SyncCoordinator<B, p2p::iroh::IrohTransport>>,
    store: Arc<impl storage::corekv::Store + 'static>,
) {
    let handler_coordinator = coordinator.clone();
    coordinator.run_event_dispatcher(events, move |event, admission| {
        let coordinator = handler_coordinator.clone();
        let store = store.clone();
        async move {
            let event_kind = event.kind();
            if let p2p::TransportEvent::PeerConnected(peer_id) = &event {
                defra_p2p_adapter::activate_retry_peer(store, peer_id).await;
            }

            if let Err(e) = coordinator
                .handle_transport_event_with_admission(event, admission)
                .await
            {
                if e.is_rate_limited() {
                    tracing::debug!(target: "defra_node", event_kind, error = %e, "P2P rate-limited");
                } else if e.is_retriable() {
                    tracing::warn!(target: "defra_node", event_kind, error = %e, "P2P transport event failed after retries");
                } else {
                    tracing::error!(target: "defra_node", event_kind, error = %e, "P2P event handler error");
                }
            }
        }
    })
    .await;
}

async fn restore_iroh_p2p_state<S, B>(
    store: Arc<S>,
    transport: &p2p::iroh::IrohTransport,
    coordinator: &Arc<p2p::sync::IrohSyncCoordinator<B>>,
) -> std::collections::HashSet<String>
where
    S: storage::corekv::Store + 'static,
    B: blockstore::Blockstore + 'static,
{
    let peerstore = storage::stores::Peerstore::new(store);

    match peerstore.list_replicators().await {
        Ok(entries) => {
            for (peer_id_str, data) in entries {
                let replicator = match p2p::ReplicatorInfo::from_bytes(&data) {
                    Ok(replicator) => replicator,
                    Err(error) => {
                        tracing::warn!(target: "defra_node",
                            peer_id = %peer_id_str,
                            error = %error,
                            "failed to decode persisted P2P replicator"
                        );
                        continue;
                    }
                };
                let peer_id = p2p::transport::PeerId::new(replicator.peer_id_str().to_string());
                if let Err(error) = coordinator
                    .create_replicator(&peer_id, replicator.collections.clone(), false)
                    .await
                {
                    tracing::warn!(target: "defra_node",
                        peer_id = %peer_id,
                        error = %error,
                        "failed to restore persisted P2P replicator"
                    );
                }
            }
        }
        Err(error) => {
            tracing::warn!(target: "defra_node", error = %error, "failed to load persisted P2P replicators")
        }
    }

    let mut restored_doc_ids = std::collections::HashSet::new();
    match peerstore.load_documents().await {
        Ok(doc_ids) => {
            for doc_id in doc_ids {
                if let Err(error) = transport
                    .subscribe(p2p::topics::DefraTopic::document(&doc_id))
                    .await
                {
                    tracing::warn!(target: "defra_node",
                        doc_id = %doc_id,
                        error = %error,
                        "failed to restore P2P document subscription"
                    );
                }
                restored_doc_ids.insert(doc_id);
            }
        }
        Err(error) => {
            tracing::warn!(target: "defra_node", error = %error, "failed to load persisted P2P document subscriptions");
        }
    }

    restored_doc_ids
}
