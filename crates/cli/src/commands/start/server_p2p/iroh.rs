use std::collections::HashSet;
use std::sync::Arc;

use p2p::P2PTransport;
use tracing::{error, info, warn};

use super::super::node::{Node, P2PTasks};
use super::P2PSetup;
use crate::config::Config;
use crate::error::{Error, Result};

impl Node {
    pub(super) async fn setup_iroh_p2p(
        store: Arc<storage::DynStore>,
        database: Arc<db::DB<storage::DynStore>>,
        event_bus: Arc<dyn events::Bus>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
        node_identity: Option<Arc<identity::RawIdentity>>,
        se_key: Option<[u8; 32]>,
    ) -> Result<P2PSetup> {
        info!("Initializing P2P network (iroh)");

        let sync_blockstore = Arc::new(blockstore::DefraBlockstore::new(store.clone(), true));
        let merge_blockstore = sync_blockstore.clone();
        let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
            Arc::new(p2p::sync::P2PCollectionStore::new(store.clone()));
        let head_provider: Arc<dyn p2p::sync::DocumentHeadProvider> =
            Arc::new(db::merge::create_head_provider(database.clone()));

        let iroh_secret_key = Self::iroh_secret_key(peer_keypair.as_ref())?;
        let (command_tx, iroh_events, replicator_registry, host_task) =
            p2p::iroh::spawn_endpoint(p2p::iroh::IrohEndpointConfig {
                secret_key: iroh_secret_key.clone(),
                node_identity,
                relay_mode: Self::iroh_relay_mode(config)?,
                discovery: Self::iroh_discovery(config)?,
                bind_port: config.net.iroh_bind_port,
                bind_addr: config.net.iroh_bind_addr,
                max_concurrent_multipath_paths: config.net.iroh_max_concurrent_multipath_paths,
                gossip_heal: p2p::iroh::GossipHealConfig::from_env(),
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
        let classifier = defra_p2p_adapter::DbBlockClassifier::new_arc(database.clone());
        let serve_acp = Arc::new(p2p::bitswap::LateBoundServeAcp::new());

        let (mut coordinator, sync_events) =
            p2p::sync::SyncCoordinator::with_head_provider_and_serve_gate(
                transport.clone(),
                sync_blockstore,
                Self::sync_config(config),
                Self::access_mode(config),
                replicator_registry,
                collection_store,
                head_provider,
                std::sync::Arc::new(replication_filter::QueryReplicationFilterMatcher::new()),
                classifier,
                serve_acp.clone(),
            )
            .await
            .map_err(Error::P2P)?;

        let failure_rx = db::merge::attach_failure_channel(&mut coordinator, 1024);
        let coordinator = Arc::new(coordinator);
        coordinator
            .install_pending_dag_store(Arc::new(p2p::sync::PendingDagStore::new(store.clone())))
            .await;
        let coordinator_for_acp = coordinator.clone();
        let serve_acp_for_acp = serve_acp.clone();

        // Build the KMS pubsub transport and install it on the coordinator so
        // raw gossip on the encryption topic is routed to it (mirrors
        // crates/embedded/src/node_p2p.rs::setup_iroh).
        let local_peer_id = transport.local_peer_id().to_string();
        let kms_transport =
            p2p::kms::PubsubKeyTransport::new(transport.clone(), Arc::new(p2p::AnonymousResolver))
                .await
                .map_err(|e| Error::Server(format!("failed to create KMS transport: {e}")))?;
        coordinator.install_kms_transport(kms_transport.clone());

        match db::merge::load_persisted_collections(&coordinator).await {
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
        let replication = db::merge::create_replication_stack_with_max_merge_depth(
            database.clone(),
            merge_blockstore,
            coordinator.clone(),
            config.datastore.max_merge_depth,
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
                    .set_se_options(db::merge::BroadcastSeOptions {
                        encryption_key: Some(zeroize::Zeroizing::new(key.to_vec())),
                        identity_pubkey: None,
                    })
            {
                warn!(error = %e, "failed to set searchable encryption options on broadcast mutator");
            }
            replication.merge_handler_inner.set_se_enc_key(key.to_vec());
        }

        let coordinator_for_replication = coordinator.clone();
        let rebroadcast_on_merge = config.net.p2p_rebroadcast_on_merge;
        let replication_task = tokio::spawn(async move {
            info!("Starting replication loop for P2P sync (iroh)");
            p2p::sync::ReplicationLoop::run(
                coordinator_for_replication,
                sync_events,
                merge_handler_for_loop,
                p2p::sync::ReplicationConfig {
                    continue_on_error: true,
                    rebroadcast_on_merge,
                    batch_size: 50,
                },
                |_| {},
            )
            .await;
            info!("Replication loop stopped (iroh)");
        });

        // Started here, not earlier, for two reasons (#1309). These are managed
        // tasks: nothing can drain them until the caller owns a shutdown handle,
        // so spawning them before the last fallible step above would leak both
        // sweeps (and through them the store) on any early return. And the sync
        // event channel only has its consumer once the replication loop above is
        // running, which is the same ordering defra-node's setup_p2p documents.
        let coordinator_for_restore = coordinator.clone();
        coordinator.spawn_background_task("pending_dag_resync", async move {
            coordinator_for_restore
                .run_pending_dag_resync(std::time::Duration::from_secs(60))
                .await;
        });

        // Receiver's re-arm loop (#1116 stage 2): dispatches due pending
        // roots at a tight cadence. Sibling of the resync sweep above.
        let coordinator_for_retry_clock = coordinator.clone();
        coordinator.spawn_background_task("pending_dag_retry_clock", async move {
            coordinator_for_retry_clock
                .run_pending_dag_retry_clock(std::time::Duration::from_secs(2))
                .await;
        });

        let coordinator_for_events = coordinator.clone();
        let event_bus_for_handler = event_bus.clone();
        let se_store = store.clone();
        let se_transport_serve = transport.clone();
        let se_correlator_for_events = se_correlator.clone();
        let se_event_bus = event_bus.clone();
        let event_handler_task = Some(tokio::spawn(async move {
            let handler_coordinator = coordinator_for_events.clone();
            coordinator_for_events.run_event_dispatcher(
                iroh_events,
                move |event, admission| {
                let coordinator_for_events = handler_coordinator.clone();
                let event_bus_for_handler = event_bus_for_handler.clone();
                let se_store = se_store.clone();
                let se_transport_serve = se_transport_serve.clone();
                let se_correlator_for_events = se_correlator_for_events.clone();
                let se_event_bus = se_event_bus.clone();
                let manage_hooks_for_events = manage_hooks_for_events.clone();
                async move {
                // SE events: store inbound artifacts and serve/route SE queries
                // over the iroh transport (mirrors the libp2p loop, #976). Rust
                // -> Rust artifact push is fire-and-forget, so use the no-ack
                // `handle_artifacts_received` (Go -> Rust over iroh, which
                // expects a PushSEArtifactsReply ack, is a follow-up).
                if admission == p2p::sync::DispatchAdmission::Saturated {
                    if let Err(error) = coordinator_for_events
                        .handle_transport_event_with_admission(event, admission)
                        .await
                    {
                        tracing::debug!(%error, "rejected saturated CLI Iroh request");
                    }
                    return;
                }
                let event = match event {
                    p2p::TransportEvent::SEArtifactsReceived { peer_id, data } => {
                        let doc_ids = db::merge::se::serve::handle_artifacts_received(
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
                        return;
                    }
                    p2p::TransportEvent::SEQueryRequest { peer_id, request } => {
                        db::merge::se::serve::handle_query_request(
                            se_store.as_ref(),
                            &se_transport_serve,
                            peer_id,
                            request,
                        )
                        .await;
                        return;
                    }
                    p2p::TransportEvent::SEQueryReply { reply, .. } => {
                        se_correlator_for_events.deliver(reply);
                        return;
                    }
                    p2p::TransportEvent::ManageRequest { peer_id, request } => {
                        if let Some(hooks) = manage_hooks_for_events.get() {
                            defra_p2p_adapter::manage::serve::serve_manage_request(
                                hooks,
                                &se_transport_serve,
                                &peer_id,
                                request,
                            )
                            .await;
                        } else {
                            tracing::debug!(%peer_id, "manage request before hooks ready; dropping");
                        }
                        return;
                    }
                    p2p::TransportEvent::ManageQueryRequest { peer_id, request } => {
                        if let Some(hooks) = manage_hooks_for_events.get() {
                            defra_p2p_adapter::manage::serve::serve_manage_query_request(
                                hooks,
                                &se_transport_serve,
                                &peer_id,
                                request,
                            )
                            .await;
                        } else {
                            tracing::debug!(%peer_id, "manage query request before hooks ready; dropping");
                        }
                        return;
                    }
                    p2p::TransportEvent::ManageReply { reply, .. } => {
                        if let Some(hooks) = manage_hooks_for_events.get() {
                            hooks.correlator.deliver(reply);
                        }
                        return;
                    }
                    p2p::TransportEvent::ManageQueryReply { reply, .. } => {
                        if let Some(hooks) = manage_hooks_for_events.get() {
                            hooks.query_correlator.deliver(reply);
                        }
                        return;
                    }
                    other => other,
                };
                match &event {
                    p2p::TransportEvent::PeerConnected(peer) => {
                        info!("Peer connected (iroh): {}", peer);
                        defra_p2p_adapter::activate_retry_peer(se_store.clone(), peer).await;
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

                if let Err(e) = coordinator_for_events
                    .handle_transport_event_with_admission(event, admission)
                    .await
                {
                    error!("Failed to handle iroh event: {}", e);
                }
                }
                },
            )
            .await;
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
            coordinator.head_hint_car_authority(),
        ));
        let doc_pusher_for_acp = doc_pusher_impl.clone();
        let doc_pusher: Arc<dyn crate::transport_doc_pusher::TransportDocPusher> = doc_pusher_impl;

        let failure_recorder_task =
            defra_p2p_adapter::spawn_failure_recorder(store.clone(), failure_rx);
        let se_repusher: Arc<dyn db::merge::SeArtifactRepusher> =
            replication.broadcast_mutator.clone();
        let retry_loop_task = defra_p2p_adapter::spawn_retry_loop(
            store.clone(),
            transport.clone(),
            doc_pusher.clone(),
            Some(se_repusher),
        );

        let restore_peerstore = storage::stores::Peerstore::new(store);
        match restore_peerstore.list_replicators().await {
            Ok(entries) => {
                for (_peer_id_str, data) in entries {
                    if let Ok(rep_info) = p2p::ReplicatorInfo::from_bytes(&data) {
                        let pid = p2p::transport::PeerId::new(rep_info.peer_id_str().to_string());
                        let _ = coordinator
                            .create_replicator_info(&pid, rep_info.clone(), false)
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
            db::node_access_checker(database.clone()),
        );
        adapter.set_initial_tracked_documents(restored_doc_ids);

        info!("P2P sync coordinator initialized (iroh)");

        let manage_controller: Arc<dyn defra_http::router::P2POperations> = Arc::new(adapter);

        // Build the SE remote query transport over iroh so encrypted queries
        // fan out to replicators (owner-queries-replicator, #976). Identity is
        // None to match the write side (iroh SE options use identity_pubkey:
        // None), so write-tags and query-tags agree.
        let se_transport: Option<Arc<dyn query::SeQueryTransport>> = se_key.map(|key| {
            Arc::new(db::merge::DbMergeSeQueryTransport::new(
                transport.clone(),
                se_correlator,
                se_replicator_registry,
                db::merge::filled_se_key_handle(key, None),
            )) as Arc<dyn query::SeQueryTransport>
        });

        // Outbound management requester over the same iroh transport, sharing the
        // requester-side manage correlators (Task 7a).
        let manage_requester: Arc<dyn defra_http::router::ManageRequester> =
            Arc::new(defra_p2p_adapter::manage::client::ManageClient::new(
                transport.clone(),
                manage_correlator.clone(),
                manage_query_correlator.clone(),
            ));

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
                serve_acp_for_acp.set(p2p::bitswap::ServeAcp {
                    resolver: Arc::new(p2p::IrohPeerIdentityResolver::new(transport.clone())),
                    gate: defra_p2p_adapter::DbBlockReadGate::new_arc(acp.clone()),
                });
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
            manage_requester: Some(manage_requester),
        })
    }
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

    fn iroh_relay_urls(config: &Config) -> Vec<String> {
        let mut urls = config.net.iroh_relay_urls.clone();
        if let Some(url) = &config.net.iroh_relay_url {
            urls.push(url.clone());
        }
        urls
    }

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
