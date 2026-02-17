use std::ffi::c_char;
use std::str::FromStr;
use std::sync::Arc;

use identity::Identity;

use blockstore::DefraBlockstore;
use p2p::bitswap::BitswapStoreAdapter;
use p2p::sync::{
    PushFailure, ReplicationConfig, ReplicationLoop, ReplicationResult, SyncConfig, SyncCoordinator,
};
use p2p::topics::DefraTopic;
use p2p::P2PHost;
use p2p::ReplicatorInfo;
use storage::stores::Peerstore;

use crate::state::{FfiStore, NodeState, P2PState, PolicyStore, NODES};
use crate::types::{c_str_to_string, NewNodeResult, NodeInitOptions};

/// Create a new DefraDB node with P2P enabled.
///
/// This creates an in-memory database instance with P2P networking.
/// The node will listen on the specified address for peer connections.
///
/// # Arguments
///
/// * `options` - Node initialization options
/// * `listen_addr` - P2P multiaddr to listen on (e.g., "/ip4/127.0.0.1/tcp/9171")
///
/// # Safety
///
/// * `listen_addr` must be a valid null-terminated UTF-8 string
/// * The returned `node_ptr` must be freed by calling `node_close`
#[no_mangle]
pub unsafe extern "C" fn new_node_with_p2p(
    options: NodeInitOptions,
    listen_addr: *const c_char,
) -> NewNodeResult {
    let rt = match crate::runtime::RUNTIME.get() {
        Some(rt) => rt,
        None => return NewNodeResult::error("runtime not initialized - call defra_init() first"),
    };

    let listen_addr_str = match c_str_to_string(listen_addr) {
        Some(s) => s,
        None => return NewNodeResult::error("listen_addr is null"),
    };

    let enable_signing = options.enable_signing != 0;

    let result = rt.block_on(async {
        let db_path_opt: Option<String>;
        let store: Arc<FfiStore> = if options.in_memory == 0 && !options.db_path.is_null() {
            let path = c_str_to_string(options.db_path)
                .ok_or_else(|| "db_path is not valid UTF-8".to_string())?;
            let redb = storage::RedbStore::open(&path)
                .map_err(|e| format!("failed to open redb store at '{}': {}", path, e))?;
            db_path_opt = Some(path);
            Arc::new(FfiStore::Redb(redb))
        } else {
            db_path_opt = None;
            Arc::new(FfiStore::Memory(storage::MemoryStore::new()))
        };

        let event_bus: Arc<dyn events::Bus> = Arc::new(events::ChannelBus::default());

        let (raw_identity_opt, node_identity_did) = if enable_signing {
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

            defra_core::signing::store_identity(
                &did_str,
                defra_core::signing::SigningConfig {
                    key_type,
                    private_key_bytes: raw_identity.private_key_bytes().to_vec(),
                    public_key_bytes: raw_identity.public_key_bytes().to_vec(),
                    public_key_hex: hex::encode(raw_identity.public_key_bytes()),
                },
            );

            tracing::debug!(did = %did_str, "node identity created");
            (Some(raw_identity), Some(did_str))
        } else {
            (None, None)
        };

        let mut db_options = db::DbOptions::default();
        if let Some(raw_id) = raw_identity_opt {
            db_options = db_options.with_node_identity(raw_id);
        }

        let mut database = db::DB::open_from_arc_with_options(store.clone(), db_options)
            .await
            .map_err(|e| format!("failed to open database: {}", e))?;

        database.set_event_bus(event_bus.clone());
        let database = Arc::new(database);

        let blockstore = Arc::new(DefraBlockstore::new(store.clone(), true));
        let bitswap_store = BitswapStoreAdapter::new(blockstore.clone());

        let p2p_keypair = {
            let ps = Peerstore::new(store.clone());
            let key_id = "__local_p2p_identity__";
            match ps.get_replicator(key_id).await {
                Ok(Some(bytes)) => {
                    match libp2p::identity::Keypair::from_protobuf_encoding(&bytes) {
                        Ok(kp) => kp,
                        Err(_) => {
                            let kp = libp2p::identity::Keypair::generate_ed25519();
                            let _ = ps.create_replicator(key_id, &kp.to_protobuf_encoding().unwrap_or_default()).await;
                            kp
                        }
                    }
                }
                _ => {
                    let kp = libp2p::identity::Keypair::generate_ed25519();
                    if let Ok(encoded) = kp.to_protobuf_encoding() {
                        let _ = ps.create_replicator(key_id, &encoded).await;
                    }
                    kp
                }
            }
        };

        let (host, handle, event_rx, _replicator_registry) =
            P2PHost::with_keypair(p2p_keypair, bitswap_store.clone())
                .await
                .map_err(|e| format!("failed to create P2P host: {}", e))?;

        tokio::spawn(async move {
            host.run().await;
        });

        let addr: libp2p::Multiaddr = listen_addr_str
            .parse()
            .map_err(|e| format!("invalid multiaddr '{}': {}", listen_addr_str, e))?;

        let rust_peer_id = handle.local_peer_id().await.map_err(|e| format!("peer id: {}", e))?;
        tracing::info!(peer_id = %rust_peer_id, listen_addr = %listen_addr_str, "P2P host created");

        handle
            .listen(addr)
            .await
            .map_err(|e| format!("failed to start listening: {}", e))?;

        for topic in &[
            DefraTopic::DocSync,
            DefraTopic::Encryption,
            DefraTopic::Custom("sync-branchable".to_string()),
        ] {
            if let Err(e) = handle.subscribe(topic.clone()).await {
                tracing::warn!(topic = %topic, error = %e, "Failed to subscribe to default topic");
            }
        }

        let head_provider: Arc<dyn p2p::sync::DocumentHeadProvider> =
            Arc::new(db::DbHeadProvider::new(database.clone()));

        let (mut coordinator, sync_events_rx) = SyncCoordinator::with_head_provider(
            handle.clone(),
            blockstore.clone(),
            SyncConfig::default(),
            p2p::bitswap::AccessMode::Open,
            Arc::new(p2p::bitswap::ReplicatorRegistry::new()),
            Arc::new(p2p::sync::NoOpCollectionStorage),
            head_provider,
        )
        .await
        .map_err(|e| format!("failed to create sync coordinator: {}", e))?;

        let (failure_tx, failure_rx) = tokio::sync::mpsc::unbounded_channel::<PushFailure>();
        coordinator.set_failure_channel(failure_tx);
        let coordinator = Arc::new(coordinator);

        let merge_handler = Arc::new(db::DbMergeHandler::new(
            database.clone(),
            blockstore.clone(),
        ));

        let coord_for_events = coordinator.clone();
        let event_bus_for_host = event_bus.clone();
        let host_event_task = tokio::spawn(async move {
            let mut rx = event_rx;
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(32));
            while let Some(event) = rx.recv().await {
                match &event {
                    p2p::HostEvent::PeerSubscribed { peer_id, topic } => {
                        event_bus_for_host.publish(events::Message::topic_peer_event(
                            events::TopicPeerEventData {
                                peer_id: peer_id.to_string(),
                                topic: topic.clone(),
                                event_type: "JOINED".to_string(),
                            },
                        ));
                    }
                    p2p::HostEvent::PeerUnsubscribed { peer_id, topic } => {
                        event_bus_for_host.publish(events::Message::topic_peer_event(
                            events::TopicPeerEventData {
                                peer_id: peer_id.to_string(),
                                topic: topic.clone(),
                                event_type: "LEFT".to_string(),
                            },
                        ));
                    }
                    _ => {}
                }
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let coord = coord_for_events.clone();
                tokio::spawn(async move {
                    if let Err(e) = coord.handle_host_event(event).await {
                        tracing::error!(error = %e, "Error handling host event");
                    }
                    drop(permit);
                });
            }
        });

        let coord_for_repl = coordinator.clone();
        let handler_for_repl = merge_handler.clone();
        let event_bus_for_repl = event_bus.clone();
        let replication_task = tokio::spawn(async move {
            let local_peer = coord_for_repl.local_peer_id().to_string();
            let ebus = event_bus_for_repl;
            ReplicationLoop::run_parallel(
                coord_for_repl,
                sync_events_rx,
                handler_for_repl,
                ReplicationConfig::default(),
                move |result| {
                    match &result {
                        ReplicationResult::Merged { cid, doc_id, collection_id } => {
                            ebus.publish(events::Message::merge_complete(events::MergeCompleteData {
                                doc_id: doc_id.clone(),
                                cid: *cid,
                                collection_id: collection_id.clone(),
                                by_peer: local_peer.clone(),
                            }));
                            if !doc_id.is_empty() {
                                ebus.publish(events::Message::se_artifact_received(
                                    events::SEArtifactReceivedData { doc_id: doc_id.clone() },
                                ));
                            }
                        }
                        ReplicationResult::MergedButBroadcastFailed { cid, doc_id, collection_id, .. } => {
                            tracing::info!(cid = %cid, doc_id = %doc_id, "Block merged (broadcast failed) - publishing MergeComplete event");
                            ebus.publish(events::Message::merge_complete(events::MergeCompleteData {
                                doc_id: doc_id.clone(),
                                cid: *cid,
                                collection_id: collection_id.clone(),
                                by_peer: local_peer.clone(),
                            }));
                            if !doc_id.is_empty() {
                                ebus.publish(events::Message::se_artifact_received(
                                    events::SEArtifactReceivedData { doc_id: doc_id.clone() },
                                ));
                            }
                        }
                        ReplicationResult::Failed { cid, error } => {
                            tracing::error!(cid = %cid, error = %error, "Block merge failed");
                        }
                        ReplicationResult::Skipped { cid, reason } => {
                            tracing::debug!(cid = %cid, reason = %reason, "replication loop: skipped");
                        }
                        ReplicationResult::BitswapFetchStarted { root_cid, .. } => {
                            tracing::debug!(root_cid = %root_cid, "replication loop: bitswap fetch started");
                        }
                        _ => {}
                    }
                },
            ).await;
            tracing::info!("FFI replication loop stopped");
        });

        let mut p2p_state = P2PState::new(
            handle.clone(),
            blockstore.clone(),
            merge_handler.clone(),
            host_event_task.abort_handle(),
            replication_task.abort_handle(),
        );

        let recorder_store = store.clone();
        let failure_recorder_task = tokio::spawn(async move {
            let mut rx = failure_rx;
            while let Some(failure) = rx.recv().await {
                tracing::debug!(peer_id = %failure.peer_id, doc_id = %failure.doc_id, collection_id = %failure.collection_id, "push failure recorded");
                let peerstore = Peerstore::new(recorder_store.clone());
                let retry_info = storage::stores::RetryInfo::new_initial();
                let info_bytes = match retry_info.to_bytes() {
                    Ok(b) => b,
                    Err(e) => { tracing::warn!(error = %e, "Failed to serialize RetryInfo"); continue; }
                };
                if let Err(e) = peerstore.record_push_failure(&failure.peer_id.to_string(), &failure.doc_id, &failure.collection_id, &info_bytes).await {
                    tracing::warn!(peer_id = %failure.peer_id, doc_id = %failure.doc_id, error = %e, "Failed to record push failure");
                } else {
                    tracing::debug!(peer_id = %failure.peer_id, doc_id = %failure.doc_id, "push failure persisted");
                }
            }
        });
        p2p_state.failure_recorder_handle = Some(failure_recorder_task.abort_handle());

        let retry_store = store.clone();
        let retry_handle = handle.clone();
        let retry_db = database.clone();
        let retry_loop_task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                let peerstore = Peerstore::new(retry_store.clone());
                let peers = match peerstore.get_all_retry_peers().await { Ok(p) => p, Err(_) => continue };
                if !peers.is_empty() { tracing::debug!(peer_count = peers.len(), "retry loop: found retry peers"); }
                for (peer_id_str, info_bytes) in peers {
                    let mut retry_info = match storage::stores::RetryInfo::from_bytes(&info_bytes) {
                        Ok(i) => i,
                        Err(e) => { tracing::warn!(peer_id = %peer_id_str, error = %e, "retry loop: failed to parse RetryInfo"); continue; }
                    };
                    if !retry_info.is_due() { continue; }
                    let peer_id = match libp2p::PeerId::from_str(&peer_id_str) {
                        Ok(p) => p,
                        Err(e) => { tracing::warn!(peer_id = %peer_id_str, error = %e, "retry loop: invalid peer ID"); continue; }
                    };
                    let connected = retry_handle.connected_peers().await.unwrap_or_default();
                    if !connected.contains(&peer_id) { continue; }
                    let docs = match peerstore.get_retry_doc_ids(&peer_id_str).await { Ok(d) => d, Err(_) => continue };
                    if docs.is_empty() { let _ = peerstore.clear_retry_peer(&peer_id_str).await; continue; }
                    tracing::debug!(doc_count = docs.len(), peer_id = %peer_id_str, "retry loop: retrying docs");
                    let mut all_succeeded = true;
                    for (doc_id, collection_id) in &docs {
                        match db::retry_doc(&retry_handle, &retry_db, peer_id, doc_id, collection_id).await {
                            Ok(()) => { tracing::debug!(doc_id = %doc_id, peer_id = %peer_id_str, "retry loop: doc push succeeded"); let _ = peerstore.remove_retry_doc(&peer_id_str, doc_id).await; }
                            Err(e) => { tracing::warn!(doc_id = %doc_id, peer_id = %peer_id_str, error = %e, "retry loop: doc push failed"); all_succeeded = false; }
                        }
                    }
                    if all_succeeded {
                        tracing::debug!(peer_id = %peer_id_str, "retry loop: all docs succeeded");
                        let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                    } else {
                        retry_info.bump();
                        if let Ok(bytes) = retry_info.to_bytes() { let _ = peerstore.update_retry_info(&peer_id_str, &bytes).await; }
                    }
                }
            }
        });
        p2p_state.retry_loop_handle = Some(retry_loop_task.abort_handle());

        let p2p_state = Arc::new(p2p_state);

        let peerstore = Peerstore::new(store.clone());
        match peerstore.list_replicators().await {
            Ok(entries) => {
                tracing::debug!(count = entries.len(), "loading stored replicators");
                for (peer_id_str, data) in entries {
                    match ReplicatorInfo::from_bytes(&data) {
                        Ok(info) => {
                            if let Some(peer_id) = info.peer_id() {
                                tracing::debug!(peer_id = %peer_id, collections = ?info.collections, "restoring replicator");
                                if let Err(e) = handle.create_replicator(peer_id, info.collections.clone()).await {
                                    tracing::warn!(error = %e, "failed to restore replicator");
                                    continue;
                                }
                                for collection_id in &info.collections {
                                    let topic = DefraTopic::collection(collection_id);
                                    if let Err(e) = handle.subscribe(topic).await {
                                        tracing::warn!(collection_id = %collection_id, error = %e, "failed to subscribe replicator topic");
                                    }
                                }
                            }
                        }
                        Err(e) => { tracing::warn!(peer_id = %peer_id_str, error = %e, "failed to deserialize replicator info"); }
                    }
                }
            }
            Err(e) => { tracing::warn!(error = %e, "failed to load replicators from storage"); }
        }

        {
            let peerstore = Peerstore::new(store.clone());
            if let Ok(collections) = peerstore.load_collections().await {
                tracing::debug!(count = collections.len(), "restoring collection subscriptions");
                for name in &collections {
                    if let Ok(Some(col)) = database.get_collection(name) {
                        let collection_id = col.collection_id().to_string();
                        let topic = DefraTopic::collection(&collection_id);
                        if let Err(e) = handle.subscribe(topic).await {
                            tracing::warn!(collection = %name, error = %e, "failed to restore collection subscription");
                        }
                    }
                }
            }
        }

        {
            let peerstore = Peerstore::new(store.clone());
            if let Ok(doc_ids) = peerstore.load_documents().await {
                tracing::debug!(count = doc_ids.len(), "restoring document subscriptions");
                for doc_id in &doc_ids {
                    let topic = DefraTopic::document(doc_id);
                    if let Err(e) = handle.subscribe(topic).await {
                        tracing::warn!(doc_id = %doc_id, error = %e, "failed to restore document subscription");
                    }
                }
            }
        }

        let fetcher = db::LensedAutoCommitFetcher::new(database.clone());
        let collection_provider: Arc<dyn query::CollectionProvider> =
            db::DbCollectionProvider::new_arc(database.clone());
        let registry = Arc::new(db::DbTransactionRegistry::new(database.clone()));
        let mutator: Arc<dyn query::DocMutator> = Arc::new(db::BroadcastMutator::new(
            database.clone(),
            coordinator.clone(),
        ));

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
                unsafe { std::slice::from_raw_parts(options.sourcehub_signer_key, options.sourcehub_signer_key_len) }
            } else {
                return Err("sourcehub_signer_key is required when SourceHub is configured".to_string());
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
            (Arc::new(acp::LocalDocumentACP::new(acp_store)) as Arc<dyn acp::DocumentACP>, None)
        } else {
            let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());
            (Arc::new(acp::LocalDocumentACP::new(acp_store)) as Arc<dyn acp::DocumentACP>, None)
        };

        merge_handler.set_document_acp(document_acp.clone());

        let nac_manager: Arc<dyn db::NacManagerApi> = if db_path_opt.is_some() {
            // File-based storage: use persistent NAC store (namespace isolated in main DB)
            let nac_store = Arc::new(acp::PersistentZanzibarStore::from_store(store.clone()));
            let nac_config = db::NacConfig::new().with_dev_mode();
            let mgr = Arc::new(db::NacManager::new(nac_store, nac_config));
            mgr.initialize(None).await.map_err(|e| format!("failed to initialize NAC from persistent store: {}", e))?;
            mgr as Arc<dyn db::NacManagerApi>
        } else {
            let nac_store = Arc::new(acp::MemoryZanzibarStore::new());
            let nac_config = db::NacConfig::new().with_dev_mode();
            Arc::new(db::NacManager::new(nac_store, nac_config))
        };

        let encryption_key = b"examplekey1234567890examplekey12".to_vec();
        let query_runner = query::QueryRunner::with_arc_registry_and_provider(
            fetcher, collection_provider, registry.clone(),
        )
        .with_mutator(mutator)
        .with_acp(document_acp.clone())
        .with_encryption_key(encryption_key)
        .with_lens_store(database.lens_store().clone());

        let runner: Arc<dyn query::QueryExecutor> = Arc::new(query_runner);
        let policy_store = Arc::new(PolicyStore::new());

        let state = NodeState {
            database,
            txn_registry: registry,
            query_runner: runner,
            nac_manager,
            document_acp,
            event_bus,
            policy_store,
            p2p: Some(p2p_state),
            node_identity_did,
            sourcehub_acp,
            se_encryption_key: None,
        };

        let handle = NODES.insert(state);
        Ok::<usize, String>(handle)
    });

    match result {
        Ok(handle) => NewNodeResult::success(handle),
        Err(e) => NewNodeResult::error(e),
    }
}
