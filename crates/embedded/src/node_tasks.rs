use std::sync::Arc;

use p2p::sync::{PushFailure, ReplicationConfig, ReplicationLoop, ReplicationResult};
#[cfg(feature = "iroh")]
use p2p::P2PTransport;

use crate::node::EmbeddedMergeHandler;

pub struct BackgroundTasks {
    downsample_task: Option<tokio::task::JoinHandle<()>>,
}

impl BackgroundTasks {
    pub(crate) fn new(downsample_task: Option<tokio::task::JoinHandle<()>>) -> Self {
        Self { downsample_task }
    }
}

impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        if let Some(task) = self.downsample_task.take() {
            task.abort();
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_libp2p_event_handler<B: blockstore::Blockstore + 'static>(
    mut events: tokio::sync::mpsc::Receiver<p2p::HostEvent>,
    coordinator: Arc<p2p::sync::Libp2pSyncCoordinator<B>>,
    store: Arc<impl storage::corekv::Store + 'static>,
    event_bus: Arc<dyn events::Bus>,
    handle: p2p::P2PHostHandle,
    se_correlator: p2p::SeQueryCorrelator,
    manage_hooks: defra_p2p_adapter::manage::hooks::ManageHooksCell,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
        while let Some(event) = events.recv().await {
            match &event {
                p2p::HostEvent::PeerSubscribed { peer_id, topic } => {
                    event_bus.publish(events::Message::topic_peer_event(
                        events::TopicPeerEventData {
                            peer_id: peer_id.to_string(),
                            topic: topic.clone(),
                            event_type: "JOINED".to_string(),
                        },
                    ));
                }
                p2p::HostEvent::PeerUnsubscribed { peer_id, topic } => {
                    event_bus.publish(events::Message::topic_peer_event(
                        events::TopicPeerEventData {
                            peer_id: peer_id.to_string(),
                            topic: topic.clone(),
                            event_type: "LEFT".to_string(),
                        },
                    ));
                }
                _ => {}
            }

            let transport_event = match p2p::convert_host_event(event) {
                p2p::TransportEvent::SEArtifactsReceived { peer_id, data } => {
                    if let Ok(pid) = peer_id.as_str().parse::<libp2p::PeerId>() {
                        // Stores artifacts AND sends the signed ack Go's push waits for.
                        let doc_ids = db_merge::se::serve::handle_artifacts_push(
                            store.as_ref(),
                            &handle,
                            pid,
                            &data,
                        )
                        .await;
                        for doc_id in doc_ids {
                            event_bus.publish(events::Message::se_artifact_received(
                                events::SEArtifactReceivedData { doc_id },
                            ));
                        }
                    } else {
                        handle_se_artifacts_received(
                            store.clone(),
                            event_bus.clone(),
                            peer_id.to_string(),
                            data,
                        )
                        .await;
                    }
                    continue;
                }
                p2p::TransportEvent::SEQueryRequest { peer_id, request } => {
                    let transport = p2p::Libp2pTransport::new(handle.clone());
                    db_merge::se::serve::handle_query_request(
                        store.as_ref(),
                        &transport,
                        peer_id,
                        request,
                    )
                    .await;
                    continue;
                }
                p2p::TransportEvent::SEQueryReply { reply, .. } => {
                    se_correlator.deliver(reply);
                    continue;
                }
                p2p::TransportEvent::ManageRequest { peer_id, request } => {
                    if let Some(hooks) = manage_hooks.get() {
                        let transport = p2p::Libp2pTransport::new(handle.clone());
                        defra_p2p_adapter::manage::serve::serve_manage_request(
                            hooks, &transport, &peer_id, request,
                        )
                        .await;
                    } else {
                        tracing::debug!(%peer_id, "manage request before hooks ready; dropping");
                    }
                    continue;
                }
                p2p::TransportEvent::ManageQueryRequest { peer_id, request } => {
                    if let Some(hooks) = manage_hooks.get() {
                        let transport = p2p::Libp2pTransport::new(handle.clone());
                        defra_p2p_adapter::manage::serve::serve_manage_query_request(
                            hooks, &transport, &peer_id, request,
                        )
                        .await;
                    } else {
                        tracing::debug!(%peer_id, "manage query request before hooks ready; dropping");
                    }
                    continue;
                }
                p2p::TransportEvent::ManageReply { reply, .. } => {
                    if let Some(hooks) = manage_hooks.get() {
                        hooks.correlator.deliver(reply);
                    }
                    continue;
                }
                p2p::TransportEvent::ManageQueryReply { reply, .. } => {
                    if let Some(hooks) = manage_hooks.get() {
                        hooks.query_correlator.deliver(reply);
                    }
                    continue;
                }
                other => other,
            };
            if transport_event.requires_inline_ordering() {
                if let Err(error) = coordinator.handle_transport_event(transport_event).await {
                    tracing::error!(error = %error, "error handling libp2p event");
                }
                continue;
            }

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let coordinator = coordinator.clone();
            tokio::spawn(async move {
                if let Err(error) = coordinator.handle_transport_event(transport_event).await {
                    tracing::error!(error = %error, "error handling libp2p event");
                }
                drop(permit);
            });
        }
    })
}

#[cfg(feature = "iroh")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_iroh_event_handler<B: blockstore::Blockstore + 'static>(
    mut events: tokio::sync::mpsc::Receiver<
        p2p::TransportEvent<<p2p::iroh::IrohTransport as P2PTransport>::ResponseToken>,
    >,
    coordinator: Arc<p2p::sync::IrohSyncCoordinator<B>>,
    store: Arc<impl storage::corekv::Store + 'static>,
    event_bus: Arc<dyn events::Bus>,
    se_correlator: p2p::SeQueryCorrelator,
    se_transport: p2p::iroh::IrohTransport,
    manage_hooks: defra_p2p_adapter::manage::hooks::ManageHooksCell,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
        while let Some(event) = events.recv().await {
            match &event {
                p2p::TransportEvent::PeerSubscribed { peer_id, topic } => {
                    event_bus.publish(events::Message::topic_peer_event(
                        events::TopicPeerEventData {
                            peer_id: peer_id.to_string(),
                            topic: topic.clone(),
                            event_type: "JOINED".to_string(),
                        },
                    ));
                }
                p2p::TransportEvent::PeerUnsubscribed { peer_id, topic } => {
                    event_bus.publish(events::Message::topic_peer_event(
                        events::TopicPeerEventData {
                            peer_id: peer_id.to_string(),
                            topic: topic.clone(),
                            event_type: "LEFT".to_string(),
                        },
                    ));
                }
                _ => {}
            }

            let event = match event {
                p2p::TransportEvent::SEArtifactsReceived { peer_id, data } => {
                    handle_se_artifacts_received(
                        store.clone(),
                        event_bus.clone(),
                        peer_id.to_string(),
                        data,
                    )
                    .await;
                    continue;
                }
                p2p::TransportEvent::SEQueryRequest { peer_id, request } => {
                    // Serve SE queries over iroh: byte-match the pushed artifacts
                    // and return a signed reply (mirrors the libp2p loop, #976).
                    db_merge::se::serve::handle_query_request(
                        store.as_ref(),
                        &se_transport,
                        peer_id,
                        request,
                    )
                    .await;
                    continue;
                }
                p2p::TransportEvent::SEQueryReply { reply, .. } => {
                    // Deliver inbound replies so the owner/querier transport's
                    // awaiting correlator slot resolves (#976).
                    se_correlator.deliver(reply);
                    continue;
                }
                p2p::TransportEvent::ManageRequest { peer_id, request } => {
                    if let Some(hooks) = manage_hooks.get() {
                        defra_p2p_adapter::manage::serve::serve_manage_request(
                            hooks,
                            &se_transport,
                            &peer_id,
                            request,
                        )
                        .await;
                    } else {
                        tracing::debug!(%peer_id, "manage request before hooks ready; dropping");
                    }
                    continue;
                }
                p2p::TransportEvent::ManageQueryRequest { peer_id, request } => {
                    if let Some(hooks) = manage_hooks.get() {
                        defra_p2p_adapter::manage::serve::serve_manage_query_request(
                            hooks,
                            &se_transport,
                            &peer_id,
                            request,
                        )
                        .await;
                    } else {
                        tracing::debug!(%peer_id, "manage query request before hooks ready; dropping");
                    }
                    continue;
                }
                p2p::TransportEvent::ManageReply { reply, .. } => {
                    if let Some(hooks) = manage_hooks.get() {
                        hooks.correlator.deliver(reply);
                    }
                    continue;
                }
                p2p::TransportEvent::ManageQueryReply { reply, .. } => {
                    if let Some(hooks) = manage_hooks.get() {
                        hooks.query_correlator.deliver(reply);
                    }
                    continue;
                }
                other => other,
            };
            if event.requires_inline_ordering() {
                if let Err(error) = coordinator.handle_transport_event(event).await {
                    tracing::error!(error = %error, "error handling iroh event");
                }
                continue;
            }

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let coordinator = coordinator.clone();
            tokio::spawn(async move {
                if let Err(error) = coordinator.handle_transport_event(event).await {
                    tracing::error!(error = %error, "error handling iroh event");
                }
                drop(permit);
            });
        }
    })
}

async fn handle_se_artifacts_received<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    event_bus: Arc<dyn events::Bus>,
    peer_id: String,
    data: Vec<u8>,
) {
    let mut txn = match store.new_txn(false).await {
        Ok(txn) => txn,
        Err(error) => {
            tracing::warn!(peer_id = %peer_id, error = %error, "failed to create SE artifact transaction");
            return;
        }
    };

    let result = match db_merge::se::receive_and_store(&mut txn, &data).await {
        Ok(result) => result,
        Err(error) => {
            tracing::warn!(peer_id = %peer_id, error = %error, "failed to receive SE artifacts");
            return;
        }
    };

    if let Err(error) = txn.commit().await {
        tracing::warn!(peer_id = %peer_id, error = %error, "failed to commit SE artifacts");
        return;
    }

    tracing::debug!(
        peer_id = %peer_id,
        collection_id = %result.collection_id,
        stored = result.stored,
        rejected = result.rejected,
        "stored incoming SE artifacts"
    );

    for doc_id in result.doc_ids {
        event_bus.publish(events::Message::se_artifact_received(
            events::SEArtifactReceivedData { doc_id },
        ));
    }
}

pub(crate) fn spawn_replication_loop<B, T, S>(
    coordinator: Arc<p2p::sync::SyncCoordinator<B, T>>,
    sync_events_rx: tokio::sync::mpsc::Receiver<p2p::sync::SyncEvent>,
    merge_handler: Arc<EmbeddedMergeHandler<S>>,
    event_bus: Arc<dyn events::Bus>,
) -> tokio::task::JoinHandle<()>
where
    B: blockstore::Blockstore + 'static,
    T: p2p::P2PTransport,
    S: storage::corekv::Store + 'static,
{
    tokio::spawn(async move {
        let local_peer = coordinator.local_peer_id().to_string();
        let config = ReplicationConfig {
            max_workers: 1,
            ..ReplicationConfig::default()
        };
        ReplicationLoop::run_parallel(
            coordinator,
            sync_events_rx,
            merge_handler,
            config,
            move |result| match &result {
                ReplicationResult::Merged {
                    cid,
                    doc_id,
                    collection_id,
                }
                | ReplicationResult::MergedButBroadcastFailed {
                    cid,
                    doc_id,
                    collection_id,
                    ..
                } => {
                    event_bus.publish(events::Message::merge_complete(events::MergeCompleteData {
                        doc_id: doc_id.clone(),
                        subject_doc_id: None,
                        cid: *cid,
                        collection_id: collection_id.clone(),
                        by_peer: local_peer.clone(),
                    }));
                    if !doc_id.is_empty() {
                        event_bus.publish(events::Message::se_artifact_received(
                            events::SEArtifactReceivedData {
                                doc_id: doc_id.clone(),
                            },
                        ));
                    }
                }
                ReplicationResult::Failed { cid, error } => {
                    tracing::error!(cid = %cid, error = %error, "block merge failed");
                }
                ReplicationResult::Skipped {
                    cid,
                    doc_id,
                    collection_id,
                    reason,
                    terminal,
                } => {
                    let is_document_terminal_skip = !doc_id.is_empty()
                        && matches!(
                            reason.as_str(),
                            "already applied" | "nonce already applied" | "already merged"
                        );
                    let is_collection_terminal_skip =
                        doc_id.is_empty() && reason == "no linked composites needed merging";
                    if *terminal && (is_document_terminal_skip || is_collection_terminal_skip) {
                        event_bus.publish(events::Message::merge_complete(
                            events::MergeCompleteData {
                                doc_id: doc_id.clone(),
                                subject_doc_id: None,
                                cid: *cid,
                                collection_id: collection_id.clone(),
                                by_peer: local_peer.clone(),
                            },
                        ));
                    }
                    tracing::debug!(cid = %cid, reason = %reason, "replication loop skipped block");
                }
                ReplicationResult::Quarantined {
                    cid,
                    doc_id,
                    collection_id,
                    reason,
                } => {
                    tracing::warn!(
                        cid = %cid,
                        doc_id = %doc_id,
                        collection_id = %collection_id,
                        reason = %reason,
                        "Block quarantined: merge deterministically rejected, will not be re-driven locally"
                    );
                    event_bus.publish(events::Message::pending_dag_quarantined(
                        events::PendingDagQuarantinedData {
                            cid: *cid,
                            doc_id: doc_id.clone(),
                            collection_id: collection_id.clone(),
                            reason: reason.clone(),
                        },
                    ));
                }
                _ => {}
            },
        )
        .await;
    })
}

pub(crate) fn spawn_failure_recorder<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    mut failure_rx: tokio::sync::mpsc::Receiver<PushFailure>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(failure) = failure_rx.recv().await {
            let peerstore = storage::stores::Peerstore::new(store.clone());
            let result = if failure.create_retry {
                let info_bytes = match storage::stores::RetryInfo::new_initial().to_bytes() {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        tracing::warn!(error = %error, "failed to serialize retry info");
                        continue;
                    }
                };
                peerstore
                    .record_push_failure(
                        &failure.peer_id,
                        &failure.doc_id,
                        &failure.collection_id,
                        &failure.cid,
                        failure.head_priority,
                        &info_bytes,
                    )
                    .await
            } else {
                peerstore
                    .observe_push_head(
                        &failure.peer_id,
                        &failure.doc_id,
                        &failure.collection_id,
                        &failure.cid,
                        failure.head_priority,
                    )
                    .await
            };
            if let Err(error) = result {
                tracing::warn!(error = %error, "failed to record push failure");
                continue;
            }
            if !failure.create_retry {
                continue;
            }
            if let Err(error) = defra_p2p_adapter::set_persisted_replicator_status(
                &peerstore,
                &failure.peer_id.to_string(),
                p2p::ReplicatorStatus::Inactive,
            )
            .await
            {
                tracing::warn!(error = %error, "failed to mark replicator inactive");
            }
        }
    })
}

/// Run a single libp2p replicator retry pass: re-push failed doc blocks AND
/// regenerate/re-push their SE artifacts to each connected replicator with due
/// (or, when `force`, any) retries. Shared by the background ticker loop and the
/// on-demand `p2p_retry_replicators` FFI trigger.
/// Redial a replicator target that dropped, using its stored addresses, so a
/// stalled connection cannot strand the peer's persisted retry ledger.
async fn redial_replicator<S: storage::corekv::Store>(
    peerstore: &storage::stores::Peerstore<S>,
    handle: &p2p::P2PHostHandle,
    peer_id_str: &str,
    peer_id: libp2p::PeerId,
) {
    let Ok(Some(bytes)) = peerstore.get_replicator(peer_id_str).await else {
        return;
    };
    let Ok(info) = p2p::ReplicatorInfo::from_bytes(&bytes) else {
        return;
    };
    let addrs: Vec<libp2p::Multiaddr> = info
        .addresses
        .iter()
        .filter_map(|addr| addr.parse().ok())
        .collect();
    if addrs.is_empty() {
        return;
    }
    if let Err(error) = handle.dial(peer_id, addrs).await {
        tracing::debug!(peer_id = %peer_id, %error, "replicator retry redial failed");
    }
}

pub(crate) async fn run_libp2p_retry_pass<S: storage::corekv::Store + 'static>(
    store: &Arc<S>,
    handle: &p2p::P2PHostHandle,
    doc_pusher: &Arc<dyn defra_p2p_adapter::DocPusher>,
    se_repusher: &Arc<dyn db_merge::SeArtifactRepusher>,
    force: bool,
) {
    let peerstore = storage::stores::Peerstore::new(store.clone());
    let peers = match peerstore.get_all_retry_peers().await {
        Ok(peers) => peers,
        Err(_) => return,
    };

    for (peer_id_str, info_bytes) in peers {
        let _legacy_retry_info = match storage::stores::RetryInfo::from_bytes(&info_bytes) {
            Ok(info) => info,
            Err(error) => {
                tracing::warn!(peer_id = %peer_id_str, error = %error, "invalid retry info");
                continue;
            }
        };
        let peer_id: libp2p::PeerId = match peer_id_str.parse() {
            Ok(peer_id) => peer_id,
            Err(error) => {
                tracing::warn!(peer_id = %peer_id_str, error = %error, "invalid peer ID");
                continue;
            }
        };

        let connected = handle.connected_peers().await.unwrap_or_default();
        if !connected.contains(&peer_id) {
            // Nothing else redials a replicator target once it restarts, so
            // without this the peer's entire persisted retry ledger stalls
            // forever behind the connectivity gate (the iroh path dials on
            // demand instead). Redial from the stored replicator addresses;
            // a later pass drains the ledger once the connection is back.
            redial_replicator(&peerstore, handle, &peer_id_str, peer_id).await;
            continue;
        }

        let mut docs = match peerstore.get_retry_documents(&peer_id_str).await {
            Ok(docs) => docs,
            Err(_) => continue,
        };
        if docs.is_empty() {
            let _ = peerstore.clear_retry_peer(&peer_id_str).await;
            let _ = defra_p2p_adapter::set_persisted_replicator_status(
                &peerstore,
                &peer_id_str,
                p2p::ReplicatorStatus::Active,
            )
            .await;
            continue;
        }

        let mut fast_failures = 0usize;
        for retry in &mut docs {
            if !force && !retry.retry_info.is_due() {
                continue;
            }
            // Bound each send so a nonresponsive peer cannot stall healthy
            // peers' retries behind it (#1099). A timeout ends the pass (the
            // peer is unreachable); a fast rejection only consumes a bounded
            // budget so one permanently rejected doc at the head of the key
            // order cannot starve the rest forever.
            // Collection commits are doc-less and replay by CID (defradb#1113).
            let replay = async {
                if retry.is_collection_commit() {
                    match retry.cid.parse::<cid::Cid>() {
                        Ok(cid) => {
                            doc_pusher
                                .retry_collection_commit(
                                    handle,
                                    peer_id,
                                    &retry.collection_id,
                                    &cid,
                                )
                                .await
                        }
                        Err(error) => Err(defra_p2p_adapter::P2PError::Internal(format!(
                            "unparseable collection-commit CID {}: {error}",
                            retry.cid
                        ))),
                    }
                } else {
                    doc_pusher
                        .retry_doc(handle, peer_id, &retry.doc_id, &retry.collection_id)
                        .await
                }
            };
            match tokio::time::timeout(std::time::Duration::from_secs(15), replay).await {
                Ok(Ok(())) => {
                    // Doc block re-push succeeded; regenerate and re-push
                    // SE artifacts for this doc too. Go re-pushes the
                    // artifact (not just the doc) on reconnect; replicators
                    // only answer SE queries from pushed artifacts.
                    se_repusher
                        .regenerate_and_push_se_artifacts(&retry.collection_id, &retry.doc_id)
                        .await;
                    let _ = peerstore.complete_retry_document(&peer_id_str, retry).await;
                }
                Ok(Err(error)) => {
                    tracing::warn!(doc_id = %retry.doc_id, peer_id = %peer_id, error = %error, "retry push failed");
                    p2p::sync::reschedule_persisted_push_retry(
                        &mut retry.retry_info,
                        &format!("{peer_id_str}:{}", retry.cid),
                        &error.to_string(),
                    );
                    let _ = peerstore.update_retry_document(&peer_id_str, retry).await;
                    fast_failures += 1;
                    if fast_failures >= 3 {
                        break;
                    }
                }
                Err(_) => {
                    tracing::warn!(doc_id = %retry.doc_id, peer_id = %peer_id, "retry push timed out");
                    retry
                        .retry_info
                        .bump_for(&format!("{peer_id_str}:{}", retry.cid));
                    let _ = peerstore.update_retry_document(&peer_id_str, retry).await;
                    break;
                }
            }
        }

        let remaining = peerstore
            .get_retry_documents(&peer_id_str)
            .await
            .unwrap_or_default();
        if remaining.is_empty() {
            let _ = peerstore.clear_retry_peer(&peer_id_str).await;
            let _ = defra_p2p_adapter::set_persisted_replicator_status(
                &peerstore,
                &peer_id_str,
                p2p::ReplicatorStatus::Active,
            )
            .await;
        } else {
            let _ = defra_p2p_adapter::set_persisted_replicator_status(
                &peerstore,
                &peer_id_str,
                p2p::ReplicatorStatus::Inactive,
            )
            .await;
        }
    }
}

pub(crate) fn spawn_libp2p_retry_loop<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    handle: p2p::P2PHostHandle,
    doc_pusher: Arc<dyn defra_p2p_adapter::DocPusher>,
    se_repusher: Arc<dyn db_merge::SeArtifactRepusher>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let peerstore = storage::stores::Peerstore::new(store.clone());
        if let Err(error) = peerstore.activate_dormant_push_retries().await {
            tracing::warn!(error = %error, "failed to reactivate push retries after restart");
        }
        loop {
            tokio::time::sleep(p2p::sync::PERSISTED_RETRY_SWEEP_INTERVAL).await;
            run_libp2p_retry_pass(&store, &handle, &doc_pusher, &se_repusher, false).await;
        }
    })
}

/// Run a single iroh replicator retry pass (see `run_libp2p_retry_pass`).
#[cfg(feature = "iroh")]
pub(crate) async fn run_iroh_retry_pass<S: storage::corekv::Store + 'static>(
    store: &Arc<S>,
    doc_pusher: &Arc<dyn defra_p2p_adapter::TransportDocPusher>,
    se_repusher: &Arc<dyn db_merge::SeArtifactRepusher>,
    force: bool,
) {
    let peerstore = storage::stores::Peerstore::new(store.clone());
    let peers = match peerstore.get_all_retry_peers().await {
        Ok(peers) => peers,
        Err(_) => return,
    };

    for (peer_id_str, info_bytes) in peers {
        let _legacy_retry_info = match storage::stores::RetryInfo::from_bytes(&info_bytes) {
            Ok(info) => info,
            Err(error) => {
                tracing::warn!(peer_id = %peer_id_str, error = %error, "invalid retry info");
                continue;
            }
        };
        let peer_id = p2p::transport::PeerId::new(peer_id_str.clone());
        // Iroh request-response reconnects on demand. The peer-map-backed
        // connected_peers snapshot is not authoritative enough to gate
        // retries here, so let the transport attempt the replay.

        let mut docs = match peerstore.get_retry_documents(&peer_id_str).await {
            Ok(docs) => docs,
            Err(_) => continue,
        };
        if docs.is_empty() {
            let _ = peerstore.clear_retry_peer(&peer_id_str).await;
            let _ = defra_p2p_adapter::set_persisted_replicator_status(
                &peerstore,
                &peer_id_str,
                p2p::ReplicatorStatus::Active,
            )
            .await;
            continue;
        }

        let mut fast_failures = 0usize;
        for retry in &mut docs {
            if !force && !retry.retry_info.is_due() {
                continue;
            }
            // Bound each send so a nonresponsive peer cannot stall healthy
            // peers' retries behind it (#1099). A timeout ends the pass (the
            // peer is unreachable); a fast rejection only consumes a bounded
            // budget so one permanently rejected doc at the head of the key
            // order cannot starve the rest forever.
            // Collection commits are doc-less and replay by CID (defradb#1113).
            let replay = async {
                if retry.is_collection_commit() {
                    match retry.cid.parse::<cid::Cid>() {
                        Ok(cid) => {
                            doc_pusher
                                .retry_collection_commit(&peer_id, &retry.collection_id, &cid)
                                .await
                        }
                        Err(error) => Err(defra_p2p_adapter::P2PError::Internal(format!(
                            "unparseable collection-commit CID {}: {error}",
                            retry.cid
                        ))),
                    }
                } else {
                    doc_pusher
                        .retry_doc(&peer_id, &retry.doc_id, &retry.collection_id)
                        .await
                }
            };
            match tokio::time::timeout(std::time::Duration::from_secs(15), replay).await {
                Ok(Ok(())) => {
                    se_repusher
                        .regenerate_and_push_se_artifacts(&retry.collection_id, &retry.doc_id)
                        .await;
                    let _ = peerstore.complete_retry_document(&peer_id_str, retry).await;
                }
                Ok(Err(error)) => {
                    tracing::warn!(doc_id = %retry.doc_id, peer_id = %peer_id, error = %error, "retry push failed");
                    p2p::sync::reschedule_persisted_push_retry(
                        &mut retry.retry_info,
                        &format!("{peer_id_str}:{}", retry.cid),
                        &error.to_string(),
                    );
                    let _ = peerstore.update_retry_document(&peer_id_str, retry).await;
                    fast_failures += 1;
                    if fast_failures >= 3 {
                        break;
                    }
                }
                Err(_) => {
                    tracing::warn!(doc_id = %retry.doc_id, peer_id = %peer_id, "retry push timed out");
                    retry
                        .retry_info
                        .bump_for(&format!("{peer_id_str}:{}", retry.cid));
                    let _ = peerstore.update_retry_document(&peer_id_str, retry).await;
                    break;
                }
            }
        }

        let remaining = peerstore
            .get_retry_documents(&peer_id_str)
            .await
            .unwrap_or_default();
        if remaining.is_empty() {
            let _ = peerstore.clear_retry_peer(&peer_id_str).await;
            let _ = defra_p2p_adapter::set_persisted_replicator_status(
                &peerstore,
                &peer_id_str,
                p2p::ReplicatorStatus::Active,
            )
            .await;
        } else {
            let _ = defra_p2p_adapter::set_persisted_replicator_status(
                &peerstore,
                &peer_id_str,
                p2p::ReplicatorStatus::Inactive,
            )
            .await;
        }
    }
}

#[cfg(feature = "iroh")]
pub(crate) fn spawn_iroh_retry_loop<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    doc_pusher: Arc<dyn defra_p2p_adapter::TransportDocPusher>,
    se_repusher: Arc<dyn db_merge::SeArtifactRepusher>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let peerstore = storage::stores::Peerstore::new(store.clone());
        if let Err(error) = peerstore.activate_dormant_push_retries().await {
            tracing::warn!(error = %error, "failed to reactivate push retries after restart");
        }
        loop {
            tokio::time::sleep(p2p::sync::PERSISTED_RETRY_SWEEP_INTERVAL).await;
            run_iroh_retry_pass(&store, &doc_pusher, &se_repusher, false).await;
        }
    })
}
