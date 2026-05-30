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
                    if let Ok(pid) = peer_id.as_str().parse::<libp2p::PeerId>() {
                        db_merge::se::serve::handle_query_request(
                            store.as_ref(),
                            &handle,
                            pid,
                            request,
                        )
                        .await;
                    }
                    continue;
                }
                p2p::TransportEvent::SEQueryReply { reply, .. } => {
                    se_correlator.deliver(reply);
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
pub(crate) fn spawn_iroh_event_handler<B: blockstore::Blockstore + 'static>(
    mut events: tokio::sync::mpsc::Receiver<
        p2p::TransportEvent<<p2p::iroh::IrohTransport as P2PTransport>::ResponseToken>,
    >,
    coordinator: Arc<p2p::sync::IrohSyncCoordinator<B>>,
    store: Arc<impl storage::corekv::Store + 'static>,
    event_bus: Arc<dyn events::Bus>,
    se_correlator: p2p::SeQueryCorrelator,
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
                p2p::TransportEvent::SEQueryReply { reply, .. } => {
                    // SE query SEND is libp2p-only; deliver any inbound reply so a
                    // correlator slot resolves rather than leaks.
                    se_correlator.deliver(reply);
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
            let retry_info = storage::stores::RetryInfo::new_initial();
            let info_bytes = match retry_info.to_bytes() {
                Ok(bytes) => bytes,
                Err(error) => {
                    tracing::warn!(error = %error, "failed to serialize retry info");
                    continue;
                }
            };

            if let Err(error) = peerstore
                .record_push_failure(
                    &failure.peer_id.to_string(),
                    &failure.doc_id,
                    &failure.collection_id,
                    &info_bytes,
                )
                .await
            {
                tracing::warn!(error = %error, "failed to record push failure");
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

pub(crate) fn spawn_libp2p_retry_loop<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    handle: p2p::P2PHostHandle,
    doc_pusher: Arc<dyn defra_p2p_adapter::DocPusher>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let peerstore = storage::stores::Peerstore::new(store.clone());
            let peers = match peerstore.get_all_retry_peers().await {
                Ok(peers) => peers,
                Err(_) => continue,
            };

            for (peer_id_str, info_bytes) in peers {
                let mut retry_info = match storage::stores::RetryInfo::from_bytes(&info_bytes) {
                    Ok(info) => info,
                    Err(error) => {
                        tracing::warn!(peer_id = %peer_id_str, error = %error, "invalid retry info");
                        continue;
                    }
                };
                if !retry_info.is_due() {
                    continue;
                }

                let peer_id: libp2p::PeerId = match peer_id_str.parse() {
                    Ok(peer_id) => peer_id,
                    Err(error) => {
                        tracing::warn!(peer_id = %peer_id_str, error = %error, "invalid peer ID");
                        continue;
                    }
                };

                let connected = handle.connected_peers().await.unwrap_or_default();
                if !connected.contains(&peer_id) {
                    continue;
                }

                let docs = match peerstore.get_retry_doc_ids(&peer_id_str).await {
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

                let mut all_succeeded = true;
                for (doc_id, collection_id) in &docs {
                    match doc_pusher
                        .retry_doc(&handle, peer_id, doc_id, collection_id)
                        .await
                    {
                        Ok(()) => {
                            let _ = peerstore.remove_retry_doc(&peer_id_str, doc_id).await;
                        }
                        Err(error) => {
                            tracing::warn!(doc_id = %doc_id, peer_id = %peer_id, error = %error, "retry push failed");
                            all_succeeded = false;
                        }
                    }
                }

                if all_succeeded {
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
                    retry_info.bump();
                    if let Ok(bytes) = retry_info.to_bytes() {
                        let _ = peerstore.update_retry_info(&peer_id_str, &bytes).await;
                    }
                }
            }
        }
    })
}

#[cfg(feature = "iroh")]
pub(crate) fn spawn_iroh_retry_loop<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    doc_pusher: Arc<dyn defra_p2p_adapter::TransportDocPusher>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let peerstore = storage::stores::Peerstore::new(store.clone());
            let peers = match peerstore.get_all_retry_peers().await {
                Ok(peers) => peers,
                Err(_) => continue,
            };

            for (peer_id_str, info_bytes) in peers {
                let mut retry_info = match storage::stores::RetryInfo::from_bytes(&info_bytes) {
                    Ok(info) => info,
                    Err(error) => {
                        tracing::warn!(peer_id = %peer_id_str, error = %error, "invalid retry info");
                        continue;
                    }
                };
                if !retry_info.is_due() {
                    continue;
                }

                let peer_id = p2p::transport::PeerId::new(peer_id_str.clone());
                // Iroh request-response reconnects on demand. The peer-map-backed
                // connected_peers snapshot is not authoritative enough to gate
                // retries here, so let the transport attempt the replay.

                let docs = match peerstore.get_retry_doc_ids(&peer_id_str).await {
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

                let mut all_succeeded = true;
                for (doc_id, collection_id) in &docs {
                    match doc_pusher.retry_doc(&peer_id, doc_id, collection_id).await {
                        Ok(()) => {
                            let _ = peerstore.remove_retry_doc(&peer_id_str, doc_id).await;
                        }
                        Err(error) => {
                            tracing::warn!(doc_id = %doc_id, peer_id = %peer_id, error = %error, "retry push failed");
                            all_succeeded = false;
                        }
                    }
                }

                if all_succeeded {
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
                    retry_info.bump();
                    if let Ok(bytes) = retry_info.to_bytes() {
                        let _ = peerstore.update_retry_info(&peer_id_str, &bytes).await;
                    }
                }
            }
        }
    })
}
