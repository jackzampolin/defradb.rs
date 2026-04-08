use std::sync::Arc;

use p2p::sync::{PushFailure, ReplicationConfig, ReplicationLoop, ReplicationResult};

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

pub(crate) fn spawn_libp2p_event_handler<B: blockstore::Blockstore + 'static>(
    mut events: tokio::sync::mpsc::Receiver<p2p::HostEvent>,
    coordinator: Arc<p2p::sync::Libp2pSyncCoordinator<B>>,
    event_bus: Arc<dyn events::Bus>,
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

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let coordinator = coordinator.clone();
            tokio::spawn(async move {
                if let Err(error) = coordinator
                    .handle_transport_event(p2p::convert_host_event(event))
                    .await
                {
                    tracing::error!(error = %error, "error handling libp2p event");
                }
                drop(permit);
            });
        }
    })
}

#[cfg(feature = "iroh")]
pub(crate) fn spawn_iroh_event_handler<B: blockstore::Blockstore + 'static>(
    mut events: tokio::sync::mpsc::Receiver<p2p::TransportEvent>,
    coordinator: Arc<p2p::sync::IrohSyncCoordinator<B>>,
    event_bus: Arc<dyn events::Bus>,
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
                    if *terminal
                        && !doc_id.is_empty()
                        && matches!(
                            reason.as_str(),
                            "already applied" | "nonce already applied" | "already merged"
                        )
                    {
                        event_bus.publish(events::Message::merge_complete(
                            events::MergeCompleteData {
                                doc_id: doc_id.clone(),
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
            }
        }
    })
}

pub(crate) fn spawn_libp2p_retry_loop<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    handle: p2p::P2PHostHandle,
    doc_pusher: Arc<dyn crate::DocPusher>,
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
                } else {
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
    transport: p2p::iroh::IrohTransport,
    doc_pusher: Arc<dyn crate::TransportDocPusher>,
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
                let connected = transport.connected_peers().await.unwrap_or_default();
                if !connected.contains(&peer_id) {
                    continue;
                }

                let docs = match peerstore.get_retry_doc_ids(&peer_id_str).await {
                    Ok(docs) => docs,
                    Err(_) => continue,
                };
                if docs.is_empty() {
                    let _ = peerstore.clear_retry_peer(&peer_id_str).await;
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
                } else {
                    retry_info.bump();
                    if let Ok(bytes) = retry_info.to_bytes() {
                        let _ = peerstore.update_retry_info(&peer_id_str, &bytes).await;
                    }
                }
            }
        }
    })
}
