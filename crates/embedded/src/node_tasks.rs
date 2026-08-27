use std::sync::Arc;

use p2p::sync::{ReplicationConfig, ReplicationLoop, ReplicationResult};
#[cfg(feature = "iroh")]
use p2p::P2PTransport;

use crate::node::EmbeddedMergeHandler;

pub struct BackgroundTasks {
    downsample_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl BackgroundTasks {
    pub(crate) fn new(downsample_task: Option<tokio::task::JoinHandle<()>>) -> Self {
        Self {
            downsample_task: std::sync::Mutex::new(downsample_task),
        }
    }

    /// Stop and await all node-owned background tasks.
    ///
    /// Awaiting cancellation is important for persistent stores: an aborted
    /// task can retain the final database handle until Tokio next polls it,
    /// which otherwise leaves the on-disk database lock held after close.
    pub async fn shutdown(&self) {
        let task = self
            .downsample_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        let task = self
            .downsample_task
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(task) = task {
            task.abort();
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_libp2p_event_handler<B: blockstore::Blockstore + 'static>(
    events: tokio::sync::mpsc::Receiver<p2p::HostEvent>,
    coordinator: Arc<p2p::sync::Libp2pSyncCoordinator<B>>,
    store: Arc<impl storage::corekv::Store + 'static>,
    event_bus: Arc<dyn events::Bus>,
    handle: p2p::P2PHostHandle,
    se_correlator: p2p::SeQueryCorrelator,
    manage_hooks: defra_p2p_adapter::manage::hooks::ManageHooksCell,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let handler_coordinator = coordinator.clone();
        coordinator.run_event_dispatcher(events, move |event, admission| {
            let coordinator = handler_coordinator.clone();
            let store = store.clone();
            let event_bus = event_bus.clone();
            let handle = handle.clone();
            let se_correlator = se_correlator.clone();
            let manage_hooks = manage_hooks.clone();
            async move {
            match &event {
                p2p::HostEvent::PeerConnected(peer_id) => {
                    defra_p2p_adapter::activate_retry_peer(
                        store.clone(),
                        &p2p::transport::PeerId::from(*peer_id),
                    )
                    .await;
                }
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

            let transport_event = p2p::convert_host_event(event);
            if admission == p2p::sync::DispatchAdmission::Saturated {
                if let Err(error) = coordinator
                    .handle_transport_event_with_admission(transport_event, admission)
                    .await
                {
                    tracing::debug!(%error, "rejected saturated embedded P2P request");
                }
                return;
            }
            let transport_event = match transport_event {
                p2p::TransportEvent::SEArtifactsReceived { peer_id, data } => {
                    if let Ok(pid) = peer_id.as_str().parse::<libp2p::PeerId>() {
                        // Stores artifacts AND sends the signed ack Go's push waits for.
                        let doc_ids = db::merge::se::serve::handle_artifacts_push(
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
                    return;
                }
                p2p::TransportEvent::SEQueryRequest { peer_id, request } => {
                    let transport = p2p::Libp2pTransport::new(handle.clone());
                    db::merge::se::serve::handle_query_request(
                        store.as_ref(),
                        &transport,
                        peer_id,
                        request,
                    )
                    .await;
                    return;
                }
                p2p::TransportEvent::SEQueryReply { reply, .. } => {
                    se_correlator.deliver(reply);
                    return;
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
                    return;
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
                    return;
                }
                p2p::TransportEvent::ManageReply { reply, .. } => {
                    if let Some(hooks) = manage_hooks.get() {
                        hooks.correlator.deliver(reply);
                    }
                    return;
                }
                p2p::TransportEvent::ManageQueryReply { reply, .. } => {
                    if let Some(hooks) = manage_hooks.get() {
                        hooks.query_correlator.deliver(reply);
                    }
                    return;
                }
                other => other,
            };
            if let Err(error) = coordinator
                .handle_transport_event_with_admission(transport_event, admission)
                .await
            {
                tracing::error!(error = %error, "error handling libp2p event");
            }
            }
        })
        .await;
    })
}

#[cfg(feature = "iroh")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_iroh_event_handler<B: blockstore::Blockstore + 'static>(
    events: tokio::sync::mpsc::Receiver<
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
        let handler_coordinator = coordinator.clone();
        coordinator.run_event_dispatcher(events, move |event, admission| {
            let coordinator = handler_coordinator.clone();
            let store = store.clone();
            let event_bus = event_bus.clone();
            let se_correlator = se_correlator.clone();
            let se_transport = se_transport.clone();
            let manage_hooks = manage_hooks.clone();
            async move {
            match &event {
                p2p::TransportEvent::PeerConnected(peer_id) => {
                    defra_p2p_adapter::activate_retry_peer(store.clone(), peer_id).await;
                }
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

            if admission == p2p::sync::DispatchAdmission::Saturated {
                if let Err(error) = coordinator
                    .handle_transport_event_with_admission(event, admission)
                    .await
                {
                    tracing::debug!(%error, "rejected saturated embedded Iroh request");
                }
                return;
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
                    return;
                }
                p2p::TransportEvent::SEQueryRequest { peer_id, request } => {
                    // Serve SE queries over iroh: byte-match the pushed artifacts
                    // and return a signed reply (mirrors the libp2p loop, #976).
                    db::merge::se::serve::handle_query_request(
                        store.as_ref(),
                        &se_transport,
                        peer_id,
                        request,
                    )
                    .await;
                    return;
                }
                p2p::TransportEvent::SEQueryReply { reply, .. } => {
                    // Deliver inbound replies so the owner/querier transport's
                    // awaiting correlator slot resolves (#976).
                    se_correlator.deliver(reply);
                    return;
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
                    return;
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
                    return;
                }
                p2p::TransportEvent::ManageReply { reply, .. } => {
                    if let Some(hooks) = manage_hooks.get() {
                        hooks.correlator.deliver(reply);
                    }
                    return;
                }
                p2p::TransportEvent::ManageQueryReply { reply, .. } => {
                    if let Some(hooks) = manage_hooks.get() {
                        hooks.query_correlator.deliver(reply);
                    }
                    return;
                }
                other => other,
            };
            if let Err(error) = coordinator
                .handle_transport_event_with_admission(event, admission)
                .await
            {
                tracing::error!(error = %error, "error handling iroh event");
            }
            }
        })
        .await;
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

    let result = match db::merge::se::receive_and_store(&mut txn, &data).await {
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
        ReplicationLoop::run(
            coordinator,
            sync_events_rx,
            merge_handler,
            ReplicationConfig::default(),
            move |result| match result {
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
