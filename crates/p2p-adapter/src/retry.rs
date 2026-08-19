//! One durable sender retry state machine shared by every runtime and transport.

use std::sync::Arc;

use p2p::transport::{P2PTransport, PeerAddr, PeerId};

use crate::TransportDocPusher;

/// Record head announcements and acknowledgements behind the peer-scoped
/// retry writer. A success acknowledgement clears only the marker still
/// covered by the acknowledged head fence.
pub fn spawn_failure_recorder<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    mut failures: tokio::sync::mpsc::Receiver<p2p::sync::PushFailure>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ack_fence = p2p::sync::HeadAckFence::default();
        while let Some(mut failure) = failures.recv().await {
            let durable_tx = failure.durable_tx.take();
            if failure.acknowledged && !ack_fence.ack_is_current(&failure) {
                tracing::debug!(
                    peer_id = %failure.peer_id,
                    doc_id = %failure.doc_id,
                    collection_id = %failure.collection_id,
                    "Ignoring stale head acknowledgement"
                );
                let _ = durable_tx.map(|tx| tx.send(false));
                continue;
            }

            let peerstore = storage::stores::Peerstore::new(Arc::clone(&store));
            let _retry_guard = match peerstore
                .acquire_replicator_retry_guard(&failure.peer_id)
                .await
            {
                Ok(Some(guard)) => guard,
                Ok(None) => {
                    let _ = durable_tx.map(|tx| tx.send(false));
                    continue;
                }
                Err(error) => {
                    tracing::warn!(%error, "failed to coordinate push failure recording");
                    let _ = durable_tx.map(|tx| tx.send(false));
                    continue;
                }
            };

            let result = if failure.acknowledged {
                peerstore
                    .complete_retry_scope(
                        &failure.peer_id,
                        &failure.doc_id,
                        &failure.collection_id,
                        failure.doc_id.is_empty(),
                    )
                    .await
            } else if failure.create_retry {
                let info_bytes = match storage::stores::RetryInfo::new_initial().to_bytes() {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        tracing::warn!(%error, "failed to serialize retry info");
                        let _ = durable_tx.map(|tx| tx.send(false));
                        continue;
                    }
                };
                peerstore
                    .record_push_failure(
                        &failure.peer_id,
                        &failure.doc_id,
                        &failure.collection_id,
                        &info_bytes,
                    )
                    .await
            } else {
                peerstore
                    .observe_push_head(&failure.peer_id, &failure.doc_id, &failure.collection_id)
                    .await
            };
            if let Err(error) = result {
                tracing::warn!(%error, "failed to record push failure");
                let _ = durable_tx.map(|tx| tx.send(false));
                continue;
            }

            if !failure.create_retry && !failure.acknowledged {
                ack_fence.observe_durable(&failure);
            }
            let _ = durable_tx.map(|tx| tx.send(true));
            if failure.acknowledged {
                ack_fence.clear_current_ack(&failure);
                let _ = peerstore.clear_retry_peer(&failure.peer_id).await;
            } else if failure.create_retry {
                if let Err(error) = crate::set_persisted_replicator_status(
                    &peerstore,
                    &failure.peer_id,
                    p2p::ReplicatorStatus::Inactive,
                )
                .await
                {
                    tracing::warn!(%error, "failed to mark replicator inactive");
                }
            }
        }
    })
}

/// Activate a dormant durable peer schedule after either transport reconnects.
pub async fn activate_retry_peer<S: storage::corekv::Store>(store: Arc<S>, peer_id: &PeerId) {
    let peerstore = storage::stores::Peerstore::new(store);
    match peerstore.activate_retry_peer(peer_id.as_str()).await {
        Ok(true) => {
            tracing::debug!(%peer_id, "Activated durable push markers after peer reconnect")
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(%peer_id, %error, "failed to activate durable push markers after peer reconnect")
        }
    }
}

async fn redial_replicator<S, T>(
    peerstore: &storage::stores::Peerstore<S>,
    transport: &T,
    peer_id: &PeerId,
) where
    S: storage::corekv::Store,
    T: P2PTransport,
{
    let Ok(Some(bytes)) = peerstore.get_replicator(peer_id.as_str()).await else {
        return;
    };
    let Ok(info) = p2p::ReplicatorInfo::from_bytes(&bytes) else {
        return;
    };
    let addrs: Vec<PeerAddr> = info
        .addresses
        .iter()
        .filter_map(|address| transport.parse_dial_addr(address).ok())
        .filter(|(addressed_peer, _)| addressed_peer == peer_id)
        .flat_map(|(_, addrs)| addrs)
        .collect();
    if let Err(error) = transport.dial(peer_id, addrs).await {
        tracing::debug!(%peer_id, %error, "replicator retry redial failed");
    }
}

/// Run one marker-plus-rederive retry pass for any transport.
pub async fn run_retry_pass<S, T>(
    store: &Arc<S>,
    transport: &T,
    doc_pusher: &Arc<dyn TransportDocPusher>,
    se_repusher: Option<&Arc<dyn db_merge::SeArtifactRepusher>>,
    force: bool,
) where
    S: storage::corekv::Store + 'static,
    T: P2PTransport,
{
    let peerstore = storage::stores::Peerstore::new(Arc::clone(store));
    let peers = match peerstore.get_replicator_retry_peers().await {
        Ok(peers) => peers,
        Err(error) => {
            tracing::debug!(%error, "failed to load retry peers");
            return;
        }
    };

    for (peer_id_str, info_bytes) in peers {
        let retry_guard = match peerstore.acquire_replicator_retry_guard(&peer_id_str).await {
            Ok(Some(guard)) => guard,
            Ok(None) | Err(_) => continue,
        };
        drop(retry_guard);
        let mut peer_retry = match storage::stores::RetryInfo::from_bytes(&info_bytes) {
            Ok(info) => info,
            Err(error) => {
                tracing::warn!(peer_id = %peer_id_str, %error, "invalid retry info");
                continue;
            }
        };
        let peer_id = PeerId::new(peer_id_str.clone());

        let mut markers = match peerstore.get_retry_documents(&peer_id_str).await {
            Ok(markers) => markers,
            Err(error) => {
                tracing::debug!(peer_id = %peer_id, %error, "failed to load retry markers");
                continue;
            }
        };
        if markers.is_empty() {
            finish_peer(&peerstore, &peer_id_str, true).await;
            continue;
        }
        if !force && !markers.iter().any(|marker| marker.retry_info.is_due()) {
            continue;
        }

        let connected = transport.connected_peers().await.unwrap_or_default();
        if !connected.contains(&peer_id) {
            // Connectivity is part of the due retry attempt. Do not create a
            // second two-second redial clock for markers whose ladder has not
            // elapsed yet.
            redial_replicator(&peerstore, transport, &peer_id).await;
            peer_retry.bump();
            let _ = peerstore
                .update_retry_document(&peer_id_str, &peer_retry)
                .await;
            finish_peer(&peerstore, &peer_id_str, false).await;
            continue;
        }

        let mut fast_failures = 0usize;
        for marker in &mut markers {
            if !force && !marker.retry_info.is_due() {
                continue;
            }
            let replay = async {
                if marker.is_collection_commit() {
                    doc_pusher
                        .retry_collection_commit(&peer_id, &marker.collection_id)
                        .await
                } else {
                    doc_pusher
                        .retry_doc(&peer_id, &marker.doc_id, &marker.collection_id)
                        .await
                }
            };
            let replay_result =
                tokio::time::timeout(std::time::Duration::from_secs(15), replay).await;
            let _transition_guard =
                match peerstore.acquire_replicator_retry_guard(&peer_id_str).await {
                    Ok(Some(guard)) => guard,
                    Ok(None) | Err(_) => break,
                };
            match replay_result {
                Ok(Ok(())) => {
                    if let Some(repusher) = se_repusher {
                        repusher
                            .regenerate_and_push_se_artifacts(&marker.collection_id, &marker.doc_id)
                            .await;
                    }
                }
                Ok(Err(error)) => {
                    tracing::warn!(
                        doc_id = %marker.doc_id,
                        %peer_id,
                        %error,
                        "retry push failed"
                    );
                    marker.retry_info.bump();
                    let _ = peerstore
                        .update_retry_document(&peer_id_str, &marker.retry_info)
                        .await;
                    fast_failures += 1;
                    if fast_failures >= 3 {
                        break;
                    }
                }
                Err(_) => {
                    tracing::warn!(doc_id = %marker.doc_id, %peer_id, "retry push timed out");
                    marker.retry_info.bump();
                    let _ = peerstore
                        .update_retry_document(&peer_id_str, &marker.retry_info)
                        .await;
                    break;
                }
            }
        }

        let complete = peerstore
            .get_retry_documents(&peer_id_str)
            .await
            .is_ok_and(|markers| markers.is_empty());
        finish_peer(&peerstore, &peer_id_str, complete).await;
    }
}

async fn finish_peer<S: storage::corekv::Store>(
    peerstore: &storage::stores::Peerstore<S>,
    peer_id: &str,
    complete: bool,
) {
    let status = if complete {
        let _ = peerstore.clear_retry_peer(peer_id).await;
        p2p::ReplicatorStatus::Active
    } else {
        p2p::ReplicatorStatus::Inactive
    };
    let _ = crate::set_persisted_replicator_status(peerstore, peer_id, status).await;
}

/// Run the one durable retry clock used by CLI, embedded, and defra-node.
pub fn spawn_retry_loop<S, T>(
    store: Arc<S>,
    transport: T,
    doc_pusher: Arc<dyn TransportDocPusher>,
    se_repusher: Option<Arc<dyn db_merge::SeArtifactRepusher>>,
) -> tokio::task::JoinHandle<()>
where
    S: storage::corekv::Store + 'static,
    T: P2PTransport,
{
    tokio::spawn(async move {
        let peerstore = storage::stores::Peerstore::new(Arc::clone(&store));
        if let Err(error) = peerstore.migrate_legacy_push_retries().await {
            tracing::warn!(%error, "failed to migrate legacy push retries after restart");
        }
        loop {
            tokio::time::sleep(p2p::sync::PERSISTED_RETRY_SWEEP_INTERVAL).await;
            run_retry_pass(&store, &transport, &doc_pusher, se_repusher.as_ref(), false).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(acknowledged: bool) -> p2p::sync::PushFailure {
        p2p::sync::PushFailure {
            peer_id: "peer-a".to_string(),
            doc_id: "doc-a".to_string(),
            collection_id: "collection-a".to_string(),
            cid: "head-a".to_string(),
            head_priority: 7,
            create_retry: false,
            acknowledged,
            durable_tx: None,
        }
    }

    #[tokio::test]
    async fn shared_failure_recorder_registers_then_clears_current_scope() {
        let store = Arc::new(storage::backends::MemoryStore::new());
        let peerstore = storage::stores::Peerstore::new(Arc::clone(&store));
        let replicator = p2p::ReplicatorInfo::from_raw(
            "peer-a".to_string(),
            vec!["collection-a".to_string()],
            Vec::new(),
        );
        peerstore
            .create_replicator("peer-a", &replicator.to_bytes().unwrap())
            .await
            .unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let task = spawn_failure_recorder(Arc::clone(&store), rx);

        let (registered_tx, registered_rx) = tokio::sync::oneshot::channel();
        let mut announced = failure(false);
        announced.durable_tx = Some(registered_tx);
        tx.send(announced).await.unwrap();
        assert!(registered_rx.await.unwrap());
        assert_eq!(
            peerstore.get_retry_documents("peer-a").await.unwrap().len(),
            1
        );

        let (cleared_tx, cleared_rx) = tokio::sync::oneshot::channel();
        let mut acknowledged = failure(true);
        acknowledged.durable_tx = Some(cleared_tx);
        tx.send(acknowledged).await.unwrap();
        assert!(cleared_rx.await.unwrap());
        assert!(peerstore
            .get_retry_documents("peer-a")
            .await
            .unwrap()
            .is_empty());

        drop(tx);
        task.await.unwrap();
    }
}
