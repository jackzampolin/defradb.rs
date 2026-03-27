use p2p::topics::DefraTopic;

pub(crate) async fn restore_libp2p_replicators<S: storage::corekv::Store + 'static>(
    handle: &p2p::P2PHostHandle,
    peerstore: &storage::stores::Peerstore<S>,
) {
    match peerstore.list_replicators().await {
        Ok(entries) => {
            for (peer_id_str, data) in entries {
                match p2p::ReplicatorInfo::from_bytes(&data) {
                    Ok(info) => {
                        if let Some(peer_id) = info.peer_id() {
                            if let Err(error) = handle
                                .create_replicator(peer_id, info.collections.clone())
                                .await
                            {
                                tracing::warn!(peer_id = %peer_id, error = %error, "failed to restore replicator");
                                continue;
                            }

                            for collection_id in &info.collections {
                                let topic = DefraTopic::collection(collection_id);
                                if let Err(error) = handle.subscribe(topic).await {
                                    tracing::warn!(collection_id = %collection_id, error = %error, "failed to restore collection topic");
                                }
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(peer_id = %peer_id_str, error = %error, "failed to decode replicator info");
                    }
                }
            }
        }
        Err(error) => tracing::warn!(error = %error, "failed to load replicators from storage"),
    }
}

pub(crate) async fn restore_libp2p_documents<S: storage::corekv::Store + 'static>(
    handle: &p2p::P2PHostHandle,
    peerstore: &storage::stores::Peerstore<S>,
) -> std::collections::HashSet<String> {
    let mut restored = std::collections::HashSet::new();
    if let Ok(doc_ids) = peerstore.load_documents().await {
        for doc_id in &doc_ids {
            let _ = handle.subscribe(DefraTopic::document(doc_id)).await;
            restored.insert(doc_id.clone());
        }
    }
    restored
}

#[cfg(feature = "iroh")]
pub(crate) async fn restore_iroh_replicators<S, B>(
    coordinator: &std::sync::Arc<p2p::sync::IrohSyncCoordinator<B>>,
    peerstore: &storage::stores::Peerstore<S>,
) where
    S: storage::corekv::Store + 'static,
    B: blockstore::Blockstore + 'static,
{
    match peerstore.list_replicators().await {
        Ok(entries) => {
            for (_peer_id_str, data) in entries {
                if let Ok(rep_info) = p2p::ReplicatorInfo::from_bytes(&data) {
                    let peer_id = p2p::transport::PeerId::new(rep_info.peer_id_str().to_string());
                    let _ = coordinator
                        .create_replicator(&peer_id, rep_info.collections.clone(), false)
                        .await;
                }
            }
        }
        Err(error) => tracing::warn!(error = %error, "failed to load replicators from storage"),
    }
}

#[cfg(feature = "iroh")]
pub(crate) async fn restore_iroh_documents<S: storage::corekv::Store + 'static>(
    transport: &p2p::iroh::IrohTransport,
    peerstore: &storage::stores::Peerstore<S>,
) -> std::collections::HashSet<String> {
    use p2p::P2PTransport;

    let mut restored = std::collections::HashSet::new();
    if let Ok(doc_ids) = peerstore.load_documents().await {
        for doc_id in &doc_ids {
            let _ = transport.subscribe(DefraTopic::document(doc_id)).await;
            restored.insert(doc_id.clone());
        }
    }
    restored
}
