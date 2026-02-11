use std::str::FromStr;

use p2p::message::PushLogRequest;
use storage::corekv::IterOptions;

use crate::database::DB;

/// Push existing documents to a replicator peer.
///
/// Matches Go's `pushHeadsForAllDocs`: for each collection, iterate all docs,
/// get composite heads from headstore, load blocks, send PushLog to peer.
/// If an SE encryption key is provided, also generates and pushes SE artifacts
/// for collections with encrypted indexes.
pub async fn push_existing_docs<S: storage::corekv::Store + 'static>(
    handle: &p2p::P2PHostHandle,
    db: &DB<S>,
    peer_id: libp2p::PeerId,
    collections: &[String],
    se_encryption_key: Option<&[u8]>,
) -> Result<(), String> {
    // Wait for the connection to be fully established (dial is non-blocking).
    // After a node restart, re-establishing connectivity can take longer than
    // the initial connection, so we allow up to 15 seconds.
    let conn_timeout = std::time::Duration::from_secs(15);
    let conn_start = std::time::Instant::now();
    loop {
        let peers = handle.connected_peers().await.unwrap_or_default();
        if peers.contains(&peer_id) {
            break;
        }
        if conn_start.elapsed() > conn_timeout {
            return Err("timeout waiting for peer connection before push".to_string());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let local_peer_id = handle
        .local_peer_id()
        .await
        .map_err(|e| format!("failed to get local peer ID: {}", e))?;

    let txn = db
        .new_txn(true)
        .await
        .map_err(|e| format!("failed to create transaction: {}", e))?;

    let headstore = txn
        .headstore()
        .map_err(|e| format!("failed to get headstore: {}", e))?;
    let blockstore_view = txn
        .blockstore()
        .map_err(|e| format!("failed to get blockstore: {}", e))?;
    let datastore = txn
        .datastore()
        .map_err(|e| format!("failed to get datastore: {}", e))?;

    // Collect JoinHandles so we can await all pushes before signaling completion.
    let mut push_handles = Vec::new();

    for col_name in collections {
        let collection = match db
            .get_collection(col_name)
            .map_err(|e| format!("failed to get collection: {}", e))?
        {
            Some(c) => c,
            None => continue,
        };

        // Iterate datastore keys-only to get doc IDs.
        // Key format: /d/{collection_id}/{doc_id}
        // Sub-keys like /d/{collection_id}/{doc_id}/v are filtered out.
        let col_prefix = format!("/d/{}/", collection.collection_id()).into_bytes();
        let opts = IterOptions::new()
            .with_prefix(col_prefix)
            .with_keys_only(true);
        let mut doc_iter = datastore
            .iterator(opts)
            .await
            .map_err(|e| format!("failed to iterate datastore: {}", e))?;

        let mut doc_ids = Vec::new();
        while let Some(pair) = doc_iter
            .next()
            .await
            .map_err(|e| format!("datastore iteration error: {}", e))?
        {
            let key_str = String::from_utf8_lossy(&pair.key);
            let parts: Vec<&str> = key_str.split('/').collect();
            // Exact doc key: ["", "d", collection_id, doc_id] = 4 parts
            if parts.len() == 4 {
                doc_ids.push(parts[3].to_string());
            }
        }
        doc_iter
            .close()
            .await
            .map_err(|e| format!("datastore close error: {}", e))?;

        // For each document, push composite head blocks to the replicator.
        for doc_id in &doc_ids {
            let prefix = storage::keys::headstore::HeadstoreDocKey::field_prefix(doc_id, "C");
            let opts = IterOptions::new().with_prefix(prefix);
            let mut iter = headstore
                .iterator(opts)
                .await
                .map_err(|e| format!("failed to iterate headstore: {}", e))?;

            while let Some(pair) = iter
                .next()
                .await
                .map_err(|e| format!("headstore iteration error: {}", e))?
            {
                // Parse CID from key: /d/{doc_id}/C/{cid}
                let key_str = String::from_utf8_lossy(&pair.key);
                let parts: Vec<&str> = key_str.split('/').collect();
                if parts.len() < 5 {
                    continue;
                }
                let head_cid = match cid::Cid::from_str(parts[4]) {
                    Ok(c) => c,
                    Err(_) => continue,
                };

                // Read block data from blockstore
                let block_key = head_cid.to_bytes();
                let block_data = match blockstore_view.get(&block_key).await {
                    Ok(Some(data)) => data,
                    _ => continue,
                };

                let mut request = PushLogRequest::new(
                    doc_id.clone(),
                    head_cid.to_bytes(),
                    collection.collection_id().to_string(),
                    local_peer_id.to_string(),
                    block_data,
                );

                if let Err(e) = p2p::signing::sign_message(handle.keypair(), &mut request) {
                    tracing::warn!(error = %e, "Failed to sign PushLog request");
                    continue;
                }

                // Spawn each push concurrently but track the handle so we can
                // await completion before emitting ReplicatorCompleted.
                let push_h = handle.clone();
                push_handles.push(tokio::spawn(async move {
                    let _ = push_h.send_two_stream_request(peer_id, request).await;
                }));
            }

            iter.close()
                .await
                .map_err(|e| format!("headstore close error: {}", e))?;
        }
    }

    // Await all push tasks so ReplicatorCompleted isn't emitted prematurely.
    // The Go test framework copies expected heads on ReplicatorCompleted, then
    // waits for merge events -- if pushes haven't landed yet, we get timeouts.
    tracing::debug!(task_count = push_handles.len(), "awaiting push tasks");
    for jh in push_handles {
        let _ = jh.await;
    }
    tracing::debug!("all push tasks completed");

    // Generate and push SE artifacts for collections with encrypted indexes.
    if let Some(se_key) = se_encryption_key {
        let coordinator = crate::se::SECoordinator::with_key(se_key.to_vec());

        for col_name in collections {
            let collection = match db.get_collection(col_name) {
                Ok(Some(c)) => c,
                _ => continue,
            };

            let encrypted_indexes = &collection.schema().encrypted_indexes;
            if encrypted_indexes.is_empty() {
                continue;
            }

            tracing::debug!(collection = %col_name, index_count = encrypted_indexes.len(), "generating SE artifacts");

            // Iterate datastore to get doc IDs (same pattern as block push above)
            let col_prefix = format!("/d/{}/", collection.collection_id()).into_bytes();
            let opts = IterOptions::new()
                .with_prefix(col_prefix)
                .with_keys_only(true);
            let mut doc_iter = datastore
                .iterator(opts)
                .await
                .map_err(|e| format!("SE: failed to iterate datastore: {}", e))?;

            let mut se_doc_ids = Vec::new();
            while let Some(pair) = doc_iter
                .next()
                .await
                .map_err(|e| format!("SE: datastore iteration error: {}", e))?
            {
                let key_str = String::from_utf8_lossy(&pair.key);
                let parts: Vec<&str> = key_str.split('/').collect();
                if parts.len() == 4 {
                    se_doc_ids.push(parts[3].to_string());
                }
            }
            doc_iter
                .close()
                .await
                .map_err(|e| format!("SE: datastore close error: {}", e))?;

            // For each document, load field values and generate artifacts.
            let mut all_artifacts = Vec::new();
            for doc_id in &se_doc_ids {
                // Read document CBOR from datastore: /d/{collection_id}/{doc_id}
                let doc_key = format!("/d/{}/{}", collection.collection_id(), doc_id).into_bytes();
                let doc_data = match datastore.get(&doc_key).await {
                    Ok(Some(data)) => data,
                    _ => continue,
                };

                let doc = match document::Document::from_cbor(&doc_data) {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::warn!(doc_id = %doc_id, error = %e, "SE: failed to deserialize document");
                        continue;
                    }
                };

                // Extract field values as HashMap<String, NormalValue>
                let field_values: std::collections::HashMap<String, document::NormalValue> = doc
                    .values()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.value().clone()))
                    .collect();

                match coordinator.generate_artifacts(
                    collection.collection_id(),
                    doc_id,
                    encrypted_indexes,
                    &[],
                    &field_values,
                ) {
                    Ok(artifacts) => {
                        for a in artifacts {
                            all_artifacts.push(p2p::message::SEArtifact::new(
                                &a.doc_id,
                                &a.index_id,
                                a.search_tag,
                            ));
                        }
                    }
                    Err(e) => {
                        tracing::warn!(doc_id = %doc_id, error = %e, "SE: failed to generate artifacts");
                    }
                }
            }

            if !all_artifacts.is_empty() {
                tracing::debug!(artifact_count = all_artifacts.len(), collection = %col_name, peer_id = %peer_id, "sending SE artifacts");

                let se_request = p2p::message::PushSEArtifactsRequest::new(
                    collection.collection_id().to_string(),
                    all_artifacts,
                );

                if let Err(e) = handle.send_se_artifacts(peer_id, se_request).await {
                    tracing::warn!(
                        peer_id = %peer_id,
                        collection = %col_name,
                        error = %e,
                        "Failed to send SE artifacts to replicator"
                    );
                }
            }
        }
    }

    Ok(())
}
