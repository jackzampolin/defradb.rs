use std::sync::Arc;

use acp::DocumentACP;
use bytes::Bytes;
use p2p::message::PushLogRequest;
use storage::corekv::{IterOptions, Reader, Store};

use crate::push_docs_common::{load_latest_composite_head_cids, load_push_dag_blocks};
use crate::push_docs_creator::resolve_push_creator;
use crate::push_docs_replay::{ReplayPushConfig, ReplayPushGate};
use db::database::DB;

#[derive(Debug, Clone, Copy, Default)]
pub struct PushExistingDocsSeOptions<'a> {
    pub encryption_key: Option<&'a [u8]>,
    pub identity_pubkey: Option<&'a [u8]>,
}

/// Push existing documents to a replicator peer.
///
/// Matches Go's `pushHeadsForAllDocs`: for each collection, iterate all docs,
/// get composite heads from headstore, load blocks, send PushLog to peer.
/// If an SE encryption key is provided, also generates and pushes SE artifacts
/// for collections with encrypted indexes. The identity pubkey is threaded
/// through SE artifact generation to ensure per-identity tag isolation.
pub async fn push_existing_docs<S: storage::corekv::Store + 'static>(
    handle: &p2p::P2PHostHandle,
    db: &DB<S>,
    document_acp: Option<&dyn DocumentACP>,
    peer_id: libp2p::PeerId,
    collections: &[String],
    se_encryption_key: Option<&[u8]>,
    se_identity_pubkey: Option<&[u8]>,
) -> Result<(), String> {
    push_existing_docs_with_config(
        handle,
        db,
        document_acp,
        peer_id,
        collections,
        PushExistingDocsSeOptions {
            encryption_key: se_encryption_key,
            identity_pubkey: se_identity_pubkey,
        },
        ReplayPushConfig::default(),
    )
    .await
}

/// Push existing documents to a replicator peer with explicit replay limits.
pub async fn push_existing_docs_with_config<S: storage::corekv::Store + 'static>(
    handle: &p2p::P2PHostHandle,
    db: &DB<S>,
    document_acp: Option<&dyn DocumentACP>,
    peer_id: libp2p::PeerId,
    collections: &[String],
    se_options: PushExistingDocsSeOptions<'_>,
    replay_config: ReplayPushConfig,
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
    let local_peer_id_str = local_peer_id.to_string();

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
    let encstore_view = txn
        .encstore()
        .map_err(|e| format!("failed to get encstore: {}", e))?;
    let datastore = txn
        .datastore()
        .map_err(|e| format!("failed to get datastore: {}", e))?;

    // Collect JoinHandles so we can await all pushes before signaling completion.
    let mut push_handles = Vec::new();
    let replay_gate = Arc::new(ReplayPushGate::new(replay_config));
    let peer_key = p2p::transport::PeerId::from(peer_id);
    let mut skipped_creator_docs = 0usize;

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

        // For each document, send field blocks before composite heads.
        // Go needs linked field (LWW) blocks in its blockstore before it
        // processes the composite block, otherwise it tries Bitswap which
        // doesn't work reliably cross-platform.
        for doc_id in &doc_ids {
            let creator = match resolve_push_creator(
                document_acp,
                &collection,
                doc_id,
                &local_peer_id_str,
            )
            .await
            {
                Ok(creator) => creator,
                Err(error) => {
                    skipped_creator_docs += 1;
                    tracing::warn!(
                        collection = %collection.name(),
                        collection_id = %collection.collection_id(),
                        doc_id = %doc_id,
                        error = %error,
                        "Skipping existing document replay because ACP creator could not be resolved"
                    );
                    continue;
                }
            };
            // Collect phase: pre-load all DAG blocks before spawning tasks.
            let mut doc_blocks = Vec::new();
            for head_cid in
                load_latest_composite_head_cids(&headstore, &blockstore_view, doc_id).await
            {
                let block_key = head_cid.to_bytes();
                let block_data = match blockstore_view.get(&block_key).await {
                    Ok(Some(data)) => data,
                    _ => continue,
                };

                doc_blocks.extend(
                    load_push_dag_blocks(&blockstore_view, &encstore_view, head_cid, block_data)
                        .await,
                );
            }

            // Send phase: build signed requests in DAG dependency order,
            // then spawn a task to send them sequentially so ordering is preserved.
            let mut requests = Vec::new();
            for (block_cid, block_data) in doc_blocks {
                let mut request = PushLogRequest::new(
                    doc_id.clone(),
                    Bytes::from(block_cid.to_bytes()),
                    collection.collection_id().to_string(),
                    creator.clone(),
                    Bytes::from(block_data),
                );
                if let Err(e) = p2p::signing::sign_message(handle.keypair(), &mut request) {
                    tracing::warn!(error = %e, "Failed to sign PushLog request");
                    continue;
                }
                requests.push(request);
            }

            if !requests.is_empty() {
                let push_h = handle.clone();
                let gate = replay_gate.clone();
                let peer_key = peer_key.clone();
                let total_blocks = requests.len();
                let permit = replay_gate
                    .acquire_document_task()
                    .await
                    .map_err(|e| format!("replay gate closed before scheduling push: {e}"))?;
                push_handles.push(tokio::spawn(async move {
                    let _permit = permit;
                    let mut completed_blocks = 0usize;
                    for req in requests {
                        let cid = req.cid.clone();
                        match gate
                            .send_pushlog(&peer_key, push_h.send_two_stream_request(peer_id, req))
                            .await
                        {
                            Ok(reply) if reply.err_message.is_some() => {
                                tracing::warn!(
                                    peer_id = %peer_id,
                                    completed_blocks,
                                    total_blocks,
                                    cid_len = cid.len(),
                                    error = %reply.err_message.as_deref().unwrap_or("unknown pushlog error"),
                                    "Existing document replay PushLog was rejected; stopping replay for this document"
                                );
                                break;
                            }
                            Ok(_) => {
                                completed_blocks += 1;
                            }
                            Err(e) => {
                                if e.is_connection_like() {
                                    tracing::debug!(
                                        peer_id = %peer_id,
                                        completed_blocks,
                                        total_blocks,
                                        cid_len = cid.len(),
                                        error = %e,
                                        "Existing document replay stopped because the connection became unavailable"
                                    );
                                } else {
                                    tracing::warn!(
                                        peer_id = %peer_id,
                                        completed_blocks,
                                        total_blocks,
                                        cid_len = cid.len(),
                                        error = %e,
                                        "Existing document replay PushLog failed; stopping replay for this document"
                                    );
                                }
                                break;
                            }
                        }
                    }
                }));
            }
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

    if skipped_creator_docs > 0 {
        return Err(format!(
            "skipped {skipped_creator_docs} existing document replay(s) because ACP creator could not be resolved"
        ));
    }

    // Generate and push SE artifacts for collections with encrypted indexes.
    if let Some(se_key) = se_options.encryption_key {
        let coordinator = match se_options.identity_pubkey {
            Some(pubkey) => {
                crate::se::SECoordinator::with_key_and_identity(se_key.to_vec(), pubkey.to_vec())
            }
            None => crate::se::SECoordinator::with_key(se_key.to_vec()),
        };

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

/// Retry pushing a single document's composite heads to a replicator peer.
///
/// Reads composite head CIDs from the headstore, loads block data
/// (field blocks + composite block) from the blockstore, and sends
/// signed PushLogRequests to the target peer.
pub async fn retry_doc<S: Store + 'static>(
    handle: &p2p::P2PHostHandle,
    db: &DB<S>,
    document_acp: Option<&dyn DocumentACP>,
    peer_id: libp2p::PeerId,
    doc_id: &str,
    collection_id: &str,
) -> Result<(), String> {
    let local_peer_id = handle
        .local_peer_id()
        .await
        .map_err(|e| format!("failed to get local peer ID: {}", e))?;
    let local_peer_id_str = local_peer_id.to_string();
    let collection = db
        .find_collection_by_id(collection_id)
        .map_err(|e| format!("failed to get collection: {}", e))?
        .ok_or_else(|| format!("collection '{}' not found", collection_id))?;
    let creator = resolve_push_creator(document_acp, &collection, doc_id, &local_peer_id_str)
        .await
        .map_err(|e| e.to_string())?;

    let headstore = storage::stores::Headstore::new(db.store().clone());
    let head_txn = headstore
        .new_txn(true)
        .await
        .map_err(|e| format!("headstore txn: {}", e))?;

    let blockstore_view = storage::stores::Blockstore::new(db.store().clone(), true);
    let block_txn = blockstore_view
        .new_txn(true)
        .await
        .map_err(|e| format!("blockstore txn: {}", e))?;
    let encstore_view = storage::stores::Blockstore::new_with_namespace(
        db.store().clone(),
        true,
        storage::namespace::Namespace::Encstore,
    );
    let enc_txn = encstore_view
        .new_txn(true)
        .await
        .map_err(|e| format!("encstore txn: {}", e))?;

    let mut any_failed = false;
    for head_cid in load_latest_composite_head_cids(&*head_txn, &*block_txn, doc_id).await {
        let block_data = match block_txn.get(&head_cid.to_bytes()).await {
            Ok(Some(data)) => data,
            _ => continue,
        };

        for (block_cid, block_data) in
            load_push_dag_blocks(&*block_txn, &*enc_txn, head_cid, block_data).await
        {
            let mut request = PushLogRequest::new(
                doc_id.to_string(),
                Bytes::from(block_cid.to_bytes()),
                collection_id.to_string(),
                creator.clone(),
                Bytes::from(block_data),
            );

            if p2p::signing::sign_message(handle.keypair(), &mut request).is_err() {
                any_failed = true;
                continue;
            }

            match handle.send_two_stream_request(peer_id, request).await {
                Ok(reply) if reply.err_message.is_some() => any_failed = true,
                Ok(_) => {}
                Err(_) => any_failed = true,
            }
        }
    }
    if any_failed {
        Err("some pushes failed".to_string())
    } else {
        Ok(())
    }
}
