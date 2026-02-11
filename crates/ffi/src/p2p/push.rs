use std::str::FromStr;
use std::sync::Arc;

use defra_core::Block;
use p2p::message::PushLogRequest;
use storage::corekv::IterOptions;

use crate::helpers::get_rt;
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

/// Push existing documents to a replicator peer.
///
/// Matches Go's `pushHeadsForAllDocs`: for each collection, iterate all docs,
/// get composite heads from headstore, load blocks, send PushLog to peer.
/// If an SE encryption key is provided, also generates and pushes SE artifacts
/// for collections with encrypted indexes.
pub(crate) async fn push_existing_docs(
    handle: &p2p::P2PHostHandle,
    db: &crate::state::FfiDatabase,
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
        let coordinator = db::se::SECoordinator::with_key(se_key.to_vec());

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

/// Retry pushing existing documents to all registered replicators.
///
/// For each registered replicator, re-pushes all existing documents.
/// `push_existing_docs` internally waits up to 5s for the peer connection
/// to be established, so this can be called immediately after dialing.
///
/// # Safety
///
/// `node_ptr` must be a valid node handle.
#[no_mangle]
pub unsafe extern "C" fn p2p_retry_replicators(node_ptr: usize) -> FfiResult {
    let rt = try_ffi!(get_rt());

    let result = NODES
        .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("no p2p system configured".to_string()),
            };
            let db = &state.database;

            rt.block_on(async {
                let replicators = p2p
                    .handle
                    .get_all_replicators()
                    .await
                    .map_err(|e| format!("failed to get replicators: {}", e))?;

                let all_collections = db
                    .list_collections()
                    .map_err(|e| format!("failed to list collections: {}", e))?;

                tracing::debug!(
                    replicator_count = replicators.len(),
                    collection_count = all_collections.len(),
                    "found replicators and collections"
                );

                let mut push_handles = Vec::new();

                for rep in &replicators {
                    let peer_id = match rep.peer_id() {
                        Some(id) => id,
                        None => continue,
                    };

                    tracing::debug!(peer_id = %peer_id, "pushing existing docs to replicator");

                    let push_handle = p2p.handle.clone();
                    let push_db = Arc::clone(db);
                    let push_collections = all_collections.clone();
                    let push_se_key = state.se_encryption_key.clone();

                    push_handles.push(tokio::spawn(async move {
                        if let Err(e) = push_existing_docs(
                            &push_handle,
                            &push_db,
                            peer_id,
                            &push_collections,
                            push_se_key.as_deref(),
                        )
                        .await
                        {
                            tracing::error!(
                                peer_id = %peer_id,
                                error = %e,
                                "Failed to retry push existing docs to replicator"
                            );
                        }
                    }));
                }

                tracing::debug!(task_count = push_handles.len(), "awaiting retry push tasks");
                for h in push_handles {
                    let _ = h.await;
                }
                tracing::debug!("all retry push tasks completed");

                Ok(())
            })
        })
        .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
        .and_then(|r| r);

    match result {
        Ok(()) => FfiResult::ok(),
        Err(e) => FfiResult::error(e),
    }
}

/// Retry pushing a single document's composite heads to a replicator peer.
///
/// Reads composite head CIDs from the headstore, loads block data from the
/// blockstore, builds signed PushLogRequests, and sends them.
pub(crate) async fn retry_doc(
    handle: &p2p::P2PHostHandle,
    store: &Arc<crate::state::FfiStore>,
    peer_id: libp2p::PeerId,
    doc_id: &str,
    collection_id: &str,
) -> Result<(), String> {
    use storage::corekv::{Reader, Store};

    let local_peer_id = handle
        .local_peer_id()
        .await
        .map_err(|e| format!("failed to get local peer ID: {}", e))?;

    let headstore = storage::stores::Headstore::new(store.clone());
    let head_txn = headstore
        .new_txn(true)
        .await
        .map_err(|e| format!("headstore txn: {}", e))?;

    let blockstore_view = storage::stores::Blockstore::new(store.clone(), true);
    let block_txn = blockstore_view
        .new_txn(true)
        .await
        .map_err(|e| format!("blockstore txn: {}", e))?;

    let prefix = storage::keys::headstore::HeadstoreDocKey::field_prefix(doc_id, "C");
    tracing::debug!(doc_id = %doc_id, "looking for composite heads");
    let opts = IterOptions::new().with_prefix(prefix);
    let mut iter = head_txn
        .iterator(opts)
        .await
        .map_err(|e| format!("headstore iterator: {}", e))?;

    let mut any_failed = false;
    let mut head_count = 0u32;
    while let Some(pair) = iter
        .next()
        .await
        .map_err(|e| format!("headstore iteration: {}", e))?
    {
        // Parse CID from key: /d/{doc_id}/C/{cid}
        let key_str = String::from_utf8_lossy(&pair.key);
        let parts: Vec<&str> = key_str.split('/').collect();
        if parts.len() < 5 {
            tracing::debug!(key = %key_str, "skipping malformed headstore key");
            continue;
        }
        let head_cid = match cid::Cid::from_str(parts[4]) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(cid_str = %parts[4], error = %e, "failed to parse CID");
                continue;
            }
        };
        head_count += 1;

        // Read composite block data from blockstore
        let block_data = match block_txn.get(&head_cid.to_bytes()).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                tracing::debug!(cid = %head_cid, "block not found in blockstore");
                continue;
            }
            Err(e) => {
                tracing::warn!(cid = %head_cid, error = %e, "block read error");
                continue;
            }
        };

        // Send the full DAG: field blocks first, then composite last.
        // This matches push_dag_to_replicators() so the receiver has all
        // blocks when the composite arrives and doesn't need Bitswap.
        if let Ok(parsed) = Block::from_dag_cbor(&block_data) {
            if let Some(ref links) = parsed.links {
                for link in links {
                    if let Ok(Some(field_data)) = block_txn.get(&link.link.to_bytes()).await {
                        let mut field_req = PushLogRequest::new(
                            doc_id.to_string(),
                            link.link.to_bytes(),
                            collection_id.to_string(),
                            local_peer_id.to_string(),
                            field_data,
                        );
                        if p2p::signing::sign_message(handle.keypair(), &mut field_req).is_ok() {
                            if let Err(e) = handle.send_two_stream_request(peer_id, field_req).await
                            {
                                tracing::warn!(cid = %link.link, error = %e, "field block send failed");
                                any_failed = true;
                            }
                        }
                    }
                }
            }
        }

        // Send composite block last
        let mut request = PushLogRequest::new(
            doc_id.to_string(),
            head_cid.to_bytes(),
            collection_id.to_string(),
            local_peer_id.to_string(),
            block_data,
        );

        if let Err(e) = p2p::signing::sign_message(handle.keypair(), &mut request) {
            tracing::warn!(error = %e, "Failed to sign retry PushLog request");
            any_failed = true;
            continue;
        }

        if let Err(e) = handle.send_two_stream_request(peer_id, request).await {
            tracing::warn!(peer_id = %peer_id, doc_id = %doc_id, cid = %head_cid, error = %e, "PushLog send failed");
            any_failed = true;
        } else {
            tracing::debug!(peer_id = %peer_id, doc_id = %doc_id, cid = %head_cid, "PushLog sent");
        }
    }

    tracing::debug!(doc_id = %doc_id, heads_found = head_count, any_failed, "retry doc complete");

    if any_failed {
        Err("some pushes failed".to_string())
    } else {
        Ok(())
    }
}
