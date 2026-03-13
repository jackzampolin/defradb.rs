use std::str::FromStr;

use acp::DocumentACP;
use p2p::message::PushLogRequest;
use p2p::transport::PeerId;
use p2p::P2PTransport;
use storage::corekv::{IterOptions, Reader, Store};

use crate::database::DB;
use crate::push_docs_common::{load_push_dag_blocks, resolve_push_creator};

/// Push existing documents to a replicator peer via a generic transport.
///
/// Transport-agnostic equivalent of `push_existing_docs`. Uses `P2PTransport`
/// methods instead of `P2PHostHandle`.
pub async fn push_existing_docs_via_transport<S: Store + 'static, T: P2PTransport>(
    transport: &T,
    db: &DB<S>,
    document_acp: Option<&dyn DocumentACP>,
    peer_id: &PeerId,
    collections: &[String],
    se_encryption_key: Option<&[u8]>,
) -> Result<(), String> {
    let conn_timeout = std::time::Duration::from_secs(15);
    let conn_start = std::time::Instant::now();
    loop {
        let peers = transport.connected_peers().await.unwrap_or_default();
        if peers.iter().any(|p| p == peer_id) {
            break;
        }
        if conn_start.elapsed() > conn_timeout {
            return Err("timeout waiting for peer connection before push".to_string());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let local_peer_id = transport.local_peer_id().to_string();

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

    let mut push_handles = Vec::new();

    for col_name in collections {
        let collection = match db
            .get_collection(col_name)
            .map_err(|e| format!("failed to get collection: {}", e))?
        {
            Some(c) => c,
            None => continue,
        };

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
            if parts.len() == 4 {
                doc_ids.push(parts[3].to_string());
            }
        }
        doc_iter
            .close()
            .await
            .map_err(|e| format!("datastore close error: {}", e))?;

        for doc_id in &doc_ids {
            let creator =
                resolve_push_creator(document_acp, &collection, doc_id, &local_peer_id).await;
            let prefix = storage::keys::headstore::HeadstoreDocKey::field_prefix(doc_id, "C");
            let opts = IterOptions::new().with_prefix(prefix);
            let mut iter = headstore
                .iterator(opts)
                .await
                .map_err(|e| format!("failed to iterate headstore: {}", e))?;

            let mut doc_blocks = Vec::new();

            while let Some(pair) = iter
                .next()
                .await
                .map_err(|e| format!("headstore iteration error: {}", e))?
            {
                let key_str = String::from_utf8_lossy(&pair.key);
                let parts: Vec<&str> = key_str.split('/').collect();
                if parts.len() < 5 {
                    continue;
                }
                let head_cid = match cid::Cid::from_str(parts[4]) {
                    Ok(c) => c,
                    Err(_) => continue,
                };

                let block_key = head_cid.to_bytes();
                let block_data = match blockstore_view.get(&block_key).await {
                    Ok(Some(data)) => data,
                    _ => continue,
                };

                doc_blocks
                    .extend(load_push_dag_blocks(&blockstore_view, head_cid, block_data).await);
            }

            iter.close()
                .await
                .map_err(|e| format!("headstore close error: {}", e))?;

            let mut requests = Vec::new();
            for (block_cid, block_data) in doc_blocks {
                let mut request = PushLogRequest::new(
                    doc_id.clone(),
                    block_cid.to_bytes(),
                    collection.collection_id().to_string(),
                    creator.clone(),
                    block_data,
                );
                if let Err(e) = p2p::signing::sign_with_transport(transport, &mut request) {
                    tracing::warn!(error = %e, "Failed to sign PushLog request");
                    continue;
                }
                requests.push(request);
            }

            if !requests.is_empty() {
                let t = transport.clone();
                let pid = peer_id.clone();
                push_handles.push(tokio::spawn(async move {
                    for req in requests {
                        let cid = req.cid.clone();
                        match t.send_two_stream_request(&pid, req).await {
                            Ok(reply) if reply.err_message.is_some() => {
                                tracing::warn!(
                                    peer_id = %pid,
                                    cid_len = cid.len(),
                                    error = %reply.err_message.as_deref().unwrap_or("unknown pushlog error"),
                                    "Existing document replay PushLog was rejected"
                                );
                            }
                            Ok(_) => {}
                            Err(e) => {
                                tracing::warn!(
                                    peer_id = %pid,
                                    cid_len = cid.len(),
                                    error = %e,
                                    "Existing document replay PushLog failed"
                                );
                            }
                        }
                    }
                }));
            }
        }
    }

    tracing::debug!(task_count = push_handles.len(), "awaiting push tasks");
    for jh in push_handles {
        let _ = jh.await;
    }
    tracing::debug!("all push tasks completed");

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

            let mut all_artifacts = Vec::new();
            for doc_id in &se_doc_ids {
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

                if let Err(e) = transport.send_se_artifacts(peer_id, se_request).await {
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

/// Retry pushing a single document's composite heads to a replicator peer
/// via a generic transport.
///
/// Transport-agnostic equivalent of `retry_doc`.
pub async fn retry_doc_via_transport<S: Store + 'static, T: P2PTransport>(
    transport: &T,
    db: &DB<S>,
    document_acp: Option<&dyn DocumentACP>,
    peer_id: &PeerId,
    doc_id: &str,
    collection_id: &str,
) -> Result<(), String> {
    let local_peer_id = transport.local_peer_id().to_string();
    let collection = db
        .find_collection_by_id(collection_id)
        .map_err(|e| format!("failed to get collection: {}", e))?
        .ok_or_else(|| format!("collection '{}' not found", collection_id))?;
    let creator = resolve_push_creator(document_acp, &collection, doc_id, &local_peer_id).await;

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

    let prefix = storage::keys::headstore::HeadstoreDocKey::field_prefix(doc_id, "C");
    let opts = IterOptions::new().with_prefix(prefix);
    let mut iter = head_txn
        .iterator(opts)
        .await
        .map_err(|e| format!("headstore iterator: {}", e))?;

    let mut any_failed = false;
    while let Some(pair) = iter
        .next()
        .await
        .map_err(|e| format!("headstore iteration: {}", e))?
    {
        let key_str = String::from_utf8_lossy(&pair.key);
        let parts: Vec<&str> = key_str.split('/').collect();
        if parts.len() < 5 {
            continue;
        }
        let head_cid = match cid::Cid::from_str(parts[4]) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let block_data = match block_txn.get(&head_cid.to_bytes()).await {
            Ok(Some(data)) => data,
            _ => continue,
        };

        for (block_cid, block_data) in load_push_dag_blocks(&*block_txn, head_cid, block_data).await
        {
            let mut request = PushLogRequest::new(
                doc_id.to_string(),
                block_cid.to_bytes(),
                collection_id.to_string(),
                creator.clone(),
                block_data,
            );

            if p2p::signing::sign_with_transport(transport, &mut request).is_err() {
                any_failed = true;
                continue;
            }

            match transport.send_two_stream_request(peer_id, request).await {
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
