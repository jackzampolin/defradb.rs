use std::sync::Arc;

use acp::DocumentACP;
use bytes::Bytes;
use p2p::message::PushLogRequest;
use p2p::transport::PeerId;
use p2p::P2PTransport;
use storage::corekv::{IterOptions, Reader, Store};

use crate::push_docs_common::load_latest_composite_head_cids;
use crate::push_docs_creator::resolve_push_creator;
use crate::push_docs_replay::{
    persist_replay_failures, ReplayDocumentFailure, ReplayPushConfig, ReplayPushGate,
};
use db::database::DB;

async fn document_matches_filter<R: Reader + ?Sized>(
    datastore: &R,
    collection_id: &str,
    doc_short_id: u64,
    filter: &p2p::ReplicationFilter,
    matcher: &dyn p2p::replicator::ReplicationFilterMatcher,
) -> Result<bool, String> {
    let mut doc_key = format!("/d/{}/", collection_id).into_bytes();
    doc_key.extend_from_slice(&storage::keys::doc_id_index::encode_doc_short_id(
        doc_short_id,
    ));
    let Some(doc_data) = datastore
        .get(&doc_key)
        .await
        .map_err(|e| format!("failed to read document for replication filter: {}", e))?
    else {
        return Ok(false);
    };

    let doc = document::Document::from_cbor(&doc_data)
        .map_err(|e| format!("failed to decode document for replication filter: {}", e))?;
    let document_json = serde_json::Value::Object(
        doc.to_map()
            .map_err(|e| format!("failed to encode document for replication filter: {}", e))?
            .into_iter()
            .collect(),
    );
    Ok(matcher.matches("", filter, &document_json))
}

/// Push existing documents to a replicator peer via a generic transport.
///
/// Transport-agnostic equivalent of `push_existing_docs`. Uses `P2PTransport`
/// methods instead of `P2PHostHandle`.
#[allow(clippy::too_many_arguments)]
pub async fn push_existing_docs_via_transport<S: Store + 'static, T: P2PTransport>(
    transport: &T,
    db: &DB<S>,
    document_acp: Option<&dyn DocumentACP>,
    peer_id: &PeerId,
    collections: &[String],
    filters: &p2p::ReplicationFilters,
    se_encryption_key: Option<&[u8]>,
    matcher: &dyn p2p::replicator::ReplicationFilterMatcher,
    car_authority: &p2p::sync::HeadHintCarAuthority,
) -> Result<(), String> {
    push_existing_docs_via_transport_with_config(
        transport,
        db,
        document_acp,
        peer_id,
        collections,
        filters,
        se_encryption_key,
        ReplayPushConfig::default(),
        matcher,
        car_authority,
    )
    .await
}

/// Push existing documents to a replicator peer via a generic transport with explicit replay limits.
#[allow(clippy::too_many_arguments)]
pub async fn push_existing_docs_via_transport_with_config<S: Store + 'static, T: P2PTransport>(
    transport: &T,
    db: &DB<S>,
    document_acp: Option<&dyn DocumentACP>,
    peer_id: &PeerId,
    collections: &[String],
    filters: &p2p::ReplicationFilters,
    se_encryption_key: Option<&[u8]>,
    replay_config: ReplayPushConfig,
    matcher: &dyn p2p::replicator::ReplicationFilterMatcher,
    car_authority: &p2p::sync::HeadHintCarAuthority,
) -> Result<(), String> {
    let conn_timeout = std::time::Duration::from_secs(15);
    let conn_start = std::time::Instant::now();
    let mut logged_conn_error = false;
    loop {
        let peers = match transport.connected_peers().await {
            Ok(peers) => peers,
            Err(e) => {
                if !logged_conn_error {
                    tracing::warn!(
                        peer_id = %peer_id,
                        error = %e,
                        "connected_peers check failed during replay wait"
                    );
                    logged_conn_error = true;
                }
                Vec::new()
            }
        };
        if peers.iter().any(|p| p == peer_id) {
            break;
        }
        if conn_start.elapsed() > conn_timeout {
            return Err("timeout waiting for peer connection before push".to_string());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let local_peer_id = transport.local_peer_id().to_string();
    let peerstore = storage::stores::Peerstore::new(db.store().clone());
    let Some(retry_guard) = peerstore
        .acquire_replicator_retry_guard(peer_id.as_str())
        .await
        .map_err(|error| format!("failed to coordinate existing-document replay: {error}"))?
    else {
        tracing::debug!(peer_id = %peer_id, "Replicator removed before existing-document replay");
        return Ok(());
    };
    // Serialize storage transitions, not the bounded network wait. A live
    // update must be able to dirty the marker while initial replay is stalled.
    drop(retry_guard);

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
    let systemstore = txn
        .systemstore()
        .map_err(|e| format!("failed to get systemstore: {}", e))?;

    let mut push_handles = Vec::new();
    let replay_gate = Arc::new(ReplayPushGate::new(replay_config));
    let mut skipped_creator_docs = 0usize;

    for col_name in collections {
        let collection = match db
            .get_collection(col_name)
            .map_err(|e| format!("failed to get collection: {}", e))?
        {
            Some(c) => c,
            None => continue,
        };

        let col_prefix = format!("/d/{}/", collection.collection_id()).into_bytes();
        let doc_prefix_len = col_prefix.len();
        let opts = IterOptions::new()
            .with_prefix(col_prefix)
            .with_keys_only(true);
        let mut doc_iter = datastore
            .iterator(opts)
            .await
            .map_err(|e| format!("failed to iterate datastore: {}", e))?;

        let mut doc_short_ids = Vec::new();
        while let Some(pair) = doc_iter
            .next()
            .await
            .map_err(|e| format!("datastore iteration error: {}", e))?
        {
            if let Ok(short_id) =
                storage::keys::doc_id_index::decode_doc_short_id(&pair.key[doc_prefix_len..])
            {
                doc_short_ids.push(short_id);
            }
        }
        doc_iter
            .close()
            .await
            .map_err(|e| format!("datastore close error: {}", e))?;

        let mut doc_ids = Vec::new();
        for short_id in doc_short_ids {
            match db::doc_id_map::get_doc_id(&systemstore, short_id).await {
                Ok(Some(doc_id)) => doc_ids.push((short_id, doc_id)),
                Ok(None) => {}
                Err(e) => return Err(format!("doc-ID mapping lookup failed: {}", e)),
            }
        }

        for (doc_short_id, doc_id) in &doc_ids {
            if let Some(filter) = filters.get(collection.collection_id()) {
                if !document_matches_filter(
                    &datastore,
                    collection.collection_id(),
                    *doc_short_id,
                    filter,
                    matcher,
                )
                .await?
                {
                    continue;
                }
            }

            let creator = match resolve_push_creator(
                document_acp,
                &collection,
                doc_id,
                &local_peer_id,
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
            let mut doc_blocks = Vec::new();
            for head_cid in
                load_latest_composite_head_cids(&headstore, &blockstore_view, *doc_short_id).await
            {
                let block_key = head_cid.to_bytes();
                let block_data = match blockstore_view.get(&block_key).await {
                    Ok(Some(data)) => data,
                    _ => continue,
                };

                doc_blocks.push((head_cid, block_data));
            }

            let mut replay_head_cids: Vec<_> = doc_blocks.iter().map(|(cid, _)| *cid).collect();
            replay_head_cids.sort_unstable();
            let mut requests = Vec::new();
            for (block_cid, block_data) in doc_blocks {
                let grant = car_authority
                    .register(peer_id.clone(), block_cid)
                    .ok_or_else(|| {
                        format!("selective CAR authority full for replay head {block_cid}")
                    })?;
                let mut request = PushLogRequest::new(
                    doc_id.clone(),
                    Bytes::from(block_cid.to_bytes()),
                    collection.collection_id().to_string(),
                    creator.clone(),
                    Bytes::from(block_data),
                );
                if let Err(e) = p2p::signing::sign_with_transport(transport, &mut request) {
                    tracing::warn!(error = %e, "Failed to sign PushLog request");
                    continue;
                }
                requests.push((grant, request));
            }

            if !requests.is_empty() {
                let Some(_marker_guard) = peerstore
                    .acquire_replicator_retry_guard(peer_id.as_str())
                    .await
                    .map_err(|error| format!("failed to coordinate replay marker: {error}"))?
                else {
                    return Ok(());
                };
                peerstore
                    .observe_push_head(peer_id.as_str(), doc_id, collection.collection_id(), "", 0)
                    .await
                    .map_err(|error| format!("failed to register replay marker: {error}"))?;
                let t = transport.clone();
                let pid = peer_id.clone();
                let gate = replay_gate.clone();
                let peer_key = pid.clone();
                let replay_doc_id = doc_id.clone();
                let replay_collection_id = collection.collection_id().to_string();
                let total_blocks = requests.len();
                let permit = replay_gate
                    .acquire_document_task()
                    .await
                    .map_err(|e| format!("replay gate closed before scheduling push: {e}"))?;
                push_handles.push((
                    replay_doc_id,
                    replay_collection_id,
                    *doc_short_id,
                    replay_head_cids,
                    tokio::spawn(async move {
                        let _permit = permit;
                        let mut completed_blocks = 0usize;
                        for (_car_grant, req) in requests {
                            let cid = req.cid.clone();
                            match gate
                                .send_pushlog(&peer_key, t.send_two_stream_request(&pid, req))
                                .await
                            {
                                Ok(reply) if reply.err_message.is_some() => {
                                    tracing::warn!(
                                        peer_id = %pid,
                                        completed_blocks,
                                        total_blocks,
                                        cid_len = cid.len(),
                                        error = %reply.err_message.as_deref().unwrap_or("unknown pushlog error"),
                                        "Existing document replay PushLog was rejected; deferring document to persisted retry"
                                    );
                                    return false;
                                }
                                Ok(_) => {
                                    completed_blocks += 1;
                                }
                                Err(e) => {
                                    if e.is_connection_like() {
                                        tracing::debug!(
                                            peer_id = %pid,
                                            completed_blocks,
                                            total_blocks,
                                            cid_len = cid.len(),
                                            error = %e,
                                            "Existing document replay stopped because the connection became unavailable"
                                        );
                                    } else {
                                        tracing::warn!(
                                            peer_id = %pid,
                                            completed_blocks,
                                            total_blocks,
                                            cid_len = cid.len(),
                                            error = %e,
                                            "Existing document replay PushLog failed; deferring document to persisted retry"
                                        );
                                    }
                                    return false;
                                }
                            }
                        }
                        true
                    }),
                ));
            }
        }
    }

    tracing::debug!(task_count = push_handles.len(), "awaiting push tasks");
    let mut replay_failures = Vec::new();
    for (doc_id, collection_id, doc_short_id, attempted_heads, jh) in push_handles {
        match jh.await {
            Ok(true) => {
                let Some(_completion_guard) = peerstore
                    .acquire_replicator_retry_guard(peer_id.as_str())
                    .await
                    .map_err(|error| format!("failed to coordinate replay completion: {error}"))?
                else {
                    continue;
                };
                let verify_txn = db
                    .new_txn(true)
                    .await
                    .map_err(|error| format!("replay head verification transaction: {error}"))?;
                let verify_heads = verify_txn.headstore().map_err(|error| error.to_string())?;
                let verify_blocks = verify_txn.blockstore().map_err(|error| error.to_string())?;
                let mut current_heads =
                    load_latest_composite_head_cids(&verify_heads, &verify_blocks, doc_short_id)
                        .await;
                current_heads.sort_unstable();
                if current_heads != attempted_heads {
                    tracing::debug!(%doc_id, "Document changed during replay; retaining dirty marker");
                    replay_failures.push(ReplayDocumentFailure {
                        doc_id,
                        collection_id,
                    });
                    continue;
                }
                let retry = storage::stores::PersistedPushRetry {
                    doc_id: doc_id.clone(),
                    collection_id: collection_id.clone(),
                    cid: String::new(),
                    priority: 0,
                    pending: true,
                    scope: storage::stores::RetryScope::Document,
                    retry_info: storage::stores::RetryInfo::new_initial(),
                };
                peerstore
                    .complete_retry_document(peer_id.as_str(), &retry)
                    .await
                    .map_err(|error| format!("failed to clear replay marker: {error}"))?;
            }
            Ok(false) => replay_failures.push(ReplayDocumentFailure {
                doc_id,
                collection_id,
            }),
            Err(error) => {
                tracing::error!(%error, %doc_id, "Replay push task panicked or was cancelled");
                replay_failures.push(ReplayDocumentFailure {
                    doc_id,
                    collection_id,
                });
            }
        }
    }
    tracing::debug!("all push tasks completed");
    persist_replay_failures(db.store().clone(), peer_id, &replay_failures).await?;

    if skipped_creator_docs > 0 {
        return Err(format!(
            "skipped {skipped_creator_docs} existing document replay(s) because ACP creator could not be resolved"
        ));
    }

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
            let doc_prefix_len = col_prefix.len();
            let opts = IterOptions::new()
                .with_prefix(col_prefix)
                .with_keys_only(true);
            let mut doc_iter = datastore
                .iterator(opts)
                .await
                .map_err(|e| format!("SE: failed to iterate datastore: {}", e))?;

            let mut se_doc_short_ids = Vec::new();
            while let Some(pair) = doc_iter
                .next()
                .await
                .map_err(|e| format!("SE: datastore iteration error: {}", e))?
            {
                if let Ok(short_id) =
                    storage::keys::doc_id_index::decode_doc_short_id(&pair.key[doc_prefix_len..])
                {
                    se_doc_short_ids.push(short_id);
                }
            }
            doc_iter
                .close()
                .await
                .map_err(|e| format!("SE: datastore close error: {}", e))?;

            let mut se_doc_ids = Vec::new();
            for short_id in se_doc_short_ids {
                match db::doc_id_map::get_doc_id(&systemstore, short_id).await {
                    Ok(Some(doc_id)) => se_doc_ids.push((short_id, doc_id)),
                    Ok(None) => {}
                    Err(e) => return Err(format!("SE: doc-ID mapping lookup failed: {}", e)),
                }
            }

            let mut all_artifacts = Vec::new();
            for (doc_short_id, doc_id) in &se_doc_ids {
                if let Some(filter) = filters.get(collection.collection_id()) {
                    if !document_matches_filter(
                        &datastore,
                        collection.collection_id(),
                        *doc_short_id,
                        filter,
                        matcher,
                    )
                    .await?
                    {
                        continue;
                    }
                }

                let doc_key = storage::keys::doc_key(collection.collection_id(), *doc_short_id);
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
#[allow(clippy::too_many_arguments)]
pub async fn retry_doc_via_transport<S: Store + 'static, T: P2PTransport>(
    transport: &T,
    db: &DB<S>,
    document_acp: Option<&dyn DocumentACP>,
    peer_id: &PeerId,
    doc_id: &str,
    collection_id: &str,
    filters: &p2p::ReplicationFilters,
    matcher: &dyn p2p::replicator::ReplicationFilterMatcher,
    car_authority: &p2p::sync::HeadHintCarAuthority,
) -> Result<(), String> {
    let local_peer_id = transport.local_peer_id().to_string();
    let resolved_collection;
    let collection_id = if collection_id.is_empty() {
        resolved_collection = crate::push_docs_common::resolve_collection_id_for_doc(db, doc_id)
            .await?
            .ok_or_else(|| format!("collection for document '{doc_id}' not found"))?;
        resolved_collection.as_str()
    } else {
        collection_id
    };
    let collection = db
        .find_collection_by_id(collection_id)
        .map_err(|e| format!("failed to get collection: {}", e))?
        .ok_or_else(|| format!("collection '{}' not found", collection_id))?;
    let doc_short_id = {
        let txn = db
            .new_txn(true)
            .await
            .map_err(|e| format!("failed to create identity transaction: {}", e))?;
        let systemstore = txn
            .systemstore()
            .map_err(|e| format!("failed to get systemstore: {}", e))?;
        match db::doc_id_map::get_doc_ref(&systemstore, doc_id)
            .await
            .map_err(|e| format!("doc-ID mapping lookup failed: {}", e))?
        {
            Some(doc_ref) => doc_ref.doc_short_id,
            None => return Ok(()),
        }
    };
    if let Some(filter) = filters.get(collection_id) {
        let txn = db
            .new_txn(true)
            .await
            .map_err(|e| format!("failed to create filter transaction: {}", e))?;
        let datastore = txn
            .datastore()
            .map_err(|e| format!("failed to get datastore: {}", e))?;
        if !document_matches_filter(&datastore, collection_id, doc_short_id, filter, matcher)
            .await?
        {
            return Ok(());
        }
    }
    let creator = resolve_push_creator(document_acp, &collection, doc_id, &local_peer_id)
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
    let mut attempted_heads =
        load_latest_composite_head_cids(&*head_txn, &*block_txn, doc_short_id).await;
    attempted_heads.sort_unstable();
    let mut successful_blocks = 0usize;
    for head_cid in attempted_heads.iter().copied() {
        let block_data = match block_txn.get(&head_cid.to_bytes()).await {
            Ok(Some(data)) => data,
            _ => continue,
        };

        {
            let block_cid = head_cid;
            let _car_grant = car_authority
                .register(peer_id.clone(), head_cid)
                .ok_or_else(|| format!("selective CAR authority full for retry head {head_cid}"))?;
            let mut request = PushLogRequest::new(
                doc_id.to_string(),
                Bytes::from(block_cid.to_bytes()),
                collection_id.to_string(),
                creator.clone(),
                Bytes::from(block_data),
            );

            if p2p::signing::sign_with_transport(transport, &mut request).is_err() {
                return Err(format!(
                    "failed to sign replay block after {successful_blocks} successful block(s)"
                ));
            }

            match transport.send_two_stream_request(peer_id, request).await {
                Ok(reply) if reply.err_message.is_some() => {
                    return Err(format!(
                        "peer rejected replay after {successful_blocks} successful block(s): {}",
                        reply
                            .err_message
                            .as_deref()
                            .unwrap_or("unknown pushlog error")
                    ));
                }
                Ok(_) => {
                    successful_blocks += 1;
                }
                Err(error) => {
                    let prefix = if error.is_connection_like() {
                        "transport became unavailable"
                    } else {
                        "replay push failed"
                    };
                    return Err(format!(
                        "{prefix} after {successful_blocks} successful block(s): {error}"
                    ));
                }
            }
        }
    }
    drop(block_txn);
    drop(head_txn);
    let verify_txn = db
        .new_txn(true)
        .await
        .map_err(|error| format!("head verification transaction: {error}"))?;
    let verify_heads = verify_txn.headstore().map_err(|error| error.to_string())?;
    let verify_blocks = verify_txn.blockstore().map_err(|error| error.to_string())?;
    let mut current_heads =
        load_latest_composite_head_cids(&verify_heads, &verify_blocks, doc_short_id).await;
    current_heads.sort_unstable();
    if current_heads != attempted_heads {
        return Err("document heads changed during retry; retaining dirty marker".to_string());
    }
    Ok(())
}

/// Rederive and announce every current collection head for a dirty collection
/// scope. No CID is retained in durable sender state.
pub async fn retry_collection_commit_via_transport<S: Store + 'static, T: P2PTransport>(
    transport: &T,
    db: &DB<S>,
    peer_id: &PeerId,
    collection_id: &str,
    car_authority: &p2p::sync::HeadHintCarAuthority,
) -> Result<(), String> {
    let creator = transport.local_peer_id().to_string();
    let txn = db
        .new_txn(true)
        .await
        .map_err(|error| format!("collection retry txn: {error}"))?;
    let systemstore = txn.systemstore().map_err(|error| error.to_string())?;
    let headstore = txn.headstore().map_err(|error| error.to_string())?;
    let block_txn = txn.blockstore().map_err(|error| error.to_string())?;
    let short_id =
        db::collection::require_persisted_collection_short_id(&systemstore, collection_id)
            .await
            .map_err(|error| format!("collection retry short id: {error}"))?;
    let mut heads =
        crate::push_docs_common::load_collection_head_cids(&headstore, short_id).await?;
    heads.sort_unstable();
    for cid in heads.iter().copied() {
        let block_data = block_txn
            .get(&cid.to_bytes())
            .await
            .map_err(|error| format!("collection head read: {error}"))?
            .ok_or_else(|| format!("current collection head {cid} is missing"))?;
        let _car_grant = car_authority
            .register(peer_id.clone(), cid)
            .ok_or_else(|| format!("selective CAR authority full for collection head {cid}"))?;
        let mut request = PushLogRequest::new(
            String::new(),
            Bytes::from(cid.to_bytes()),
            collection_id.to_string(),
            creator.clone(),
            Bytes::from(block_data),
        );
        if p2p::signing::sign_with_transport(transport, &mut request).is_err() {
            return Err(format!("failed to sign current collection head {cid}"));
        }
        match transport.send_two_stream_request(peer_id, request).await {
            Ok(reply) if reply.err_message.is_some() => {
                return Err(format!(
                    "peer rejected current collection head {cid}: {}",
                    reply
                        .err_message
                        .as_deref()
                        .unwrap_or("unknown pushlog error")
                ));
            }
            Ok(_) => {}
            Err(error) => return Err(format!("collection head {cid} push failed: {error}")),
        }
    }
    drop(txn);
    let verify_txn = db
        .new_txn(true)
        .await
        .map_err(|error| format!("collection head verification transaction: {error}"))?;
    let verify_systemstore = verify_txn
        .systemstore()
        .map_err(|error| error.to_string())?;
    let verify_headstore = verify_txn.headstore().map_err(|error| error.to_string())?;
    let verify_short_id =
        db::collection::require_persisted_collection_short_id(&verify_systemstore, collection_id)
            .await
            .map_err(|error| format!("collection retry verification short id: {error}"))?;
    let mut current_heads =
        crate::push_docs_common::load_collection_head_cids(&verify_headstore, verify_short_id)
            .await?;
    current_heads.sort_unstable();
    if current_heads != heads {
        return Err("collection heads changed during retry; retaining dirty marker".to_string());
    }
    Ok(())
}
