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
/// Delegates to `db::push_existing_docs` which contains the shared logic.
pub(crate) async fn push_existing_docs(
    handle: &p2p::P2PHostHandle,
    db: &crate::state::FfiDatabase,
    peer_id: libp2p::PeerId,
    collections: &[String],
    se_encryption_key: Option<&[u8]>,
) -> Result<(), String> {
    db::push_existing_docs(handle, db, peer_id, collections, se_encryption_key).await
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
