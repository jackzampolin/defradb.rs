use std::sync::Arc;

use crate::ffi_entry;
use crate::helpers::get_rt;
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

/// Push existing documents to a replicator peer.
///
/// Delegates to `db::push_existing_docs` which contains the shared logic.
/// The node identity DID is passed as the SE identity pubkey to ensure
/// per-identity tag isolation in SE artifact generation.
pub(crate) async fn push_existing_docs(
    handle: &p2p::P2PHostHandle,
    db: &crate::state::FfiDatabase,
    peer_id: libp2p::PeerId,
    collections: &[String],
    se_encryption_key: Option<&[u8]>,
    se_identity_pubkey: Option<&[u8]>,
) -> Result<(), String> {
    db::push_existing_docs(
        handle,
        db,
        peer_id,
        collections,
        se_encryption_key,
        se_identity_pubkey,
    )
    .await
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
    ffi_entry! {
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
                        .list_replicators()
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
                        let push_identity = state.node_identity_did.clone();

                        push_handles.push(tokio::spawn(async move {
                            let identity_bytes: Option<Vec<u8>> =
                                push_identity.as_deref().map(|s| s.as_bytes().to_vec());
                            if let Err(e) = push_existing_docs(
                                &push_handle,
                                &push_db,
                                peer_id,
                                &push_collections,
                                push_se_key.as_ref().map(|k| k.as_slice()),
                                identity_bytes.as_deref(),
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
}
