//! Searchable-encryption serve side (replicator) and artifact receive helper.
//!
//! A replicator stores SE artifacts pushed by document owners and answers their
//! `QuerySEArtifactsRequest`s by byte-matching the request's search tags against
//! its locally stored artifacts (`fetch_doc_ids`). This is the consumer half of
//! Go's `internal/se` producer-consumer model.
//!
//! Both the standalone CLI and the embedded node route inbound SE events here so
//! the serve/receive logic lives in one place.

use p2p::message::{QuerySEArtifactsReply, QuerySEArtifactsRequest};
use p2p::{P2PHostHandle, PeerId};
use storage::corekv::Store;

use super::receive_and_store;
use super::storage::{fetch_doc_ids, FieldQuery};

/// Handle an inbound `QuerySEArtifactsRequest` on the serving (replicator) node.
///
/// Opens a read transaction, byte-matches the request's search tags against
/// locally stored artifacts, and sends back a **signed** reply. On failure a
/// signed error reply is sent so the requester's correlator slot resolves
/// instead of timing out.
pub async fn handle_query_request<S: Store>(
    store: &S,
    handle: &P2PHostHandle,
    peer_id: PeerId,
    request: QuerySEArtifactsRequest,
) {
    let reply = match build_reply(store, &request).await {
        Ok(doc_ids) => QuerySEArtifactsReply::success(&request.message_id, doc_ids),
        Err(error) => {
            tracing::warn!(
                peer_id = %peer_id,
                message_id = %request.message_id,
                collection_id = %request.collection_id,
                error = %error,
                "SE query serve failed; replying with error"
            );
            QuerySEArtifactsReply::error(&request.message_id, &error)
        }
    };

    let mut reply = reply;
    if let Err(error) = p2p::sign_message(handle.keypair(), &mut reply) {
        tracing::warn!(peer_id = %peer_id, error = %error, "failed to sign SE query reply");
        return;
    }

    if let Err(error) = handle.send_se_query_response(peer_id, reply).await {
        tracing::warn!(peer_id = %peer_id, error = %error, "failed to send SE query reply");
    }
}

async fn build_reply<S: Store>(
    store: &S,
    request: &QuerySEArtifactsRequest,
) -> std::result::Result<Vec<String>, String> {
    let txn = store
        .new_txn(true)
        .await
        .map_err(|e| format!("failed to open SE read txn: {e}"))?;

    let queries: Vec<FieldQuery> = request
        .queries
        .iter()
        .map(|q| FieldQuery::new(&q.field_name, &q.index_id, q.search_tag.clone()))
        .collect();

    let doc_ids = fetch_doc_ids(&txn, &request.collection_id, &queries)
        .await
        .map_err(|e| format!("SE artifact lookup failed: {e}"))?;

    tracing::debug!(
        collection_id = %request.collection_id,
        query_count = queries.len(),
        match_count = doc_ids.len(),
        "served SE query"
    );

    Ok(doc_ids)
}

/// Receive, validate, and store SE artifacts pushed by a document owner.
///
/// Shared by the CLI and embedded event loops (both forward
/// `SEArtifactsReceived` here). Returns the stored doc IDs so callers can emit
/// `se_artifact_received` bus events.
pub async fn handle_artifacts_received<S: Store>(
    store: &S,
    peer_id: &str,
    data: &[u8],
) -> Vec<String> {
    let mut txn = match store.new_txn(false).await {
        Ok(txn) => txn,
        Err(error) => {
            tracing::warn!(peer_id = %peer_id, error = %error, "failed to create SE artifact transaction");
            return Vec::new();
        }
    };

    let result = match receive_and_store(&mut txn, data).await {
        Ok(result) => result,
        Err(error) => {
            tracing::warn!(peer_id = %peer_id, error = %error, "failed to receive SE artifacts");
            return Vec::new();
        }
    };

    if let Err(error) = txn.commit().await {
        tracing::warn!(peer_id = %peer_id, error = %error, "failed to commit SE artifacts");
        return Vec::new();
    }

    tracing::debug!(
        peer_id = %peer_id,
        collection_id = %result.collection_id,
        stored = result.stored,
        rejected = result.rejected,
        "stored incoming SE artifacts"
    );

    result.doc_ids
}
