//! P2P implementation of `query::SeQueryTransport` (the requester side).
//!
//! Mirrors Go's `Coordinator.QueryDocIDsByValues`/`QuerySEArtifacts`: generate a
//! search tag per `_eq` condition (using the shared SE key + node identity),
//! then fan a `QuerySEArtifactsRequest` out to the collection's replicators,
//! returning the first non-error reply's doc IDs. Zero replicators → empty.
//!
//! The querying node is the document owner; it never serves from local state.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use document::encoding::json_to_normal_value;
use p2p::message::{QuerySEArtifactsRequest, SEFieldQuery};
use p2p::transport::{P2PTransport, PeerId};
use p2p::{ReplicatorRegistry, SeQueryCorrelator};
use serde_json::Value as JsonValue;
use zeroize::Zeroizing;

use crate::se::{FieldValueQuery, SECoordinator};

/// How long to wait for a replicator's reply before trying the next one.
const SE_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Transport-agnostic SE query transport. Constructed once per node when the
/// P2P stack comes up and injected into the `QueryRunner` via
/// `with_se_transport`. Generic over the concrete [`P2PTransport`] (libp2p or
/// iroh); the genericity is erased when boxed into `Arc<dyn SeQueryTransport>`.
pub struct DbMergeSeQueryTransport<T: P2PTransport> {
    transport: T,
    correlator: SeQueryCorrelator,
    replicators: Arc<ReplicatorRegistry>,
    enc_key: Zeroizing<Vec<u8>>,
    /// Identity pubkey baked into search tags. MUST match the write side
    /// (artifact generation). The CLI/embedded write side uses `None`
    /// (anonymous), so the query side uses `None` too. See #976 trap 3.
    identity_pubkey: Option<Vec<u8>>,
}

impl<T: P2PTransport> DbMergeSeQueryTransport<T> {
    /// Create a transport. `enc_key` is the shared 32-byte SE key;
    /// `identity_pubkey` must equal the value used at artifact-generation time.
    pub fn new(
        transport: T,
        correlator: SeQueryCorrelator,
        replicators: Arc<ReplicatorRegistry>,
        enc_key: [u8; 32],
        identity_pubkey: Option<Vec<u8>>,
    ) -> Self {
        Self {
            transport,
            correlator,
            replicators,
            enc_key: Zeroizing::new(enc_key.to_vec()),
            identity_pubkey,
        }
    }

    fn build_field_queries(
        &self,
        collection_id: &str,
        eq_conditions: Vec<(String, JsonValue)>,
    ) -> std::result::Result<Vec<SEFieldQuery>, String> {
        let coordinator = match &self.identity_pubkey {
            Some(pubkey) => {
                SECoordinator::with_key_and_identity(self.enc_key.to_vec(), pubkey.clone())
            }
            None => SECoordinator::with_key(self.enc_key.to_vec()),
        };

        let mut value_queries = Vec::with_capacity(eq_conditions.len());
        for (field, value) in eq_conditions {
            let normal = json_to_normal_value(value)
                .map_err(|e| format!("invalid SE query value for '{field}': {e}"))?;
            value_queries.push(FieldValueQuery::equality(field, normal));
        }

        let field_queries = coordinator
            .to_field_queries(collection_id, &value_queries)
            .map_err(|e| format!("failed to generate SE search tags: {e}"))?;

        Ok(field_queries
            .into_iter()
            .map(|q| SEFieldQuery::new(q.field_name, q.index_id, q.search_tag))
            .collect())
    }

    /// Send a signed request to one replicator and await its reply.
    async fn query_one(
        &self,
        peer_id: PeerId,
        collection_id: &str,
        queries: Vec<SEFieldQuery>,
    ) -> std::result::Result<Vec<String>, String> {
        let mut request = QuerySEArtifactsRequest::new(collection_id, queries);

        // Sign FIRST: this generates the UUID message_id used as the correlation
        // key and is required by the receiver's verify_message check (trap 1).
        p2p::signing::sign_with_transport(&self.transport, &mut request)
            .map_err(|e| format!("failed to sign SE query request: {e}"))?;

        let message_id = request.message_id.clone();
        let mut pending = self.correlator.register(message_id.clone());

        self.transport
            .send_se_query_request(&peer_id, request)
            .await
            .map_err(|e| format!("failed to send SE query request: {e}"))?;

        match tokio::time::timeout(SE_QUERY_TIMEOUT, pending.recv()).await {
            Ok(Ok(reply)) => {
                if let Some(err) = reply.err_message {
                    Err(format!("replicator returned SE query error: {err}"))
                } else {
                    Ok(reply.doc_ids)
                }
            }
            Ok(Err(_)) => Err("SE query reply channel closed".to_string()),
            Err(_) => Err("SE query timed out waiting for replicator".to_string()),
        }
    }
}

#[async_trait]
impl<T: P2PTransport> query::SeQueryTransport for DbMergeSeQueryTransport<T> {
    async fn query_doc_ids(
        &self,
        collection_id: &str,
        eq_conditions: Vec<(String, JsonValue)>,
    ) -> std::result::Result<Vec<String>, String> {
        let queries = self.build_field_queries(collection_id, eq_conditions)?;

        let replicator_ids = self.replicators.get_replicators(collection_id);
        if replicator_ids.is_empty() {
            // Go semantics: no replicators → empty result, no local fallback.
            tracing::debug!(collection_id, "SE query: no replicators registered");
            return Ok(Vec::new());
        }

        let mut last_error = None;
        for peer_str in replicator_ids {
            let peer_id = PeerId::new(peer_str.clone());

            match self
                .query_one(peer_id, collection_id, queries.clone())
                .await
            {
                Ok(doc_ids) => return Ok(doc_ids),
                Err(e) => {
                    tracing::debug!(peer_id = %peer_str, error = %e, "SE query to replicator failed");
                    last_error = Some(e);
                }
            }
        }

        // All replicators failed: surface the last error rather than masking it.
        Err(last_error.unwrap_or_else(|| "SE query: all replicators failed".to_string()))
    }
}
