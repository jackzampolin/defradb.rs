//! Broadcasting local updates to the network.

use blockstore::Blockstore;
use cid::Cid;

use super::SyncCoordinator;
use crate::error::Result;
use crate::message::PushLogRequest;
use crate::signing::sign_with_transport;
use crate::sync::broadcaster::Broadcaster;
use crate::sync::BroadcastResult;
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Broadcast a local update to the network.
    pub async fn broadcast_local_update(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) -> Result<BroadcastResult> {
        self.broadcast_local_update_with_creator(cid, block, doc_id, collection_id, None)
            .await
    }

    /// Broadcast a local update with an optional creator override.
    ///
    /// When `creator_override` is Some, the PushLog Creator field uses the
    /// given DID instead of this node's PeerId. This enables ACP owner
    /// registration on the receiving node during merge.
    pub async fn broadcast_local_update_with_creator(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator_override: Option<&str>,
    ) -> Result<BroadcastResult> {
        let creator = creator_override.unwrap_or(&self.local_peer_id);
        let broadcast = Broadcaster::<T>::create_broadcast(cid, block, doc_id, collection_id, creator);
        self.broadcaster.broadcast_update(&broadcast).await
    }

    /// Push a composite block and all its linked field blocks to replicator peers.
    pub async fn push_dag_to_replicators(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) {
        self.push_dag_to_replicators_with_creator(cid, block, doc_id, collection_id, None)
            .await
    }

    /// Push a composite block and field blocks to replicators with optional creator override.
    pub async fn push_dag_to_replicators_with_creator(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator_override: Option<&str>,
    ) {
        let creator = creator_override.unwrap_or(&self.local_peer_id);
        let replicators = match self.transport.list_replicators().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get replicators for push");
                return;
            }
        };

        if replicators.is_empty() {
            return;
        }

        let field_blocks = self.load_linked_blocks(block).await;

        tracing::debug!(
            cid = %cid,
            doc_id = %doc_id,
            collection_id = %collection_id,
            replicator_count = replicators.len(),
            field_block_count = field_blocks.len(),
            "Pushing DAG to replicators"
        );

        for rep in &replicators {
            if !rep.collections.is_empty() && !rep.collections.contains(&collection_id.to_string())
            {
                continue;
            }

            let peer_id_str = rep.peer_id_str().to_string();
            if peer_id_str.is_empty() {
                continue;
            }
            let peer_id = PeerId::new(peer_id_str);

            let mut requests: Vec<(Cid, PushLogRequest)> = Vec::new();

            for (field_cid, field_data) in &field_blocks {
                let mut req = PushLogRequest::new(
                    doc_id.to_string(),
                    field_cid.to_bytes(),
                    collection_id.to_string(),
                    creator.to_string(),
                    field_data.clone(),
                );
                if sign_with_transport(&self.transport, &mut req).is_ok() {
                    requests.push((*field_cid, req));
                }
            }

            let mut composite_req = PushLogRequest::new(
                doc_id.to_string(),
                cid.to_bytes(),
                collection_id.to_string(),
                creator.to_string(),
                block.to_vec(),
            );
            if let Err(e) = sign_with_transport(&self.transport, &mut composite_req) {
                tracing::debug!(error = %e, "Failed to sign composite PushLog request");
                continue;
            }
            requests.push((*cid, composite_req));

            // Spawn a task per peer, bounded by push_semaphore to prevent
            // resource exhaustion during document creation bursts.
            let transport = self.transport.clone();
            let failure_tx = self.failure_tx.clone();
            let doc_id_owned = doc_id.to_string();
            let collection_id_owned = collection_id.to_string();
            let semaphore = self.push_semaphore.clone();
            tokio::spawn(async move {
                let _permit = semaphore.acquire().await;
                let any_failed =
                    Self::send_ordered_pushlogs_via_transport(&transport, &peer_id, requests).await;
                if any_failed {
                    if let Some(tx) = failure_tx {
                        let _ = tx.send(super::PushFailure {
                            peer_id: peer_id.to_string(),
                            doc_id: doc_id_owned,
                            collection_id: collection_id_owned,
                        });
                    }
                }
            });
        }
    }

    /// Push a single block to replicator peers (no DAG expansion).
    pub async fn push_to_replicators(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) {
        self.push_to_replicators_with_creator(cid, block, doc_id, collection_id, None)
            .await
    }

    /// Push a single block to replicators with optional creator override.
    pub async fn push_to_replicators_with_creator(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator_override: Option<&str>,
    ) {
        let creator = creator_override.unwrap_or(&self.local_peer_id);
        let replicators = match self.transport.list_replicators().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get replicators for push");
                return;
            }
        };

        for rep in &replicators {
            if !rep.collections.is_empty() && !rep.collections.contains(&collection_id.to_string())
            {
                continue;
            }

            let peer_id_str = rep.peer_id_str().to_string();
            if peer_id_str.is_empty() {
                continue;
            }
            let peer_id = PeerId::new(peer_id_str);

            let mut request = PushLogRequest::new(
                doc_id.to_string(),
                cid.to_bytes(),
                collection_id.to_string(),
                creator.to_string(),
                block.to_vec(),
            );

            if let Err(e) = sign_with_transport(&self.transport, &mut request) {
                tracing::debug!(error = %e, "Failed to sign PushLog request");
                continue;
            }

            let transport = self.transport.clone();
            let cid_clone = *cid;
            let failure_tx = self.failure_tx.clone();
            let doc_id_owned = doc_id.to_string();
            let collection_id_owned = collection_id.to_string();
            let semaphore = self.push_semaphore.clone();
            let peer_id_clone = peer_id.clone();
            tokio::spawn(async move {
                let _permit = semaphore.acquire().await;
                if let Err(e) = transport
                    .send_two_stream_request(&peer_id_clone, request)
                    .await
                {
                    tracing::debug!(
                        peer_id = %peer_id_clone,
                        cid = %cid_clone,
                        error = %e,
                        "PushLog to replicator failed"
                    );
                    if let Some(tx) = failure_tx {
                        let _ = tx.send(super::PushFailure {
                            peer_id: peer_id_clone.to_string(),
                            doc_id: doc_id_owned,
                            collection_id: collection_id_owned,
                        });
                    }
                }
            });
        }
    }

    /// Load all blocks linked from a composite block's DAG links.
    async fn load_linked_blocks(&self, composite_bytes: &[u8]) -> Vec<(Cid, Vec<u8>)> {
        let parsed = match defra_core::Block::from_dag_cbor(composite_bytes) {
            Ok(b) => b,
            Err(_) => return vec![],
        };

        let links = match parsed.links {
            Some(ref links) => links,
            None => return vec![],
        };

        let mut blocks = Vec::with_capacity(links.len());
        for link in links {
            match self.blockstore().get(&link.link).await {
                Ok(Some(data)) => blocks.push((link.link, data)),
                Ok(None) => {
                    tracing::debug!(cid = %link.link, "Linked block not found in blockstore");
                }
                Err(e) => {
                    tracing::debug!(cid = %link.link, error = %e, "Failed to load linked block");
                }
            }
        }
        blocks
    }

    /// Send PushLog requests to a peer in order via the transport, waiting for each to complete.
    async fn send_ordered_pushlogs_via_transport(
        transport: &T,
        peer_id: &PeerId,
        requests: Vec<(Cid, PushLogRequest)>,
    ) -> bool {
        let mut any_failed = false;
        for (cid, request) in requests {
            if let Err(e) = transport.send_two_stream_request(peer_id, request).await {
                tracing::debug!(
                    peer_id = %peer_id,
                    cid = %cid,
                    error = %e,
                    "PushLog to replicator failed"
                );
                any_failed = true;
            }
        }
        any_failed
    }
}
