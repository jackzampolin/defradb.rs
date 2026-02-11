//! Broadcasting local updates to the network.

use blockstore::Blockstore;
use cid::Cid;
use libp2p::PeerId;

use super::SyncCoordinator;
use crate::error::Result;
use crate::message::PushLogRequest;
use crate::signing::sign_message;
use crate::sync::broadcaster::Broadcaster;
use crate::sync::BroadcastResult;

impl<B: Blockstore + 'static> SyncCoordinator<B> {
    /// Broadcast a local update to the network.
    ///
    /// Call this after successfully creating a local block to propagate it
    /// to other nodes.
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the block
    /// * `block` - The raw block data
    /// * `doc_id` - The document ID
    /// * `collection_id` - The collection ID
    ///
    /// # Returns
    ///
    /// Returns `Ok(BroadcastResult)` indicating success or partial success.
    /// Partial success means one topic received the message but not both.
    pub async fn broadcast_local_update(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) -> Result<BroadcastResult> {
        let broadcast =
            Broadcaster::create_broadcast(cid, block, doc_id, collection_id, &self.local_peer_id);
        self.broadcaster.broadcast_update(&broadcast).await
    }

    /// Push a composite block and all its linked field blocks to replicator peers.
    ///
    /// This decodes the composite block to find field block CIDs, loads them from
    /// the blockstore, and sends field blocks BEFORE the composite to each replicator.
    /// This ensures the receiver has all DAG blocks when it processes the composite,
    /// avoiding Bitswap fetches (which can fail after node restarts).
    pub async fn push_dag_to_replicators(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) {
        let replicators = match self.host.get_all_replicators().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get replicators for push");
                return;
            }
        };

        if replicators.is_empty() {
            return;
        }

        // Decode composite block to find linked field block CIDs.
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

            let peer_id = match rep.peer_id() {
                Some(id) => id,
                None => continue,
            };

            // Build signed requests for field blocks and composite.
            let mut requests: Vec<(Cid, PushLogRequest)> = Vec::new();

            // Field blocks first so receiver has them when composite arrives.
            for (field_cid, field_data) in &field_blocks {
                let mut req = PushLogRequest::new(
                    doc_id.to_string(),
                    field_cid.to_bytes(),
                    collection_id.to_string(),
                    self.local_peer_id.clone(),
                    field_data.clone(),
                );
                if sign_message(self.host.keypair(), &mut req).is_ok() {
                    requests.push((*field_cid, req));
                }
            }

            // Composite block last.
            let mut composite_req = PushLogRequest::new(
                doc_id.to_string(),
                cid.to_bytes(),
                collection_id.to_string(),
                self.local_peer_id.clone(),
                block.to_vec(),
            );
            if let Err(e) = sign_message(self.host.keypair(), &mut composite_req) {
                tracing::debug!(error = %e, "Failed to sign composite PushLog request");
                continue;
            }
            requests.push((*cid, composite_req));

            // Fire-and-forget: spawn a task per peer that sends blocks in order.
            let host = self.host.clone();
            tokio::spawn(async move {
                Self::send_ordered_pushlogs(host, peer_id, requests).await;
            });
        }
    }

    /// Push a single block to replicator peers (no DAG expansion).
    ///
    /// Used for collection blocks and other non-composite blocks that
    /// don't have linked field blocks.
    pub async fn push_to_replicators(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) {
        let replicators = match self.host.get_all_replicators().await {
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

            let peer_id = match rep.peer_id() {
                Some(id) => id,
                None => continue,
            };

            let mut request = PushLogRequest::new(
                doc_id.to_string(),
                cid.to_bytes(),
                collection_id.to_string(),
                self.local_peer_id.clone(),
                block.to_vec(),
            );

            if let Err(e) = sign_message(self.host.keypair(), &mut request) {
                tracing::debug!(error = %e, "Failed to sign PushLog request");
                continue;
            }

            let host = self.host.clone();
            let cid_clone = *cid;
            tokio::spawn(async move {
                if let Err(e) = host.send_two_stream_request(peer_id, request).await {
                    tracing::debug!(
                        peer_id = %peer_id,
                        cid = %cid_clone,
                        error = %e,
                        "PushLog to replicator failed"
                    );
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

    /// Send PushLog requests to a peer in order, waiting for each to complete.
    async fn send_ordered_pushlogs(
        host: crate::host::P2PHostHandle,
        peer_id: PeerId,
        requests: Vec<(Cid, PushLogRequest)>,
    ) {
        for (cid, request) in requests {
            if let Err(e) = host.send_two_stream_request(peer_id, request).await {
                tracing::debug!(
                    peer_id = %peer_id,
                    cid = %cid,
                    error = %e,
                    "PushLog to replicator failed"
                );
            }
        }
    }
}
