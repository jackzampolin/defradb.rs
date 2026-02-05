//! Broadcasting local updates to the network.

use blockstore::Blockstore;
use cid::Cid;

use super::SyncCoordinator;
use crate::error::Result;
use crate::message::PushLogRequest;
use crate::signing::sign_message;
use crate::sync::BroadcastResult;
use crate::sync::broadcaster::Broadcaster;

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

    /// Push a local update to all registered replicator peers via direct PushLog.
    ///
    /// This complements `broadcast_local_update` (GossipSub) by sending the update
    /// directly to each replicator peer that is registered for the given collection.
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
                eprintln!("[PUSH-REPLICATORS] Failed to get replicators: {}", e);
                return;
            }
        };

        eprintln!(
            "[PUSH-REPLICATORS] cid={} doc_id={} collection={} replicator_count={}",
            cid,
            doc_id,
            collection_id,
            replicators.len()
        );

        for rep in &replicators {
            // Check if this replicator is registered for the collection
            // Empty collections means "all collections" in Go semantics
            if !rep.collections.is_empty() && !rep.collections.contains(&collection_id.to_string())
            {
                eprintln!(
                    "[PUSH-REPLICATORS] Skipping replicator (collection mismatch): {:?}",
                    rep.collections
                );
                continue;
            }

            let peer_id = match rep.peer_id() {
                Some(id) => id,
                None => continue,
            };
            eprintln!("[PUSH-REPLICATORS] Sending to peer={} cid={}", peer_id, cid);

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

            // Fire-and-forget: spawn each push so we don't block the broadcast loop.
            let host = self.host.clone();
            tokio::spawn(async move {
                let _ = host.send_two_stream_request(peer_id, request).await;
            });
        }
    }
}
