//! GossipSub message handling.

use blockstore::Blockstore;
use cid::Cid;

use super::super::SyncCoordinator;
use crate::error::Result;
use crate::message::PushLogBroadcast;
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    pub(super) async fn handle_gossip_message(
        &self,
        propagation_source: PeerId,
        message: PushLogBroadcast,
        topic: String,
    ) -> Result<()> {
        tracing::debug!(
            peer_id = %propagation_source,
            doc_id = %message.doc_id,
            collection_id = %message.collection_id,
            topic = %topic,
            "Received GossipSub message"
        );

        // Access control check
        if let Err(e) = self
            .check_access_str(propagation_source.as_str(), &message.collection_id)
            .await
        {
            tracing::warn!(
                peer_id = %propagation_source,
                collection_id = %message.collection_id,
                doc_id = %message.doc_id,
                "Dropping GossipSub message from unauthorized peer"
            );
            return Err(e);
        }

        // Parse CID
        match Cid::try_from(message.cid.as_slice()) {
            Ok(cid) => {
                self.peer_state
                    .peer_has_cid(propagation_source.as_str(), cid);
            }
            Err(e) => {
                tracing::warn!(
                    peer_id = %propagation_source,
                    cid_bytes_len = message.cid.len(),
                    error = %e,
                    "Failed to parse CID from gossip message - skipping message"
                );
                return Err(crate::error::Error::InvalidCid(format!(
                    "Failed to parse CID from gossip message: {}",
                    e
                )));
            }
        }

        self.manager
            .process_pushlog(&message, Some(propagation_source.as_str()), false, None)
            .await
    }
}
