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

        if !self.access.access_mode.is_open() {
            let is_connected = self.peer_is_connected(&propagation_source).await;
            let is_authorized_replicator = self
                .authorizer
                .peer_authorized_for_collection(
                    propagation_source.as_str(),
                    &message.collection_id,
                )
                .await;
            let is_subscribed = self
                .subscriptions
                .subscribed_collections
                .read()
                .await
                .contains(&message.collection_id);

            if !is_connected || (!is_authorized_replicator && !is_subscribed) {
                tracing::warn!(
                    peer_id = %propagation_source,
                    collection_id = %message.collection_id,
                    doc_id = %message.doc_id,
                    is_connected,
                    is_authorized_replicator,
                    is_subscribed,
                    "Dropping GossipSub message from unauthorized peer"
                );
                return Err(crate::error::Error::AccessDenied {
                    peer_id: propagation_source.to_string(),
                    collection_id: message.collection_id.clone(),
                });
            }
        }

        // Parse CID
        match Cid::try_from(message.cid.as_ref()) {
            Ok(cid) => {
                self.access
                    .peer_state
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
