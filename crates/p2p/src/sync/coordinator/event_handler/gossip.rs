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

        // Ingress authorization is TOPIC-scoped, never source-scoped.
        //
        // Gossip only reaches us on topics we joined, so a local subscription
        // (or open access) is what admits a message; the payload must simply
        // match the topic it arrived on, which is the real anti-spoofing guard.
        //
        // Replicator membership must NOT veto ingress. It records *outbound*
        // intent ("push my writes for C to this peer") and says nothing about
        // whether that peer may speak to us. Using it as a receive-side veto
        // broke every symmetric mesh: two peers that replicate to each other are
        // each other's outbound targets, so both dropped every collection-topic
        // message from the other, in every access mode (defra-agent#696, ~330
        // drops on empty stores). Registry membership was simultaneously a grant
        // (direct PushLog, `peer_authorized_for_collection`) and a veto (here).
        //
        // Collection-commit broadcasts carry an empty `doc_id`, so they have no
        // document-topic fallback and can ONLY arrive on the collection topic —
        // exactly the path the veto killed. That closed their last delivery
        // route and left receivers holding heads whose parents never arrive.
        //
        // One-way replication is expressed by the receiver NOT subscribing, not
        // by blacklisting the identity of a peer we happen to push to.
        let topic_matches_collection = topic == message.collection_id;
        let topic_matches_document = !message.doc_id.is_empty() && topic == message.doc_id;
        let is_subscribed = self
            .is_locally_subscribed_collection(&message.collection_id)
            .await;
        let is_open_access = self.access.access_mode.is_open();

        if !topic_matches_document
            && (!topic_matches_collection || !(is_open_access || is_subscribed))
        {
            tracing::warn!(
                peer_id = %propagation_source,
                topic = %topic,
                collection_id = %message.collection_id,
                doc_id = %message.doc_id,
                topic_matches_collection,
                topic_matches_document,
                is_subscribed,
                "Dropping GossipSub message: topic mismatch or collection not subscribed"
            );
            return Err(crate::error::Error::AccessDenied {
                peer_id: propagation_source.to_string(),
                collection_id: message.collection_id.clone(),
            });
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

        let is_explicit_replicator =
            self.is_registered_replicator(propagation_source.as_str(), &message.collection_id);

        self.manager
            .process_pushlog(
                &message,
                Some(propagation_source.as_str()),
                is_explicit_replicator,
                None,
            )
            .await
    }
}
