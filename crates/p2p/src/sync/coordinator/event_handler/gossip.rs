//! GossipSub message handling.

use std::sync::atomic::Ordering;

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
            source_peer_id = ?message.source_peer_id,
            doc_id = %message.doc_id,
            collection_id = %message.collection_id,
            topic = %topic,
            "Received GossipSub message"
        );

        let topic_matches_collection = topic == message.collection_id;
        let topic_matches_document = !message.doc_id.is_empty() && topic == message.doc_id;
        let is_subscribed = self
            .is_locally_subscribed_collection(&message.collection_id)
            .await;
        let is_open_access = self.access.access_mode.is_open();
        let is_outbound_replicator_target =
            self.is_registered_replicator(propagation_source.as_str(), &message.collection_id);
        let is_direction_filtered = !topic_matches_document
            && topic_matches_collection
            && is_outbound_replicator_target
            && !is_subscribed;

        if !topic_matches_document
            && (!topic_matches_collection
                || is_direction_filtered
                || (!is_open_access && !is_subscribed))
        {
            if is_direction_filtered {
                self.access
                    .gossip_direction_filtered
                    .fetch_add(1, Ordering::Relaxed);
            }
            tracing::warn!(
                peer_id = %propagation_source,
                topic = %topic,
                collection_id = %message.collection_id,
                doc_id = %message.doc_id,
                topic_matches_collection,
                topic_matches_document,
                is_subscribed,
                is_outbound_replicator_target,
                is_direction_filtered,
                "Dropping GossipSub message outside accepted replication policy"
            );
            return Err(crate::error::Error::AccessDenied {
                peer_id: propagation_source.to_string(),
                collection_id: message.collection_id.clone(),
            });
        }

        // The propagation peer remains the authenticated ingress principal.
        // Prefer the independently authenticated publisher only when the
        // transport already has a live route to it. In a sparse mesh, retain
        // the authenticated connected hop instead of persisting an unroutable
        // origin. A directly connected hub therefore fetches from the actual
        // DAG owner rather than needlessly chaining through a partial relay.
        let authenticated_hop = message
            .authenticated_source_peer_id()
            .filter(|peer_id| !peer_id.is_empty())
            .map(|peer_id| PeerId::new(peer_id.to_owned()))
            .ok_or_else(|| {
                tracing::warn!(
                    peer_id = %propagation_source,
                    claimed_source_peer_id = ?message.source_peer_id,
                    topic = %topic,
                    collection_id = %message.collection_id,
                    doc_id = %message.doc_id,
                    "Dropping head hint without an authenticated recovery provider"
                );
                crate::error::Error::Unauthorized(
                    "head hint has no authenticated recovery provider".to_string(),
                )
            })?;
        let authenticated_origin = message
            .authenticated_origin_peer_id()
            .filter(|peer_id| !peer_id.is_empty());
        let origin_is_routable = authenticated_origin.is_some_and(|origin| {
            origin == propagation_source.as_str() || self.access.peer_state.is_connected(origin)
        });
        let recovery_source = authenticated_origin
            .filter(|_| origin_is_routable)
            .map(|origin| PeerId::new(origin.to_owned()))
            .unwrap_or_else(|| authenticated_hop.clone());
        tracing::debug!(
            propagation_source = %propagation_source,
            authenticated_origin = ?authenticated_origin,
            authenticated_hop = %authenticated_hop,
            origin_is_routable,
            recovery_source = %recovery_source,
            "Selected authenticated head-hint recovery provider"
        );

        // Parse CID
        match Cid::try_from(message.cid.as_ref()) {
            Ok(cid) => {
                self.access
                    .peer_state
                    .peer_has_cid(recovery_source.as_str(), cid);
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
                Some(recovery_source.as_str()),
                is_explicit_replicator,
                None,
            )
            .await
    }
}
