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

        // The propagation peer remains the authenticated ingress principal,
        // but it is not necessarily a content provider. A gossip relay can
        // hold the announced root while none of its linked descendants are in
        // its blockstore. Durable recovery therefore binds only to the
        // independently authenticated publisher, and only while the transport
        // has a route to that publisher. A relayed hint without such a route
        // is advisory and must not create a success-acked receiver obligation.
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
            .filter(|peer_id| !peer_id.is_empty())
            .ok_or_else(|| {
                tracing::warn!(
                    peer_id = %propagation_source,
                    authenticated_hop = %authenticated_hop,
                    topic = %topic,
                    collection_id = %message.collection_id,
                    doc_id = %message.doc_id,
                    "Dropping head hint without an authenticated content origin"
                );
                crate::error::Error::Unauthorized(
                    "head hint has no authenticated content origin".to_string(),
                )
            })?;
        let origin_is_routable = authenticated_origin == propagation_source.as_str()
            || self.access.peer_state.is_connected(authenticated_origin);
        if !origin_is_routable {
            tracing::warn!(
                peer_id = %propagation_source,
                authenticated_origin,
                authenticated_hop = %authenticated_hop,
                topic = %topic,
                collection_id = %message.collection_id,
                doc_id = %message.doc_id,
                "Dropping relayed head hint whose content origin is not routable"
            );
            return Err(crate::error::Error::Unauthorized(
                "authenticated head-hint origin is not transport-routable".to_string(),
            ));
        }
        let recovery_source = PeerId::new(authenticated_origin.to_owned());
        tracing::debug!(
            propagation_source = %propagation_source,
            authenticated_origin,
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

        // Gossip authenticates the propagation hop and the signed origin, but
        // it is not the explicit-replicator handshake. In particular, a
        // configured relay must not confer its merge authorization on a
        // different origin principal carried by the envelope.
        if authenticated_origin == propagation_source.as_str() {
            self.manager
                .process_pushlog_from_dag_provider(
                    &message,
                    Some(recovery_source.as_str()),
                    false,
                    None,
                )
                .await
        } else {
            self.manager
                .process_pushlog(&message, Some(recovery_source.as_str()), false, None)
                .await
        }
    }
}
