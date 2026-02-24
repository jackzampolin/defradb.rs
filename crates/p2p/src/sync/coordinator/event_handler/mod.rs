//! Transport event handling for the sync coordinator.

mod bitswap;
mod branchable_sync;
pub(crate) mod car;
mod doc_sync;
mod gossip;
mod pushlog;

use blockstore::Blockstore;

use super::SyncCoordinator;
use crate::error::{Error, Result};
use crate::transport::{P2PTransport, TransportEvent};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Handle an event from the transport layer.
    ///
    /// This should be called from the event loop that processes TransportEvents.
    pub async fn handle_transport_event(&self, event: TransportEvent) -> Result<()> {
        match event {
            TransportEvent::PeerConnected(peer_id) => {
                tracing::debug!(peer_id = %peer_id, "Peer connected");
                if let Ok(pid) = peer_id.as_str().parse::<libp2p::PeerId>() {
                    self.peer_state.peer_connected(pid);
                }
            }
            TransportEvent::PeerDisconnected(peer_id) => {
                tracing::debug!(peer_id = %peer_id, "Peer disconnected");
                if let Ok(pid) = peer_id.as_str().parse::<libp2p::PeerId>() {
                    self.peer_state.peer_disconnected(&pid);
                }
                self.rate_limiter.remove_peer(&peer_id);
            }
            TransportEvent::PeerSubscribed { peer_id, topic } => {
                tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer subscribed to topic");
                if let Ok(pid) = peer_id.as_str().parse::<libp2p::PeerId>() {
                    self.peer_state.peer_subscribed(&pid, topic);
                }
            }
            TransportEvent::PeerUnsubscribed { peer_id, topic } => {
                tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer unsubscribed from topic");
                if let Ok(pid) = peer_id.as_str().parse::<libp2p::PeerId>() {
                    self.peer_state.peer_unsubscribed(&pid, &topic);
                }
            }
            TransportEvent::GossipMessage {
                propagation_source,
                message,
                topic,
                ..
            } => {
                if !self.rate_limiter.check(&propagation_source) {
                    tracing::warn!(
                        peer_id = %propagation_source,
                        "Rate limit exceeded for GossipMessage, dropping"
                    );
                    return Err(Error::AccessDenied {
                        peer_id: propagation_source.to_string(),
                        collection_id: "rate-limited".into(),
                    });
                }
                self.handle_gossip_message(propagation_source, message, topic)
                    .await?;
            }
            TransportEvent::PushLogRequest {
                peer_id,
                request,
                token,
            } => {
                if !self.rate_limiter.check(&peer_id) {
                    tracing::warn!(
                        peer_id = %peer_id,
                        "Rate limit exceeded for PushLogRequest, dropping"
                    );
                    return Err(Error::AccessDenied {
                        peer_id: peer_id.to_string(),
                        collection_id: "rate-limited".into(),
                    });
                }
                self.handle_pushlog_request(peer_id, request, token).await?;
            }
            TransportEvent::TwoStreamRequest {
                peer_id,
                request,
                token,
            } => {
                if !self.rate_limiter.check(&peer_id) {
                    tracing::warn!(
                        peer_id = %peer_id,
                        "Rate limit exceeded for TwoStreamRequest, dropping"
                    );
                    return Err(Error::AccessDenied {
                        peer_id: peer_id.to_string(),
                        collection_id: "rate-limited".into(),
                    });
                }
                self.handle_two_stream_request(peer_id, request, token)
                    .await?;
            }
            TransportEvent::BitswapBlockReceived {
                query_id,
                cid,
                data,
            } => {
                self.handle_bitswap_block_received(query_id, cid, data)
                    .await?;
            }
            TransportEvent::BitswapComplete {
                query_id,
                success,
                error,
            } => {
                self.handle_bitswap_complete(query_id, success, error)
                    .await?;
            }
            TransportEvent::DocSyncRequest { peer_id, request } => {
                if !self.rate_limiter.check(&peer_id) {
                    tracing::warn!(
                        peer_id = %peer_id,
                        "Rate limit exceeded for DocSyncRequest, dropping"
                    );
                    return Err(Error::AccessDenied {
                        peer_id: peer_id.to_string(),
                        collection_id: "rate-limited".into(),
                    });
                }
                self.handle_doc_sync_request(peer_id, request).await?;
            }
            TransportEvent::DocSyncReply { peer_id, reply } => {
                self.handle_doc_sync_reply(peer_id, reply).await?;
            }
            TransportEvent::BranchableSyncRequest { peer_id, request } => {
                if !self.rate_limiter.check(&peer_id) {
                    tracing::warn!(
                        peer_id = %peer_id,
                        "Rate limit exceeded for BranchableSyncRequest, dropping"
                    );
                    return Err(Error::AccessDenied {
                        peer_id: peer_id.to_string(),
                        collection_id: "rate-limited".into(),
                    });
                }
                self.handle_branchable_sync_request(peer_id, request)
                    .await?;
            }
            TransportEvent::BranchableSyncReply { peer_id, reply } => {
                self.handle_branchable_sync_reply(peer_id, reply).await?;
            }
            TransportEvent::CarFetchRequest {
                peer_id,
                root_cid,
                token,
            } => {
                if !self.rate_limiter.check(&peer_id) {
                    tracing::warn!(
                        peer_id = %peer_id,
                        "Rate limit exceeded for CarFetchRequest, dropping"
                    );
                    return Err(Error::AccessDenied {
                        peer_id: peer_id.to_string(),
                        collection_id: "rate-limited".into(),
                    });
                }
                self.handle_car_fetch_request(peer_id, root_cid, token)
                    .await?;
            }
            TransportEvent::CarFetchResponse {
                peer_id,
                root_cid,
                car_data,
            } => {
                self.handle_car_fetch_response(peer_id, root_cid, car_data)
                    .await?;
            }
            other => {
                tracing::trace!(event = ?other, "Ignoring non-sync transport event");
            }
        }
        Ok(())
    }
}
