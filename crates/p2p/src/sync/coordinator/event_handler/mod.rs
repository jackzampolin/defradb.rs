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
use crate::message::{BranchableSyncReply, DocSyncReply, PushLogReply};
use crate::signing::sign_with_transport;
use crate::transport::{P2PTransport, PeerId, ResponseToken, TransportEvent};

const RATE_LIMITED_MSG: &str = "rate limited: too many requests, retry later";

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    fn rate_limited_error(peer_id: &PeerId) -> Error {
        Error::AccessDenied {
            peer_id: peer_id.to_string(),
            collection_id: "rate-limited".into(),
        }
    }

    async fn reject_rate_limited_pushlog(
        &self,
        peer_id: &PeerId,
        message_id: &str,
        token: ResponseToken,
    ) -> Error {
        tracing::debug!(
            peer_id = %peer_id,
            "Rate limit exceeded for PushLogRequest, sending backpressure reply"
        );
        let reply = PushLogReply::error(message_id, RATE_LIMITED_MSG);
        let _ = self
            .runtime
            .transport
            .send_pushlog_response(token, reply)
            .await;
        Self::rate_limited_error(peer_id)
    }

    async fn reject_rate_limited_two_stream(
        &self,
        peer_id: &PeerId,
        message_id: &str,
        token: Option<ResponseToken>,
    ) -> Error {
        tracing::debug!(
            peer_id = %peer_id,
            "Rate limit exceeded for TwoStreamRequest, sending backpressure reply"
        );
        let mut reply = PushLogReply::error(message_id, RATE_LIMITED_MSG);
        let _ = sign_with_transport(&self.runtime.transport, &mut reply);
        self.send_two_stream_reply(peer_id, reply, token).await;
        Self::rate_limited_error(peer_id)
    }

    async fn reject_rate_limited_doc_sync(
        &self,
        peer_id: &PeerId,
        message_id: &str,
        token: Option<ResponseToken>,
    ) -> Error {
        tracing::debug!(
            peer_id = %peer_id,
            "Rate limit exceeded for DocSyncRequest, sending backpressure reply"
        );
        let reply = DocSyncReply::error(message_id, RATE_LIMITED_MSG);
        if let Some(token) = token {
            let _ = self
                .runtime
                .transport
                .send_doc_sync_response_token(token, reply)
                .await;
        } else {
            let _ = self
                .runtime
                .transport
                .send_doc_sync_response(peer_id, reply)
                .await;
        }
        Self::rate_limited_error(peer_id)
    }

    async fn reject_rate_limited_branchable_sync(
        &self,
        peer_id: &PeerId,
        message_id: &str,
        collection_id: &str,
        token: Option<ResponseToken>,
    ) -> Error {
        tracing::debug!(
            peer_id = %peer_id,
            "Rate limit exceeded for BranchableSyncRequest, sending backpressure reply"
        );
        let reply = BranchableSyncReply::error(message_id, collection_id, RATE_LIMITED_MSG);
        if let Some(token) = token {
            let _ = self
                .runtime
                .transport
                .send_branchable_sync_response_token(token, reply)
                .await;
        } else {
            let _ = self
                .runtime
                .transport
                .send_branchable_sync_response(peer_id, reply)
                .await;
        }
        Self::rate_limited_error(peer_id)
    }

    async fn reject_rate_limited_car_fetch(
        &self,
        peer_id: &PeerId,
        token: Option<ResponseToken>,
    ) -> Error {
        tracing::debug!(
            peer_id = %peer_id,
            "Rate limit exceeded for CarFetchRequest, sending empty backpressure reply"
        );
        // CAR has no error reply type — send empty response so the
        // sender sees an explicit (parseable) rejection rather than
        // a hung stream / timeout.
        if let Some(token) = token {
            let _ = self
                .runtime
                .transport
                .send_car_response_token(token, Vec::new())
                .await;
        } else {
            let _ = self
                .runtime
                .transport
                .send_car_response(peer_id, Vec::new())
                .await;
        }
        Self::rate_limited_error(peer_id)
    }

    /// Handle an event from the transport layer.
    ///
    /// This should be called from the event loop that processes TransportEvents.
    pub async fn handle_transport_event(&self, event: TransportEvent) -> Result<()> {
        match event {
            TransportEvent::PeerConnected(peer_id) => {
                tracing::debug!(peer_id = %peer_id, "Peer connected");
                self.access.peer_state.peer_connected(peer_id.as_str());
            }
            TransportEvent::PeerDisconnected(peer_id) => {
                tracing::debug!(peer_id = %peer_id, "Peer disconnected");
                self.access.peer_state.peer_disconnected(peer_id.as_str());
                self.runtime.rate_limiter.remove_peer(&peer_id);
            }
            TransportEvent::PeerSubscribed { peer_id, topic } => {
                tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer subscribed to topic");
                self.access
                    .peer_state
                    .peer_subscribed(peer_id.as_str(), topic);
            }
            TransportEvent::PeerUnsubscribed { peer_id, topic } => {
                tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer unsubscribed from topic");
                self.access
                    .peer_state
                    .peer_unsubscribed(peer_id.as_str(), &topic);
            }
            TransportEvent::GossipMessage {
                propagation_source,
                message,
                topic,
                ..
            } => {
                if !self.runtime.rate_limiter.check(&propagation_source) {
                    tracing::debug!(
                        peer_id = %propagation_source,
                        "Rate limit exceeded for GossipMessage, dropping"
                    );
                    return Err(Self::rate_limited_error(&propagation_source));
                }
                self.handle_gossip_message(propagation_source, message, topic)
                    .await?;
            }
            TransportEvent::PushLogRequest {
                peer_id,
                request,
                token,
            } => {
                if !self.runtime.rate_limiter.check(&peer_id) {
                    return Err(self
                        .reject_rate_limited_pushlog(&peer_id, &request.metadata.message_id, token)
                        .await);
                }
                self.handle_pushlog_request(peer_id, request, token).await?;
            }
            TransportEvent::TwoStreamRequest {
                peer_id,
                request,
                token,
                is_explicit_replicator,
                explicit_replay_authorization,
            } => {
                if !self.runtime.rate_limiter.check(&peer_id) {
                    return Err(self
                        .reject_rate_limited_two_stream(
                            &peer_id,
                            &request.metadata.message_id,
                            token,
                        )
                        .await);
                }
                self.handle_two_stream_request(
                    peer_id,
                    request,
                    token,
                    is_explicit_replicator,
                    explicit_replay_authorization,
                )
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
            TransportEvent::DocSyncRequest {
                peer_id,
                request,
                token,
            } => {
                if !self.runtime.rate_limiter.check(&peer_id) {
                    return Err(self
                        .reject_rate_limited_doc_sync(&peer_id, &request.metadata.message_id, token)
                        .await);
                }
                self.handle_doc_sync_request(peer_id, request, token)
                    .await?;
            }
            TransportEvent::DocSyncReply { peer_id, reply } => {
                self.handle_doc_sync_reply(peer_id, reply).await?;
            }
            TransportEvent::BranchableSyncRequest {
                peer_id,
                request,
                token,
            } => {
                if !self.runtime.rate_limiter.check(&peer_id) {
                    return Err(self
                        .reject_rate_limited_branchable_sync(
                            &peer_id,
                            &request.metadata.message_id,
                            &request.collection_id,
                            token,
                        )
                        .await);
                }
                self.handle_branchable_sync_request(peer_id, request, token)
                    .await?;
            }
            TransportEvent::BranchableSyncReply { peer_id, reply } => {
                self.handle_branchable_sync_reply(peer_id, reply).await?;
            }
            TransportEvent::CarFetchRequest {
                peer_id,
                request,
                token,
            } => {
                if !self.runtime.rate_limiter.check(&peer_id) {
                    return Err(self.reject_rate_limited_car_fetch(&peer_id, token).await);
                }
                self.handle_car_fetch_request(peer_id, request, token)
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
