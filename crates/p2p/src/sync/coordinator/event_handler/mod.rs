//! Transport event handling for the sync coordinator.

mod bitswap;
mod branchable_sync;
pub(crate) mod car;
mod doc_sync;
mod gossip;
mod pushlog;

use blockstore::Blockstore;
use std::time::Duration;

use super::SyncCoordinator;
use crate::error::{Error, Result};
use crate::transport::{P2PTransport, PeerId, TransportEvent};

const MAX_RETRIABLE_EVENT_ATTEMPTS: usize = 4;

fn retriable_event_delay(attempt: usize) -> Duration {
    match attempt {
        1 => Duration::from_millis(10),
        2 => Duration::from_millis(25),
        _ => Duration::from_millis(50),
    }
}

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    async fn retry_retriable_event<F, Fut>(&self, event_kind: &'static str, mut op: F) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut attempt = 1;
        loop {
            match op().await {
                Ok(()) => return Ok(()),
                Err(error) if error.is_retriable() && attempt < MAX_RETRIABLE_EVENT_ATTEMPTS => {
                    tracing::debug!(
                        event_kind,
                        attempt,
                        max_attempts = MAX_RETRIABLE_EVENT_ATTEMPTS,
                        error = %error,
                        "Retryable transport event failed; backing off and retrying"
                    );
                    tokio::time::sleep(retriable_event_delay(attempt)).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn handle_peer_connected(&self, peer_id: PeerId) {
        tracing::debug!(peer_id = %peer_id, "Peer connected");
        self.access.peer_state.peer_connected(peer_id.as_str());
    }

    fn handle_peer_disconnected(&self, peer_id: PeerId) {
        tracing::debug!(peer_id = %peer_id, "Peer disconnected");
        self.access.peer_state.peer_disconnected(peer_id.as_str());
        self.runtime.rate_limiter.remove_peer(&peer_id);
    }

    fn handle_peer_subscribed(&self, peer_id: PeerId, topic: String) {
        tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer subscribed to topic");
        self.access
            .peer_state
            .peer_subscribed(peer_id.as_str(), topic);
    }

    fn handle_peer_unsubscribed(&self, peer_id: PeerId, topic: String) {
        tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer unsubscribed from topic");
        self.access
            .peer_state
            .peer_unsubscribed(peer_id.as_str(), &topic);
    }

    /// Handle an event from the transport layer.
    ///
    /// This should be called from the event loop that processes TransportEvents.
    pub async fn handle_transport_event(
        &self,
        event: TransportEvent<T::ResponseToken>,
    ) -> Result<()> {
        if self.runtime.shutdown.is_shutting_down() {
            tracing::trace!("Ignoring transport event because coordinator is shutting down");
            return Ok(());
        }

        match event {
            TransportEvent::PeerConnected(peer_id) => {
                self.handle_peer_connected(peer_id);
            }
            TransportEvent::PeerDisconnected(peer_id) => {
                self.handle_peer_disconnected(peer_id);
            }
            TransportEvent::PeerSubscribed { peer_id, topic } => {
                self.handle_peer_subscribed(peer_id, topic);
            }
            TransportEvent::PeerUnsubscribed { peer_id, topic } => {
                self.handle_peer_unsubscribed(peer_id, topic);
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
                self.handle_pushlog_request(peer_id, request, token).await?;
            }
            TransportEvent::TwoStreamRequest {
                peer_id,
                request,
                token,
                is_explicit_replicator,
                explicit_replay_authorization,
            } => {
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
                self.retry_retriable_event("bitswap_block_received", || {
                    self.handle_bitswap_block_received(query_id, cid, data.clone())
                })
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
                self.handle_car_fetch_request(peer_id, request, token)
                    .await?;
            }
            TransportEvent::CarFetchResponse {
                peer_id,
                root_cid,
                car_data,
            } => {
                self.retry_retriable_event("car_fetch_response", || {
                    self.handle_car_fetch_response(peer_id.clone(), root_cid, car_data.clone())
                })
                .await?;
            }
            other => {
                let _ = other;
                tracing::trace!("Ignoring non-sync transport event");
            }
        }
        Ok(())
    }
}
