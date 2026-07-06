//! Transport event handling for the sync coordinator.

mod bitswap;
mod branchable_sync;
pub(crate) mod car;
mod doc_sync;
mod gossip;
mod pubsub_raw;
mod pushlog;

use blockstore::Blockstore;
use std::time::Duration;

use super::SyncCoordinator;
use crate::error::{Error, Result, RATE_LIMITED_MESSAGE};
use crate::message::{BranchableSyncReply, DocSyncReply, PushLogReply};
use crate::signing::sign_with_transport;
use crate::sync::rate_limiter::RateLimitDecision;
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

    fn rate_limited_error(peer_id: &PeerId) -> Error {
        Error::AccessDenied {
            peer_id: peer_id.to_string(),
            collection_id: "rate-limited".into(),
        }
    }

    /// Consume one rate-limiter token for `peer_id`, returning the synthetic
    /// rate-limit error when the peer is over budget.
    ///
    /// Gossip events are simply dropped on rejection (no reply channel), while
    /// request events (#1088 W4, re-landing #592's intake scope removed by
    /// fa4a84f7) additionally send a `RATE_LIMITED_MESSAGE` nack via the
    /// `reject_rate_limited_*` helpers so the pusher's retry/backoff engages.
    /// Nack-on-overload is the Go-aligned behavior: Go's direct replicator
    /// channel drives its retryInterval ladder off error replies
    /// (`replicator.go`); overload replies are orthogonal to the trust/ACP
    /// bypasses fa4a84f7 aligned with Go.
    fn check_rate_limit(&self, peer_id: &PeerId, event_kind: &'static str) -> Result<()> {
        match self.runtime.rate_limiter.check(peer_id) {
            RateLimitDecision::Allowed => Ok(()),
            RateLimitDecision::Limited {
                retry_after,
                consecutive_failures,
            } => {
                tracing::debug!(
                    peer_id = %peer_id,
                    event_kind,
                    retry_after_ms = retry_after.as_millis(),
                    consecutive_failures,
                    "Rate limit exceeded, rejecting event"
                );
                Err(Self::rate_limited_error(peer_id))
            }
        }
    }

    async fn reject_rate_limited_pushlog(
        &self,
        message_id: &str,
        token: T::ResponseToken,
        error: Error,
    ) -> Error {
        let reply = PushLogReply::error(message_id, RATE_LIMITED_MESSAGE);
        let _ = self
            .runtime
            .transport
            .send_pushlog_response(token, reply)
            .await;
        error
    }

    async fn reject_rate_limited_two_stream(
        &self,
        peer_id: &PeerId,
        message_id: &str,
        token: Option<T::ResponseToken>,
        error: Error,
    ) -> Error {
        let mut reply = PushLogReply::error(message_id, RATE_LIMITED_MESSAGE);
        let _ = sign_with_transport(&self.runtime.transport, &mut reply);
        self.send_two_stream_reply(peer_id, reply, token).await;
        error
    }

    async fn reject_rate_limited_doc_sync(
        &self,
        peer_id: &PeerId,
        message_id: &str,
        token: Option<T::ResponseToken>,
        error: Error,
    ) -> Error {
        let mut reply = DocSyncReply::error(message_id, RATE_LIMITED_MESSAGE);
        let _ = sign_with_transport(&self.runtime.transport, &mut reply);
        let _ = if let Some(token) = token {
            self.runtime
                .transport
                .send_doc_sync_response_token(token, reply)
                .await
        } else {
            self.runtime
                .transport
                .send_doc_sync_response(peer_id, reply)
                .await
        };
        error
    }

    async fn reject_rate_limited_branchable_sync(
        &self,
        peer_id: &PeerId,
        message_id: &str,
        collection_id: &str,
        token: Option<T::ResponseToken>,
        error: Error,
    ) -> Error {
        let mut reply = BranchableSyncReply::error(message_id, collection_id, RATE_LIMITED_MESSAGE);
        let _ = sign_with_transport(&self.runtime.transport, &mut reply);
        let _ = if let Some(token) = token {
            self.runtime
                .transport
                .send_branchable_sync_response_token(token, reply)
                .await
        } else {
            self.runtime
                .transport
                .send_branchable_sync_response(peer_id, reply)
                .await
        };
        error
    }

    async fn reject_rate_limited_car_fetch(
        &self,
        peer_id: &PeerId,
        token: Option<T::ResponseToken>,
        error: Error,
    ) -> Error {
        // CAR has no error reply type — send an empty response so the sender
        // sees an explicit (parseable) rejection rather than a hung stream.
        let _ = if let Some(token) = token {
            self.runtime
                .transport
                .send_car_response_token(token, Vec::new())
                .await
        } else {
            self.runtime
                .transport
                .send_car_response(peer_id, Vec::new())
                .await
        };
        error
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
                self.check_rate_limit(&propagation_source, "GossipMessage")?;
                self.handle_gossip_message(propagation_source, message, topic)
                    .await?;
            }
            TransportEvent::GossipRawMessage {
                propagation_source,
                topic,
                data,
                ..
            } => {
                self.check_rate_limit(&propagation_source, "GossipRawMessage")?;
                self.handle_gossip_raw_message(propagation_source, topic, data)
                    .await?;
            }
            TransportEvent::PushLogRequest {
                peer_id,
                request,
                token,
            } => {
                if let Err(error) = self.check_rate_limit(&peer_id, "PushLogRequest") {
                    return Err(self
                        .reject_rate_limited_pushlog(&request.message_id, token, error)
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
                if let Err(error) = self.check_rate_limit(&peer_id, "TwoStreamRequest") {
                    return Err(self
                        .reject_rate_limited_two_stream(&peer_id, &request.message_id, token, error)
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
                if let Err(error) = self.check_rate_limit(&peer_id, "DocSyncRequest") {
                    return Err(self
                        .reject_rate_limited_doc_sync(&peer_id, &request.message_id, token, error)
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
                if let Err(error) = self.check_rate_limit(&peer_id, "BranchableSyncRequest") {
                    return Err(self
                        .reject_rate_limited_branchable_sync(
                            &peer_id,
                            &request.message_id,
                            &request.collection_id,
                            token,
                            error,
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
                if let Err(error) = self.check_rate_limit(&peer_id, "CarFetchRequest") {
                    return Err(self
                        .reject_rate_limited_car_fetch(&peer_id, token, error)
                        .await);
                }
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
