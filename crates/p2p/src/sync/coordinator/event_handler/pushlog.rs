//! PushLog request handling (both standard and two-stream).

use blockstore::Blockstore;
use cid::Cid;

use super::super::SyncCoordinator;
use crate::error::Result;
use crate::message::{PushLogBroadcast, PushLogReply};
use crate::signing::sign_with_transport;
use crate::transport::{P2PTransport, PeerId};
use crate::ExplicitReplayAuthorization;

/// Build the PushLogReply for a `process_pushlog` outcome.
///
/// Invariant (#1088 M1): a success reply implies the block is either merged or
/// registered as pending — no path may ack success after discarding state.
/// Backpressure failures (pending-DAG capacity, rate limit) reply the
/// byte-exact `RATE_LIMITED_MESSAGE` the pusher matches to drive its
/// retry/backoff. Nack-on-overload is the Go-aligned behavior: Go's direct
/// replicator channel drives its retryInterval ladder off error replies
/// (`replicator.go`), so these overload nacks are orthogonal to the trust/ACP
/// bypasses fa4a84f7 aligned with Go when it removed the #592 nacks.
fn build_pushlog_reply(message_id: &str, process_result: &Result<()>) -> PushLogReply {
    match process_result {
        Ok(()) => PushLogReply::success(message_id),
        Err(e) => match e.backpressure_reply_message() {
            Some(nack) => PushLogReply::error(message_id, nack),
            None => PushLogReply::error(message_id, &e.to_string()),
        },
    }
}

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    pub(super) async fn handle_pushlog_request(
        &self,
        peer_id: PeerId,
        request: crate::message::PushLogRequest,
        token: T::ResponseToken,
    ) -> Result<()> {
        tracing::debug!(
            peer_id = %peer_id,
            doc_id = %request.doc_id,
            collection_id = %request.collection_id,
            "Received PushLog request"
        );

        if let Err(e) = self
            .check_pushlog_access_str(peer_id.as_str(), &request.collection_id)
            .await
        {
            tracing::warn!(
                peer_id = %peer_id,
                collection_id = %request.collection_id,
                doc_id = %request.doc_id,
                "Rejecting PushLog request from unauthorized peer"
            );
            let reply = PushLogReply::error(
                &request.message_id,
                &format!(
                    "access denied: not authorized for collection {}",
                    request.collection_id
                ),
            );
            if let Err(send_err) = self
                .runtime
                .transport
                .send_pushlog_response(token, reply)
                .await
            {
                tracing::warn!(
                    peer_id = %peer_id,
                    error = %send_err,
                    "Failed to send access denied response"
                );
            }
            return Err(e);
        }

        let is_explicit_replicator =
            self.is_registered_replicator(peer_id.as_str(), &request.collection_id);

        // Parse CID - if invalid, send error response
        let cid = match Cid::try_from(request.cid.as_ref()) {
            Ok(cid) => {
                self.access.peer_state.peer_has_cid(peer_id.as_str(), cid);
                cid
            }
            Err(e) => {
                let error_msg = format!("Failed to parse CID: {}", e);
                tracing::warn!(
                    peer_id = %peer_id,
                    cid_bytes_len = request.cid.len(),
                    error = %e,
                    "Failed to parse CID from PushLog request - sending error response"
                );
                let reply = PushLogReply::error(&request.message_id, &error_msg);
                if let Err(send_err) = self
                    .runtime
                    .transport
                    .send_pushlog_response(token, reply)
                    .await
                {
                    tracing::warn!(
                        peer_id = %peer_id,
                        error = %send_err,
                        "Failed to send error response for invalid CID"
                    );
                }
                return Err(crate::error::Error::InvalidCid(error_msg));
            }
        };

        tracing::trace!(?cid, "Parsed valid CID from PushLog request");

        let broadcast = PushLogBroadcast::from_request(&request);
        let process_result = self
            .manager
            .process_pushlog_from_dag_provider(
                &broadcast,
                Some(peer_id.as_str()),
                is_explicit_replicator,
                None,
            )
            .await;

        if let Err(e) = &process_result {
            tracing::warn!(
                peer_id = %peer_id,
                doc_id = %request.doc_id,
                collection_id = %request.collection_id,
                cid = %cid,
                error = %e,
                "PushLog request processing failed"
            );
        }

        let reply = build_pushlog_reply(&request.message_id, &process_result);

        if let Err(e) = self
            .runtime
            .transport
            .send_pushlog_response(token, reply)
            .await
        {
            tracing::warn!(
                peer_id = %peer_id,
                doc_id = %request.doc_id,
                error = %e,
                "Failed to send PushLog response"
            );
        } else {
            tracing::trace!(
                peer_id = %peer_id,
                doc_id = %request.doc_id,
                "Sent PushLog response"
            );
        }

        process_result
    }

    pub(super) async fn handle_two_stream_request(
        &self,
        peer_id: PeerId,
        request: crate::message::PushLogRequest,
        token: Option<T::ResponseToken>,
        is_explicit_replicator: bool,
        explicit_replay_authorization: Option<ExplicitReplayAuthorization>,
    ) -> Result<()> {
        let supports_same_stream_reply = request.supports_same_stream_reply;
        tracing::debug!(
            peer_id = %peer_id,
            doc_id = %request.doc_id,
            collection_id = %request.collection_id,
            message_id = %request.message_id,
            "Received PushLog request via two-stream protocol (Go compatibility)"
        );

        // Go's direct replicator comm channel calls `processPushlogRequest`
        // with `isReplicator=true`, which skips the receiver-side pre-merge
        // collection access check. This two-stream protocol is that direct
        // replicator channel in Rust. Keep pubsub PushLog ingress guarded
        // above, and keep merge-time ACP/explicit replay checks downstream.

        // Parse CID
        let cid = match Cid::try_from(request.cid.as_ref()) {
            Ok(cid) => {
                self.access.peer_state.peer_has_cid(peer_id.as_str(), cid);
                cid
            }
            Err(e) => {
                let error_msg = format!("Failed to parse CID: {}", e);
                tracing::warn!(
                    peer_id = %peer_id,
                    cid_bytes_len = request.cid.len(),
                    error = %e,
                    "Failed to parse CID from two-stream request - sending error response"
                );
                let mut reply = PushLogReply::error(&request.message_id, &error_msg);
                if let Err(sign_err) = sign_with_transport(&self.runtime.transport, &mut reply) {
                    tracing::error!(error = %sign_err, "Failed to sign invalid CID response");
                }
                self.send_two_stream_reply(&peer_id, reply, token, supports_same_stream_reply)
                    .await;
                return Err(crate::error::Error::InvalidCid(error_msg));
            }
        };

        tracing::trace!(?cid, "Parsed valid CID from two-stream request");

        let broadcast = PushLogBroadcast::from_request(&request);
        let process_result = self
            .manager
            .process_pushlog_from_dag_provider(
                &broadcast,
                Some(peer_id.as_str()),
                is_explicit_replicator,
                explicit_replay_authorization,
            )
            .await;

        let mut reply = build_pushlog_reply(&request.message_id, &process_result);

        if let Err(e) = sign_with_transport(&self.runtime.transport, &mut reply) {
            tracing::error!(
                peer_id = %peer_id,
                error = %e,
                "Failed to sign two-stream response"
            );
            return Err(e);
        }

        self.send_two_stream_reply(&peer_id, reply, token, supports_same_stream_reply)
            .await;

        process_result
    }

    /// Send a two-stream reply on the request stream when the sender advertised
    /// support, falling back to the legacy reverse-stream response otherwise.
    pub(in crate::sync::coordinator) async fn send_two_stream_reply(
        &self,
        peer_id: &PeerId,
        reply: PushLogReply,
        token: Option<T::ResponseToken>,
        supports_same_stream_reply: bool,
    ) {
        let send_result = if supports_same_stream_reply {
            if let Some(token) = token {
                self.runtime
                    .transport
                    .send_pushlog_response(token, reply)
                    .await
            } else {
                self.runtime
                    .transport
                    .send_two_stream_response(peer_id, reply)
                    .await
            }
        } else {
            self.runtime
                .transport
                .send_two_stream_response(peer_id, reply)
                .await
        };

        if let Err(e) = send_result {
            if e.is_connection_like() {
                tracing::debug!(
                    peer_id = %peer_id,
                    error = %e,
                    "Peer disconnected before the two-stream response could be sent"
                );
            } else {
                tracing::warn!(
                    peer_id = %peer_id,
                    error = %e,
                    "Failed to send two-stream response"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_flight_single_flight_suppression_replies_with_backpressure() {
        let result = Err(crate::error::Error::PushLogInFlight {
            cid: "bafy-head".to_string(),
        });

        let reply = build_pushlog_reply("message-1", &result);

        assert_eq!(
            reply.err_message.as_deref(),
            Some(crate::error::RATE_LIMITED_MESSAGE)
        );
    }
}
