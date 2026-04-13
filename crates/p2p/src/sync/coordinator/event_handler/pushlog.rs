//! PushLog request handling (both standard and two-stream).

use blockstore::Blockstore;
use cid::Cid;

use super::super::SyncCoordinator;
use crate::error::Result;
use crate::message::{PushLogBroadcast, PushLogReply};
use crate::signing::sign_with_transport;
use crate::transport::{P2PTransport, PeerId};
use crate::ExplicitReplayAuthorization;

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

        // Access control check
        if let Err(e) = self
            .check_access_str(peer_id.as_str(), &request.collection_id)
            .await
        {
            tracing::warn!(
                peer_id = %peer_id,
                collection_id = %request.collection_id,
                doc_id = %request.doc_id,
                "Rejecting PushLog request from unauthorized peer"
            );
            let reply = PushLogReply::error(
                &request.metadata.message_id,
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
                let reply = PushLogReply::error(&request.metadata.message_id, &error_msg);
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
            .process_pushlog(
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

        let reply = match &process_result {
            Ok(()) => PushLogReply::success(&request.metadata.message_id),
            Err(e) => PushLogReply::error(&request.metadata.message_id, &e.to_string()),
        };

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
        tracing::debug!(
            peer_id = %peer_id,
            doc_id = %request.doc_id,
            collection_id = %request.collection_id,
            message_id = %request.metadata.message_id,
            "Received PushLog request via two-stream protocol (Go compatibility)"
        );

        // Access control check
        if let Err(e) = self
            .check_access_str(peer_id.as_str(), &request.collection_id)
            .await
        {
            tracing::warn!(
                peer_id = %peer_id,
                collection_id = %request.collection_id,
                doc_id = %request.doc_id,
                "Rejecting two-stream request from unauthorized peer"
            );
            let mut reply = PushLogReply::error(
                &request.metadata.message_id,
                &format!(
                    "access denied: not authorized for collection {}",
                    request.collection_id
                ),
            );
            if let Err(sign_err) = sign_with_transport(&self.runtime.transport, &mut reply) {
                tracing::error!(error = %sign_err, "Failed to sign access denied response");
            }
            self.send_two_stream_reply(&peer_id, reply, token).await;
            return Err(e);
        }

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
                let mut reply = PushLogReply::error(&request.metadata.message_id, &error_msg);
                if let Err(sign_err) = sign_with_transport(&self.runtime.transport, &mut reply) {
                    tracing::error!(error = %sign_err, "Failed to sign invalid CID response");
                }
                self.send_two_stream_reply(&peer_id, reply, token).await;
                return Err(crate::error::Error::InvalidCid(error_msg));
            }
        };

        tracing::trace!(?cid, "Parsed valid CID from two-stream request");

        let broadcast = PushLogBroadcast::from_request(&request);
        let process_result = self
            .manager
            .process_pushlog(
                &broadcast,
                Some(peer_id.as_str()),
                is_explicit_replicator,
                explicit_replay_authorization,
            )
            .await;

        let mut reply = match &process_result {
            Ok(()) => PushLogReply::success(&request.metadata.message_id),
            Err(e) => PushLogReply::error(&request.metadata.message_id, &e.to_string()),
        };

        if let Err(e) = sign_with_transport(&self.runtime.transport, &mut reply) {
            tracing::error!(
                peer_id = %peer_id,
                error = %e,
                "Failed to sign two-stream response"
            );
            return Err(e);
        }

        self.send_two_stream_reply(&peer_id, reply, token).await;

        process_result
    }

    /// Send a two-stream reply using the response token if available,
    /// falling back to the transport's send_two_stream_response.
    pub(in crate::sync::coordinator) async fn send_two_stream_reply(
        &self,
        peer_id: &PeerId,
        reply: PushLogReply,
        token: Option<T::ResponseToken>,
    ) {
        if let Some(token) = token {
            if let Err(e) = self
                .runtime
                .transport
                .send_pushlog_response(token, reply)
                .await
            {
                tracing::warn!(
                    peer_id = %peer_id,
                    error = %e,
                    "Failed to send two-stream response via token"
                );
            }
        } else if let Err(e) = self
            .runtime
            .transport
            .send_two_stream_response(peer_id, reply)
            .await
        {
            tracing::warn!(
                peer_id = %peer_id,
                error = %e,
                "Failed to send two-stream response"
            );
        }
    }
}
