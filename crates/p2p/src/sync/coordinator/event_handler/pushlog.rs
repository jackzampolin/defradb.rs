//! PushLog request handling (both standard and two-stream).

use blockstore::Blockstore;
use cid::Cid;

use super::super::SyncCoordinator;
use crate::error::Result;
use crate::message::{PushLogBroadcast, PushLogReply};
use crate::signing::sign_message;

impl<B: Blockstore + 'static> SyncCoordinator<B> {
    pub(super) async fn handle_pushlog_request(
        &self,
        peer_id: libp2p::PeerId,
        request: crate::message::PushLogRequest,
        channel: crate::host::ResponseChannel,
    ) -> Result<()> {
        tracing::debug!(
            peer_id = %peer_id,
            doc_id = %request.doc_id,
            collection_id = %request.collection_id,
            "Received PushLog request"
        );

        // Access control check
        if let Err(e) = self.check_access(&peer_id, &request.collection_id) {
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
            if let Err(send_err) = self.host.send_pushlog_response(channel, reply).await {
                tracing::warn!(
                    peer_id = %peer_id,
                    error = %send_err,
                    "Failed to send access denied response"
                );
            }
            return Err(e);
        }

        // Parse CID - if invalid, send error response
        let cid = match Cid::try_from(request.cid.as_slice()) {
            Ok(cid) => {
                self.peer_state.peer_has_cid(&peer_id, cid);
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
                if let Err(send_err) = self.host.send_pushlog_response(channel, reply).await {
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

        // Convert request to broadcast format and process
        let broadcast = PushLogBroadcast::from_request(&request);
        let process_result = self.manager.process_pushlog(&broadcast).await;

        // Send response based on processing result
        let reply = match &process_result {
            Ok(()) => PushLogReply::success(&request.metadata.message_id),
            Err(e) => PushLogReply::error(&request.metadata.message_id, &e.to_string()),
        };

        if let Err(e) = self.host.send_pushlog_response(channel, reply).await {
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
        peer_id: libp2p::PeerId,
        request: crate::message::PushLogRequest,
    ) -> Result<()> {
        tracing::debug!(
            peer_id = %peer_id,
            doc_id = %request.doc_id,
            collection_id = %request.collection_id,
            message_id = %request.metadata.message_id,
            "Received PushLog request via two-stream protocol (Go compatibility)"
        );

        // Access control check
        if let Err(e) = self.check_access(&peer_id, &request.collection_id) {
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
            if let Err(sign_err) = sign_message(self.host.keypair(), &mut reply) {
                tracing::error!(error = %sign_err, "Failed to sign access denied response");
            }
            if let Err(send_err) = self.host.send_two_stream_response(peer_id, reply).await {
                tracing::warn!(
                    peer_id = %peer_id,
                    error = %send_err,
                    "Failed to send access denied response via two-stream"
                );
            }
            return Err(e);
        }

        // Parse CID - if invalid, send error response
        let cid = match Cid::try_from(request.cid.as_slice()) {
            Ok(cid) => {
                self.peer_state.peer_has_cid(&peer_id, cid);
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
                if let Err(sign_err) = sign_message(self.host.keypair(), &mut reply) {
                    tracing::error!(error = %sign_err, "Failed to sign invalid CID response");
                }
                if let Err(send_err) = self.host.send_two_stream_response(peer_id, reply).await {
                    tracing::warn!(
                        peer_id = %peer_id,
                        error = %send_err,
                        "Failed to send error response for invalid CID via two-stream"
                    );
                }
                return Err(crate::error::Error::InvalidCid(error_msg));
            }
        };

        tracing::trace!(?cid, "Parsed valid CID from two-stream request");

        // Convert request to broadcast format and process
        let broadcast = PushLogBroadcast::from_request(&request);
        let process_result = self.manager.process_pushlog(&broadcast).await;

        // Send response via two-stream protocol (on a NEW stream)
        let mut reply = match &process_result {
            Ok(()) => PushLogReply::success(&request.metadata.message_id),
            Err(e) => PushLogReply::error(&request.metadata.message_id, &e.to_string()),
        };

        // Sign the response (required for Go compatibility)
        if let Err(e) = sign_message(self.host.keypair(), &mut reply) {
            tracing::error!(
                peer_id = %peer_id,
                error = %e,
                "Failed to sign two-stream response"
            );
            return Err(e);
        }

        if let Err(e) = self.host.send_two_stream_response(peer_id, reply).await {
            tracing::warn!(
                peer_id = %peer_id,
                doc_id = %request.doc_id,
                error = %e,
                "Failed to send two-stream response"
            );
        } else {
            tracing::trace!(
                peer_id = %peer_id,
                doc_id = %request.doc_id,
                "Sent two-stream response"
            );
        }

        process_result
    }
}
