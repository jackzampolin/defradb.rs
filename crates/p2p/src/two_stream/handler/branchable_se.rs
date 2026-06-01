//! BranchableSync and SE artifact send methods.

use libp2p::PeerId;

use crate::codec::write_message;
use crate::error::{Error, Result};
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, PushSEArtifactsReply, PushSEArtifactsRequest,
};

use super::{PendingResponseKey, TwoStreamHandler};

impl TwoStreamHandler {
    /// Send a BranchableSync request to a peer without waiting for response.
    ///
    /// The response will arrive asynchronously via TwoStreamEvent::BranchableSyncReply.
    pub async fn send_branchable_sync_request_fire_and_forget(
        &mut self,
        peer_id: PeerId,
        request: BranchableSyncRequest,
    ) -> Result<()> {
        let message_id = request.message_id.clone();
        let pending_key = PendingResponseKey::new(peer_id, message_id.clone());

        {
            let mut pending = self.pending.lock();
            pending.register_branchable_sync_request(pending_key);
        }

        let mut stream = self
            .control
            .open_stream(peer_id, Self::request_protocol())
            .await
            .map_err(|e| {
                self.cleanup_pending_branchable_sync(peer_id, &message_id);
                Error::Transport(format!("failed to open stream: {}", e))
            })?;

        write_message(&mut stream, &request).await.map_err(|e| {
            self.cleanup_pending_branchable_sync(peer_id, &message_id);
            Error::CborSerialization(format!("failed to write request: {}", e))
        })?;

        tracing::info!(
            peer_id = %peer_id,
            message_id = %message_id,
            collection_id = %request.collection_id,
            "Sent BranchableSync request on two-stream protocol (fire-and-forget)"
        );

        Ok(())
    }

    /// Send a BranchableSync response to a peer.
    ///
    /// This opens a new stream on the response protocol and sends the reply.
    pub async fn send_branchable_sync_response(
        &mut self,
        peer_id: PeerId,
        response: BranchableSyncReply,
    ) -> Result<()> {
        let message_id = response.message_id.clone();

        tracing::info!(
            peer_id = %peer_id,
            message_id = %message_id,
            collection_id = %response.collection_id,
            heads_count = response.heads.len(),
            "Opening response stream for BranchableSync reply"
        );

        let mut stream = self
            .control
            .open_stream(peer_id, Self::response_protocol())
            .await
            .map_err(|e| Error::Transport(format!("failed to open response stream: {}", e)))?;

        write_message(&mut stream, &response)
            .await
            .map_err(|e| Error::CborSerialization(format!("failed to write response: {}", e)))?;

        tracing::info!(
            peer_id = %peer_id,
            message_id = %message_id,
            "Sent BranchableSync response on two-stream protocol"
        );

        Ok(())
    }

    /// Send SE artifacts to a peer (fire-and-forget).
    ///
    /// Opens a stream on the SE request protocol, sends the request, and returns.
    /// The Go receiver will process the artifacts and send a reply on the SE response
    /// protocol, but we don't wait for it.
    pub async fn send_se_artifacts_fire_and_forget(
        &mut self,
        peer_id: PeerId,
        request: PushSEArtifactsRequest,
    ) -> Result<()> {
        let message_id = request.message_id.clone();
        let artifacts_count = request.artifacts.len();

        let mut stream = self
            .control
            .open_stream(peer_id, Self::se_request_protocol())
            .await
            .map_err(|e| Error::Transport(format!("failed to open SE stream: {}", e)))?;

        write_message(&mut stream, &request)
            .await
            .map_err(|e| Error::CborSerialization(format!("failed to write SE request: {}", e)))?;

        tracing::info!(
            peer_id = %peer_id,
            message_id = %message_id,
            artifacts_count = artifacts_count,
            collection_id = %request.collection_id,
            "Sent PushSEArtifacts request on SE protocol (fire-and-forget)"
        );

        Ok(())
    }

    /// Send a PushSEArtifacts reply on the SE response protocol.
    ///
    /// Go's SE artifact push (`storeSEProto.SendRequest`) WAITS for this reply on
    /// the response stream; without it a Go owner's write blocks. Rust must
    /// acknowledge inbound artifact pushes.
    pub async fn send_se_artifacts_response(
        &mut self,
        peer_id: PeerId,
        reply: PushSEArtifactsReply,
    ) -> Result<()> {
        let mut stream = self
            .control
            .open_stream(peer_id, Self::se_response_protocol())
            .await
            .map_err(|e| Error::Transport(format!("failed to open SE response stream: {}", e)))?;

        write_message(&mut stream, &reply)
            .await
            .map_err(|e| Error::CborSerialization(format!("failed to write SE reply: {}", e)))?;

        tracing::debug!(
            peer_id = %peer_id,
            message_id = %reply.message_id,
            "Sent PushSEArtifacts reply on SE response protocol"
        );

        Ok(())
    }
}
