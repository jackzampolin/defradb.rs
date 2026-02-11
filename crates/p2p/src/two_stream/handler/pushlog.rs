//! PushLog request and response methods.

use libp2p::PeerId;
use tokio::sync::oneshot;

use crate::codec::write_message;
use crate::error::{Error, Result};
use crate::message::{PushLogReply, PushLogRequest};

use super::TwoStreamHandler;

impl TwoStreamHandler {
    /// Send a request to a peer and return a receiver for the response.
    ///
    /// Opens a stream on the request protocol, sends the request, and returns
    /// the oneshot receiver. The caller MUST drop the handler lock before
    /// awaiting the receiver — otherwise the response handler cannot route
    /// the reply back (deadlock).
    pub async fn start_request(
        &mut self,
        peer_id: PeerId,
        request: PushLogRequest,
    ) -> Result<(String, oneshot::Receiver<PushLogReply>)> {
        let message_id = request.metadata.message_id.clone();

        // Create response channel
        let (tx, rx) = oneshot::channel();

        // Register pending response
        {
            let mut pending = self.pending.lock();
            pending.channels.insert(message_id.clone(), tx);
        }

        // Open stream and send request
        let mut stream = self
            .control
            .open_stream(peer_id, Self::request_protocol())
            .await
            .map_err(|e| {
                // Clean up pending on failure
                let mut pending = self.pending.lock();
                pending.channels.remove(&message_id);
                Error::Transport(format!("failed to open stream: {}", e))
            })?;

        write_message(&mut stream, &request).await.map_err(|e| {
            // Clean up pending on failure
            let mut pending = self.pending.lock();
            pending.channels.remove(&message_id);
            Error::CborSerialization(format!("failed to write request: {}", e))
        })?;

        tracing::debug!(
            peer_id = %peer_id,
            message_id = %message_id,
            doc_id = %request.doc_id,
            "Sent PushLog request on two-stream protocol"
        );

        Ok((message_id, rx))
    }

    /// Send a response to a peer.
    ///
    /// This opens a new stream on the response protocol and sends the reply.
    pub async fn send_response(&mut self, peer_id: PeerId, response: PushLogReply) -> Result<()> {
        let message_id = response.message_id.clone();

        tracing::info!(
            peer_id = %peer_id,
            message_id = %message_id,
            pubkey_len = response.pubkey.len(),
            "Opening response stream for two-stream protocol"
        );

        // Open stream and send response
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
            "Sent PushLog response on two-stream protocol"
        );

        Ok(())
    }
}
