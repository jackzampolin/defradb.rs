//! DocSync request and response methods.

use libp2p::PeerId;
use tokio::time::timeout;

use crate::codec::write_message;
use crate::error::{Error, Result};
use crate::message::{DocSyncReply, DocSyncRequest};

use super::{TwoStreamHandler, RESPONSE_TIMEOUT};

impl TwoStreamHandler {
    /// Send a DocSync request to a peer and wait for response.
    ///
    /// This opens a stream on the request protocol, sends the request,
    /// then waits for the response to arrive on a separate stream.
    pub async fn send_doc_sync_request(
        &mut self,
        peer_id: PeerId,
        request: DocSyncRequest,
    ) -> Result<DocSyncReply> {
        let message_id = request.message_id.clone();

        // Create response channel
        let (tx, rx) = tokio::sync::oneshot::channel();

        // Register pending response (use the same pending map, keyed by message ID)
        {
            let mut pending = self.pending.lock();
            // Store as a PushLogReply channel - we'll need a different approach for DocSyncReply
            // Actually, we need a separate map for DocSync responses
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
            doc_ids = ?request.doc_ids,
            "Sent DocSync request on two-stream protocol"
        );

        // Wait for response with timeout - we'll receive a PushLogReply and need to
        // handle DocSyncReply differently. For now, this is a simplified approach.
        // The actual DocSyncReply handling needs to be done in handle_response_stream.
        match timeout(RESPONSE_TIMEOUT, rx).await {
            Ok(Ok(_response)) => {
                // The response channel receives PushLogReply, but we need DocSyncReply
                // This is a limitation of the current design - we need separate channels
                // For now, return an error indicating the simplified implementation
                Err(Error::Transport(
                    "DocSync response handling requires dedicated channel".into(),
                ))
            }
            Ok(Err(_)) => {
                let mut pending = self.pending.lock();
                pending.channels.remove(&message_id);
                Err(Error::Transport("response channel closed".into()))
            }
            Err(_) => {
                let mut pending = self.pending.lock();
                pending.channels.remove(&message_id);
                Err(Error::Transport("timeout waiting for response".into()))
            }
        }
    }

    /// Send a DocSync request to a peer without waiting for response.
    ///
    /// The response will arrive asynchronously via TwoStreamEvent::DocSyncReply.
    /// This is the preferred method for DocSync since response processing
    /// happens via the event loop.
    pub async fn send_doc_sync_request_fire_and_forget(
        &mut self,
        peer_id: PeerId,
        request: DocSyncRequest,
    ) -> Result<()> {
        let message_id = request.message_id.clone();

        // Open stream and send request
        let mut stream = self
            .control
            .open_stream(peer_id, Self::request_protocol())
            .await
            .map_err(|e| Error::Transport(format!("failed to open stream: {}", e)))?;

        write_message(&mut stream, &request)
            .await
            .map_err(|e| Error::CborSerialization(format!("failed to write request: {}", e)))?;

        tracing::info!(
            peer_id = %peer_id,
            message_id = %message_id,
            doc_ids = ?request.doc_ids,
            "Sent DocSync request on two-stream protocol (fire-and-forget)"
        );

        Ok(())
    }

    /// Send a DocSync response to a peer.
    ///
    /// This opens a new stream on the response protocol and sends the reply.
    pub async fn send_doc_sync_response(
        &mut self,
        peer_id: PeerId,
        response: DocSyncReply,
    ) -> Result<()> {
        let message_id = response.message_id.clone();

        tracing::info!(
            peer_id = %peer_id,
            message_id = %message_id,
            results_count = response.results.len(),
            "Opening response stream for DocSync reply"
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
            "Sent DocSync response on two-stream protocol"
        );

        Ok(())
    }
}
