//! DocSync request and response methods.

use libp2p::PeerId;
use tokio::time::timeout;

use crate::codec::write_message;
use crate::error::{Error, Result};
use crate::message::{DocSyncReply, DocSyncRequest};

use super::{PendingResponseKey, TwoStreamHandler, RESPONSE_TIMEOUT};

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
        let pending_key = PendingResponseKey::new(peer_id, message_id.clone());

        // Create response channel
        let (tx, rx) = tokio::sync::oneshot::channel();

        // Register pending response against the expected peer as well as the message ID.
        {
            let mut pending = self.pending.lock();
            pending.doc_sync_channels.insert(pending_key.clone(), tx);
        }

        // Open stream and send request
        let mut stream = self
            .control
            .open_stream(peer_id, Self::request_protocol())
            .await
            .map_err(|e| {
                // Clean up pending on failure
                let mut pending = self.pending.lock();
                pending.doc_sync_channels.remove(&pending_key);
                Error::Transport(format!("failed to open stream: {}", e))
            })?;

        write_message(&mut stream, &request).await.map_err(|e| {
            // Clean up pending on failure
            let mut pending = self.pending.lock();
            pending.doc_sync_channels.remove(&pending_key);
            Error::CborSerialization(format!("failed to write request: {}", e))
        })?;

        tracing::debug!(
            peer_id = %peer_id,
            message_id = %message_id,
            doc_ids = ?request.doc_ids,
            "Sent DocSync request on two-stream protocol"
        );

        // Wait for the response routed by handle_response_stream.
        match timeout(RESPONSE_TIMEOUT, rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => {
                let mut pending = self.pending.lock();
                pending.doc_sync_channels.remove(&pending_key);
                Err(Error::Transport("response channel closed".into()))
            }
            Err(_) => {
                let mut pending = self.pending.lock();
                pending.doc_sync_channels.remove(&pending_key);
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
        let pending_key = PendingResponseKey::new(peer_id, message_id.clone());

        {
            let mut pending = self.pending.lock();
            pending.register_doc_sync_request(pending_key.clone());
        }

        // Open stream and send request
        let mut stream = self
            .control
            .open_stream(peer_id, Self::request_protocol())
            .await
            .map_err(|e| {
                self.cleanup_pending_doc_sync(peer_id, &message_id);
                Error::Transport(format!("failed to open stream: {}", e))
            })?;

        write_message(&mut stream, &request).await.map_err(|e| {
            self.cleanup_pending_doc_sync(peer_id, &message_id);
            Error::CborSerialization(format!("failed to write request: {}", e))
        })?;

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
