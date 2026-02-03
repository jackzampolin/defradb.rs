//! Two-stream protocol handler for Go compatibility.
//!
//! Go's DefraDB uses a two-stream pattern for request-response:
//! 1. Sender opens stream on `/defradb/rep_req/0.0.1`, sends request, closes stream
//! 2. Receiver processes request, opens NEW stream on `/defradb/rep_resp/0.0.1` to send response
//!
//! This is different from libp2p-rust's request-response which uses bidirectional streams.
//! This module implements Go's pattern for interoperability using libp2p-stream.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use libp2p::{PeerId, Stream, StreamProtocol};
use libp2p_stream as stream;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::codec::write_message;
use crate::error::{Error, Result};
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogReply,
    PushLogRequest,
};
use crate::protocol::{REP_REQUEST_PROTOCOL, REP_RESPONSE_PROTOCOL};

/// Timeout for waiting for a response.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Event emitted by the two-stream handler.
#[derive(Debug)]
pub enum TwoStreamEvent {
    /// Received a PushLog request from a peer.
    InboundRequest {
        peer_id: PeerId,
        request: PushLogRequest,
    },
    /// Received a DocSync request from a peer.
    DocSyncRequest {
        peer_id: PeerId,
        request: DocSyncRequest,
    },
    /// Received a DocSync reply from a peer.
    DocSyncReply {
        peer_id: PeerId,
        reply: DocSyncReply,
    },
    /// Received a BranchableSync request from a peer.
    BranchableSyncRequest {
        peer_id: PeerId,
        request: BranchableSyncRequest,
    },
    /// Received a BranchableSync reply from a peer.
    BranchableSyncReply {
        peer_id: PeerId,
        reply: BranchableSyncReply,
    },
    /// Failed to decode an incoming message.
    DecodeError { peer_id: PeerId, error: String },
}

/// State for tracking pending responses.
#[derive(Default)]
struct PendingResponses {
    /// Map of MessageID to response channel.
    channels: HashMap<String, oneshot::Sender<PushLogReply>>,
}

/// Two-stream protocol handler.
///
/// Handles Go's two-stream request-response pattern where requests and responses
/// flow on separate streams identified by different protocol IDs.
///
/// Uses `libp2p-stream` for stream management.
pub struct TwoStreamHandler {
    /// Control for the stream behaviour (for opening streams).
    control: stream::Control,
    /// Pending response channels keyed by MessageID.
    pending: Arc<Mutex<PendingResponses>>,
}

impl TwoStreamHandler {
    /// Create a new two-stream handler from a stream::Control.
    pub fn new(control: stream::Control) -> Self {
        Self {
            control,
            pending: Arc::new(Mutex::new(PendingResponses::default())),
        }
    }

    /// Get the request protocol.
    pub fn request_protocol() -> StreamProtocol {
        StreamProtocol::new(REP_REQUEST_PROTOCOL)
    }

    /// Get the response protocol.
    pub fn response_protocol() -> StreamProtocol {
        StreamProtocol::new(REP_RESPONSE_PROTOCOL)
    }

    /// Handle an incoming stream on the request protocol.
    ///
    /// Reads the request and returns an event for processing.
    /// Supports both PushLogRequest and DocSyncRequest message types.
    pub async fn handle_request_stream(
        peer_id: PeerId,
        mut stream: Stream,
    ) -> Result<TwoStreamEvent> {
        use futures::AsyncReadExt;

        tracing::info!(peer_id = %peer_id, "Reading message from two-stream request");

        // Read raw bytes from the stream
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.map_err(|e| {
            tracing::error!(peer_id = %peer_id, error = %e, "Failed to read stream bytes");
            Error::CborDeserialization(format!("failed to read stream: {}", e))
        })?;

        // Try to deserialize as PushLogRequest first
        if let Ok(request) = serde_cbor::from_slice::<PushLogRequest>(&buf) {
            tracing::info!(
                peer_id = %peer_id,
                message_id = %request.metadata.message_id,
                doc_id = %request.doc_id,
                "Successfully read PushLog request on two-stream protocol"
            );
            return Ok(TwoStreamEvent::InboundRequest { peer_id, request });
        }

        // Try to deserialize as DocSyncRequest
        if let Ok(request) = serde_cbor::from_slice::<DocSyncRequest>(&buf) {
            tracing::info!(
                peer_id = %peer_id,
                message_id = %request.metadata.message_id,
                doc_ids = ?request.doc_ids,
                "Successfully read DocSync request on two-stream protocol"
            );
            return Ok(TwoStreamEvent::DocSyncRequest { peer_id, request });
        }

        // Try to deserialize as BranchableSyncRequest
        if let Ok(request) = serde_cbor::from_slice::<BranchableSyncRequest>(&buf) {
            tracing::info!(
                peer_id = %peer_id,
                message_id = %request.metadata.message_id,
                collection_id = %request.collection_id,
                "Successfully read BranchableSync request on two-stream protocol"
            );
            return Ok(TwoStreamEvent::BranchableSyncRequest { peer_id, request });
        }

        // None worked - return error
        Err(Error::CborDeserialization(
            "failed to deserialize as PushLog, DocSync, or BranchableSync request".to_string(),
        ))
    }

    /// Handle an incoming stream on the response protocol.
    ///
    /// Reads the response and routes it to the appropriate handler.
    /// Returns an optional TwoStreamEvent for DocSyncReply (to be forwarded to coordinator).
    /// PushLogReply is routed directly to pending channels.
    ///
    /// DocSyncReply is a superset of PushLogReply (same fields plus Results),
    /// so we deserialize as DocSyncReply first. We then check if there's a
    /// pending PushLog channel for the message_id to determine the routing.
    pub async fn handle_response_stream(
        &self,
        peer_id: PeerId,
        mut stream: Stream,
    ) -> Result<Option<TwoStreamEvent>> {
        use futures::AsyncReadExt;

        // Read raw bytes first to try different message types
        let mut buf = Vec::new();
        stream
            .read_to_end(&mut buf)
            .await
            .map_err(|e| Error::CborDeserialization(format!("failed to read response: {}", e)))?;

        // Try BranchableSyncReply first (has CollectionID + Heads fields).
        // Must come before DocSyncReply since serde_cbor ignores unknown fields.
        if let Ok(reply) = serde_cbor::from_slice::<BranchableSyncReply>(&buf) {
            // Only treat as BranchableSync if it has a non-empty collection_id
            // (DocSyncReply wouldn't have this field, so it would be empty)
            if !reply.collection_id.is_empty() {
                tracing::debug!(
                    peer_id = %peer_id,
                    message_id = %reply.message_id,
                    collection_id = %reply.collection_id,
                    heads_count = reply.heads.len(),
                    "Received BranchableSync response on two-stream protocol"
                );
                return Ok(Some(TwoStreamEvent::BranchableSyncReply { peer_id, reply }));
            }
        }

        // Deserialize as DocSyncReply since it's a superset of PushLogReply.
        // PushLogReply deserialization would also succeed on DocSyncReply data
        // (serde_cbor ignores unknown fields), which would silently drop the
        // Results field and misroute the message.
        if let Ok(reply) = serde_cbor::from_slice::<DocSyncReply>(&buf) {
            let message_id = reply.message_id.clone();

            // Check if there's a pending PushLog channel for this message_id.
            // If yes, this is actually a PushLogReply being routed.
            let pending_sender = {
                let mut pending = self.pending.lock();
                pending.channels.remove(&message_id)
            };

            if let Some(sender) = pending_sender {
                // Route as PushLogReply to the pending channel
                tracing::debug!(
                    peer_id = %peer_id,
                    message_id = %message_id,
                    "Received PushLog response on two-stream protocol"
                );
                let pushlog_reply = PushLogReply {
                    version: reply.version,
                    message_id: reply.message_id,
                    sender_id: reply.sender_id,
                    pubkey: reply.pubkey,
                    signature: reply.signature,
                    err_message: reply.err_message,
                };
                let _ = sender.send(pushlog_reply);
                return Ok(None);
            }

            // No pending channel - this is a DocSyncReply for the coordinator
            tracing::debug!(
                peer_id = %peer_id,
                message_id = %message_id,
                results_count = reply.results.len(),
                "Received DocSync response on two-stream protocol"
            );
            return Ok(Some(TwoStreamEvent::DocSyncReply { peer_id, reply }));
        }

        // Fallback: try PushLogReply in case the message doesn't parse as DocSyncReply
        if let Ok(response) = serde_cbor::from_slice::<PushLogReply>(&buf) {
            let message_id = response.message_id.clone();

            tracing::debug!(
                peer_id = %peer_id,
                message_id = %message_id,
                "Received PushLog response on two-stream protocol (fallback)"
            );

            let sender = {
                let mut pending = self.pending.lock();
                pending.channels.remove(&message_id)
            };

            if let Some(sender) = sender {
                let _ = sender.send(response);
            } else {
                tracing::warn!(
                    peer_id = %peer_id,
                    message_id = %message_id,
                    "Received PushLog response for unknown message ID"
                );
            }

            return Ok(None);
        }

        // None worked - log and return error
        Err(Error::CborDeserialization(
            "failed to deserialize as BranchableSync, DocSync, or PushLog response".to_string(),
        ))
    }

    /// Send a request to a peer and wait for response.
    ///
    /// This opens a stream on the request protocol, sends the request,
    /// then waits for the response to arrive on a separate stream.
    pub async fn send_request(
        &mut self,
        peer_id: PeerId,
        request: PushLogRequest,
    ) -> Result<PushLogReply> {
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

        // Wait for response with timeout
        match timeout(RESPONSE_TIMEOUT, rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => {
                // Channel closed without response
                let mut pending = self.pending.lock();
                pending.channels.remove(&message_id);
                Err(Error::Transport("response channel closed".into()))
            }
            Err(_) => {
                // Timeout
                let mut pending = self.pending.lock();
                pending.channels.remove(&message_id);
                Err(Error::Transport("timeout waiting for response".into()))
            }
        }
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

    /// Create a success reply for a request.
    pub fn success_reply(request: &PushLogRequest) -> PushLogReply {
        PushLogReply::success(&request.metadata.message_id)
    }

    /// Create an error reply for a request.
    pub fn error_reply(request: &PushLogRequest, error: &str) -> PushLogReply {
        PushLogReply::error(&request.metadata.message_id, error)
    }

    /// Send a DocSync request to a peer and wait for response.
    ///
    /// This opens a stream on the request protocol, sends the request,
    /// then waits for the response to arrive on a separate stream.
    pub async fn send_doc_sync_request(
        &mut self,
        peer_id: PeerId,
        request: DocSyncRequest,
    ) -> Result<DocSyncReply> {
        let message_id = request.metadata.message_id.clone();

        // Create response channel
        let (tx, rx) = oneshot::channel();

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
        let message_id = request.metadata.message_id.clone();

        // Open stream and send request
        let mut stream = self
            .control
            .open_stream(peer_id, Self::request_protocol())
            .await
            .map_err(|e| Error::Transport(format!("failed to open stream: {}", e)))?;

        write_message(&mut stream, &request).await.map_err(|e| {
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

    /// Send a BranchableSync request to a peer without waiting for response.
    ///
    /// The response will arrive asynchronously via TwoStreamEvent::BranchableSyncReply.
    pub async fn send_branchable_sync_request_fire_and_forget(
        &mut self,
        peer_id: PeerId,
        request: BranchableSyncRequest,
    ) -> Result<()> {
        let message_id = request.metadata.message_id.clone();

        let mut stream = self
            .control
            .open_stream(peer_id, Self::request_protocol())
            .await
            .map_err(|e| Error::Transport(format!("failed to open stream: {}", e)))?;

        write_message(&mut stream, &request).await.map_err(|e| {
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

/// Runner that accepts incoming streams and emits events.
///
/// This should be spawned as a separate task.
pub struct TwoStreamRunner {
    /// Handler for processing streams.
    handler: Arc<tokio::sync::Mutex<TwoStreamHandler>>,
    /// Incoming request streams.
    request_streams: stream::IncomingStreams,
    /// Incoming response streams.
    response_streams: stream::IncomingStreams,
    /// Channel to send events.
    event_tx: tokio::sync::mpsc::Sender<TwoStreamEvent>,
}

impl TwoStreamRunner {
    /// Create a new runner.
    pub fn new(
        handler: Arc<tokio::sync::Mutex<TwoStreamHandler>>,
        request_streams: stream::IncomingStreams,
        response_streams: stream::IncomingStreams,
        event_tx: tokio::sync::mpsc::Sender<TwoStreamEvent>,
    ) -> Self {
        Self {
            handler,
            request_streams,
            response_streams,
            event_tx,
        }
    }

    /// Run the stream handler loop.
    pub async fn run(mut self) {
        tracing::info!(
            "Two-stream runner started - listening for Go request/response streams on {} and {}",
            TwoStreamHandler::request_protocol(),
            TwoStreamHandler::response_protocol()
        );

        loop {
            tokio::select! {
                // Handle incoming request streams
                Some((peer_id, stream)) = self.request_streams.next() => {
                    tracing::info!(
                        peer_id = %peer_id,
                        "Received incoming stream on request protocol"
                    );
                    let event_tx = self.event_tx.clone();
                    tokio::spawn(async move {
                        match TwoStreamHandler::handle_request_stream(peer_id, stream).await {
                            Ok(event) => {
                                tracing::info!(peer_id = %peer_id, "Sending TwoStreamEvent to host channel");
                                if event_tx.send(event).await.is_err() {
                                    tracing::warn!(
                                        peer_id = %peer_id,
                                        "Failed to send two-stream event - receiver dropped"
                                    );
                                } else {
                                    tracing::info!(peer_id = %peer_id, "Successfully sent TwoStreamEvent to host channel");
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    peer_id = %peer_id,
                                    error = %e,
                                    "Failed to handle request stream"
                                );
                                let _ = event_tx.send(TwoStreamEvent::DecodeError {
                                    peer_id,
                                    error: e.to_string(),
                                }).await;
                            }
                        }
                    });
                }
                // Handle incoming response streams
                Some((peer_id, stream)) = self.response_streams.next() => {
                    eprintln!("[DOCSYNC] TwoStreamRunner received incoming response stream from peer={}", peer_id);
                    tracing::info!(
                        peer_id = %peer_id,
                        "Received incoming stream on response protocol"
                    );
                    let handler = self.handler.clone();
                    let event_tx = self.event_tx.clone();
                    tokio::spawn(async move {
                        let h = handler.lock().await;
                        match h.handle_response_stream(peer_id, stream).await {
                            Ok(Some(event)) => {
                                // DocSyncReply events should be forwarded to the coordinator
                                eprintln!("[DOCSYNC] TwoStreamRunner got DocSyncReply event, sending to host channel");
                                tracing::info!(peer_id = %peer_id, "Sending DocSyncReply event to host channel");
                                if event_tx.send(event).await.is_err() {
                                    eprintln!("[DOCSYNC] TwoStreamRunner failed to send DocSyncReply event - receiver dropped");
                                    tracing::warn!(
                                        peer_id = %peer_id,
                                        "Failed to send DocSyncReply event - receiver dropped"
                                    );
                                } else {
                                    eprintln!("[DOCSYNC] TwoStreamRunner sent DocSyncReply event to host channel");
                                }
                            }
                            Ok(None) => {
                                eprintln!("[DOCSYNC] TwoStreamRunner got PushLogReply (handled internally)");
                                // PushLogReply was handled internally via pending channels
                            }
                            Err(e) => {
                                eprintln!("[DOCSYNC] TwoStreamRunner failed to handle response stream: {}", e);
                                tracing::warn!(
                                    peer_id = %peer_id,
                                    error = %e,
                                    "Failed to handle response stream"
                                );
                            }
                        }
                    });
                }
                else => {
                    tracing::info!("Two-stream runner shutting down");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_ids() {
        assert_eq!(
            TwoStreamHandler::request_protocol().as_ref(),
            "/defradb/rep_req/0.0.1"
        );
        assert_eq!(
            TwoStreamHandler::response_protocol().as_ref(),
            "/defradb/rep_resp/0.0.1"
        );
    }

    #[test]
    fn test_success_reply() {
        let request = PushLogRequest::new(
            "doc123".to_string(),
            vec![1, 2, 3],
            "col123".to_string(),
            "creator".to_string(),
            vec![4, 5, 6],
        );

        let reply = TwoStreamHandler::success_reply(&request);
        // PushLogReply has flat fields (no metadata struct) for CBOR wire compatibility
        assert_eq!(reply.message_id, request.metadata.message_id);
        assert!(reply.err_message.is_none());
    }

    #[test]
    fn test_error_reply() {
        let request = PushLogRequest::new(
            "doc123".to_string(),
            vec![1, 2, 3],
            "col123".to_string(),
            "creator".to_string(),
            vec![4, 5, 6],
        );

        let reply = TwoStreamHandler::error_reply(&request, "test error");
        // PushLogReply has flat fields (no metadata struct) for CBOR wire compatibility
        assert_eq!(reply.message_id, request.metadata.message_id);
        assert_eq!(reply.err_message, Some("test error".to_string()));
    }
}
