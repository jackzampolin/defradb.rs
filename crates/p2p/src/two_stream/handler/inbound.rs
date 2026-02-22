//! Inbound stream handling for requests and responses.

use std::sync::Arc;

use libp2p::{PeerId, Stream};
use parking_lot::Mutex;

use crate::error::{Error, Result};
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogReply,
    PushLogRequest,
};
use crate::two_stream::{MAX_MSG_SIZE, STREAM_READ_TIMEOUT};

use super::PendingResponses;
use crate::two_stream::event::TwoStreamEvent;

use super::TwoStreamHandler;

impl TwoStreamHandler {
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

        // Read raw bytes from the stream with a size cap and timeout.
        // The cap prevents OOM from a malicious peer sending unbounded data.
        // The timeout guards against Slowloris-style stream exhaustion.
        let mut buf = Vec::new();
        tokio::time::timeout(
            STREAM_READ_TIMEOUT,
            (&mut stream).take(MAX_MSG_SIZE).read_to_end(&mut buf),
        )
        .await
        .map_err(|_| {
            tracing::warn!(peer_id = %peer_id, "Request stream read timed out");
            Error::CborDeserialization("request stream read timed out".to_string())
        })?
        .map_err(|e| {
            tracing::error!(peer_id = %peer_id, error = %e, "Failed to read stream bytes");
            Error::CborDeserialization(format!("failed to read stream: {}", e))
        })?;

        // Try to deserialize as PushLogRequest first
        if let Ok(request) = serde_cbor::from_slice::<PushLogRequest>(&buf) {
            crate::verify_message(&request)?;
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
            crate::verify_message(&request)?;
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
            crate::verify_message(&request)?;
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
    ///
    /// This is an associated function (no `&self`) so it can be called without
    /// holding the handler lock. Only needs the pending responses Arc.
    pub(crate) async fn handle_response_stream(
        pending: &Arc<Mutex<PendingResponses>>,
        peer_id: PeerId,
        mut stream: Stream,
    ) -> Result<Option<TwoStreamEvent>> {
        use futures::AsyncReadExt;

        // Read raw bytes first to try different message types.
        // Size cap and timeout prevent OOM and Slowloris attacks.
        let mut buf = Vec::new();
        tokio::time::timeout(
            STREAM_READ_TIMEOUT,
            (&mut stream).take(MAX_MSG_SIZE).read_to_end(&mut buf),
        )
        .await
        .map_err(|_| {
            tracing::warn!(peer_id = %peer_id, "Response stream read timed out");
            Error::CborDeserialization("response stream read timed out".to_string())
        })?
        .map_err(|e| Error::CborDeserialization(format!("failed to read response: {}", e)))?;

        tracing::trace!(
            peer_id = %peer_id,
            buf_len = buf.len(),
            "Reading response on two-stream protocol"
        );

        // Try BranchableSyncReply first (has CollectionID + Heads fields).
        // Must come before DocSyncReply since serde_cbor ignores unknown fields.
        match serde_cbor::from_slice::<BranchableSyncReply>(&buf) {
            Ok(reply) if !reply.collection_id.is_empty() => {
                crate::verify_message(&reply)?;
                tracing::debug!(
                    peer_id = %peer_id,
                    message_id = %reply.message_id,
                    collection_id = %reply.collection_id,
                    heads_count = reply.heads.len(),
                    "Received BranchableSync response on two-stream protocol"
                );
                return Ok(Some(TwoStreamEvent::BranchableSyncReply { peer_id, reply }));
            }
            Ok(_) => {
                tracing::trace!(
                    "BranchableSyncReply parsed but collection_id empty, trying other types"
                );
            }
            Err(_) => {
                // Not a BranchableSyncReply, will try other types
            }
        }

        // Deserialize as DocSyncReply since it's a superset of PushLogReply.
        // PushLogReply deserialization would also succeed on DocSyncReply data
        // (serde_cbor ignores unknown fields), which would silently drop the
        // Results field and misroute the message.
        if let Ok(reply) = serde_cbor::from_slice::<DocSyncReply>(&buf) {
            crate::verify_message(&reply)?;
            let message_id = reply.message_id.clone();

            // Check if there's a pending PushLog channel for this message_id.
            // If yes, this is actually a PushLogReply being routed.
            let pending_sender = {
                let mut pending = pending.lock();
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
            crate::verify_message(&response)?;
            let message_id = response.message_id.clone();

            tracing::debug!(
                peer_id = %peer_id,
                message_id = %message_id,
                "Received PushLog response on two-stream protocol (fallback)"
            );

            let sender = {
                let mut pending = pending.lock();
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
}
