//! Inbound stream handling for requests and responses.

use std::sync::Arc;

use libp2p::{PeerId, Stream};
use parking_lot::Mutex;

use super::PendingResponses;
use crate::error::{Error, Result};
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, Message,
    PushLogReply, PushLogRequest,
};
use crate::two_stream::event::TwoStreamEvent;

use super::TwoStreamHandler;

fn ensure_transport_sender<M: Message>(peer_id: &PeerId, msg: &M) -> Result<()> {
    if msg.sender_id() == peer_id.to_string() {
        Ok(())
    } else {
        Err(Error::Transport(format!(
            "transport peer {} did not match signed sender {}",
            peer_id,
            msg.sender_id()
        )))
    }
}

impl TwoStreamHandler {
    /// Handle an incoming stream on the request protocol.
    ///
    /// Reads the request and returns an event for processing.
    /// Supports both PushLogRequest and DocSyncRequest message types.
    pub async fn handle_request_stream(
        peer_id: PeerId,
        mut stream: Stream,
        max_msg_size: u64,
        stream_read_timeout: std::time::Duration,
    ) -> Result<TwoStreamEvent> {
        use futures::AsyncReadExt;

        tracing::info!(peer_id = %peer_id, "Reading message from two-stream request");

        let mut buf = Vec::new();
        tokio::time::timeout(
            stream_read_timeout,
            (&mut stream).take(max_msg_size).read_to_end(&mut buf),
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
            ensure_transport_sender(&peer_id, &request)?;
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
            ensure_transport_sender(&peer_id, &request)?;
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
            ensure_transport_sender(&peer_id, &request)?;
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
        max_msg_size: u64,
        stream_read_timeout: std::time::Duration,
    ) -> Result<Option<TwoStreamEvent>> {
        use futures::AsyncReadExt;

        let mut buf = Vec::new();
        tokio::time::timeout(
            stream_read_timeout,
            (&mut stream).take(max_msg_size).read_to_end(&mut buf),
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

        // Route pending PushLog replies before trying DocSyncReply.
        // A PushLogReply will also deserialize as DocSyncReply (with default
        // empty Results), but verifying the signature against the DocSyncReply
        // shape changes the serialized bytes and fails validation.
        if let Ok(response) = serde_cbor::from_slice::<PushLogReply>(&buf) {
            let message_id = response.message_id.clone();
            let is_pending_pushlog = {
                let pending = pending.lock();
                pending.channels.contains_key(&message_id)
            };

            if is_pending_pushlog {
                crate::verify_message(&response)?;

                tracing::debug!(
                    peer_id = %peer_id,
                    message_id = %message_id,
                    "Received PushLog response on two-stream protocol"
                );

                let sender = {
                    let mut pending = pending.lock();
                    pending.channels.remove(&message_id)
                };

                if let Some(sender) = sender {
                    let _ = sender.send(response);
                }

                return Ok(None);
            }
        }

        // Deserialize as DocSyncReply once we've ruled out a pending PushLogReply.
        if let Ok(reply) = serde_cbor::from_slice::<DocSyncReply>(&buf) {
            crate::verify_message(&reply)?;
            let message_id = reply.message_id.clone();

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
