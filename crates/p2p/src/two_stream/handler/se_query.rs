//! Searchable Encryption query request and response methods.

use futures::AsyncReadExt;
use libp2p::{PeerId, Stream};

use crate::codec::write_message;
use crate::error::{Error, Result};
use crate::message::{QuerySEArtifactsReply, QuerySEArtifactsRequest};
use crate::two_stream::event::TwoStreamEvent;

use super::{ensure_transport_sender, TwoStreamHandler};

impl TwoStreamHandler {
    /// Send an SE query request to a peer without waiting for response.
    ///
    /// The response arrives asynchronously via [`TwoStreamEvent::SEQueryReply`].
    pub async fn send_se_query_request_fire_and_forget(
        &mut self,
        peer_id: PeerId,
        request: QuerySEArtifactsRequest,
    ) -> Result<()> {
        let message_id = request.message_id.clone();
        let query_count = request.queries.len();

        let mut stream = self
            .control
            .open_stream(peer_id, Self::se_query_request_protocol())
            .await
            .map_err(|e| Error::Transport(format!("failed to open SE query stream: {e}")))?;

        write_message(&mut stream, &request).await.map_err(|e| {
            Error::CborSerialization(format!("failed to write SE query request: {e}"))
        })?;

        tracing::info!(
            peer_id = %peer_id,
            message_id = %message_id,
            collection_id = %request.collection_id,
            query_count = query_count,
            "Sent QuerySEArtifacts request on SE query protocol"
        );

        Ok(())
    }

    /// Send an SE query response to a peer.
    pub async fn send_se_query_response(
        &mut self,
        peer_id: PeerId,
        reply: QuerySEArtifactsReply,
    ) -> Result<()> {
        let message_id = reply.message_id.clone();
        let doc_count = reply.doc_ids.len();

        let mut stream = self
            .control
            .open_stream(peer_id, Self::se_query_response_protocol())
            .await
            .map_err(|e| {
                Error::Transport(format!("failed to open SE query response stream: {e}"))
            })?;

        write_message(&mut stream, &reply).await.map_err(|e| {
            Error::CborSerialization(format!("failed to write SE query response: {e}"))
        })?;

        tracing::info!(
            peer_id = %peer_id,
            message_id = %message_id,
            doc_count = doc_count,
            "Sent QuerySEArtifacts response on SE query protocol"
        );

        Ok(())
    }

    /// Handle an incoming SE query request stream.
    pub async fn handle_se_query_request_stream(
        peer_id: PeerId,
        stream: Stream,
        max_msg_size: u64,
        stream_read_timeout: std::time::Duration,
    ) -> Result<TwoStreamEvent> {
        let request = read_cbor_message::<QuerySEArtifactsRequest>(
            peer_id,
            stream,
            max_msg_size,
            stream_read_timeout,
            "SE query request",
        )
        .await?;

        crate::verify_message(&request)?;
        ensure_transport_sender(&peer_id, &request)?;

        tracing::info!(
            peer_id = %peer_id,
            message_id = %request.message_id,
            collection_id = %request.collection_id,
            query_count = request.queries.len(),
            "Received QuerySEArtifacts request on SE query protocol"
        );

        Ok(TwoStreamEvent::SEQueryRequest { peer_id, request })
    }

    /// Handle an incoming SE query response stream.
    pub async fn handle_se_query_response_stream(
        peer_id: PeerId,
        stream: Stream,
        max_msg_size: u64,
        stream_read_timeout: std::time::Duration,
    ) -> Result<TwoStreamEvent> {
        let reply = read_cbor_message::<QuerySEArtifactsReply>(
            peer_id,
            stream,
            max_msg_size,
            stream_read_timeout,
            "SE query response",
        )
        .await?;

        crate::verify_message(&reply)?;
        ensure_transport_sender(&peer_id, &reply)?;

        tracing::debug!(
            peer_id = %peer_id,
            message_id = %reply.message_id,
            doc_count = reply.doc_ids.len(),
            "Received QuerySEArtifacts response on SE query protocol"
        );

        Ok(TwoStreamEvent::SEQueryReply { peer_id, reply })
    }
}

async fn read_cbor_message<T>(
    peer_id: PeerId,
    mut stream: Stream,
    max_msg_size: u64,
    stream_read_timeout: std::time::Duration,
    label: &'static str,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let mut buf = Vec::new();
    tokio::time::timeout(
        stream_read_timeout,
        (&mut stream).take(max_msg_size).read_to_end(&mut buf),
    )
    .await
    .map_err(|_| {
        tracing::warn!(peer_id = %peer_id, "SE query stream read timed out");
        Error::CborDeserialization(format!("{label} stream read timed out"))
    })?
    .map_err(|e| Error::CborDeserialization(format!("failed to read {label}: {e}")))?;

    serde_cbor::from_slice(&buf)
        .map_err(|e| Error::CborDeserialization(format!("failed to decode {label}: {e}")))
}
