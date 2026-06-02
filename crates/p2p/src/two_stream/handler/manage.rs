//! Management channel request and response methods (mutate + query).

use libp2p::{PeerId, Stream};

use crate::codec::write_message;
use crate::error::{Error, Result};
use crate::message::{ManageQueryReply, ManageQueryRequest, ManageReply, ManageRequest};
use crate::two_stream::event::TwoStreamEvent;

use super::{ensure_transport_sender, TwoStreamHandler};

// Methods are wired into the inbound dispatcher in Task 3.3.
#[allow(dead_code)]
impl TwoStreamHandler {
    /// Send a management mutate request to a peer without waiting for response.
    ///
    /// The response arrives asynchronously via [`TwoStreamEvent::ManageReply`].
    pub async fn send_manage_request_fire_and_forget(
        &mut self,
        peer_id: PeerId,
        request: crate::message::ManageRequest,
    ) -> Result<()> {
        let message_id = request.message_id.clone();

        let mut stream = self
            .control
            .open_stream(peer_id, Self::manage_request_protocol())
            .await
            .map_err(|e| Error::Transport(format!("failed to open manage request stream: {e}")))?;

        write_message(&mut stream, &request).await.map_err(|e| {
            Error::CborSerialization(format!("failed to write manage request: {e}"))
        })?;

        tracing::info!(
            peer_id = %peer_id,
            message_id = %message_id,
            "Sent ManageRequest on manage protocol"
        );

        Ok(())
    }

    /// Send a management mutate response to a peer.
    pub async fn send_manage_response(
        &mut self,
        peer_id: PeerId,
        reply: crate::message::ManageReply,
    ) -> Result<()> {
        let message_id = reply.message_id.clone();

        let mut stream = self
            .control
            .open_stream(peer_id, Self::manage_response_protocol())
            .await
            .map_err(|e| {
                Error::Transport(format!("failed to open manage response stream: {e}"))
            })?;

        write_message(&mut stream, &reply).await.map_err(|e| {
            Error::CborSerialization(format!("failed to write manage response: {e}"))
        })?;

        tracing::info!(
            peer_id = %peer_id,
            message_id = %message_id,
            "Sent ManageReply on manage protocol"
        );

        Ok(())
    }

    /// Handle an incoming management mutate request stream.
    pub async fn handle_manage_request_stream(
        peer_id: PeerId,
        stream: Stream,
        max_msg_size: u64,
        stream_read_timeout: std::time::Duration,
    ) -> Result<TwoStreamEvent> {
        let request = super::read_cbor_message::<ManageRequest>(
            peer_id,
            stream,
            max_msg_size,
            stream_read_timeout,
            "manage request",
        )
        .await?;

        crate::verify_message(&request)?;
        ensure_transport_sender(&peer_id, &request)?;

        tracing::info!(
            peer_id = %peer_id,
            message_id = %request.message_id,
            "Received ManageRequest on manage protocol"
        );

        Ok(TwoStreamEvent::ManageRequest { peer_id, request })
    }

    /// Handle an incoming management mutate response stream.
    pub async fn handle_manage_response_stream(
        peer_id: PeerId,
        stream: Stream,
        max_msg_size: u64,
        stream_read_timeout: std::time::Duration,
    ) -> Result<TwoStreamEvent> {
        let reply = super::read_cbor_message::<ManageReply>(
            peer_id,
            stream,
            max_msg_size,
            stream_read_timeout,
            "manage response",
        )
        .await?;

        crate::verify_message(&reply)?;
        ensure_transport_sender(&peer_id, &reply)?;

        tracing::debug!(
            peer_id = %peer_id,
            message_id = %reply.message_id,
            "Received ManageReply on manage protocol"
        );

        Ok(TwoStreamEvent::ManageReply { peer_id, reply })
    }

    /// Send a management query request to a peer without waiting for response.
    ///
    /// The response arrives asynchronously via [`TwoStreamEvent::ManageQueryReply`].
    pub async fn send_manage_query_request_fire_and_forget(
        &mut self,
        peer_id: PeerId,
        request: crate::message::ManageQueryRequest,
    ) -> Result<()> {
        let message_id = request.message_id.clone();

        let mut stream = self
            .control
            .open_stream(peer_id, Self::manage_query_request_protocol())
            .await
            .map_err(|e| {
                Error::Transport(format!("failed to open manage query request stream: {e}"))
            })?;

        write_message(&mut stream, &request).await.map_err(|e| {
            Error::CborSerialization(format!("failed to write manage query request: {e}"))
        })?;

        tracing::info!(
            peer_id = %peer_id,
            message_id = %message_id,
            "Sent ManageQueryRequest on manage query protocol"
        );

        Ok(())
    }

    /// Send a management query response to a peer.
    pub async fn send_manage_query_response(
        &mut self,
        peer_id: PeerId,
        reply: crate::message::ManageQueryReply,
    ) -> Result<()> {
        let message_id = reply.message_id.clone();

        let mut stream = self
            .control
            .open_stream(peer_id, Self::manage_query_response_protocol())
            .await
            .map_err(|e| {
                Error::Transport(format!("failed to open manage query response stream: {e}"))
            })?;

        write_message(&mut stream, &reply).await.map_err(|e| {
            Error::CborSerialization(format!("failed to write manage query response: {e}"))
        })?;

        tracing::info!(
            peer_id = %peer_id,
            message_id = %message_id,
            "Sent ManageQueryReply on manage query protocol"
        );

        Ok(())
    }

    /// Handle an incoming management query request stream.
    pub async fn handle_manage_query_request_stream(
        peer_id: PeerId,
        stream: Stream,
        max_msg_size: u64,
        stream_read_timeout: std::time::Duration,
    ) -> Result<TwoStreamEvent> {
        let request = super::read_cbor_message::<ManageQueryRequest>(
            peer_id,
            stream,
            max_msg_size,
            stream_read_timeout,
            "manage query request",
        )
        .await?;

        crate::verify_message(&request)?;
        ensure_transport_sender(&peer_id, &request)?;

        tracing::info!(
            peer_id = %peer_id,
            message_id = %request.message_id,
            "Received ManageQueryRequest on manage query protocol"
        );

        Ok(TwoStreamEvent::ManageQueryRequest { peer_id, request })
    }

    /// Handle an incoming management query response stream.
    pub async fn handle_manage_query_response_stream(
        peer_id: PeerId,
        stream: Stream,
        max_msg_size: u64,
        stream_read_timeout: std::time::Duration,
    ) -> Result<TwoStreamEvent> {
        let reply = super::read_cbor_message::<ManageQueryReply>(
            peer_id,
            stream,
            max_msg_size,
            stream_read_timeout,
            "manage query response",
        )
        .await?;

        crate::verify_message(&reply)?;
        ensure_transport_sender(&peer_id, &reply)?;

        tracing::debug!(
            peer_id = %peer_id,
            message_id = %reply.message_id,
            "Received ManageQueryReply on manage query protocol"
        );

        Ok(TwoStreamEvent::ManageQueryReply { peer_id, reply })
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn manage_request_decodes() {
        use crate::message::{ManageMutateOp, ManageRequest};
        let req = ManageRequest::new(ManageMutateOp::CollectionAdd { collection_ids: vec!["c1".into()] }, b"t".to_vec());
        let back: ManageRequest = serde_cbor::from_slice(&serde_cbor::to_vec(&req).unwrap()).unwrap();
        assert!(matches!(back.op, ManageMutateOp::CollectionAdd { .. }));
    }
}
