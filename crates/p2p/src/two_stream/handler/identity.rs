use libp2p::PeerId;
use tokio::sync::oneshot;

use crate::codec::write_message;
use crate::error::{Error, Result};
use crate::message::{IdentityRequest, IdentityResponse};

use super::{TwoStreamHandler, RESPONSE_TIMEOUT};

impl TwoStreamHandler {
    pub async fn send_identity_request(
        &mut self,
        peer_id: PeerId,
        request: IdentityRequest,
    ) -> Result<IdentityResponse> {
        let message_id = request.metadata.message_id.clone();
        let (tx, rx) = oneshot::channel();

        {
            let mut pending = self.pending.lock();
            pending.identity_channels.insert(message_id.clone(), tx);
        }

        let mut stream = self
            .control
            .open_stream(peer_id, Self::identity_request_protocol())
            .await
            .map_err(|e| {
                self.cleanup_pending_identity(&message_id);
                Error::Transport(format!("failed to open identity stream: {}", e))
            })?;

        write_message(&mut stream, &request).await.map_err(|e| {
            self.cleanup_pending_identity(&message_id);
            Error::CborSerialization(format!("failed to write identity request: {}", e))
        })?;

        match tokio::time::timeout(RESPONSE_TIMEOUT, rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => {
                self.cleanup_pending_identity(&message_id);
                Err(Error::Transport(
                    "identity response channel closed".to_string(),
                ))
            }
            Err(_) => {
                self.cleanup_pending_identity(&message_id);
                Err(Error::ResponseTimeout)
            }
        }
    }

    pub async fn send_identity_response(
        &mut self,
        peer_id: PeerId,
        response: IdentityResponse,
    ) -> Result<()> {
        let mut stream = self
            .control
            .open_stream(peer_id, Self::identity_response_protocol())
            .await
            .map_err(|e| {
                Error::Transport(format!("failed to open identity response stream: {}", e))
            })?;

        write_message(&mut stream, &response).await.map_err(|e| {
            Error::CborSerialization(format!("failed to write identity response: {}", e))
        })?;

        Ok(())
    }
}
