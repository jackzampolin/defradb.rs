//! CAR stream protocol handler methods.

use cid::Cid;
use libp2p::PeerId;
use libp2p::Stream;

use crate::error::{Error, Result};
use crate::two_stream::event::TwoStreamEvent;

use super::TwoStreamHandler;

async fn read_stream(stream: &mut Stream) -> std::io::Result<Vec<u8>> {
    use futures::AsyncReadExt;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    Ok(buf)
}

impl TwoStreamHandler {
    /// Handle an incoming CAR request stream.
    ///
    /// The request is raw CID bytes (self-describing, no CBOR wrapper).
    pub async fn handle_car_request_stream(
        peer_id: PeerId,
        mut stream: Stream,
    ) -> Result<TwoStreamEvent> {
        let buf = read_stream(&mut stream)
            .await
            .map_err(|e| Error::Transport(format!("failed to read CAR request: {}", e)))?;

        let root_cid = Cid::read_bytes(std::io::Cursor::new(&buf))
            .map_err(|e| Error::InvalidCid(format!("invalid CID in CAR request: {}", e)))?;

        Ok(TwoStreamEvent::CarFetchRequest { peer_id, root_cid })
    }

    /// Handle an incoming CAR response stream.
    ///
    /// The response is raw CARv1 bytes. The first root CID from the header
    /// is extracted to identify which request this responds to.
    pub async fn handle_car_response_stream(
        peer_id: PeerId,
        mut stream: Stream,
    ) -> Result<TwoStreamEvent> {
        let car_data = read_stream(&mut stream)
            .await
            .map_err(|e| Error::Transport(format!("failed to read CAR response: {}", e)))?;

        // Extract the root CID from the CAR header for event routing
        let (roots, _) = crate::sync::car::decode_car(&car_data)?;
        let root_cid = roots
            .into_iter()
            .next()
            .ok_or_else(|| Error::Codec("CAR response has no roots".into()))?;

        Ok(TwoStreamEvent::CarFetchResponse {
            peer_id,
            root_cid,
            car_data,
        })
    }

    /// Send a CAR request to a peer (raw CID bytes on the CAR request protocol).
    pub async fn send_car_request(&mut self, peer_id: PeerId, root_cid: Cid) -> Result<()> {
        use futures::AsyncWriteExt;

        let mut stream = self
            .control
            .open_stream(peer_id, Self::car_request_protocol())
            .await
            .map_err(|e| Error::Transport(format!("failed to open CAR request stream: {}", e)))?;

        let cid_bytes = root_cid.to_bytes();
        stream
            .write_all(&cid_bytes)
            .await
            .map_err(|e| Error::Transport(format!("failed to write CAR request: {}", e)))?;
        stream
            .close()
            .await
            .map_err(|e| Error::Transport(format!("failed to close CAR request stream: {}", e)))?;

        tracing::debug!(
            peer_id = %peer_id,
            root_cid = %root_cid,
            "Sent CAR request"
        );

        Ok(())
    }

    /// Send a CAR response to a peer (raw CARv1 bytes on the CAR response protocol).
    pub async fn send_car_response(&mut self, peer_id: PeerId, car_data: Vec<u8>) -> Result<()> {
        use futures::AsyncWriteExt;

        let mut stream = self
            .control
            .open_stream(peer_id, Self::car_response_protocol())
            .await
            .map_err(|e| Error::Transport(format!("failed to open CAR response stream: {}", e)))?;

        stream
            .write_all(&car_data)
            .await
            .map_err(|e| Error::Transport(format!("failed to write CAR response: {}", e)))?;
        stream
            .close()
            .await
            .map_err(|e| Error::Transport(format!("failed to close CAR response stream: {}", e)))?;

        tracing::debug!(
            peer_id = %peer_id,
            car_bytes = car_data.len(),
            "Sent CAR response"
        );

        Ok(())
    }
}
