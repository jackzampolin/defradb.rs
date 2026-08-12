use async_trait::async_trait;
use defra_http::P2PResult;
use p2p::message::DocSyncRequest;

use crate::{P2PError, P2PErrorExt as _};

/// What `sync_documents` needs from a transport in order to reach peers.
///
/// Narrow on purpose: the two transports disagree on peer-id type and on how a
/// request is signed, and neither difference belongs in the sync logic.
///
/// `send_doc_sync_request`'s `Ok` means different things per transport: iroh's
/// send is a request-response with a 30s timeout, so `Ok` means the peer
/// replied; libp2p's send is fire-and-forget, so `Ok` only means the bytes
/// left. This asymmetry is pre-existing and is not normalised here.
#[async_trait]
pub(crate) trait DocSyncDispatch: Send + Sync {
    /// Peer identifier, which differs per transport: iroh uses
    /// `p2p::transport::PeerId`, libp2p uses `libp2p::PeerId`.
    type Peer: Send + Sync;

    /// Whether `Ok` from `send_doc_sync_request` proves the peer answered.
    ///
    /// True on iroh, whose send is a request-response: `Err` there means the
    /// peer stayed silent, which is Go's `pendingPeers` condition. False on
    /// libp2p, whose send is fire-and-forget: a failure means the bytes did
    /// not leave, not that the peer would have declined to reply.
    const SEND_CONFIRMS_REPLY: bool;

    async fn connected_peers(&self) -> P2PResult<Vec<Self::Peer>>;

    /// Signs in place using whatever identity the transport already uses.
    fn sign_request(&self, request: &mut DocSyncRequest) -> P2PResult<()>;

    async fn send_doc_sync_request(
        &self,
        peer: &Self::Peer,
        request: DocSyncRequest,
    ) -> P2PResult<()>;
}

#[cfg(feature = "iroh")]
#[async_trait]
impl DocSyncDispatch for p2p::iroh::IrohTransport {
    type Peer = p2p::transport::PeerId;

    const SEND_CONFIRMS_REPLY: bool = true;

    async fn connected_peers(&self) -> P2PResult<Vec<Self::Peer>> {
        p2p::P2PTransport::connected_peers(self)
            .await
            .map_err(|error| P2PError::transport(format!("failed to get connected peers: {error}")))
    }

    fn sign_request(&self, request: &mut DocSyncRequest) -> P2PResult<()> {
        p2p::signing::sign_with_transport(self, request)
            .map_err(|error| P2PError::internal(format!("failed to sign DocSync request: {error}")))
    }

    async fn send_doc_sync_request(
        &self,
        peer: &Self::Peer,
        request: DocSyncRequest,
    ) -> P2PResult<()> {
        p2p::P2PTransport::send_doc_sync_request(self, peer, request)
            .await
            .map_err(|error| P2PError::transport(error.to_string()))
    }
}

#[cfg(feature = "libp2p")]
#[async_trait]
impl DocSyncDispatch for p2p::P2PHostHandle {
    type Peer = libp2p::PeerId;

    const SEND_CONFIRMS_REPLY: bool = false;

    async fn connected_peers(&self) -> P2PResult<Vec<Self::Peer>> {
        p2p::P2PHostHandle::connected_peers(self)
            .await
            .map_err(|error| P2PError::transport(format!("failed to get connected peers: {error}")))
    }

    fn sign_request(&self, request: &mut DocSyncRequest) -> P2PResult<()> {
        p2p::signing::sign_message(self.keypair(), request)
            .map_err(|error| P2PError::internal(format!("failed to sign DocSync request: {error}")))
    }

    async fn send_doc_sync_request(
        &self,
        peer: &Self::Peer,
        request: DocSyncRequest,
    ) -> P2PResult<()> {
        p2p::P2PHostHandle::send_doc_sync_request(self, *peer, request)
            .await
            .map_err(|error| P2PError::transport(error.to_string()))
    }
}
