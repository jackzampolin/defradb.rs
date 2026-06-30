//! Peer ACP identity resolution for serve-boundary checks.

use async_trait::async_trait;

use crate::transport::PeerId;

#[async_trait]
pub trait PeerIdentityResolver: Send + Sync {
    async fn resolve(&self, peer_id: &PeerId) -> Option<identity::Did>;
}

#[cfg(feature = "libp2p-transport")]
#[derive(Clone)]
pub struct HandlePeerIdentityResolver {
    handle: crate::P2PHostHandle,
}

#[cfg(feature = "libp2p-transport")]
impl HandlePeerIdentityResolver {
    pub fn new(handle: crate::P2PHostHandle) -> Self {
        Self { handle }
    }
}

#[cfg(feature = "libp2p-transport")]
#[async_trait]
impl PeerIdentityResolver for HandlePeerIdentityResolver {
    async fn resolve(&self, peer_id: &PeerId) -> Option<identity::Did> {
        let peer_id = peer_id.as_str().parse::<libp2p::PeerId>().ok()?;
        self.handle.get_peer_identity(peer_id).await.ok().flatten()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AnonymousResolver;

#[async_trait]
impl PeerIdentityResolver for AnonymousResolver {
    async fn resolve(&self, _peer_id: &PeerId) -> Option<identity::Did> {
        None
    }
}
