//! Adapter to bridge P2PHostHandle to HTTP's P2POperations trait.

use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::{P2POperations, ReplicatorInfo};
use p2p::P2PHostHandle;

/// Adapter that implements P2POperations using P2PHostHandle.
pub struct P2PAdapter {
    handle: P2PHostHandle,
}

impl P2PAdapter {
    /// Create a new adapter wrapping the given P2P handle.
    pub fn new(handle: P2PHostHandle) -> Self {
        Self { handle }
    }

    /// Create an Arc-wrapped adapter.
    pub fn new_arc(handle: P2PHostHandle) -> Arc<dyn P2POperations> {
        Arc::new(Self::new(handle))
    }
}

/// Parse a peer ID and multiaddr from a full multiaddr string.
fn parse_peer_id_from_multiaddr(addr: &str) -> Result<(libp2p::PeerId, libp2p::Multiaddr), String> {
    let multiaddr: libp2p::Multiaddr = addr
        .parse()
        .map_err(|e| format!("invalid multiaddr: {}", e))?;

    let peer_id = multiaddr
        .iter()
        .find_map(|proto| match proto {
            libp2p::multiaddr::Protocol::P2p(peer_id) => Some(peer_id),
            _ => None,
        })
        .ok_or_else(|| "multiaddr must contain /p2p/<peer_id> component".to_string())?;

    Ok((peer_id, multiaddr))
}

#[async_trait]
impl P2POperations for P2PAdapter {
    async fn local_peer_id(&self) -> Result<String, String> {
        self.handle
            .local_peer_id()
            .await
            .map(|id| id.to_string())
            .map_err(|e| e.to_string())
    }

    async fn listen_addresses(&self) -> Result<Vec<String>, String> {
        self.handle
            .listen_addresses()
            .await
            .map(|addrs| addrs.into_iter().map(|a| a.to_string()).collect())
            .map_err(|e| e.to_string())
    }

    async fn connected_peers(&self) -> Result<Vec<String>, String> {
        self.handle
            .connected_peers()
            .await
            .map(|peers| peers.into_iter().map(|p| p.to_string()).collect())
            .map_err(|e| e.to_string())
    }

    async fn connect_peer(&self, addr: &str) -> Result<(), String> {
        // Parse multiaddr and extract peer ID
        let multiaddr: libp2p::Multiaddr = addr
            .parse()
            .map_err(|e| format!("invalid multiaddr: {}", e))?;

        // Extract peer ID from multiaddr (should be in /p2p/<peer_id> component)
        let peer_id = multiaddr
            .iter()
            .find_map(|proto| match proto {
                libp2p::multiaddr::Protocol::P2p(peer_id) => Some(peer_id),
                _ => None,
            })
            .ok_or_else(|| "multiaddr must contain /p2p/<peer_id> component".to_string())?;

        // Remove the p2p component from the address for dialing
        let dial_addr: libp2p::Multiaddr = multiaddr
            .iter()
            .filter(|proto| !matches!(proto, libp2p::multiaddr::Protocol::P2p(_)))
            .collect();

        self.handle
            .dial(peer_id, vec![dial_addr])
            .await
            .map_err(|e| e.to_string())
    }

    async fn get_replicators(&self) -> Result<Vec<ReplicatorInfo>, String> {
        let p2p_infos = self
            .handle
            .get_all_replicators()
            .await
            .map_err(|e| e.to_string())?;

        let http_infos: Vec<ReplicatorInfo> = p2p_infos
            .into_iter()
            .map(|info| {
                let address = info.addresses_str().first().map(|s| s.to_string());
                ReplicatorInfo {
                    id: Some(info.peer_id_str().to_string()),
                    collections: info.collections,
                    address,
                }
            })
            .collect();

        Ok(http_infos)
    }

    async fn add_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
    ) -> Result<(), String> {
        let addr_str = addr.ok_or_else(|| "address is required".to_string())?;
        let (peer_id, _) = parse_peer_id_from_multiaddr(addr_str)?;

        self.handle
            .set_replicator(peer_id, collections)
            .await
            .map_err(|e| e.to_string())
    }

    async fn remove_replicator(
        &self,
        _collections: Vec<String>,
        addr: Option<&str>,
    ) -> Result<(), String> {
        let addr_str = addr.ok_or_else(|| "address is required".to_string())?;
        let (peer_id, _) = parse_peer_id_from_multiaddr(addr_str)?;

        self.handle
            .delete_replicator(peer_id)
            .await
            .map_err(|e| e.to_string())
    }

    async fn get_collections(&self) -> Result<Vec<String>, String> {
        // P2P collections not implemented yet
        Ok(Vec::new())
    }

    async fn add_collections(&self, _collections: Vec<String>) -> Result<(), String> {
        Err("p2p collections functionality not yet implemented".to_string())
    }

    async fn remove_collections(&self, _collections: Vec<String>) -> Result<(), String> {
        Err("p2p collections functionality not yet implemented".to_string())
    }
}
