//! Mapping between iroh `EndpointId` and transport `PeerId`, plus connection tracking.

use std::collections::HashMap;
use std::net::SocketAddr;

use iroh::EndpointId;

use crate::transport::PeerId;

/// Parse a transport `PeerId` string into an iroh `EndpointId`.
pub fn parse_endpoint_id(peer_id: &PeerId) -> crate::error::Result<EndpointId> {
    peer_id
        .as_str()
        .parse::<EndpointId>()
        .map_err(|e| crate::error::Error::InvalidPeerId(e.to_string()))
}

/// Convert an iroh `EndpointId` into a transport `PeerId`.
pub fn endpoint_id_to_peer_id(id: &EndpointId) -> PeerId {
    PeerId::new(id.to_string())
}

/// Connection info for a connected peer.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ConnectionInfo {
    pub endpoint_id: EndpointId,
    pub remote_addr: Option<SocketAddr>,
}

/// Tracks connected peers and their connection info.
#[derive(Debug, Default)]
pub struct PeerMap {
    connections: HashMap<EndpointId, ConnectionInfo>,
}

impl PeerMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, id: EndpointId, info: ConnectionInfo) {
        self.connections.insert(id, info);
    }

    #[allow(dead_code)]
    pub fn remove(&mut self, id: &EndpointId) -> Option<ConnectionInfo> {
        self.connections.remove(id)
    }

    pub fn get(&self, id: &EndpointId) -> Option<&ConnectionInfo> {
        self.connections.get(id)
    }

    pub fn contains(&self, id: &EndpointId) -> bool {
        self.connections.contains_key(id)
    }

    pub fn connected_peers(&self) -> Vec<PeerId> {
        self.connections
            .keys()
            .map(endpoint_id_to_peer_id)
            .collect()
    }

    pub fn peer_addresses(&self) -> Vec<String> {
        self.connections
            .values()
            .filter_map(|info| info.remote_addr.map(|a: SocketAddr| a.to_string()))
            .collect()
    }

    /// Return all connected endpoint IDs.
    pub fn endpoint_ids(&self) -> impl Iterator<Item = EndpointId> + '_ {
        self.connections.keys().copied()
    }
}
