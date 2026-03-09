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
pub struct ConnectionInfo {
    pub remote_addr: Option<SocketAddr>,
    pub active_connections: u32,
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

    /// Increment connection count for a peer. Returns `true` if this is the
    /// first connection (0 -> 1), meaning PeerConnected should be emitted.
    pub fn increment_connections(
        &mut self,
        id: EndpointId,
        remote_addr: Option<SocketAddr>,
    ) -> bool {
        if let Some(info) = self.connections.get_mut(&id) {
            info.active_connections += 1;
            if let Some(addr) = remote_addr {
                info.remote_addr = Some(addr);
            }
            false
        } else {
            self.connections.insert(
                id,
                ConnectionInfo {
                    remote_addr,
                    active_connections: 1,
                },
            );
            true
        }
    }

    /// Decrement connection count for a peer. Returns `true` if the count
    /// reached zero (fully disconnected), meaning PeerDisconnected should be emitted.
    pub fn decrement_connections(&mut self, id: &EndpointId) -> bool {
        if let Some(info) = self.connections.get_mut(id) {
            info.active_connections = info.active_connections.saturating_sub(1);
            if info.active_connections == 0 {
                self.connections.remove(id);
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn get(&self, id: &EndpointId) -> Option<&ConnectionInfo> {
        self.connections.get(id)
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
