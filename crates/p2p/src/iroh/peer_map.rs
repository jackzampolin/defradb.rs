//! Mapping between iroh `EndpointId` and transport `PeerId`, plus connection tracking.

use std::collections::HashMap;
use std::net::SocketAddr;

use iroh::endpoint::Connection;
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
    /// A handle to the underlying QUIC connection, used to hang up on
    /// `disconnect`. iroh `Connection` clones share the same QUIC connection,
    /// so closing this handle closes the connection for the peer.
    connection: Option<Connection>,
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
    ///
    /// `connection` is a handle to the underlying QUIC connection, retained so
    /// `disconnect` can hang up on the peer.
    pub fn increment_connections(
        &mut self,
        id: EndpointId,
        remote_addr: Option<SocketAddr>,
        connection: Connection,
    ) -> bool {
        if let Some(info) = self.connections.get_mut(&id) {
            info.active_connections += 1;
            if let Some(addr) = remote_addr {
                info.remote_addr = Some(addr);
            }
            info.connection = Some(connection);
            false
        } else {
            self.connections.insert(
                id,
                ConnectionInfo {
                    remote_addr,
                    active_connections: 1,
                    connection: Some(connection),
                },
            );
            true
        }
    }

    /// Take the retained connection handle for a peer without removing the
    /// connection-count entry. Returns `None` if the peer is unknown or no
    /// handle was retained. Used by `disconnect` to close the live connection;
    /// the count entry is then cleared by the stream task on `accept_bi` error.
    pub fn take_connection(&mut self, id: &EndpointId) -> Option<Connection> {
        self.connections
            .get_mut(id)
            .and_then(|info| info.connection.take())
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
