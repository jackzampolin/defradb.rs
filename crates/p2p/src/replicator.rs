// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Replicator types for persistent peer replication configuration.
//!
//! A replicator is a peer that is authorized to replicate specific collections.
//! This module defines the types used to persist and manage replicator state.

use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};

/// Information about a replicator peer.
///
/// A replicator is a peer that is authorized to replicate one or more collections.
/// This struct is persisted to the Peerstore and loaded on startup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatorInfo {
    /// The peer ID of the replicator.
    pub peer_id: String,

    /// Collections this peer is authorized to replicate.
    pub collections: Vec<String>,

    /// Known addresses for this peer.
    #[serde(default)]
    pub addresses: Vec<String>,
}

impl ReplicatorInfo {
    /// Create a new replicator info.
    pub fn new(peer_id: PeerId, collections: Vec<String>) -> Self {
        Self {
            peer_id: peer_id.to_string(),
            collections,
            addresses: Vec::new(),
        }
    }

    /// Create a new replicator info with addresses.
    pub fn with_addresses(
        peer_id: PeerId,
        collections: Vec<String>,
        addresses: Vec<Multiaddr>,
    ) -> Self {
        Self {
            peer_id: peer_id.to_string(),
            collections,
            addresses: addresses.into_iter().map(|a| a.to_string()).collect(),
        }
    }

    /// Get the peer ID.
    ///
    /// Returns None if the stored peer_id is invalid.
    pub fn peer_id(&self) -> Option<PeerId> {
        self.peer_id.parse().ok()
    }

    /// Get the addresses as Multiaddr.
    ///
    /// Invalid addresses are filtered out.
    pub fn addresses(&self) -> Vec<Multiaddr> {
        self.addresses
            .iter()
            .filter_map(|a| a.parse().ok())
            .collect()
    }

    /// Serialize to CBOR bytes for storage.
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_cbor::Error> {
        serde_cbor::to_vec(self)
    }

    /// Deserialize from CBOR bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_cbor::Error> {
        serde_cbor::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replicator_info_new() {
        let peer_id = PeerId::random();
        let collections = vec!["users".to_string(), "posts".to_string()];

        let info = ReplicatorInfo::new(peer_id, collections.clone());

        assert_eq!(info.peer_id(), Some(peer_id));
        assert_eq!(info.collections, collections);
        assert!(info.addresses.is_empty());
    }

    #[test]
    fn test_replicator_info_with_addresses() {
        let peer_id = PeerId::random();
        let collections = vec!["users".to_string()];
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();

        let info = ReplicatorInfo::with_addresses(peer_id, collections.clone(), vec![addr.clone()]);

        assert_eq!(info.peer_id(), Some(peer_id));
        assert_eq!(info.collections, collections);
        assert_eq!(info.addresses(), vec![addr]);
    }

    #[test]
    fn test_replicator_info_serialization() {
        let peer_id = PeerId::random();
        let collections = vec!["users".to_string()];
        let info = ReplicatorInfo::new(peer_id, collections);

        let bytes = info.to_bytes().unwrap();
        let restored = ReplicatorInfo::from_bytes(&bytes).unwrap();

        assert_eq!(info, restored);
    }

    #[test]
    fn test_replicator_info_invalid_peer_id() {
        let info = ReplicatorInfo {
            peer_id: "invalid".to_string(),
            collections: vec![],
            addresses: vec![],
        };

        assert!(info.peer_id().is_none());
    }

    #[test]
    fn test_replicator_info_invalid_address_filtered() {
        let peer_id = PeerId::random();
        let info = ReplicatorInfo {
            peer_id: peer_id.to_string(),
            collections: vec![],
            addresses: vec![
                "/ip4/127.0.0.1/tcp/4001".to_string(),
                "invalid-address".to_string(),
            ],
        };

        let addrs = info.addresses();
        assert_eq!(addrs.len(), 1);
    }
}
