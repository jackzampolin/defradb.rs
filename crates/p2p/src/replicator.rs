//! Replicator types for persistent peer replication configuration.
//!
//! A replicator is a peer that is authorized to replicate specific collections.
//! This module defines the types used to persist and manage replicator state.

use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Error type for replicator operations.
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum ReplicatorError {
    /// The peer ID string is invalid.
    #[error("invalid peer ID: {0}")]
    InvalidPeerId(String),
    /// No collections specified.
    #[error("collections cannot be empty")]
    EmptyCollections,
}

/// Information about a replicator peer.
///
/// A replicator is a peer that is authorized to replicate one or more collections.
/// This struct is persisted to the Peerstore and loaded on startup.
///
/// # Field Privacy
///
/// Fields are public for serde deserialization from storage, but prefer using
/// the constructor methods which provide validation. Access fields through
/// the getter methods which handle parsing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatorInfo {
    /// The peer ID of the replicator (stored as string for serialization).
    #[serde(rename = "peer_id")]
    peer_id_str: String,

    /// Collections this peer is authorized to replicate.
    pub collections: Vec<String>,

    /// Known addresses for this peer.
    #[serde(default)]
    addresses_str: Vec<String>,
}

impl ReplicatorInfo {
    /// Create a new replicator info.
    ///
    /// This constructor accepts a validated PeerId, ensuring the peer ID is valid.
    pub fn new(peer_id: PeerId, collections: Vec<String>) -> Self {
        Self {
            peer_id_str: peer_id.to_string(),
            collections,
            addresses_str: Vec::new(),
        }
    }

    /// Create a new replicator info with validation.
    ///
    /// Returns an error if collections is empty. Use this when you want
    /// to enforce that replicators must have at least one collection.
    pub fn try_new(peer_id: PeerId, collections: Vec<String>) -> Result<Self, ReplicatorError> {
        if collections.is_empty() {
            return Err(ReplicatorError::EmptyCollections);
        }
        Ok(Self::new(peer_id, collections))
    }

    /// Create from raw strings (for deserialization or testing).
    ///
    /// This is useful when loading from storage where the peer ID might be invalid.
    /// Use `peer_id()` to check validity after construction.
    pub fn from_raw(peer_id: String, collections: Vec<String>, addresses: Vec<String>) -> Self {
        Self {
            peer_id_str: peer_id,
            collections,
            addresses_str: addresses,
        }
    }

    /// Get the peer ID.
    ///
    /// Returns None if the stored peer_id is invalid.
    pub fn peer_id(&self) -> Option<PeerId> {
        self.peer_id_str.parse().ok()
    }

    /// Get the peer ID string (raw, possibly invalid).
    pub fn peer_id_str(&self) -> &str {
        &self.peer_id_str
    }

    /// Try to get the peer ID, returning an error if invalid.
    ///
    /// Use this when you need to distinguish between "missing" and "invalid".
    pub fn try_peer_id(&self) -> Result<PeerId, ReplicatorError> {
        self.peer_id_str
            .parse()
            .map_err(|_| ReplicatorError::InvalidPeerId(self.peer_id_str.clone()))
    }

    /// Get the addresses as Multiaddr.
    ///
    /// Invalid addresses are filtered out.
    pub fn addresses(&self) -> Vec<Multiaddr> {
        self.addresses_str
            .iter()
            .filter_map(|a| a.parse().ok())
            .collect()
    }

    /// Get the raw address strings.
    pub fn addresses_str(&self) -> &[String] {
        &self.addresses_str
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
