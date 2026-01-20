//! Peer-to-DID mapping for P2P identity verification.
//!
//! This module provides utilities to convert libp2p PeerIds to DIDs for
//! use in ACP permission checks during P2P sync.
//!
//! # How it works
//!
//! 1. libp2p PeerId is derived from a peer's public key
//! 2. We extract the public key from the PeerId
//! 3. Convert it to a did:key DID using multicodec encoding
//!
//! # Supported Key Types
//!
//! - Ed25519 (most common for libp2p)
//! - secp256k1 (used by some networks)

use crypto::{create_did_key, KeyType};
use identity::Did;
use libp2p::identity::PublicKey;
use libp2p::PeerId;

/// Error type for peer identity conversion.
#[derive(Debug, thiserror::Error)]
pub enum PeerIdentityError {
    /// The peer's key type is not supported for DID conversion.
    #[error("unsupported key type for DID conversion: {0}")]
    UnsupportedKeyType(String),

    /// Failed to extract public key from PeerId.
    #[error("failed to extract public key from PeerId: {0}")]
    KeyExtraction(String),

    /// Failed to create DID from public key.
    #[error("failed to create DID: {0}")]
    DidCreation(String),

    /// Failed to parse DID string.
    #[error("failed to parse DID: {0}")]
    DidParse(#[from] identity::Error),
}

/// Convert a libp2p PeerId to a DID.
///
/// This extracts the public key from the PeerId and encodes it as a did:key DID.
///
/// # Arguments
///
/// * `peer_id` - The libp2p PeerId to convert
///
/// # Returns
///
/// The corresponding DID, or an error if the key type is unsupported.
///
/// # Example
///
/// ```ignore
/// use libp2p::PeerId;
/// use db::peer_identity::peer_id_to_did;
///
/// let peer_id: PeerId = "12D3KooWTest...".parse().unwrap();
/// let did = peer_id_to_did(&peer_id)?;
/// ```
pub fn peer_id_to_did(peer_id: &PeerId) -> Result<Did, PeerIdentityError> {
    // Try to extract the public key from the PeerId
    // Note: This only works for PeerIds that contain an inline public key
    // (which is the case for Ed25519 keys used by most libp2p implementations)
    let public_key = PublicKey::try_decode_protobuf(&peer_id.to_bytes()[2..])
        .map_err(|e| PeerIdentityError::KeyExtraction(e.to_string()))?;

    public_key_to_did(&public_key)
}

/// Convert a libp2p PublicKey to a DID.
///
/// This is useful when you have direct access to the peer's public key
/// (e.g., from connection handshake) rather than just the PeerId.
///
/// # Arguments
///
/// * `public_key` - The libp2p PublicKey to convert
///
/// # Returns
///
/// The corresponding DID, or an error if the key type is unsupported.
pub fn public_key_to_did(public_key: &PublicKey) -> Result<Did, PeerIdentityError> {
    let (key_type, key_bytes) = match public_key.key_type() {
        libp2p::identity::KeyType::Ed25519 => {
            let encoded = public_key.encode_protobuf();
            // Ed25519 protobuf encoding: type (1 byte) + length (1 byte) + key (32 bytes)
            // We need just the raw 32-byte key
            if encoded.len() < 36 {
                return Err(PeerIdentityError::KeyExtraction(
                    "Ed25519 key too short".to_string(),
                ));
            }
            (KeyType::Ed25519, encoded[4..36].to_vec())
        }
        libp2p::identity::KeyType::Secp256k1 => {
            let encoded = public_key.encode_protobuf();
            // secp256k1 protobuf encoding varies, extract the key portion
            // The format is: type varint + data
            if encoded.len() < 35 {
                return Err(PeerIdentityError::KeyExtraction(
                    "secp256k1 key too short".to_string(),
                ));
            }
            // For secp256k1, we need uncompressed format (65 bytes) for DID
            // libp2p uses compressed format (33 bytes), need to expand
            // For now, return error - this needs proper implementation
            return Err(PeerIdentityError::UnsupportedKeyType(
                "secp256k1 DID conversion requires uncompressed key expansion".to_string(),
            ));
        }
        other => {
            return Err(PeerIdentityError::UnsupportedKeyType(format!("{:?}", other)));
        }
    };

    let did_string =
        create_did_key(key_type, &key_bytes).map_err(|e| PeerIdentityError::DidCreation(e.to_string()))?;

    Did::new(did_string).map_err(PeerIdentityError::DidParse)
}

/// Create a peer-to-DID mapping function for use with AcpMergeHandler.
///
/// This returns a closure that can be passed to `AcpMergeHandler::with_peer_to_did()`.
///
/// # Example
///
/// ```ignore
/// use db::peer_identity::create_peer_to_did_mapper;
/// use db::AcpMergeHandler;
///
/// let handler = AcpMergeHandler::new(inner, acp, collections)
///     .with_peer_to_did(create_peer_to_did_mapper());
/// ```
pub fn create_peer_to_did_mapper() -> impl Fn(&str) -> Option<Did> + Send + Sync + 'static {
    move |peer_id_str: &str| {
        let peer_id: PeerId = peer_id_str.parse().ok()?;
        peer_id_to_did(&peer_id).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity::Keypair;

    #[test]
    fn test_ed25519_peer_to_did() {
        // Generate an Ed25519 keypair
        let keypair = Keypair::generate_ed25519();
        let peer_id = PeerId::from_public_key(&keypair.public());

        // Convert to DID
        let did = peer_id_to_did(&peer_id);

        // Should succeed for Ed25519
        assert!(did.is_ok(), "Ed25519 peer should convert to DID: {:?}", did);
        let did = did.unwrap();
        assert!(did.as_str().starts_with("did:key:z"));
    }

    #[test]
    fn test_public_key_to_did_ed25519() {
        let keypair = Keypair::generate_ed25519();
        let public_key = keypair.public();

        let did = public_key_to_did(&public_key);
        assert!(did.is_ok());
        assert!(did.unwrap().as_str().starts_with("did:key:z"));
    }

    #[test]
    fn test_mapper_function() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = PeerId::from_public_key(&keypair.public());
        let peer_id_str = peer_id.to_string();

        let mapper = create_peer_to_did_mapper();
        let did = mapper(&peer_id_str);

        assert!(did.is_some());
        assert!(did.unwrap().as_str().starts_with("did:key:z"));
    }

    #[test]
    fn test_mapper_invalid_peer_id() {
        let mapper = create_peer_to_did_mapper();
        let did = mapper("invalid-peer-id");

        assert!(did.is_none());
    }

    #[test]
    fn test_deterministic_did() {
        // Same peer should always produce same DID
        let keypair = Keypair::generate_ed25519();
        let peer_id = PeerId::from_public_key(&keypair.public());

        let did1 = peer_id_to_did(&peer_id).unwrap();
        let did2 = peer_id_to_did(&peer_id).unwrap();

        assert_eq!(did1, did2);
    }
}
