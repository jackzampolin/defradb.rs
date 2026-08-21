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
//! - secp256k1 (used by some networks, common with Go peers)
//!
//! Both key types have short public keys that libp2p inlines into the PeerId
//! under the 42-byte identity-hash threshold, so `peer_id_to_did` can recover
//! the key material from the PeerId alone.

use crypto::keys::PublicKey as _;
use crypto::{create_did_key, KeyType, Secp256k1PublicKey};
use identity::Did;
use libp2p::identity::PublicKey;
use libp2p::PeerId;

/// Error type for peer identity conversion.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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
/// use crate::peer_identity::peer_id_to_did;
///
/// let peer_id: PeerId = "12D3KooWTest...".parse().unwrap();
/// let did = peer_id_to_did(&peer_id)?;
/// ```
pub fn peer_id_to_did(peer_id: &PeerId) -> Result<Did, PeerIdentityError> {
    // Works for PeerIds that inline their public key. Ed25519 (36-byte
    // protobuf) and secp256k1 (37-byte protobuf) both fit under libp2p's
    // 42-byte identity-hash threshold, so inline decoding is the common
    // case for both supported key types.
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
    let did_string = match public_key.key_type() {
        libp2p::identity::KeyType::Ed25519 => {
            let encoded = public_key.encode_protobuf();
            // libp2p protobuf PublicKey for Ed25519:
            //   0x08 0x01       (field 1, KeyType = Ed25519)
            //   0x12 0x20       (field 2, Data length 32)
            //   <32 raw bytes>
            if encoded.len() < 36 {
                return Err(PeerIdentityError::KeyExtraction(
                    "Ed25519 key too short".to_string(),
                ));
            }
            create_did_key(KeyType::Ed25519, &encoded[4..36])
                .map_err(|e| PeerIdentityError::DidCreation(e.to_string()))?
        }
        libp2p::identity::KeyType::Secp256k1 => {
            // Go's defradb/crypto/keys.go:276-282 derives the secp256k1 DID
            // from the uncompressed SEC1 public key (65 bytes, 0x04 prefix).
            // libp2p hands us the compressed 33-byte form via protobuf, so
            // route through crypto::Secp256k1PublicKey which decompresses
            // the point and reuses the same createDIDKey encoding Go uses.
            let encoded = public_key.encode_protobuf();
            // libp2p protobuf PublicKey for secp256k1:
            //   0x08 0x02       (field 1, KeyType = Secp256k1)
            //   0x12 0x21       (field 2, Data length 33)
            //   <33 compressed SEC1 bytes>
            if encoded.len() < 37 {
                return Err(PeerIdentityError::KeyExtraction(
                    "secp256k1 key too short".to_string(),
                ));
            }
            let compressed = &encoded[4..37];
            let crypto_key = Secp256k1PublicKey::from_bytes(compressed)
                .map_err(|e| PeerIdentityError::KeyExtraction(e.to_string()))?;
            crypto_key
                .did()
                .map_err(|e| PeerIdentityError::DidCreation(e.to_string()))?
        }
        other => {
            return Err(PeerIdentityError::UnsupportedKeyType(format!(
                "{:?}",
                other
            )));
        }
    };

    Did::new(did_string).map_err(PeerIdentityError::DidParse)
}

/// Create a peer-to-DID mapping function for use with AcpMergeHandler.
///
/// This returns a closure that can be passed to `AcpMergeHandler::with_peer_to_did()`.
///
/// # Example
///
/// ```ignore
/// use crate::peer_identity::create_peer_to_did_mapper;
/// use crate::AcpMergeHandler;
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
