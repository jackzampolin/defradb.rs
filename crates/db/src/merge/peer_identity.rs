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

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity::{secp256k1, Keypair};

    // Known secp256k1 fixture that Go's `crypto.NewPublicKey(...).DID()`
    // produces for the same 32-byte private key. Mirrors the constants in
    // crates/crypto/tests/go_compat_keys.rs so this test exercises the
    // full extraction pipeline (libp2p protobuf → compressed 33 bytes →
    // crypto::Secp256k1PublicKey → uncompressed 65 bytes → DID).
    const GO_SECP256K1_PRIVATE_KEY: [u8; 32] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f, 0x20,
    ];
    const GO_SECP256K1_DID: &str =
        "did:key:z7r8or8ecagY9LD87s54K2arcXmgmw6bUhyvq83RrnB2hJiUb2ug5YGAk1ZUaimewnoLL1ZGzXuTCnWRSrRZgR3v2PLPH";

    fn libp2p_keypair_from_go_fixture() -> Keypair {
        let mut sk_bytes = GO_SECP256K1_PRIVATE_KEY;
        let sk = secp256k1::SecretKey::try_from_bytes(&mut sk_bytes).unwrap();
        secp256k1::Keypair::from(sk).into()
    }

    // Go's defradb/crypto/keys.go:276-282 derives the secp256k1 DID from
    // the uncompressed SEC1 public key. `crypto::Secp256k1PublicKey::did`
    // already does the equivalent. Peer-identity conversion must produce
    // the same DID starting from libp2p's compressed key so Rust and Go
    // agree on the DID for the same peer.
    #[test]
    fn test_secp256k1_peer_to_did_matches_crypto_did() {
        let libp2p_keypair = Keypair::generate_secp256k1();
        let libp2p_pk = libp2p_keypair.public();
        let peer_id = PeerId::from_public_key(&libp2p_pk);

        let did_from_peer_id =
            peer_id_to_did(&peer_id).expect("secp256k1 peer_id must convert to did");
        let did_from_public_key =
            public_key_to_did(&libp2p_pk).expect("secp256k1 public key must convert to did");

        assert_eq!(
            did_from_peer_id, did_from_public_key,
            "peer_id and public_key conversions must agree"
        );
        // Go produces the uncompressed secp256k1 DID with the base58btc
        // `z7r8` prefix (multicodec 0xe7 + 65-byte uncompressed point).
        assert!(
            did_from_peer_id.as_str().starts_with("did:key:z7r8"),
            "secp256k1 DID must start with did:key:z7r8, got {}",
            did_from_peer_id.as_str()
        );

        // Sanity: parsing the DID back must round-trip to a secp256k1
        // key type with uncompressed SEC1 bytes (65 bytes starting 0x04).
        let (kt, bytes) =
            crypto::parse_did_key(did_from_peer_id.as_str()).expect("DID must round-trip");
        assert_eq!(kt, crypto::KeyType::Secp256k1);
        assert_eq!(bytes.len(), 65);
        assert_eq!(bytes[0], 0x04);
    }

    // Exercises the full libp2p-side extraction against a fixture that
    // matches `crates/crypto/tests/go_compat_keys.rs::test_secp256k1_did_matches_go`.
    // Rust must produce the same DID Go produces for the same 32-byte
    // private key when routed through libp2p's protobuf encoding.
    #[test]
    fn test_secp256k1_public_key_to_did_matches_go_fixture() {
        let kp = libp2p_keypair_from_go_fixture();
        let did = public_key_to_did(&kp.public()).expect("secp256k1 DID conversion must succeed");
        assert_eq!(
            did.as_str(),
            GO_SECP256K1_DID,
            "libp2p secp256k1 path must produce the same DID Go produces for the fixture key"
        );
    }

    #[test]
    fn test_secp256k1_peer_id_to_did_matches_go_fixture() {
        let kp = libp2p_keypair_from_go_fixture();
        let peer_id = PeerId::from_public_key(&kp.public());
        let did = peer_id_to_did(&peer_id).expect("secp256k1 DID conversion must succeed");
        assert_eq!(did.as_str(), GO_SECP256K1_DID);
    }

    #[test]
    fn test_secp256k1_peer_to_did_is_deterministic() {
        let libp2p_keypair = Keypair::generate_secp256k1();
        let peer_id = PeerId::from_public_key(&libp2p_keypair.public());

        let did1 = peer_id_to_did(&peer_id).unwrap();
        let did2 = peer_id_to_did(&peer_id).unwrap();
        assert_eq!(did1, did2);
    }

    #[test]
    fn test_secp256k1_mapper_function() {
        let libp2p_keypair = Keypair::generate_secp256k1();
        let peer_id = PeerId::from_public_key(&libp2p_keypair.public());

        let mapper = create_peer_to_did_mapper();
        let did = mapper(&peer_id.to_string()).expect("secp256k1 peer must map to DID");
        assert!(did.as_str().starts_with("did:key:z"));
    }

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
