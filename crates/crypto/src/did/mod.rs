//! DID (Decentralized Identifier) key generation
//!
//! This module implements did:key format generation for cryptographic keys.

use defra_core::Result;

use crate::types::KeyType;

pub mod multicodec;

/// Create a DID key representation of a public key
///
/// # Parameters
/// * `key_type` - The type of cryptographic key
/// * `public_key` - The raw public key bytes
///
/// # Returns
/// A DID key string in the format `did:key:<multibase-encoded-key>`
///
/// # Example
/// ```ignore
/// let did = create_did_key(KeyType::Ed25519, &public_key_bytes)?;
/// assert!(did.starts_with("did:key:"));
/// ```
pub fn create_did_key(key_type: KeyType, public_key: &[u8]) -> Result<String> {
    // Get multicodec identifier for key type
    // Multicodec table: https://github.com/multiformats/multicodec/blob/master/table.csv
    let multicodec = match key_type {
        KeyType::Ed25519 => 0xed,     // ed25519-pub (multicodec 0xed)
        KeyType::Secp256k1 => 0xe7,   // secp256k1-pub (multicodec 0xe7)
        KeyType::Secp256r1 => 0x1200, // p256-pub (multicodec 0x1200)
    };

    // Encode with varint prefix
    let mut codec_bytes = Vec::new();
    let mut buf = unsigned_varint::encode::u64_buffer();
    let encoded = unsigned_varint::encode::u64(multicodec, &mut buf);
    codec_bytes.extend_from_slice(encoded);
    codec_bytes.extend_from_slice(public_key);

    // Multibase encode (base58btc)
    let encoded = multibase::encode(multibase::Base::Base58Btc, &codec_bytes);

    // Build did:key
    Ok(format!("did:key:{}", encoded))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::PrivateKey;

    #[test]
    fn test_create_did_key_ed25519() {
        let public_key = vec![1u8; 32];
        let did = create_did_key(KeyType::Ed25519, &public_key).unwrap();
        assert!(did.starts_with("did:key:"));
    }

    #[test]
    fn test_create_did_key_secp256k1() {
        let public_key = vec![2u8; 33];
        let did = create_did_key(KeyType::Secp256k1, &public_key).unwrap();
        assert!(did.starts_with("did:key:"));
    }

    #[test]
    fn test_create_did_key_secp256r1() {
        let public_key = vec![3u8; 33];
        let did = create_did_key(KeyType::Secp256r1, &public_key).unwrap();
        assert!(did.starts_with("did:key:"));
    }

    #[test]
    fn test_did_key_deterministic() {
        // Same public key should always produce same DID
        let public_key = vec![1u8; 32];
        let did1 = create_did_key(KeyType::Ed25519, &public_key).unwrap();
        let did2 = create_did_key(KeyType::Ed25519, &public_key).unwrap();
        assert_eq!(did1, did2, "DIDs should be deterministic");
    }

    #[test]
    fn test_did_key_different_types() {
        // Same bytes but different key types should produce different DIDs
        let bytes = vec![1u8; 33];
        let did_secp256k1 = create_did_key(KeyType::Secp256k1, &bytes).unwrap();
        let did_secp256r1 = create_did_key(KeyType::Secp256r1, &bytes).unwrap();
        assert_ne!(did_secp256k1, did_secp256r1, "Different key types should produce different DIDs");
    }

    #[test]
    fn test_did_key_multicodec_encoding() {
        use crate::keys::generation::generate_ed25519;

        // Generate a real Ed25519 key and create DID
        let private_key = generate_ed25519().unwrap();
        let public_key = private_key.public_key();
        let did = public_key.did().unwrap();

        // Verify DID format
        assert!(did.starts_with("did:key:z"), "DID should start with did:key:z (base58btc)");

        // Verify DID is longer than just the prefix (contains encoded data)
        assert!(did.len() > 15, "DID should contain encoded key data");
    }

    #[test]
    fn test_did_key_roundtrip_format() {
        // Test that we can decode the DID back to verify format
        let public_key = vec![0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89,
                             0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89,
                             0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89,
                             0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89];

        let did = create_did_key(KeyType::Ed25519, &public_key).unwrap();

        // Remove "did:key:" prefix
        let encoded = did.strip_prefix("did:key:").unwrap();

        // Decode multibase
        let decoded = multibase::decode(encoded).unwrap();

        // Decode the varint multicodec prefix
        let (multicodec, remaining_bytes) = unsigned_varint::decode::u64(&decoded.1)
            .expect("Failed to decode multicodec");

        // Verify multicodec is correct for Ed25519
        assert_eq!(multicodec, 0xed, "Multicodec should be 0xed for Ed25519");

        // Verify public key bytes are included after the varint prefix
        // remaining_bytes is the slice after the varint
        assert_eq!(remaining_bytes, &public_key[..], "DID should contain original public key after multicodec");
    }

    #[test]
    fn test_did_key_with_real_keys() {
        use crate::keys::generation::{generate_ed25519, generate_secp256k1};

        // Test with real Ed25519 key
        let ed25519_key = generate_ed25519().unwrap();
        let ed25519_pub = ed25519_key.public_key();
        let ed25519_did = ed25519_pub.did().unwrap();
        assert!(ed25519_did.starts_with("did:key:z"));

        // Test with real secp256k1 key
        let secp256k1_key = generate_secp256k1().unwrap();
        let secp256k1_pub = secp256k1_key.public_key();
        let secp256k1_did = secp256k1_pub.did().unwrap();
        assert!(secp256k1_did.starts_with("did:key:z"));

        // Verify they're different
        assert_ne!(ed25519_did, secp256k1_did, "Different key types should produce different DIDs");
    }
}
