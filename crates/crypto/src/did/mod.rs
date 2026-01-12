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
    // Get multicodec for key type
    let multicodec = match key_type {
        KeyType::Ed25519 => 0xed,     // ed25519-pub
        KeyType::Secp256k1 => 0xe7,   // secp256k1-pub
        KeyType::Secp256r1 => 0x1200, // p256-pub
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
}
