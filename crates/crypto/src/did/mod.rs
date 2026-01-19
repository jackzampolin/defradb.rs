//! DID (Decentralized Identifier) key generation and parsing
//!
//! This module implements did:key format generation and parsing for cryptographic keys.

use defra_core::Result;

use crate::error::crypto_error;
use crate::types::KeyType;

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

/// Parse a DID key string and extract the key type and public key bytes
///
/// # Parameters
/// * `did` - A DID key string in the format `did:key:<multibase-encoded-key>`
///
/// # Returns
/// A tuple of (KeyType, Vec<u8>) containing the key type and raw public key bytes
///
/// # Example
/// ```ignore
/// let (key_type, public_key) = parse_did_key(did)?;
/// assert_eq!(key_type, KeyType::Ed25519);
/// ```
pub fn parse_did_key(did: &str) -> Result<(KeyType, Vec<u8>)> {
    // Remove "did:key:" prefix
    let encoded = did
        .strip_prefix("did:key:")
        .ok_or_else(|| crypto_error("invalid DID format: missing 'did:key:' prefix"))?;

    // Decode multibase
    let (_, decoded) = multibase::decode(encoded)
        .map_err(|e| crypto_error(format!("failed to decode multibase: {}", e)))?;

    // Decode the varint multicodec prefix
    let (multicodec, remaining_bytes) = unsigned_varint::decode::u64(&decoded)
        .map_err(|e| crypto_error(format!("failed to decode multicodec varint: {}", e)))?;

    // Map multicodec to KeyType
    let key_type = match multicodec {
        0xed => KeyType::Ed25519,
        0xe7 => KeyType::Secp256k1,
        0x1200 => KeyType::Secp256r1,
        _ => {
            return Err(crypto_error(format!(
                "unknown multicodec: 0x{:x}",
                multicodec
            )))
        }
    };

    Ok((key_type, remaining_bytes.to_vec()))
}
