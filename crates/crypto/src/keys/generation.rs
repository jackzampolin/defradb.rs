//! Key generation functions
//!
//! This module provides functions for generating cryptographic key pairs
//! for various key types (secp256k1, Ed25519, X25519).

use ed25519_dalek::SigningKey as Ed25519SigningKey;
use k256::ecdsa::SigningKey as Secp256k1SigningKey;
use rand::rngs::OsRng;
use x25519_dalek::StaticSecret;

use defra_core::Result;

use crate::error::{crypto_error, unsupported_key_type};
use crate::keys::{
    ed25519::{Ed25519PrivateKey, Ed25519PublicKey},
    secp256k1::{Secp256k1PrivateKey, Secp256k1PublicKey},
    secp256r1::Secp256r1PublicKey,
    PrivateKey,
};
use crate::types::{KeyType, AES_KEY_SIZE};

/// Generate a new private key of the specified type
///
/// # Parameters
/// * `key_type` - The type of key to generate
///
/// # Returns
/// A boxed private key implementing the PrivateKey trait
///
/// # Example
/// ```ignore
/// let private_key = generate_key(KeyType::Ed25519)?;
/// let signature = private_key.sign(b"message")?;
/// ```
pub fn generate_key(key_type: KeyType) -> Result<Box<dyn PrivateKey>> {
    match key_type {
        KeyType::Secp256k1 => {
            let key = generate_secp256k1()?;
            Ok(Box::new(key))
        }
        KeyType::Ed25519 => {
            let key = generate_ed25519()?;
            Ok(Box::new(key))
        }
        KeyType::Secp256r1 => {
            // secp256r1 private keys are not supported (JS clients manage them)
            Err(unsupported_key_type(key_type))
        }
    }
}

/// Generate a new secp256k1 private key
///
/// # Returns
/// A new secp256k1 private key
///
/// # Example
/// ```ignore
/// let private_key = generate_secp256k1()?;
/// let public_key = private_key.public_key();
/// ```
pub fn generate_secp256k1() -> Result<Secp256k1PrivateKey> {
    let signing_key = Secp256k1SigningKey::random(&mut OsRng);
    Secp256k1PrivateKey::from_bytes(&signing_key.to_bytes())
        .ok_or_else(|| crypto_error("failed to create secp256k1 private key"))
}

/// Generate a new Ed25519 private key
///
/// # Returns
/// A new Ed25519 private key
///
/// # Example
/// ```ignore
/// let private_key = generate_ed25519()?;
/// let signature = private_key.sign(b"message")?;
/// ```
pub fn generate_ed25519() -> Result<Ed25519PrivateKey> {
    // Generate random 32-byte seed
    use rand::RngCore;
    let mut seed = [0u8; 32];
    OsRng.try_fill_bytes(&mut seed)
        .map_err(|e| {
            // CRITICAL: Log RNG failures - this is a security event
            eprintln!("CRITICAL: RNG failure in Ed25519 key generation: {}", e);
            crypto_error(format!("RNG failure in Ed25519 key generation: {}", e))
        })?;

    let signing_key = Ed25519SigningKey::from_bytes(&seed);

    // Create 64-byte representation (32-byte seed + 32-byte public key)
    let public = signing_key.verifying_key().to_bytes();
    let mut key_bytes = Vec::with_capacity(64);
    key_bytes.extend_from_slice(&seed);
    key_bytes.extend_from_slice(&public);

    Ed25519PrivateKey::from_bytes(&key_bytes)
        .ok_or_else(|| crypto_error("failed to create ed25519 private key"))
}

/// Generate a new X25519 private key for ECIES
///
/// # Returns
/// A new X25519 static secret
///
/// # Example
/// ```ignore
/// let private_key = generate_x25519()?;
/// let public_key = x25519_dalek::PublicKey::from(&private_key);
/// ```
pub fn generate_x25519() -> Result<StaticSecret> {
    Ok(StaticSecret::random_from_rng(OsRng))
}

/// Generate a new AES-256 key
///
/// # Returns
/// A 32-byte AES-256 key
///
/// # Example
/// ```ignore
/// let aes_key = generate_aes256()?;
/// let (ciphertext, nonce) = encrypt_aes(plaintext, &aes_key, &[], true)?;
/// ```
pub fn generate_aes256() -> Result<Vec<u8>> {
    let mut key = vec![0u8; AES_KEY_SIZE];
    use rand::RngCore;
    OsRng.try_fill_bytes(&mut key)
        .map_err(|e| {
            // CRITICAL: Log RNG failures - this is a security event
            eprintln!("CRITICAL: RNG failure in AES-256 key generation: {}", e);
            crypto_error(format!("RNG failure in AES-256 key generation: {}", e))
        })?;
    Ok(key)
}

/// Parse a private key from bytes
///
/// # Parameters
/// * `key_type` - The type of key
/// * `bytes` - The raw key bytes
///
/// # Returns
/// A boxed private key implementing the PrivateKey trait
pub fn private_key_from_bytes(key_type: KeyType, bytes: &[u8]) -> Result<Box<dyn PrivateKey>> {
    // Validate against obviously weak keys
    if !bytes.is_empty() {
        if bytes.iter().all(|&b| b == 0) {
            return Err(crypto_error("private key is all zeros (cryptographically weak)"));
        }
        if bytes.iter().all(|&b| b == 0xFF) {
            return Err(crypto_error("private key is all ones (cryptographically weak)"));
        }
    }

    match key_type {
        KeyType::Secp256k1 => {
            let key = Secp256k1PrivateKey::from_bytes(bytes)
                .ok_or_else(|| crypto_error(format!(
                    "invalid secp256k1 private key: expected 32 bytes, got {}",
                    bytes.len()
                )))?;
            Ok(Box::new(key))
        }
        KeyType::Ed25519 => {
            let key = Ed25519PrivateKey::from_bytes(bytes)
                .ok_or_else(|| crypto_error(format!(
                    "invalid ed25519 private key: expected 64 bytes, got {}",
                    bytes.len()
                )))?;
            Ok(Box::new(key))
        }
        KeyType::Secp256r1 => Err(unsupported_key_type(key_type)),
    }
}

/// Parse a private key from a hex string
///
/// # Parameters
/// * `key_type` - The type of key
/// * `hex_str` - The hex-encoded key string
///
/// # Returns
/// A boxed private key implementing the PrivateKey trait
pub fn private_key_from_string(
    key_type: KeyType,
    hex_str: &str,
) -> Result<Box<dyn PrivateKey>> {
    let bytes = hex::decode(hex_str).map_err(|e| {
        let preview = if hex_str.len() > 64 {
            format!("{}...(truncated, length: {})", &hex_str[..64], hex_str.len())
        } else {
            hex_str.to_string()
        };
        crypto_error(format!("invalid hex string: {}, input: {}", e, preview))
    })?;
    private_key_from_bytes(key_type, &bytes)
}

/// Parse a public key from bytes
///
/// # Parameters
/// * `key_type` - The type of key
/// * `bytes` - The raw key bytes
///
/// # Returns
/// A boxed public key implementing the PublicKey trait
pub fn public_key_from_bytes(
    key_type: KeyType,
    bytes: &[u8],
) -> Result<Box<dyn crate::keys::PublicKey>> {
    match key_type {
        KeyType::Secp256k1 => {
            let key = Secp256k1PublicKey::from_bytes(bytes)
                .ok_or_else(|| crypto_error("invalid secp256k1 public key bytes"))?;
            Ok(Box::new(key))
        }
        KeyType::Ed25519 => {
            let key = Ed25519PublicKey::from_bytes(bytes)
                .ok_or_else(|| crypto_error("invalid ed25519 public key bytes"))?;
            Ok(Box::new(key))
        }
        KeyType::Secp256r1 => {
            let key = Secp256r1PublicKey::from_bytes(bytes)
                .ok_or_else(|| crypto_error("invalid secp256r1 public key bytes"))?;
            Ok(Box::new(key))
        }
    }
}

/// Parse a public key from a hex string
///
/// # Parameters
/// * `key_type` - The type of key
/// * `hex_str` - The hex-encoded key string
///
/// # Returns
/// A boxed public key implementing the PublicKey trait
pub fn public_key_from_string(
    key_type: KeyType,
    hex_str: &str,
) -> Result<Box<dyn crate::keys::PublicKey>> {
    let bytes = hex::decode(hex_str).map_err(|e| {
        let preview = if hex_str.len() > 64 {
            format!("{}...(truncated, length: {})", &hex_str[..64], hex_str.len())
        } else {
            hex_str.to_string()
        };
        crypto_error(format!("invalid hex string: {}, input: {}", e, preview))
    })?;
    public_key_from_bytes(key_type, &bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::{Key, PrivateKey};

    #[test]
    fn test_generate_secp256k1() {
        let key = generate_secp256k1().unwrap();
        assert_eq!(key.key_type(), KeyType::Secp256k1);
        assert_eq!(key.raw().len(), 32);
    }

    #[test]
    fn test_generate_ed25519() {
        let key = generate_ed25519().unwrap();
        assert_eq!(key.key_type(), KeyType::Ed25519);
        assert_eq!(key.raw().len(), 64);
    }

    #[test]
    fn test_generate_key_secp256k1() {
        let key = generate_key(KeyType::Secp256k1).unwrap();
        assert_eq!(key.key_type(), KeyType::Secp256k1);
    }

    #[test]
    fn test_generate_key_ed25519() {
        let key = generate_key(KeyType::Ed25519).unwrap();
        assert_eq!(key.key_type(), KeyType::Ed25519);
    }

    #[test]
    fn test_generate_key_secp256r1_fails() {
        let result = generate_key(KeyType::Secp256r1);
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_x25519() {
        let key = generate_x25519().unwrap();
        let public = x25519_dalek::PublicKey::from(&key);
        assert_eq!(public.as_bytes().len(), 32);
    }

    #[test]
    fn test_generate_aes256() {
        let key = generate_aes256().unwrap();
        assert_eq!(key.len(), 32);

        // Generate multiple keys to ensure they're different (random)
        let key2 = generate_aes256().unwrap();
        assert_ne!(key, key2);
    }

    #[test]
    fn test_private_key_from_bytes_secp256k1() {
        let original = generate_secp256k1().unwrap();
        let bytes = original.raw();

        let parsed = private_key_from_bytes(KeyType::Secp256k1, &bytes).unwrap();
        assert_eq!(parsed.raw(), bytes);
    }

    #[test]
    fn test_private_key_from_bytes_ed25519() {
        let original = generate_ed25519().unwrap();
        let bytes = original.raw();

        let parsed = private_key_from_bytes(KeyType::Ed25519, &bytes).unwrap();
        assert_eq!(parsed.raw(), bytes);
    }

    #[test]
    fn test_private_key_from_string() {
        let original = generate_secp256k1().unwrap();
        let hex_str = original.to_hex_string();

        let parsed = private_key_from_string(KeyType::Secp256k1, &hex_str).unwrap();
        assert_eq!(parsed.raw(), original.raw());
    }

    #[test]
    fn test_public_key_from_bytes() {
        let private_key = generate_secp256k1().unwrap();
        let public_key = private_key.public_key();
        let bytes = public_key.raw();

        let parsed = public_key_from_bytes(KeyType::Secp256k1, &bytes).unwrap();
        assert_eq!(parsed.raw(), bytes);
    }

    #[test]
    fn test_public_key_from_string() {
        let private_key = generate_ed25519().unwrap();
        let public_key = private_key.public_key();
        let hex_str = public_key.to_hex_string();

        let parsed = public_key_from_string(KeyType::Ed25519, &hex_str).unwrap();
        assert_eq!(parsed.raw(), public_key.raw());
    }

    #[test]
    fn test_private_key_from_string_invalid_hex() {
        // Invalid hex characters
        let result = private_key_from_string(KeyType::Secp256k1, "gggggggg");
        assert!(result.is_err(), "Invalid hex characters should fail");

        // Non-hex characters
        let result = private_key_from_string(KeyType::Secp256k1, "xyz123");
        assert!(result.is_err(), "Non-hex characters should fail");

        // Odd length hex string
        let result = private_key_from_string(KeyType::Secp256k1, "abc");
        assert!(result.is_err(), "Odd-length hex should fail");

        // Empty string
        let result = private_key_from_string(KeyType::Secp256k1, "");
        assert!(result.is_err(), "Empty hex string should fail");
    }

    #[test]
    fn test_public_key_from_string_invalid_hex() {
        // Invalid hex characters
        let result = public_key_from_string(KeyType::Ed25519, "gggggggg");
        assert!(result.is_err(), "Invalid hex characters should fail");

        // Valid hex but wrong length for Ed25519 public key (needs 32 bytes = 64 hex chars)
        let result = public_key_from_string(KeyType::Ed25519, "abcd1234");
        assert!(result.is_err(), "Wrong length hex should fail");

        // Empty string
        let result = public_key_from_string(KeyType::Ed25519, "");
        assert!(result.is_err(), "Empty hex string should fail");
    }

    #[test]
    fn test_private_key_from_string_wrong_length() {
        // Valid hex but wrong length for secp256k1 (needs 32 bytes = 64 hex chars)
        let short_hex = "0123456789abcdef";  // Only 8 bytes
        let result = private_key_from_string(KeyType::Secp256k1, short_hex);
        assert!(result.is_err(), "Short key should fail");

        // Valid hex but wrong length for Ed25519 (needs 64 bytes = 128 hex chars)
        let wrong_length_hex = "0123456789abcdef0123456789abcdef";  // Only 16 bytes
        let result = private_key_from_string(KeyType::Ed25519, wrong_length_hex);
        assert!(result.is_err(), "Wrong length should fail");
    }

    #[test]
    fn test_private_key_from_weak_bytes() {
        // All-zero key should be rejected
        let zero_key = vec![0u8; 32];
        let result = private_key_from_bytes(KeyType::Secp256k1, &zero_key);
        assert!(result.is_err(), "All-zero key should be rejected");
        match result {
            Err(e) => assert!(e.to_string().contains("all zeros"), "Error should mention weak key"),
            Ok(_) => panic!("Should have failed for all-zero key"),
        }

        // All-ones key should be rejected
        let ones_key = vec![0xFFu8; 32];
        let result = private_key_from_bytes(KeyType::Secp256k1, &ones_key);
        assert!(result.is_err(), "All-ones key should be rejected");
        match result {
            Err(e) => assert!(e.to_string().contains("all ones"), "Error should mention weak key"),
            Ok(_) => panic!("Should have failed for all-ones key"),
        }

        // Same for Ed25519
        let zero_key = vec![0u8; 64];
        let result = private_key_from_bytes(KeyType::Ed25519, &zero_key);
        assert!(result.is_err(), "All-zero Ed25519 key should be rejected");

        let ones_key = vec![0xFFu8; 64];
        let result = private_key_from_bytes(KeyType::Ed25519, &ones_key);
        assert!(result.is_err(), "All-ones Ed25519 key should be rejected");
    }
}
