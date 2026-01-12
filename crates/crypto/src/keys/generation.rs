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
    OsRng.fill_bytes(&mut seed);

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
    OsRng.fill_bytes(&mut key);
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
    match key_type {
        KeyType::Secp256k1 => {
            let key = Secp256k1PrivateKey::from_bytes(bytes)
                .ok_or_else(|| crypto_error("invalid secp256k1 private key bytes"))?;
            Ok(Box::new(key))
        }
        KeyType::Ed25519 => {
            let key = Ed25519PrivateKey::from_bytes(bytes)
                .ok_or_else(|| crypto_error("invalid ed25519 private key bytes"))?;
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
    let bytes = hex::decode(hex_str).map_err(|e| crypto_error(format!("invalid hex: {}", e)))?;
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
    let bytes = hex::decode(hex_str).map_err(|e| crypto_error(format!("invalid hex: {}", e)))?;
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
}
