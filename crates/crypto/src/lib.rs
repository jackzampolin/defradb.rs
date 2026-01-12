//! DefraDB Cryptography Library
//!
//! This library provides cryptographic primitives for DefraDB including:
//! - Key management (secp256k1, Ed25519, secp256r1)
//! - Digital signatures (ECDSA, EdDSA)
//! - Symmetric encryption (AES-256-GCM)
//! - Asymmetric encryption (ECIES)
//! - DID key generation
//!
//! All implementations prioritize security and compatibility with the Go
//! implementation of DefraDB.

pub mod did;
pub mod encryption;
pub mod error;
pub mod keys;
pub mod signature;
pub mod types;

// Re-export commonly used types
pub use defra_core::{Error, Result};
pub use types::KeyType;

// Re-export key types
pub use keys::{
    ed25519::{Ed25519PrivateKey, Ed25519PublicKey},
    generation::{
        generate_aes256, generate_ed25519, generate_key, generate_secp256k1, generate_x25519,
        private_key_from_bytes, private_key_from_string, public_key_from_bytes,
        public_key_from_string,
    },
    secp256k1::{Secp256k1PrivateKey, Secp256k1PublicKey},
    secp256r1::Secp256r1PublicKey,
    Key, PrivateKey, PublicKey,
};

// Re-export DID functions
pub use did::create_did_key;

// Re-export encryption functions
pub use encryption::{
    aes::{decrypt_aes, encrypt_aes},
    ecies::{decrypt_ecies, encrypt_ecies, EciesOptions, EciesOptionsBuilder},
    nonce::generate_nonce,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_secp256k1_key() {
        let key = generate_secp256k1().unwrap();
        assert_eq!(key.key_type(), KeyType::Secp256k1);
    }

    #[test]
    fn test_generate_ed25519_key() {
        let key = generate_ed25519().unwrap();
        assert_eq!(key.key_type(), KeyType::Ed25519);
    }

    #[test]
    fn test_sign_verify_secp256k1() {
        let private_key = generate_secp256k1().unwrap();
        let message = b"test message";

        let signature = private_key.sign(message).unwrap();
        let public_key = private_key.public_key();

        let valid = public_key.verify(message, &signature).unwrap();
        assert!(valid);
    }

    #[test]
    fn test_sign_verify_ed25519() {
        let private_key = generate_ed25519().unwrap();
        let message = b"test message";

        let signature = private_key.sign(message).unwrap();
        let public_key = private_key.public_key();

        let valid = public_key.verify(message, &signature).unwrap();
        assert!(valid);
    }

    #[test]
    fn test_did_key_generation() {
        let private_key = generate_ed25519().unwrap();
        let public_key = private_key.public_key();

        let did = public_key.did().unwrap();
        assert!(did.starts_with("did:key:"));
    }

    #[test]
    fn test_cross_key_type_signature_verification_fails() {
        let message = b"test message";

        // Sign with Ed25519
        let ed25519_key = generate_ed25519().unwrap();
        let ed25519_signature = ed25519_key.sign(message).unwrap();

        // Sign with secp256k1
        let secp256k1_key = generate_secp256k1().unwrap();
        let secp256k1_signature = secp256k1_key.sign(message).unwrap();

        // Try to verify Ed25519 signature with secp256k1 public key (should fail)
        let secp256k1_pub = secp256k1_key.public_key();
        let result = secp256k1_pub.verify(message, &ed25519_signature);
        assert!(result.is_ok(), "Should return Ok with false, not error");
        assert!(!result.unwrap(), "Cross-key-type verification should fail");

        // Try to verify secp256k1 signature with Ed25519 public key (should fail)
        let ed25519_pub = ed25519_key.public_key();
        let result = ed25519_pub.verify(message, &secp256k1_signature);
        assert!(result.is_ok(), "Should return Ok with false, not error");
        assert!(!result.unwrap(), "Cross-key-type verification should fail");
    }

    #[test]
    fn test_wrong_key_same_type_verification_fails() {
        let message = b"test message";

        // Two different Ed25519 keys
        let key1 = generate_ed25519().unwrap();
        let key2 = generate_ed25519().unwrap();

        let signature1 = key1.sign(message).unwrap();
        let pub2 = key2.public_key();

        // Verify signature from key1 with public key from key2 (should fail)
        let valid = pub2.verify(message, &signature1).unwrap();
        assert!(!valid, "Signature from different key should not verify");

        // Two different secp256k1 keys
        let key1 = generate_secp256k1().unwrap();
        let key2 = generate_secp256k1().unwrap();

        let signature1 = key1.sign(message).unwrap();
        let pub2 = key2.public_key();

        // Verify signature from key1 with public key from key2 (should fail)
        let valid = pub2.verify(message, &signature1).unwrap();
        assert!(!valid, "Signature from different key should not verify");
    }
}
