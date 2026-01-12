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
}
