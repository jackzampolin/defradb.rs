//! Identity types for DefraDB
//!
//! This crate provides identity management for DefraDB nodes, including:
//! - `Identity` trait for public identity operations (DID, public key)
//! - `FullIdentity` trait for identity operations requiring a private key
//! - `RawIdentity` concrete implementation supporting secp256k1 and ed25519
//! - `IdentityKeyType` enum for compile-time key type safety
//! - `Did` newtype for validated DID strings
//! - `IdentityContext` for propagating identity through request handling
//!
//! # Supported Key Types
//!
//! - **Ed25519**: Fast, secure signing with 64-byte signatures
//! - **secp256k1**: Bitcoin/Ethereum compatible with DER-encoded signatures (70-73 bytes typical)
//!
//! Note: secp256r1 (P-256) is NOT supported for identity operations.
//! Use `IdentityKeyType` instead of `crypto::KeyType` to ensure compile-time
//! safety when working with identity operations.

mod context;
mod did;
mod error;
mod key_type;
mod raw;

pub use context::IdentityContext;
pub use crypto::KeyType;
pub use did::{Did, DID_KEY_PREFIX};
pub use error::{Error, Result};
pub use key_type::IdentityKeyType;
pub use raw::RawIdentity;

use crypto::keys::{PrivateKey, PublicKey};

/// Identity represents an entity with a public key and DID.
///
/// This trait provides read-only access to identity information that can be
/// shared publicly. Implementations must be Send + Sync for use in async contexts.
pub trait Identity: Send + Sync {
    /// Returns the public key associated with this identity.
    fn pub_key(&self) -> &dyn PublicKey;

    /// Returns the DID (Decentralized Identifier) for this identity.
    ///
    /// The DID is derived from the public key using the did:key method.
    /// Returns a validated `Did` type that guarantees proper format.
    fn did(&self) -> Result<Did>;
}

/// FullIdentity extends Identity with private key operations.
///
/// This trait provides access to signing operations and the private key.
/// Implementations should take care to protect the private key material.
pub trait FullIdentity: Identity {
    /// Returns the private key associated with this identity.
    fn priv_key(&self) -> &dyn PrivateKey;

    /// Signs the provided data using this identity's private key.
    ///
    /// The signature format depends on the key type:
    /// - Ed25519: 64-byte raw signature
    /// - secp256k1: DER-encoded ECDSA signature
    ///
    /// The default implementation delegates to `self.priv_key().sign(data)`.
    fn sign(&self, data: &[u8]) -> defra_core::Result<Vec<u8>> {
        self.priv_key().sign(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crypto::{generate_ed25519, generate_secp256k1};

    #[test]
    fn test_raw_identity_from_ed25519() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let did = identity.did().unwrap();
        assert!(did.as_str().starts_with("did:key:"));
    }

    #[test]
    fn test_raw_identity_from_secp256k1() {
        let private_key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let did = identity.did().unwrap();
        assert!(did.as_str().starts_with("did:key:"));
    }

    #[test]
    fn test_raw_identity_sign_verify() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let message = b"test message";
        let signature = identity.sign(message).unwrap();

        let verified = identity.pub_key().verify(message, &signature).unwrap();
        assert!(verified);
    }

    #[test]
    fn test_raw_identity_did_deterministic() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let did1 = identity.did().unwrap();
        let did2 = identity.did().unwrap();
        assert_eq!(did1, did2, "DID should be deterministic");
    }

    #[test]
    fn test_identity_key_type() {
        let ed25519_key = generate_ed25519().unwrap();
        let ed25519_identity = RawIdentity::from_private_key(ed25519_key).unwrap();
        assert_eq!(ed25519_identity.key_type(), KeyType::Ed25519);

        let secp256k1_key = generate_secp256k1().unwrap();
        let secp256k1_identity = RawIdentity::from_private_key(secp256k1_key).unwrap();
        assert_eq!(secp256k1_identity.key_type(), KeyType::Secp256k1);
    }

    #[test]
    fn test_raw_identity_secp256k1_sign_verify() {
        let private_key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let message = b"test message for secp256k1";
        let signature = identity.sign(message).unwrap();

        let verified = identity.pub_key().verify(message, &signature).unwrap();
        assert!(verified);
    }

    #[test]
    fn test_different_identities_have_different_dids() {
        let identity1 = RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap();
        let identity2 = RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap();

        let did1 = identity1.did().unwrap();
        let did2 = identity2.did().unwrap();

        assert_ne!(did1, did2, "Different keys should produce different DIDs");
    }

    #[test]
    fn test_signature_not_reusable() {
        let identity = RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap();

        let message1 = b"first message";
        let message2 = b"second message";

        let signature = identity.sign(message1).unwrap();

        let valid_for_msg1 = identity.pub_key().verify(message1, &signature).unwrap();
        let valid_for_msg2 = identity.pub_key().verify(message2, &signature).unwrap();

        assert!(
            valid_for_msg1,
            "Signature should verify for original message"
        );
        assert!(
            !valid_for_msg2,
            "Signature should not verify for different message"
        );
    }

    #[test]
    fn test_wrong_key_verification_fails() {
        let identity1 = RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap();
        let identity2 = RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap();

        let message = b"test message";
        let signature = identity1.sign(message).unwrap();

        let valid = identity2.pub_key().verify(message, &signature).unwrap();
        assert!(!valid, "Signature should not verify with wrong key");
    }
}
