//! Identity types for DefraDB
//!
//! This crate provides identity management for DefraDB nodes, including:
//! - `Identity` trait for public identity operations (DID, public key)
//! - `FullIdentity` trait for identity operations requiring a private key
//! - `RawIdentity` concrete implementation supporting Ed25519, secp256k1, and secp256r1
//! - `IdentityKeyType` enum for compile-time key type safety
//! - `Did` newtype for validated DID strings
//! - `IdentityContext` for propagating identity through request handling
//!
//! # Supported Key Types
//!
//! - **Ed25519**: Fast, secure signing with 64-byte signatures
//! - **secp256k1**: Bitcoin/Ethereum compatible with DER-encoded signatures (70-73 bytes typical)
//! - **secp256r1**: P-256 / NIST curve, browser Web Crypto compatible (ES256 JWTs)

mod context;
mod did;
mod error;
mod key_type;
mod raw;
mod token;

pub use context::IdentityContext;
pub use crypto::KeyType;
pub use did::{Did, DID_KEY_PREFIX};
pub use error::{Error, Result};
pub use key_type::IdentityKeyType;
pub use raw::RawIdentity;
pub use token::{
    from_token, new_token, new_token_with_custom_claims, verify_auth_token, IdentityClaims,
    TokenIdentity,
};

use crypto::keys::{PrivateKey, PublicKey};
use defra_core::thread_bounds::MaybeSendSync;

/// Identity represents an entity with a public key and DID.
///
/// This trait provides read-only access to identity information that can be
/// shared publicly. On native targets, implementations must be Send + Sync for async contexts.
pub trait Identity: MaybeSendSync {
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
    /// - secp256r1: DER-encoded ECDSA signature
    ///
    /// The default implementation delegates to `self.priv_key().sign(data)`.
    fn sign(&self, data: &[u8]) -> defra_core::Result<Vec<u8>> {
        self.priv_key().sign(data)
    }
}
