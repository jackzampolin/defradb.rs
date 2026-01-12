//! ECDSA signature implementation (secp256k1)
//!
//! This module provides ECDSA signing and verification using the secp256k1 curve
//! with SHA-256 hashing and DER-encoded signatures for compatibility with the Go implementation.
//!
//! # Implementation Note
//!
//! The actual signing and verification implementations are located in the key modules:
//! - [`crate::keys::secp256k1::Secp256k1PrivateKey::sign`] - ECDSA signing
//! - [`crate::keys::secp256k1::Secp256k1PublicKey::verify`] - ECDSA verification
//!
//! This architectural decision keeps signature operations closely tied to their
//! respective key types, making the API more ergonomic and type-safe.
//!
//! # Example
//!
//! ```ignore
//! use crypto::keys::generation::generate_secp256k1;
//! use crypto::keys::PrivateKey;
//!
//! let private_key = generate_secp256k1()?;
//! let message = b"Hello, World!";
//!
//! // Sign the message
//! let signature = private_key.sign(message)?;
//!
//! // Verify the signature
//! let public_key = private_key.public_key();
//! let is_valid = public_key.verify(message, &signature)?;
//! assert!(is_valid);
//! ```
