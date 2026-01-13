//! Ed25519 signature implementation
//!
//! This module provides Ed25519 signing and verification operations.
//!
//! # Implementation Note
//!
//! The actual signing and verification implementations are located in the key modules:
//! - [`crate::keys::ed25519::Ed25519PrivateKey::sign`] - Ed25519 signing
//! - [`crate::keys::ed25519::Ed25519PublicKey::verify`] - Ed25519 verification
//!
//! This architectural decision keeps signature operations closely tied to their
//! respective key types, making the API more ergonomic and type-safe.
//!
//! # Example
//!
//! ```ignore
//! use crypto::keys::generation::generate_ed25519;
//! use crypto::keys::PrivateKey;
//!
//! let private_key = generate_ed25519()?;
//! let message = b"Hello, World!";
//!
//! // Sign the message
//! let signature = private_key.sign(message)?;
//! assert_eq!(signature.len(), 64); // Ed25519 signatures are 64 bytes
//!
//! // Verify the signature
//! let public_key = private_key.public_key();
//! let is_valid = public_key.verify(message, &signature)?;
//! assert!(is_valid);
//! ```
