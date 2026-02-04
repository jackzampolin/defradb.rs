//! DefraDB Cryptography Library
//!
//! This library provides cryptographic primitives for DefraDB including:
//! - Key management (secp256k1, Ed25519, secp256r1)
//! - Digital signatures (ECDSA, EdDSA)
//! - Symmetric encryption (AES-256-GCM)
//! - Asymmetric encryption (ECIES)
//! - Searchable encryption (HMAC-based equality tags)
//! - DID key generation
//! - Merkle proof generation and verification
//!
//! All implementations prioritize security and compatibility with the Go
//! implementation of DefraDB.

pub mod did;
pub mod encryption;
pub mod error;
pub mod hash;
pub mod keys;
pub mod merkle_proof;
pub mod se;
pub mod signature;
pub mod types;

// Go compatibility tests extracted to crates/crypto/tests/go_compat_*.rs

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
pub use did::{create_did_key, parse_did_key};

// Re-export encryption functions
pub use encryption::{
    aes::{decrypt_aes, encrypt_aes},
    ecies::{decrypt_ecies, encrypt_ecies, EciesOptions, EciesOptionsBuilder},
    nonce::generate_nonce,
};

// Re-export hash functions
pub use hash::{sha256, sha256_hash, Sha256Hash};

// Re-export merkle proof types
pub use merkle_proof::{
    extract_proof, verify_proof, verify_signed_proof, MerkleProof, ProofBlockstore, ProofNode,
    SignedMerkleProof,
};

// Re-export searchable encryption types
pub use se::{
    generate_equality_tag, generate_equality_tag_str, Artifact, ArtifactBatch, ArtifactType,
    OperationType, SEARCH_TAG_SIZE,
};
