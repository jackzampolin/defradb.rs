//! Crypto type definitions and constants

use serde::{Deserialize, Serialize};

/// Cryptographic key types supported by DefraDB
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyType {
    /// secp256k1 elliptic curve (used by Bitcoin, Ethereum)
    Secp256k1,
    /// Ed25519 elliptic curve (modern, recommended)
    Ed25519,
    /// secp256r1 (P-256) elliptic curve (NIST standard)
    Secp256r1,
    /// BLS12-381 threshold keys (Orbis ring signing)
    Bls12381,
}

impl std::fmt::Display for KeyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyType::Secp256k1 => write!(f, "secp256k1"),
            KeyType::Ed25519 => write!(f, "ed25519"),
            KeyType::Secp256r1 => write!(f, "secp256r1"),
            KeyType::Bls12381 => write!(f, "bls12381"),
        }
    }
}

/// AES nonce size in bytes (96 bits)
pub const AES_NONCE_SIZE: usize = 12;

/// AES-256 key size in bytes
pub const AES_KEY_SIZE: usize = 32;

/// X25519 public key size in bytes
pub const X25519_PUBLIC_KEY_SIZE: usize = 32;

/// HMAC-SHA256 output size in bytes
pub const HMAC_SIZE: usize = 32;

/// Ed25519 public key size in bytes
pub const ED25519_PUBLIC_KEY_SIZE: usize = 32;

/// Ed25519 private key size in bytes
pub const ED25519_PRIVATE_KEY_SIZE: usize = 64;

/// secp256k1 compressed public key size in bytes
pub const SECP256K1_COMPRESSED_PUBLIC_KEY_SIZE: usize = 33;

/// secp256k1 private key size in bytes
pub const SECP256K1_PRIVATE_KEY_SIZE: usize = 32;

/// secp256r1 (P-256) private key size in bytes
pub const SECP256R1_PRIVATE_KEY_SIZE: usize = 32;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_type_display() {
        assert_eq!(KeyType::Secp256k1.to_string(), "secp256k1");
        assert_eq!(KeyType::Ed25519.to_string(), "ed25519");
        assert_eq!(KeyType::Secp256r1.to_string(), "secp256r1");
    }

    #[test]
    fn test_key_type_serialization() {
        let kt = KeyType::Secp256k1;
        let json = serde_json::to_string(&kt).unwrap();
        assert_eq!(json, "\"secp256k1\"");

        let kt = KeyType::Ed25519;
        let json = serde_json::to_string(&kt).unwrap();
        assert_eq!(json, "\"ed25519\"");

        let kt = KeyType::Secp256r1;
        let json = serde_json::to_string(&kt).unwrap();
        assert_eq!(json, "\"secp256r1\"");
    }

    #[test]
    fn test_key_type_deserialization() {
        let kt: KeyType = serde_json::from_str("\"secp256k1\"").unwrap();
        assert_eq!(kt, KeyType::Secp256k1);

        let kt: KeyType = serde_json::from_str("\"ed25519\"").unwrap();
        assert_eq!(kt, KeyType::Ed25519);

        let kt: KeyType = serde_json::from_str("\"secp256r1\"").unwrap();
        assert_eq!(kt, KeyType::Secp256r1);
    }

    #[test]
    fn test_constants() {
        assert_eq!(AES_NONCE_SIZE, 12);
        assert_eq!(AES_KEY_SIZE, 32);
        assert_eq!(X25519_PUBLIC_KEY_SIZE, 32);
        assert_eq!(HMAC_SIZE, 32);
        assert_eq!(ED25519_PUBLIC_KEY_SIZE, 32);
        assert_eq!(ED25519_PRIVATE_KEY_SIZE, 64);
        assert_eq!(SECP256K1_COMPRESSED_PUBLIC_KEY_SIZE, 33);
        assert_eq!(SECP256K1_PRIVATE_KEY_SIZE, 32);
    }
}
