//! Identity-specific key type enum.

use crypto::KeyType;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::Error;

/// Key types supported for identity operations.
///
/// # Supported Types
///
/// - **Ed25519**: Fast, secure signing with 64-byte signatures
/// - **Secp256k1**: Bitcoin/Ethereum compatible with DER-encoded signatures
/// - **Secp256r1**: P-256 / NIST curve, used by browser Web Crypto API (ES256 JWTs)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum IdentityKeyType {
    /// Ed25519 elliptic curve (used by Solana, etc.)
    Ed25519,
    /// secp256k1 elliptic curve (used by Bitcoin, Ethereum)
    Secp256k1,
    /// secp256r1 (P-256) elliptic curve (NIST standard, browser Web Crypto)
    Secp256r1,
}

impl IdentityKeyType {
    /// Returns the corresponding `crypto::KeyType`.
    pub fn to_crypto_key_type(self) -> KeyType {
        match self {
            IdentityKeyType::Ed25519 => KeyType::Ed25519,
            IdentityKeyType::Secp256k1 => KeyType::Secp256k1,
            IdentityKeyType::Secp256r1 => KeyType::Secp256r1,
        }
    }
}

impl TryFrom<KeyType> for IdentityKeyType {
    type Error = Error;

    fn try_from(key_type: KeyType) -> Result<Self, Self::Error> {
        match key_type {
            KeyType::Ed25519 => Ok(IdentityKeyType::Ed25519),
            KeyType::Secp256k1 => Ok(IdentityKeyType::Secp256k1),
            KeyType::Secp256r1 => Ok(IdentityKeyType::Secp256r1),
            KeyType::Bls12381 => Err(Error::UnsupportedKeyType(key_type)),
            _ => Err(Error::UnsupportedKeyType(key_type)),
        }
    }
}

impl From<IdentityKeyType> for KeyType {
    fn from(ikt: IdentityKeyType) -> Self {
        ikt.to_crypto_key_type()
    }
}

impl fmt::Display for IdentityKeyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IdentityKeyType::Ed25519 => write!(f, "ed25519"),
            IdentityKeyType::Secp256k1 => write!(f, "secp256k1"),
            IdentityKeyType::Secp256r1 => write!(f, "secp256r1"),
        }
    }
}

impl std::str::FromStr for IdentityKeyType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ed25519" => Ok(IdentityKeyType::Ed25519),
            "secp256k1" => Ok(IdentityKeyType::Secp256k1),
            "secp256r1" => Ok(IdentityKeyType::Secp256r1),
            other => Err(Error::UnknownKeyType(other.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_key_type_to_crypto() {
        assert_eq!(
            IdentityKeyType::Ed25519.to_crypto_key_type(),
            KeyType::Ed25519
        );
        assert_eq!(
            IdentityKeyType::Secp256k1.to_crypto_key_type(),
            KeyType::Secp256k1
        );
        assert_eq!(
            IdentityKeyType::Secp256r1.to_crypto_key_type(),
            KeyType::Secp256r1
        );
    }

    #[test]
    fn test_try_from_crypto_key_type_ed25519() {
        let result = IdentityKeyType::try_from(KeyType::Ed25519);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IdentityKeyType::Ed25519);
    }

    #[test]
    fn test_try_from_crypto_key_type_secp256k1() {
        let result = IdentityKeyType::try_from(KeyType::Secp256k1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IdentityKeyType::Secp256k1);
    }

    #[test]
    fn test_try_from_crypto_key_type_secp256r1() {
        let result = IdentityKeyType::try_from(KeyType::Secp256r1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IdentityKeyType::Secp256r1);
    }

    #[test]
    fn test_try_from_crypto_key_type_bls12381_fails() {
        let result = IdentityKeyType::try_from(KeyType::Bls12381);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedKeyType(KeyType::Bls12381)
        ));
    }

    #[test]
    fn test_into_crypto_key_type() {
        let kt: KeyType = IdentityKeyType::Ed25519.into();
        assert_eq!(kt, KeyType::Ed25519);

        let kt: KeyType = IdentityKeyType::Secp256k1.into();
        assert_eq!(kt, KeyType::Secp256k1);

        let kt: KeyType = IdentityKeyType::Secp256r1.into();
        assert_eq!(kt, KeyType::Secp256r1);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", IdentityKeyType::Ed25519), "ed25519");
        assert_eq!(format!("{}", IdentityKeyType::Secp256k1), "secp256k1");
        assert_eq!(format!("{}", IdentityKeyType::Secp256r1), "secp256r1");
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            "ed25519".parse::<IdentityKeyType>().unwrap(),
            IdentityKeyType::Ed25519
        );
        assert_eq!(
            "Ed25519".parse::<IdentityKeyType>().unwrap(),
            IdentityKeyType::Ed25519
        );
        assert_eq!(
            "secp256k1".parse::<IdentityKeyType>().unwrap(),
            IdentityKeyType::Secp256k1
        );
        assert_eq!(
            "SECP256K1".parse::<IdentityKeyType>().unwrap(),
            IdentityKeyType::Secp256k1
        );
        assert_eq!(
            "secp256r1".parse::<IdentityKeyType>().unwrap(),
            IdentityKeyType::Secp256r1
        );
        assert_eq!(
            "SECP256R1".parse::<IdentityKeyType>().unwrap(),
            IdentityKeyType::Secp256r1
        );
    }

    #[test]
    fn test_from_str_invalid() {
        assert!("invalid".parse::<IdentityKeyType>().is_err());
    }

    #[test]
    fn test_serde_roundtrip() {
        let ed25519 = IdentityKeyType::Ed25519;
        let json = serde_json::to_string(&ed25519).unwrap();
        assert_eq!(json, "\"ed25519\"");
        let parsed: IdentityKeyType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, ed25519);

        let secp256k1 = IdentityKeyType::Secp256k1;
        let json = serde_json::to_string(&secp256k1).unwrap();
        assert_eq!(json, "\"secp256k1\"");
        let parsed: IdentityKeyType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, secp256k1);

        let secp256r1 = IdentityKeyType::Secp256r1;
        let json = serde_json::to_string(&secp256r1).unwrap();
        assert_eq!(json, "\"secp256r1\"");
        let parsed: IdentityKeyType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, secp256r1);
    }
}
