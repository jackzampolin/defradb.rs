//! Error types for the identity crate

use crypto::KeyType;
use thiserror::Error;

/// Result type alias for identity operations
pub type Result<T> = std::result::Result<T, Error>;

/// Identity-specific error types
#[derive(Debug, Error)]
pub enum Error {
    /// Key type is not supported for identity operations
    #[error("{0:?} is not supported for identity operations")]
    UnsupportedKeyType(KeyType),

    /// Failed to derive public key from private key
    #[error("failed to derive public key: {0}")]
    PublicKeyDerivation(String),

    /// Invalid key bytes for the specified key type
    #[error("invalid {0:?} key bytes: {1}")]
    InvalidKeyBytes(KeyType, String),

    /// Underlying crypto error
    #[error("crypto error: {0}")]
    Crypto(#[from] defra_core::Error),
}
