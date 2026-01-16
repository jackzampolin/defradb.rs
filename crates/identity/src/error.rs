//! Error types for the identity crate

use thiserror::Error;

/// Result type alias for identity operations
pub type Result<T> = std::result::Result<T, Error>;

/// Identity-specific error types
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid key type for operation
    #[error("invalid key type: {0}")]
    InvalidKeyType(String),

    /// Key generation failed
    #[error("key generation failed: {0}")]
    KeyGenerationFailed(String),

    /// Underlying crypto error
    #[error("crypto error: {0}")]
    Crypto(#[from] defra_core::Error),
}
