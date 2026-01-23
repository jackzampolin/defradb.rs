
//! Keyring error types

use thiserror::Error;

/// Keyring-specific errors
#[derive(Debug, Error)]
pub enum Error {
    #[error("key not found: {0}")]
    NotFound(String),

    #[error("invalid key name: {0}")]
    InvalidKeyName(String),

    #[error("listing keys is not supported by OS keyring")]
    SystemKeyringListNotSupported,

    #[error("keyring secret not set: DEFRA_KEYRING_SECRET environment variable is required")]
    SecretNotSet,

    #[error("encryption failed: {0}")]
    Encryption(String),

    #[error("decryption failed: {0}")]
    Decryption(String),

    #[error("system keyring error: {0}")]
    SystemKeyring(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for keyring operations
pub type Result<T> = std::result::Result<T, Error>;
