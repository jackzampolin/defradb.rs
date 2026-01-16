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

    /// Unknown key type string
    #[error("unknown key type: {0}")]
    UnknownKeyType(String),

    /// Invalid DID format
    #[error("invalid DID: {0}")]
    InvalidDid(String),

    /// Failed to derive public key from private key
    #[error("failed to derive public key: {0}")]
    PublicKeyDerivation(String),

    /// Invalid key bytes for the specified key type
    #[error("invalid {0:?} key bytes: {1}")]
    InvalidKeyBytes(KeyType, String),

    /// JWT token encoding failed
    #[error("token encoding failed: {0}")]
    TokenEncoding(String),

    /// JWT token decoding failed
    #[error("token decoding failed: {0}")]
    TokenDecoding(String),

    /// Token has expired
    #[error("token has expired (expired at {exp}, current time {now})")]
    TokenExpired { exp: u64, now: u64 },

    /// Token is not yet valid (nbf claim is in the future)
    #[error("token not yet valid (valid from {nbf}, current time {now})")]
    TokenNotYetValid { nbf: u64, now: u64 },

    /// Token audience mismatch
    #[error("token audience mismatch: expected {expected}, got {actual:?}")]
    AudienceMismatch {
        expected: String,
        actual: Vec<String>,
    },

    /// Missing claim in token
    #[error("missing required claim: {0}")]
    MissingClaim(String),

    /// Invalid claim value in token
    #[error("invalid claim value for {claim}: {reason}")]
    InvalidClaimValue { claim: String, reason: String },

    /// Underlying crypto error
    #[error("crypto error: {0}")]
    Crypto(#[from] defra_core::Error),
}
