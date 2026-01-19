//! Error types for the ACP crate

use thiserror::Error;

/// Result type alias for ACP operations
pub type Result<T> = std::result::Result<T, Error>;

/// ACP-specific error types
#[derive(Debug, Error)]
pub enum Error {
    /// Permission denied for the requested operation
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// Document is not registered with ACP
    #[error("document not registered: {0}")]
    DocumentNotRegistered(String),

    /// Document is already registered with ACP
    #[error("document already registered: {0}")]
    DocumentAlreadyRegistered(String),

    /// Invalid relation name
    #[error("invalid relation: {0}")]
    InvalidRelation(String),

    /// Not the owner of the document
    #[error("not document owner: only owner can {operation}")]
    NotOwner { operation: String },

    /// Invalid policy configuration
    #[error("invalid policy: {0}")]
    InvalidPolicy(String),

    /// Storage operation failed
    #[error("storage error: {0}")]
    Storage(String),

    /// Serialization/deserialization error
    #[error("serialization error: {0}")]
    Serialization(String),
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}
