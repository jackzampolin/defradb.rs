//! Error types for DefraDB

use thiserror::Error;

/// Result type alias using DefraDB's Error type
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for DefraDB operations
#[derive(Debug, Error)]
pub enum Error {
    /// Storage-related errors
    #[error("storage error: {0}")]
    Storage(String),

    /// Serialization/deserialization errors
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Schema validation errors
    #[error("schema error: {0}")]
    Schema(String),

    /// Document not found
    #[error("document not found: {0}")]
    DocumentNotFound(String),

    /// Collection not found
    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    /// Invalid document ID
    #[error("invalid document ID: {0}")]
    InvalidDocumentId(String),

    /// CRDT merge errors
    #[error("merge error: {0}")]
    MergeError(String),

    /// Block-related errors
    #[error("block error: {0}")]
    BlockError(String),

    /// Network/P2P errors
    #[error("network error: {0}")]
    Network(String),

    /// Cryptographic errors
    #[error("crypto error: {0}")]
    Crypto(String),

    /// Query parsing or execution errors
    #[error("query error: {0}")]
    Query(String),

    /// Transaction errors
    #[error("transaction error: {0}")]
    Transaction(String),

    /// Access control errors
    #[error("access denied: {0}")]
    AccessDenied(String),

    /// Generic errors
    #[error("{0}")]
    Other(String),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Storage(err.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}
