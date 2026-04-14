//! Error types for DefraDB

use std::{collections::TryReserveError, convert::Infallible};

use thiserror::Error;

/// Result type alias using DefraDB's Error type
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for DefraDB operations
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Standard IO errors
    #[error("storage error: {0}")]
    Io(#[from] std::io::Error),

    /// Storage-related errors
    #[error("storage error: {0}")]
    Storage(String),

    /// JSON serialization/deserialization errors
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// DAG-CBOR encoding errors
    #[error("serialization error: {0}")]
    DagCborEncode(#[from] serde_ipld_dagcbor::EncodeError<TryReserveError>),

    /// DAG-CBOR decoding errors
    #[error("serialization error: {0}")]
    DagCborDecode(#[from] serde_ipld_dagcbor::DecodeError<Infallible>),

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

    /// Invalid CID
    #[error("invalid CID: {0}")]
    InvalidCID(String),

    /// CID parsing/decoding errors
    #[error("invalid CID: {0}")]
    Cid(#[from] cid::Error),

    /// CRDT merge errors
    #[error("merge error: {0}")]
    MergeError(String),

    /// Block-related errors
    #[error("block error: {0}")]
    BlockError(String),

    /// IPLD conversion/traversal errors
    #[error("ipld error: {0}")]
    IpldError(String),

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

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn io_errors_preserve_their_type() {
        let err = std::io::Error::other("boom");
        let error: Error = err.into();
        assert!(matches!(error, Error::Io(_)));
        assert_eq!(error.to_string(), "storage error: boom");
    }

    #[test]
    fn json_errors_preserve_their_type() {
        let err = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let error: Error = err.into();
        assert!(matches!(error, Error::Json(_)));
        assert!(error.to_string().starts_with("serialization error: "));
    }

    #[test]
    fn cid_errors_preserve_stable_display_message() {
        let err = cid::Cid::try_from("not-a-cid").unwrap_err();
        let error: Error = err.into();
        assert!(matches!(error, Error::Cid(_)));
        assert!(error.to_string().starts_with("invalid CID: "));
    }
}
