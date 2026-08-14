//! Error type for the index manager.
//!
//! Narrow subset of what `db::Error` covers — index_manager only
//! produces storage errors, invalid-document errors, and generic
//! index-validation errors. A `From<db_index::Error> for db::Error`
//! impl lives in the `db` crate so callers that still thread errors
//! through the broader hierarchy get automatic conversion.

use thiserror::Error;

/// Result alias used throughout the index_manager module.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur during index operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] storage::Error),

    #[error("invalid document: {0}")]
    InvalidDocument(String),

    #[error("vector index entry point {entry_point} is not in the store")]
    VectorEntryPointNotFound { entry_point: u64 },

    #[error("vector index holds {indexed}-dimension vectors, got {got}")]
    VectorDimensionMismatch { indexed: usize, got: usize },

    #[error("{0}")]
    Other(String),
}
