//! Errors raised by index maintenance.

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
