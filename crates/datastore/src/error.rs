use thiserror::Error;

/// Errors that can occur in the datastore module.
#[derive(Debug, Error)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] storage::Error),

    #[error("transaction already committed")]
    TxnAlreadyCommitted,

    #[error("transaction already discarded")]
    TxnAlreadyDiscarded,
}

/// Result type for datastore operations.
pub type Result<T> = std::result::Result<T, Error>;
