use thiserror::Error;

/// Errors that can occur in the database layer.
#[derive(Debug, Error)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] storage::Error),

    #[error("datastore error: {0}")]
    Datastore(#[from] datastore::Error),

    #[error("schema error: {0}")]
    Schema(#[from] schema::SchemaError),

    #[error("document error: {0}")]
    Document(#[from] document::Error),

    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    #[error("collection already exists: {0}")]
    CollectionAlreadyExists(String),

    #[error("document not found: {0}")]
    DocumentNotFound(String),

    #[error("invalid document: {0}")]
    InvalidDocument(String),

    #[error("transaction not active")]
    TxnNotActive,

    #[error("unsupported transaction type")]
    UnsupportedTxnType,

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("{0}")]
    Other(String),
}

/// Result type for database operations.
pub type Result<T> = std::result::Result<T, Error>;
