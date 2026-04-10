use thiserror::Error;

/// Errors that can occur in the database layer.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] storage::Error),

    #[error("datastore error: {0}")]
    Datastore(#[from] datastore::Error),

    #[error("schema error: {0}")]
    Schema(#[from] schema::SchemaError),

    #[error("document error: {0}")]
    Document(#[from] document::Error),

    #[error("failed to deserialize document at key {key:?}: {source}")]
    DocumentAtKey {
        key: String,
        source: document::Error,
    },

    #[error("collection '{0}' not found")]
    CollectionNotFound(String),

    #[error("collection not found. collection version: {0}")]
    CollectionVersionNotFound(String),

    #[error("collection version ID can't be empty")]
    CollectionVersionIDEmpty,

    #[error("collection already exists. Name: {0}")]
    CollectionAlreadyExists(String),

    #[error("invalid patch: {0}")]
    InvalidPatch(String),

    #[error("document not found: {0}")]
    DocumentNotFound(String),

    #[error("invalid document: {0}")]
    InvalidDocument(String),

    #[error("invalid collection name: {0}")]
    InvalidCollectionName(String),

    #[error("database is closed")]
    DatabaseClosed,

    #[error("transaction not active")]
    TxnNotActive,

    #[error("explicit transaction must use force_commit/force_discard")]
    ExplicitTxnMustUseForce,

    #[error("unsupported transaction type")]
    UnsupportedTxnType,

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("query error: {0}")]
    Query(#[from] query::error::QueryError),

    #[error("transaction not found: {0}")]
    TransactionNotFound(String),

    #[error("transaction registry lock poisoned: {0}")]
    LockPoisoned(String),

    #[error(
        "cache update failed after successful commit for collection '{0}' - call reload_cache() or restart to recover"
    )]
    CacheUpdateFailedAfterCommit(String),

    #[error("{0}")]
    Other(String),

    #[error("unsafe policy transition blocked: {0}")]
    UnsafePolicyTransition(String),

    #[error("acp error: {0}")]
    Acp(String),

    #[error("lens error: {0}")]
    Lens(String),

    #[error("json patch error: {0}")]
    JsonPatch(#[from] crate::json_patch::JsonPatchError),
}

impl From<acp::Error> for Error {
    fn from(err: acp::Error) -> Self {
        Error::Acp(err.to_string())
    }
}

impl From<lens::Error> for Error {
    fn from(err: lens::Error) -> Self {
        Error::Lens(err.to_string())
    }
}

impl Error {
    pub fn document_at_key(key: &[u8], source: document::Error) -> Self {
        Self::DocumentAtKey {
            key: String::from_utf8_lossy(key).into_owned(),
            source,
        }
    }
}

/// Result type for database operations.
pub type Result<T> = std::result::Result<T, Error>;
