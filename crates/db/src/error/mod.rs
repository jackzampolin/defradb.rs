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

    #[error("action already in progress. CollectionID: {collection_id}, Action: {action}")]
    ActionInProgress { collection_id: String, action: u16 },

    #[error("transaction not active")]
    TxnNotActive,

    #[error("explicit transaction must use force_commit/force_discard")]
    ExplicitTxnMustUseForce,

    #[error("transaction has unfinalized counter ops; commit via the transaction registry")]
    UnfinalizedCounterOps,

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("{context}: {source}")]
    CollectionSchemaJson {
        context: String,
        source: serde_json::Error,
    },

    #[error("{context}: {source}")]
    LensConfigJson {
        context: String,
        source: serde_json::Error,
    },

    #[error("{context}: {source}")]
    TextDecode {
        context: String,
        source: std::string::FromUtf8Error,
    },

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

    #[error("not authorized to perform operation. Permission: {permission}")]
    NotAuthorized { permission: String },

    #[error("lens error: {0}")]
    Lens(String),

    #[error("json patch error: {0}")]
    JsonPatch(#[from] crate::definition::patch::json::JsonPatchError),
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

impl From<db_index::Error> for Error {
    fn from(err: db_index::Error) -> Self {
        match err {
            db_index::Error::Storage(e) => Error::Storage(e),
            db_index::Error::InvalidDocument(msg) => Error::InvalidDocument(msg),
            other @ db_index::Error::VectorEntryPointNotFound { .. } => {
                Error::Other(other.to_string())
            }
            other @ db_index::Error::VectorDimensionMismatch { .. } => {
                Error::InvalidDocument(other.to_string())
            }
            db_index::Error::Other(msg) => Error::Other(msg),
        }
    }
}

impl Error {
    pub fn is_txn_conflict(&self) -> bool {
        matches!(self, Error::Storage(source) if source.is_txn_conflict())
            || matches!(
                self,
                Error::Datastore(datastore::Error::Storage(source))
                    if source.is_txn_conflict()
            )
    }

    pub fn is_unique_constraint_violation(&self) -> bool {
        matches!(self, Error::Storage(source) if source.is_unique_constraint_violation())
    }

    pub fn document_at_key(key: &[u8], source: document::Error) -> Self {
        Self::DocumentAtKey {
            key: String::from_utf8_lossy(key).into_owned(),
            source,
        }
    }

    pub fn collection_schema_json(context: impl Into<String>, source: serde_json::Error) -> Self {
        Self::CollectionSchemaJson {
            context: context.into(),
            source,
        }
    }

    pub fn text_decode(context: impl Into<String>, source: std::string::FromUtf8Error) -> Self {
        Self::TextDecode {
            context: context.into(),
            source,
        }
    }

    pub fn lens_config_json(context: impl Into<String>, source: serde_json::Error) -> Self {
        Self::LensConfigJson {
            context: context.into(),
            source,
        }
    }
}

pub fn index_write_query_error(operation: &str, error: Error) -> query::error::QueryError {
    if error.is_unique_constraint_violation() {
        query::error::QueryError::execution(storage::corekv::UNIQUE_CONSTRAINT_VIOLATION_MESSAGE)
    } else {
        query::error::QueryError::execution(format!("{operation} error: {error}"))
    }
}

pub fn commit_query_error(error: Error) -> query::error::QueryError {
    let message = format!("commit error: {error}");
    if error.is_txn_conflict() {
        query::error::QueryError::transaction_conflict(message)
    } else {
        query::error::QueryError::execution(message)
    }
}

/// Result type for database operations.
pub type Result<T> = std::result::Result<T, Error>;
