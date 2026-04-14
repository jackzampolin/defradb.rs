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

    #[error("core error: {0}")]
    Core(#[from] defra_core::Error),

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

impl From<db_index::Error> for Error {
    fn from(err: db_index::Error) -> Self {
        match err {
            db_index::Error::Storage(e) => Error::Storage(e),
            db_index::Error::InvalidDocument(msg) => Error::InvalidDocument(msg),
            db_index::Error::Other(msg) => Error::Other(msg),
        }
    }
}

impl Error {
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

/// Result type for database operations.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn document_at_key_preserves_display_message() {
        let error = Error::document_at_key(
            b"doc-key",
            document::Error::CborDecode("bad cbor".to_string()),
        );

        assert!(matches!(error, Error::DocumentAtKey { .. }));
        assert_eq!(
            error.to_string(),
            "failed to deserialize document at key \"doc-key\": CBOR decode error: bad cbor"
        );
    }

    #[test]
    fn collection_schema_json_preserves_display_message() {
        let source = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let error = Error::collection_schema_json(
            "failed to deserialize schema for collection 'users'",
            source,
        );

        assert!(matches!(error, Error::CollectionSchemaJson { .. }));
        assert!(error
            .to_string()
            .starts_with("failed to deserialize schema for collection 'users': "));
    }

    #[test]
    fn lens_config_json_preserves_display_message() {
        let source = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let error = Error::lens_config_json("failed to serialize lens config", source);

        assert!(matches!(error, Error::LensConfigJson { .. }));
        assert!(error
            .to_string()
            .starts_with("failed to serialize lens config: "));
    }

    #[test]
    fn text_decode_preserves_display_message() {
        let source = String::from_utf8(vec![0x80]).unwrap_err();
        let error = Error::text_decode("invalid version encoding", source);

        assert!(matches!(error, Error::TextDecode { .. }));
        assert!(error.to_string().starts_with("invalid version encoding: "));
    }
}
