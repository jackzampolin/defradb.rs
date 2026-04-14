//! Unified error type for DefraDB.
//!
//! Shared base error used by db, query, and extracted subsystem crates
//! (db-backup, db-index, db-txn, db-commits-fetcher, db-lensed-fetcher,
//! db-patch, etc.). Instead of every extracted crate carrying its own
//! narrow `Error` enum + `From` conversion boilerplate, they all use
//! [`Error`] from this crate.
//!
//! `db::Error` and `query_types::QueryError` are separate, more
//! specialized errors that carry variants genuinely unique to those
//! layers. They each provide a `From<defra_errors::Error>` impl so
//! subsystem errors thread up into the broader hierarchy cleanly.
//!
//! # Design notes
//!
//! - Foundation errors (storage, datastore, schema, document, defra-core)
//!   are brought in via `#[from]` so the `?` operator works across crate
//!   boundaries without manual mapping.
//! - Common free-form variants (`InvalidDocument`, `NotFound`,
//!   `AlreadyExists`, `Other`) live here so multiple extracted subsystems
//!   can reuse them.
//! - The enum is `#[non_exhaustive]` so new variants can be added
//!   without breaking downstream matches that use `_` wildcards.

use thiserror::Error;

/// Result alias used across the DefraDB crates that consume
/// [`Error`] directly.
pub type Result<T> = std::result::Result<T, Error>;

/// Unified error type shared across DefraDB subsystem crates.
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

    #[error("invalid document: {0}")]
    InvalidDocument(String),

    #[error("document not found: {0}")]
    DocumentNotFound(String),

    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("{0}")]
    Other(String),
}

impl Error {
    /// Construct an `InvalidDocument` error from any displayable value.
    pub fn invalid_document(msg: impl Into<String>) -> Self {
        Self::InvalidDocument(msg.into())
    }

    /// Construct a `DocumentNotFound` error from any displayable value.
    pub fn document_not_found(msg: impl Into<String>) -> Self {
        Self::DocumentNotFound(msg.into())
    }

    /// Construct a `CollectionNotFound` error from any displayable value.
    pub fn collection_not_found(msg: impl Into<String>) -> Self {
        Self::CollectionNotFound(msg.into())
    }

    /// Construct a `NotFound` error from any displayable value.
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }

    /// Construct an `AlreadyExists` error from any displayable value.
    pub fn already_exists(msg: impl Into<String>) -> Self {
        Self::AlreadyExists(msg.into())
    }

    /// Construct an `InvalidArgument` error from any displayable value.
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Self::InvalidArgument(msg.into())
    }

    /// Construct an `Other` free-form error from any displayable value.
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_storage_error_display_matches_inner() {
        let inner = storage::Error::Other("disk on fire".to_string());
        let expected = format!("storage error: {}", inner);
        let wrapped: Error = inner.into();
        assert_eq!(wrapped.to_string(), expected);
    }

    #[test]
    fn invalid_document_constructor_roundtrip() {
        let err = Error::invalid_document("missing _docID");
        assert!(matches!(err, Error::InvalidDocument(_)));
        assert_eq!(err.to_string(), "invalid document: missing _docID");
    }

    #[test]
    fn not_found_constructor_roundtrip() {
        let err = Error::not_found("txn foo");
        assert!(matches!(err, Error::NotFound(_)));
        assert_eq!(err.to_string(), "not found: txn foo");
    }

    #[test]
    fn wrapper_chains_inner_source() {
        let inner = document::Error::CborDecode("bad cbor".to_string());
        let wrapped: Error = inner.into();
        // thiserror's #[from] preserves the source chain; std::error::Error::source() returns Some.
        use std::error::Error as _;
        assert!(wrapped.source().is_some());
    }
}
