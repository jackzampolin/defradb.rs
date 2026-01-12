/// CoreKV error types matching the Go corekv implementation.
///
/// These errors represent the various failure modes that can occur
/// in key-value storage operations, transactions, and iteration.

use thiserror::Error;

/// Result type alias for CoreKV operations.
pub type Result<T> = std::result::Result<T, Error>;

/// CoreKV error types.
///
/// These errors match the error types defined in the Go corekv package
/// to maintain compatibility and consistent error handling.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum Error {
    /// Key was not found in the store.
    ///
    /// This is a normal condition and not necessarily an error in many cases.
    /// Callers should handle this gracefully.
    #[error("key not found")]
    NotFound,

    /// Attempted to use an empty key.
    ///
    /// Empty keys are not allowed as they would be ambiguous.
    #[error("empty key")]
    EmptyKey,

    /// Attempted to set a nil/null value.
    ///
    /// While Get operations can return None, Set operations should not
    /// accept None values. Use Delete instead to remove keys.
    #[error("value is nil")]
    ValueNil,

    /// Attempted to use a transaction that has already been discarded.
    ///
    /// Once a transaction is discarded, it cannot be used for any further operations.
    #[error("transaction has been discarded")]
    DiscardedTxn,

    /// Attempted to use a closed datastore.
    ///
    /// The database has been closed and no further operations can be performed.
    #[error("datastore is closed")]
    DBClosed,

    /// Transaction conflict detected.
    ///
    /// This occurs when two transactions attempt to modify the same keys concurrently.
    /// The transaction should typically be retried.
    #[error("transaction conflict, retry required")]
    TxnConflict,

    /// Attempted a write operation on a read-only transaction.
    ///
    /// Read-only transactions cannot perform Set or Delete operations.
    #[error("write attempted on read-only transaction")]
    ReadOnlyTxn,

    /// I/O error occurred during storage operation.
    ///
    /// This wraps underlying I/O errors from the storage backend.
    #[error("I/O error: {0}")]
    Io(String),

    /// Serialization error occurred.
    ///
    /// This wraps errors from serializing or deserializing data.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Iterator error occurred.
    ///
    /// This represents errors that occur during iteration.
    #[error("iterator error: {0}")]
    Iterator(String),

    /// Backend-specific error.
    ///
    /// This wraps errors specific to the storage backend (RocksDB, Memory, etc.).
    #[error("backend error: {0}")]
    Backend(String),

    /// Generic error with custom message.
    ///
    /// Used for errors that don't fit other categories.
    #[error("{0}")]
    Other(String),
}

impl Error {
    /// Check if this error indicates a key was not found.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound)
    }

    /// Check if this error indicates a transaction conflict.
    pub fn is_txn_conflict(&self) -> bool {
        matches!(self, Error::TxnConflict)
    }

    /// Check if this error indicates a closed database.
    pub fn is_db_closed(&self) -> bool {
        matches!(self, Error::DBClosed)
    }

    /// Check if this error is retriable.
    ///
    /// Currently only transaction conflicts are retriable.
    pub fn is_retriable(&self) -> bool {
        self.is_txn_conflict()
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err.to_string())
    }
}

impl From<serde_cbor::Error> for Error {
    fn from(err: serde_cbor::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<rocksdb::Error> for Error {
    fn from(err: rocksdb::Error) -> Self {
        // Check for specific RocksDB errors that map to CoreKV errors
        let err_str = err.to_string();
        if err_str.contains("Conflict") || err_str.contains("TryAgain") {
            Error::TxnConflict
        } else {
            Error::Backend(err_str)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_is_not_found() {
        assert!(Error::NotFound.is_not_found());
        assert!(!Error::EmptyKey.is_not_found());
    }

    #[test]
    fn test_error_is_txn_conflict() {
        assert!(Error::TxnConflict.is_txn_conflict());
        assert!(!Error::NotFound.is_txn_conflict());
    }

    #[test]
    fn test_error_is_retriable() {
        assert!(Error::TxnConflict.is_retriable());
        assert!(!Error::NotFound.is_retriable());
        assert!(!Error::DBClosed.is_retriable());
    }

    #[test]
    fn test_error_display() {
        assert_eq!(Error::NotFound.to_string(), "key not found");
        assert_eq!(Error::EmptyKey.to_string(), "empty key");
        assert_eq!(Error::ValueNil.to_string(), "value is nil");
        assert_eq!(Error::TxnConflict.to_string(), "transaction conflict, retry required");
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn test_error_clone() {
        let err = Error::NotFound;
        let cloned = err.clone();
        assert_eq!(err, cloned);
    }
}
