use crate::corekv::Error;

// Convert redb errors to our error type with context and classification
impl From<redb::Error> for Error {
    fn from(err: redb::Error) -> Self {
        match err {
            // I/O errors
            redb::Error::Io(io_err) => Error::Io(io_err.to_string()),
            redb::Error::PreviousIo => {
                tracing::error!("Previous I/O error - database must be closed and reopened");
                Error::Io(
                    "previous I/O error occurred - database must be closed and reopened".into(),
                )
            }

            // Critical: Database corruption
            redb::Error::Corrupted(ref msg) => {
                tracing::error!(message = %msg, "Database corruption detected");
                Error::Backend(format!(
                    "database corrupted: {}. Recovery options: \
                     (1) restore from backup, \
                     (2) run check_integrity() to assess damage extent, \
                     (3) delete database and resync from network peers (if available), \
                     (4) check disk for hardware errors. \
                     Consider preserving the corrupted file for forensic analysis before deletion.",
                    msg
                ))
            }

            // Lock poisoned: A thread panicked while holding a lock (fatal condition)
            redb::Error::LockPoisoned(location) => {
                tracing::error!(location = %location, "Lock poisoned - a thread panicked while holding a lock");
                Error::Backend(format!(
                    "internal error: lock poisoned at {} - database may be in undefined state",
                    location
                ))
            }

            // Database already open (useful for diagnosing lock issues)
            redb::Error::DatabaseAlreadyOpen => {
                tracing::warn!("Database is locked by another process");
                Error::Backend(
                    "database is locked by another process. \
                     Check for other running processes or stale lock files"
                        .into(),
                )
            }

            // Upgrade required (file format migration needed)
            redb::Error::UpgradeRequired(version) => {
                tracing::warn!(version = version, "Database file format upgrade required");
                Error::Backend(format!(
                    "database uses file format version {} which requires upgrade. \
                     Backup database and use redb migration tools",
                    version
                ))
            }

            // Transaction still in use (resource management issue, not a conflict)
            redb::Error::ReadTransactionStillInUse(_) => {
                tracing::warn!("Transaction still held by table or iterator");
                Error::Backend(
                    "transaction still in use - ensure all tables and iterators are dropped \
                     before committing or discarding the transaction"
                        .into(),
                )
            }

            // Table errors with useful context
            redb::Error::TableDoesNotExist(ref name) => {
                Error::Backend(format!("table '{}' does not exist", name))
            }
            redb::Error::TableTypeMismatch { ref table, .. } => {
                tracing::error!(table = %table, "Table type mismatch - possible schema corruption");
                Error::Backend(format!("table type mismatch for '{}': {}", table, err))
            }
            redb::Error::TableAlreadyOpen(ref name, location) => {
                tracing::warn!(table = %name, location = %location, "Table already open");
                Error::Backend(format!("table '{}' already open at {}", name, location))
            }

            // Value size limit
            redb::Error::ValueTooLarge(size) => {
                const MAX_VALUE_SIZE: usize = 3 * 1024 * 1024 * 1024; // 3 GiB
                Error::Backend(format!(
                    "value too large: {} bytes exceeds redb maximum of {} bytes (3 GiB). \
                     Consider chunking large values or using a different storage backend",
                    size, MAX_VALUE_SIZE
                ))
            }

            // Handle remaining variants (non-exhaustive enum)
            other => Error::Backend(format!("redb error: {}", other)),
        }
    }
}

impl From<redb::DatabaseError> for Error {
    fn from(err: redb::DatabaseError) -> Self {
        match err {
            redb::DatabaseError::DatabaseAlreadyOpen => {
                tracing::warn!("Database is locked by another process");
                Error::Backend(
                    "database is locked by another process. \
                     Check for other running processes or stale lock files"
                        .into(),
                )
            }
            redb::DatabaseError::UpgradeRequired(version) => {
                tracing::warn!(version = version, "Database file format upgrade required");
                Error::Backend(format!(
                    "database uses file format version {} which requires upgrade. \
                     Backup database and use redb migration tools",
                    version
                ))
            }
            redb::DatabaseError::RepairAborted => {
                tracing::warn!("Database repair was aborted");
                Error::Backend(
                    "database repair was aborted before completion. \
                     Database may be in inconsistent state - restore from backup recommended"
                        .into(),
                )
            }
            redb::DatabaseError::Storage(storage_err) => storage_err.into(),
            // Handle future variants (non-exhaustive enum)
            other => Error::Backend(format!("redb database error: {}", other)),
        }
    }
}

impl From<redb::TransactionError> for Error {
    fn from(err: redb::TransactionError) -> Self {
        match err {
            // Resource management issue, NOT a transaction conflict
            // This means a transaction is still held by a table or iterator
            redb::TransactionError::ReadTransactionStillInUse(_) => {
                tracing::warn!("Transaction still held by table or iterator");
                Error::Backend(
                    "transaction still in use - ensure all tables and iterators are dropped".into(),
                )
            }
            redb::TransactionError::Storage(storage_err) => storage_err.into(),
            // Handle future variants (non-exhaustive enum)
            other => Error::Backend(format!("redb transaction error: {}", other)),
        }
    }
}

impl From<redb::TableError> for Error {
    fn from(err: redb::TableError) -> Self {
        match err {
            redb::TableError::Storage(storage_err) => storage_err.into(),
            redb::TableError::TableDoesNotExist(ref name) => {
                Error::Backend(format!("table '{}' does not exist", name))
            }
            redb::TableError::TableTypeMismatch { ref table, .. } => {
                tracing::error!(table = %table, "Table type mismatch - possible schema corruption");
                Error::Backend(format!("table type mismatch for '{}': {}", table, err))
            }
            redb::TableError::TableAlreadyOpen(ref name, location) => {
                tracing::warn!(table = %name, location = %location, "Table already open");
                Error::Backend(format!("table '{}' already open at {}", name, location))
            }
            redb::TableError::TableIsMultimap(ref name) => {
                Error::Backend(format!("table '{}' is a multimap table", name))
            }
            redb::TableError::TableIsNotMultimap(ref name) => {
                Error::Backend(format!("table '{}' is not a multimap table", name))
            }
            redb::TableError::TableExists(ref name) => {
                Error::Backend(format!("table '{}' already exists", name))
            }
            // Handle future variants (non-exhaustive enum)
            other => Error::Backend(format!("redb table error: {}", other)),
        }
    }
}

impl From<redb::StorageError> for Error {
    fn from(err: redb::StorageError) -> Self {
        match err {
            redb::StorageError::Io(io_err) => Error::Io(io_err.to_string()),
            redb::StorageError::PreviousIo => {
                tracing::error!("Previous I/O error - database must be closed and reopened");
                Error::Io(
                    "previous I/O error occurred - database must be closed and reopened".into(),
                )
            }
            redb::StorageError::Corrupted(ref msg) => {
                tracing::error!(message = %msg, "Database corruption detected");
                Error::Backend(format!(
                    "database corrupted: {}. Recovery options: \
                     (1) restore from backup, \
                     (2) run check_integrity() to assess damage extent, \
                     (3) delete database and resync from network peers (if available), \
                     (4) check disk for hardware errors. \
                     Consider preserving the corrupted file for forensic analysis before deletion.",
                    msg
                ))
            }
            redb::StorageError::ValueTooLarge(size) => {
                // redb has a 3GB maximum value size
                const MAX_VALUE_SIZE: usize = 3 * 1024 * 1024 * 1024; // 3 GiB
                Error::Backend(format!(
                    "value too large: {} bytes exceeds redb maximum of {} bytes (3 GiB). \
                     Consider chunking large values or using a different storage backend",
                    size, MAX_VALUE_SIZE
                ))
            }
            // CRITICAL: LockPoisoned is NOT a transaction conflict!
            // It indicates a thread panicked while holding a lock - this is fatal.
            redb::StorageError::LockPoisoned(location) => {
                tracing::error!(location = %location, "Lock poisoned - a thread panicked while holding a lock");
                Error::Backend(format!(
                    "internal error: lock poisoned at {} - database may be in undefined state",
                    location
                ))
            }
            // Handle future variants (non-exhaustive enum)
            other => Error::Backend(format!("redb storage error: {}", other)),
        }
    }
}

impl From<redb::CommitError> for Error {
    fn from(err: redb::CommitError) -> Self {
        match err {
            // Delegate storage errors to the StorageError handler for consistent classification
            redb::CommitError::Storage(storage_err) => storage_err.into(),
            // Handle future variants (non-exhaustive enum)
            other => Error::Backend(format!("redb commit error: {}", other)),
        }
    }
}
