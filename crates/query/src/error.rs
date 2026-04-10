//! Query error types

use thiserror::Error;

pub use query_model::error::{QueryError, Result};

/// Error type for transaction operations.
///
/// This provides structured errors for transaction lifecycle operations
/// (`begin_txn`, `commit_txn`, `rollback_txn`) enabling callers to handle
/// different error conditions appropriately.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TransactionError {
    /// The transaction was not found (may have been committed/rolled back).
    #[error("transaction not found: {0}")]
    NotFound(String),

    /// The transaction was already finalized (double commit/rollback).
    #[error("transaction already finalized: {0}")]
    AlreadyFinalized(String),

    /// Transactions are not supported in this configuration.
    #[error("transactions not supported: {0}")]
    NotSupported(String),

    /// A storage or execution error occurred.
    #[error("transaction error: {0}")]
    Execution(String),

    /// The transaction registry lock is poisoned (indicates a panic elsewhere).
    #[error("lock poisoned: {0}")]
    LockPoisoned(String),
}

impl TransactionError {
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }

    pub fn already_finalized(msg: impl Into<String>) -> Self {
        Self::AlreadyFinalized(msg.into())
    }

    pub fn not_supported(msg: impl Into<String>) -> Self {
        Self::NotSupported(msg.into())
    }

    pub fn execution(msg: impl Into<String>) -> Self {
        Self::Execution(msg.into())
    }

    pub fn lock_poisoned(msg: impl Into<String>) -> Self {
        Self::LockPoisoned(msg.into())
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            Self::NotFound(_) => false,
            Self::AlreadyFinalized(_) => false,
            Self::NotSupported(_) => false,
            Self::Execution(_) => true,
            Self::LockPoisoned(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = QueryError::parse("unexpected token");
        assert_eq!(err.to_string(), "parse error: unexpected token");

        let err = QueryError::unknown_field("foo");
        assert_eq!(err.to_string(), "foo");

        let err = QueryError::TypeMismatch {
            expected: "String".into(),
            actual: "Int".into(),
        };
        assert_eq!(err.to_string(), "type mismatch: expected String, got Int");
    }

    #[test]
    fn test_transaction_error_is_retryable() {
        assert!(!TransactionError::not_found("test").is_retryable());
        assert!(!TransactionError::already_finalized("test").is_retryable());
        assert!(!TransactionError::not_supported("test").is_retryable());
        assert!(TransactionError::execution("test").is_retryable());
        assert!(!TransactionError::lock_poisoned("test").is_retryable());
    }
}
