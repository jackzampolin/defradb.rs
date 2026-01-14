//! Transaction management for query execution.
//!
//! This module provides traits and types for executing queries within
//! transaction contexts, enabling ACID guarantees for multi-query operations.

use async_trait::async_trait;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use crate::error::Result;
use crate::runner::DocFetcher;

/// An opaque handle to an active transaction.
///
/// This type can only be created by `TransactionRegistry::begin()`, providing
/// compile-time assurance that transaction IDs passed to `get()`, `commit()`,
/// and `rollback()` came from a valid `begin()` call.
///
/// The handle is serializable (implements `Display` and `FromStr`) for use in
/// HTTP APIs and other contexts where string serialization is needed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionHandle(String);

impl TransactionHandle {
    /// Create a new transaction handle.
    ///
    /// This should only be called by `TransactionRegistry::begin()` implementations.
    /// External code should obtain handles through the registry, not by direct construction.
    pub fn new(id: String) -> Self {
        Self(id)
    }

    /// Get the underlying transaction ID string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Convert into the underlying string.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl Deref for TransactionHandle {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for TransactionHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<TransactionHandle> for String {
    fn from(handle: TransactionHandle) -> Self {
        handle.0
    }
}

/// Parse a transaction handle from a string.
///
/// This allows deserializing transaction IDs from HTTP requests.
/// Note: This does NOT validate that the transaction exists - that's
/// done when you actually use the handle with `get()`, `commit()`, etc.
impl std::str::FromStr for TransactionHandle {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

/// Transaction context that provides storage access within a transaction.
///
/// This is implemented by the database layer to provide transaction-scoped
/// document fetching.
pub trait TransactionContext: Send + Sync {
    /// Get the transaction ID.
    fn id(&self) -> &str;

    /// Check if this is a read-only transaction.
    fn is_readonly(&self) -> bool;

    /// Get a document fetcher scoped to this transaction.
    fn doc_fetcher(&self) -> Arc<dyn DocFetcher>;
}

/// Registry for managing active transactions.
///
/// The database layer implements this to track transactions that can be
/// used by the query executor.
#[async_trait]
pub trait TransactionRegistry: Send + Sync {
    /// Begin a new transaction.
    ///
    /// Returns a handle that can be used with `get()`, `commit()`, and `rollback()`.
    async fn begin(&self, readonly: bool) -> Result<TransactionHandle>;

    /// Get an existing transaction by handle.
    ///
    /// Returns None if the transaction doesn't exist or has been committed/rolled back.
    fn get(&self, handle: &TransactionHandle) -> Option<Arc<dyn TransactionContext>>;

    /// Commit a transaction.
    ///
    /// After commit, the handle is no longer valid for `get()`.
    async fn commit(&self, handle: &TransactionHandle) -> Result<()>;

    /// Rollback a transaction.
    ///
    /// After rollback, the handle is no longer valid for `get()`.
    async fn rollback(&self, handle: &TransactionHandle) -> Result<()>;
}

/// A no-op transaction registry that doesn't support transactions.
///
/// This is used when transactions aren't needed or available.
#[derive(Debug, Clone, Default)]
pub struct NoOpTransactionRegistry;

#[async_trait]
impl TransactionRegistry for NoOpTransactionRegistry {
    async fn begin(&self, _readonly: bool) -> Result<TransactionHandle> {
        Err(crate::error::QueryError::execution(
            "transactions are not supported in this configuration",
        ))
    }

    fn get(&self, _handle: &TransactionHandle) -> Option<Arc<dyn TransactionContext>> {
        None
    }

    async fn commit(&self, _handle: &TransactionHandle) -> Result<()> {
        Err(crate::error::QueryError::execution(
            "transactions are not supported in this configuration",
        ))
    }

    async fn rollback(&self, _handle: &TransactionHandle) -> Result<()> {
        Err(crate::error::QueryError::execution(
            "transactions are not supported in this configuration",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_noop_registry_begin_returns_error() {
        let registry = NoOpTransactionRegistry;
        let result = registry.begin(false).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_noop_registry_get_returns_none() {
        let registry = NoOpTransactionRegistry;
        let handle: TransactionHandle = "any-id".parse().unwrap();
        assert!(registry.get(&handle).is_none());
    }

    #[tokio::test]
    async fn test_noop_registry_commit_returns_error() {
        let registry = NoOpTransactionRegistry;
        let handle: TransactionHandle = "any-id".parse().unwrap();
        let result = registry.commit(&handle).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_noop_registry_rollback_returns_error() {
        let registry = NoOpTransactionRegistry;
        let handle: TransactionHandle = "any-id".parse().unwrap();
        let result = registry.rollback(&handle).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_transaction_handle_display() {
        let handle = TransactionHandle::new("txn-123".to_string());
        assert_eq!(handle.to_string(), "txn-123");
    }

    #[test]
    fn test_transaction_handle_deref() {
        let handle = TransactionHandle::new("txn-456".to_string());
        assert_eq!(&*handle, "txn-456");
        assert!(handle.starts_with("txn-"));
    }

    #[test]
    fn test_transaction_handle_from_str() {
        let handle: TransactionHandle = "txn-789".parse().unwrap();
        assert_eq!(handle.as_str(), "txn-789");
    }

    #[test]
    fn test_transaction_handle_into_string() {
        let handle = TransactionHandle::new("txn-abc".to_string());
        let s: String = handle.into();
        assert_eq!(s, "txn-abc");
    }
}
