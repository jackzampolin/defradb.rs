//! Transaction management for query execution.
//!
//! This module provides traits and types for executing queries within
//! transaction contexts, enabling ACID guarantees for multi-query operations.

use async_trait::async_trait;
use std::sync::Arc;

use crate::error::Result;
use crate::runner::DocFetcher;

/// Transaction context that provides storage access within a transaction.
///
/// This is implemented by the database layer to provide transaction-scoped
/// document fetching.
#[async_trait]
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
/// used by the query executor. Transaction IDs are strings to allow
/// flexibility in ID generation (UUIDs, sequential, etc.).
#[async_trait]
pub trait TransactionRegistry: Send + Sync {
    /// Begin a new transaction.
    ///
    /// Returns the transaction ID that can be used with `execute_in_txn`.
    async fn begin(&self, readonly: bool) -> Result<String>;

    /// Get an existing transaction by ID.
    ///
    /// Returns None if the transaction doesn't exist or has been committed/rolled back.
    fn get(&self, txn_id: &str) -> Option<Arc<dyn TransactionContext>>;

    /// Commit a transaction.
    ///
    /// After commit, the transaction ID is no longer valid.
    async fn commit(&self, txn_id: &str) -> Result<()>;

    /// Rollback a transaction.
    ///
    /// After rollback, the transaction ID is no longer valid.
    async fn rollback(&self, txn_id: &str) -> Result<()>;
}

/// A no-op transaction registry that doesn't support transactions.
///
/// This is used when transactions aren't needed or available.
#[derive(Debug, Clone, Default)]
pub struct NoOpTransactionRegistry;

#[async_trait]
impl TransactionRegistry for NoOpTransactionRegistry {
    async fn begin(&self, _readonly: bool) -> Result<String> {
        Err(crate::error::QueryError::execution(
            "transactions are not supported in this configuration",
        ))
    }

    fn get(&self, _txn_id: &str) -> Option<Arc<dyn TransactionContext>> {
        None
    }

    async fn commit(&self, _txn_id: &str) -> Result<()> {
        Err(crate::error::QueryError::execution(
            "transactions are not supported in this configuration",
        ))
    }

    async fn rollback(&self, _txn_id: &str) -> Result<()> {
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
        assert!(registry.get("any-id").is_none());
    }

    #[tokio::test]
    async fn test_noop_registry_commit_returns_error() {
        let registry = NoOpTransactionRegistry;
        let result = registry.commit("any-id").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_noop_registry_rollback_returns_error() {
        let registry = NoOpTransactionRegistry;
        let result = registry.rollback("any-id").await;
        assert!(result.is_err());
    }
}
