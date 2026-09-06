//! Owning transaction guard with synchronous cancellation cleanup.

use crate::error::TransactionError;

use super::TransactionHandle;

/// A guard that ensures a transaction is properly finalized.
///
/// This type provides compile-time safety by consuming itself on `commit()` or
/// `rollback()`, preventing use-after-finalization bugs.
///
/// Dropping the guard removes the registry entry without committing, including
/// when its task is aborted or the async runtime has stopped. Cloned handles
/// are non-owning IDs and do not keep the entry alive. In-flight context users
/// may delay storage release until they also finish or are dropped.
///
/// Cancellation before finalization discards uncommitted writes. Once a commit
/// becomes durable it cannot be undone, even if cancellation interrupts its
/// post-commit callbacks. A cancelled commit therefore has an unknown outcome;
/// do not blindly retry non-idempotent writes. A competing explicit finalizer
/// that already removed the entry owns that outcome.
///
/// # Example
///
/// ```ignore
/// use query::{QueryExecutor, QueryRequest, TransactionGuard};
/// use query::error::TransactionError;
///
/// async fn batch_operations<E: QueryExecutor>(
///     executor: &E,
///     queries: Vec<QueryRequest>,
/// ) -> Result<Vec<QueryResponse>, TransactionError> {
///     let guard = TransactionGuard::begin(executor, false).await?;
///     let mut responses = Vec::new();
///
///     for query in queries {
///         let resp = guard.execute(query).await;
///         if resp.has_errors() {
///             guard.rollback().await?;  // Consumes guard, rolls back transaction
///             return Err(TransactionError::execution("query failed"));
///         }
///         responses.push(resp);
///     }
///
///     guard.commit().await?;  // Consumes guard, commits transaction
///     Ok(responses)
/// }
/// ```
#[must_use = "dropping the guard abandons the transaction"]
pub struct TransactionGuard<'a, E: crate::QueryExecutor + ?Sized> {
    executor: &'a E,
    pub(crate) handle: Option<TransactionHandle>,
}

impl<'a, E: crate::QueryExecutor + ?Sized> TransactionGuard<'a, E> {
    /// Begin a new transaction and return a guard for it.
    ///
    /// Dropping the returned guard abandons the transaction.
    pub async fn begin(
        executor: &'a E,
        readonly: bool,
    ) -> std::result::Result<Self, TransactionError> {
        let handle = executor.begin_txn(readonly).await?;
        Ok(Self {
            executor,
            handle: Some(handle),
        })
    }

    /// Execute a query within this transaction.
    pub async fn execute(&self, request: crate::QueryRequest) -> crate::QueryResponse {
        match &self.handle {
            Some(handle) => self.executor.execute_in_txn(request, handle).await,
            None => crate::QueryResponse::error("transaction already finalized"),
        }
    }

    /// Get the transaction handle for inspection (e.g., logging).
    pub fn handle(&self) -> Option<&TransactionHandle> {
        self.handle.as_ref()
    }

    /// Commit the transaction and consume the guard.
    ///
    /// After calling this, the guard cannot be used again (compile-time enforced).
    pub async fn commit(mut self) -> std::result::Result<(), TransactionError> {
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| TransactionError::already_finalized("transaction already finalized"))?;
        self.executor.commit_txn(handle).await?;
        self.handle = None;
        Ok(())
    }

    /// Rollback the transaction and consume the guard.
    ///
    /// After calling this, the guard cannot be used again (compile-time enforced).
    pub async fn rollback(mut self) -> std::result::Result<(), TransactionError> {
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| TransactionError::already_finalized("transaction already finalized"))?;
        self.executor.rollback_txn(handle).await?;
        self.handle = None;
        Ok(())
    }
}

impl<E: crate::QueryExecutor + ?Sized> Drop for TransactionGuard<'_, E> {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            self.executor.abandon_txn(handle);
        }
    }
}
