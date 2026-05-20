//! Transaction guard for compile-time safety.

use query_types::error::TransactionError;

use super::TransactionHandle;

/// A guard that ensures a transaction is properly finalized.
///
/// This type provides compile-time safety by consuming itself on `commit()` or
/// `rollback()`, preventing use-after-finalization bugs.
///
/// # Warning
///
/// If dropped without explicit `commit()` or `rollback()`, the guard will log
/// an error but **cannot perform async rollback**. The transaction will remain
/// in the registry until cleaned up by the idle transaction cleanup policy.
/// Always ensure you call `commit()` or `rollback()` explicitly before the
/// guard goes out of scope.
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
pub struct TransactionGuard<'a, E: crate::QueryExecutor + ?Sized> {
    executor: &'a E,
    pub(crate) handle: Option<TransactionHandle>,
}

impl<'a, E: crate::QueryExecutor + ?Sized> TransactionGuard<'a, E> {
    /// Begin a new transaction and return a guard for it.
    ///
    /// The returned guard must be explicitly finalized with `commit()` or `rollback()`.
    /// Dropping the guard without finalization will leak the transaction in the registry.
    #[must_use = "TransactionGuard must be explicitly committed or rolled back"]
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
        match self.handle.take() {
            Some(handle) => self.executor.commit_txn(&handle).await,
            None => Err(TransactionError::already_finalized(
                "transaction already finalized",
            )),
        }
    }

    /// Rollback the transaction and consume the guard.
    ///
    /// After calling this, the guard cannot be used again (compile-time enforced).
    pub async fn rollback(mut self) -> std::result::Result<(), TransactionError> {
        match self.handle.take() {
            Some(handle) => self.executor.rollback_txn(&handle).await,
            None => Err(TransactionError::already_finalized(
                "transaction already finalized",
            )),
        }
    }
}

impl<E: crate::QueryExecutor + ?Sized> Drop for TransactionGuard<'_, E> {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            // Transaction was not finalized - this is a bug in user code.
            // We can't do async rollback in drop, so we log an error.
            // The transaction will remain in the registry until idle cleanup
            // or explicit cleanup removes it.
            tracing::error!(
                txn_id = %handle,
                "TransactionGuard dropped without commit/rollback - transaction leaked! \
                 This is a BUG: always call commit() or rollback() explicitly."
            );
        }
    }
}
