//! Transaction management for query execution.
//!
//! This module provides traits and types for executing queries within
//! transaction contexts, enabling ACID guarantees for multi-query operations.

use async_trait::async_trait;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use crate::error::TransactionError;
use crate::runner::DocFetcher;

/// An opaque handle to an active transaction.
///
/// Handles should be obtained from `TransactionRegistry::begin()`. While the
/// `new()` constructor and `FromStr` implementation are public (for registry
/// implementors and HTTP deserialization), handles not registered with a
/// registry will fail validation when used with `get()`, `commit()`, or
/// `rollback()`.
///
/// The handle is serializable (implements `Display` and `FromStr`) for use in
/// HTTP APIs and other contexts where string serialization is needed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionHandle(String);

impl TransactionHandle {
    /// Create a new transaction handle.
    ///
    /// # For `TransactionRegistry` Implementors Only
    ///
    /// This constructor is intended for use by `TransactionRegistry::begin()`
    /// implementations. Application code should obtain handles through
    /// `TransactionRegistry::begin()`, not by direct construction.
    ///
    /// Handles created outside of a registry will fail when used with
    /// `get()`, `commit()`, or `rollback()` - the registry won't find them.
    ///
    /// # Panics
    ///
    /// Panics if `id` is empty. Transaction IDs must be non-empty strings.
    #[doc(hidden)]
    pub fn new(id: String) -> Self {
        assert!(!id.is_empty(), "transaction ID cannot be empty");
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
///
/// Returns an error if the string is empty, since transaction IDs must be non-empty.
impl std::str::FromStr for TransactionHandle {
    type Err = TransactionError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(TransactionError::execution(
                "transaction ID cannot be empty",
            ));
        }
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

    /// Check if the transaction is still active (not yet committed or rolled back).
    ///
    /// Returns `true` if the transaction can still be used for queries.
    /// Returns `false` if the transaction has been consumed via commit/rollback.
    ///
    /// # Implementation Note
    ///
    /// Concrete implementations SHOULD override this method if they track
    /// consumption state. The default returns `true`, which is appropriate
    /// for implementations that don't track state or where checking state
    /// synchronously isn't feasible (e.g., when state is behind an async mutex).
    fn is_active(&self) -> bool {
        true
    }
}

/// Result of looking up a transaction by handle.
pub enum GetTransactionResult {
    /// Transaction found.
    Found(Arc<dyn TransactionContext>),
    /// Transaction not found (never existed, or already committed/rolled back).
    NotFound,
    /// Lock is poisoned (a panic occurred elsewhere, system may be corrupted).
    LockPoisoned,
}

impl std::fmt::Debug for GetTransactionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Found(ctx) => f
                .debug_tuple("Found")
                .field(&format!("TransactionContext(id={})", ctx.id()))
                .finish(),
            Self::NotFound => f.write_str("NotFound"),
            Self::LockPoisoned => f.write_str("LockPoisoned"),
        }
    }
}

impl GetTransactionResult {
    /// Get the transaction context if found, logging an error if lock was poisoned.
    ///
    /// This method logs at ERROR level if `LockPoisoned` is encountered, since
    /// that indicates potential system corruption. Use `into_result()` if you
    /// need to handle `LockPoisoned` differently.
    pub fn ok(self) -> Option<Arc<dyn TransactionContext>> {
        match self {
            Self::Found(ctx) => Some(ctx),
            Self::NotFound => None,
            Self::LockPoisoned => {
                tracing::error!(
                    "Transaction registry lock poisoned during lookup - system may be corrupted"
                );
                None
            }
        }
    }

    /// Convert to a Result, treating NotFound as None and LockPoisoned as an error.
    ///
    /// Use this when you need to distinguish between "not found" and "lock poisoned"
    /// for proper error handling.
    pub fn into_result(
        self,
    ) -> std::result::Result<Option<Arc<dyn TransactionContext>>, TransactionError> {
        match self {
            Self::Found(ctx) => Ok(Some(ctx)),
            Self::NotFound => Ok(None),
            Self::LockPoisoned => Err(TransactionError::lock_poisoned(
                "transaction registry lock poisoned",
            )),
        }
    }

    /// Check if the transaction was found.
    pub fn is_found(&self) -> bool {
        matches!(self, Self::Found(_))
    }

    /// Check if the lock is poisoned.
    pub fn is_lock_poisoned(&self) -> bool {
        matches!(self, Self::LockPoisoned)
    }
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
    async fn begin(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError>;

    /// Get an existing transaction by handle.
    ///
    /// Returns `Found(context)` if the transaction exists,
    /// `NotFound` if it doesn't exist or has been committed/rolled back,
    /// or `LockPoisoned` if the registry lock is poisoned.
    fn get(&self, handle: &TransactionHandle) -> GetTransactionResult;

    /// Commit a transaction.
    ///
    /// After commit, the handle is no longer valid for `get()`.
    async fn commit(&self, handle: &TransactionHandle)
        -> std::result::Result<(), TransactionError>;

    /// Rollback a transaction.
    ///
    /// After rollback, the handle is no longer valid for `get()`.
    async fn rollback(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError>;
}

/// A no-op transaction registry that doesn't support transactions.
///
/// This is used when transactions aren't needed or available.
#[derive(Debug, Clone, Default)]
pub struct NoOpTransactionRegistry;

#[async_trait]
impl TransactionRegistry for NoOpTransactionRegistry {
    async fn begin(
        &self,
        _readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        Err(TransactionError::not_supported(
            "transactions are not supported in this configuration",
        ))
    }

    fn get(&self, _handle: &TransactionHandle) -> GetTransactionResult {
        GetTransactionResult::NotFound
    }

    async fn commit(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Err(TransactionError::not_supported(
            "transactions are not supported in this configuration",
        ))
    }

    async fn rollback(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Err(TransactionError::not_supported(
            "transactions are not supported in this configuration",
        ))
    }
}

/// A guard that ensures a transaction is properly finalized.
///
/// This type provides compile-time safety by consuming itself on `commit()` or
/// `rollback()`, preventing use-after-finalization bugs.
///
/// # Warning
///
/// If dropped without explicit `commit()` or `rollback()`, the guard will log
/// a warning but **cannot perform async rollback**. The transaction will be
/// leaked in the registry. Always ensure you call `commit()` or `rollback()`
/// explicitly before the guard goes out of scope.
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
    handle: Option<TransactionHandle>,
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
            // The transaction will remain in the registry until it times out
            // or is explicitly cleaned up.
            tracing::error!(
                txn_id = %handle,
                "TransactionGuard dropped without commit/rollback - transaction leaked! \
                 This is a BUG: always call commit() or rollback() explicitly."
            );
        }
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
    async fn test_noop_registry_get_returns_not_found() {
        let registry = NoOpTransactionRegistry;
        let handle: TransactionHandle = "any-id".parse().unwrap();
        assert!(matches!(
            registry.get(&handle),
            GetTransactionResult::NotFound
        ));
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
    fn test_transaction_handle_from_str_empty_returns_error() {
        let result: std::result::Result<TransactionHandle, _> = "".parse();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_get_transaction_result_into_result() {
        use std::sync::Arc;

        // Test Found case
        struct MockCtx;
        impl TransactionContext for MockCtx {
            fn id(&self) -> &str {
                "test"
            }
            fn is_readonly(&self) -> bool {
                false
            }
            fn doc_fetcher(&self) -> Arc<dyn crate::runner::DocFetcher> {
                unimplemented!()
            }
        }

        let result = GetTransactionResult::Found(Arc::new(MockCtx));
        let converted = result.into_result();
        assert!(converted.is_ok());
        assert!(converted.unwrap().is_some());

        // Test NotFound case
        let result = GetTransactionResult::NotFound;
        let converted = result.into_result();
        assert!(converted.is_ok());
        assert!(converted.unwrap().is_none());

        // Test LockPoisoned case
        let result = GetTransactionResult::LockPoisoned;
        let converted = result.into_result();
        assert!(converted.is_err());
    }

    #[test]
    fn test_transaction_handle_into_string() {
        let handle = TransactionHandle::new("txn-abc".to_string());
        let s: String = handle.into();
        assert_eq!(s, "txn-abc");
    }

    // Mock executor for testing TransactionGuard
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    struct MockExecutor {
        txn_counter: AtomicU64,
        committed: AtomicBool,
        rolled_back: AtomicBool,
    }

    impl MockExecutor {
        fn new() -> Self {
            Self {
                txn_counter: AtomicU64::new(0),
                committed: AtomicBool::new(false),
                rolled_back: AtomicBool::new(false),
            }
        }

        fn was_committed(&self) -> bool {
            self.committed.load(Ordering::SeqCst)
        }

        fn was_rolled_back(&self) -> bool {
            self.rolled_back.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl crate::QueryExecutor for MockExecutor {
        async fn execute(&self, _request: crate::QueryRequest) -> crate::QueryResponse {
            crate::QueryResponse::success(serde_json::json!({"mock": true}))
        }

        async fn execute_in_txn(
            &self,
            _request: crate::QueryRequest,
            _handle: &TransactionHandle,
        ) -> crate::QueryResponse {
            crate::QueryResponse::success(serde_json::json!({"in_txn": true}))
        }

        async fn begin_txn(
            &self,
            _readonly: bool,
        ) -> std::result::Result<TransactionHandle, TransactionError> {
            let id = self.txn_counter.fetch_add(1, Ordering::SeqCst);
            Ok(TransactionHandle::new(format!("mock-txn-{}", id)))
        }

        async fn commit_txn(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            self.committed.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn rollback_txn(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            self.rolled_back.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn schema(&self) -> crate::error::Result<String> {
            Ok("type Query { mock: String }".to_string())
        }
    }

    #[tokio::test]
    async fn test_guard_begin_creates_transaction() {
        let executor = MockExecutor::new();
        let guard = TransactionGuard::begin(&executor, false).await.unwrap();

        assert!(guard.handle().is_some());
        assert!(guard.handle().unwrap().as_str().starts_with("mock-txn-"));

        // Clean up
        guard.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn test_guard_execute_runs_in_transaction() {
        let executor = MockExecutor::new();
        let guard = TransactionGuard::begin(&executor, false).await.unwrap();

        let request = crate::QueryRequest::new("{ test }");
        let response = guard.execute(request).await;

        assert!(!response.has_errors());
        let data = response.data.unwrap();
        assert_eq!(data.get("in_txn").unwrap(), true);

        guard.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_guard_commit_consumes_guard() {
        let executor = MockExecutor::new();
        let guard = TransactionGuard::begin(&executor, false).await.unwrap();

        assert!(!executor.was_committed());
        guard.commit().await.unwrap();
        assert!(executor.was_committed());
        // guard is now consumed - can't use it anymore (compile-time enforced)
    }

    #[tokio::test]
    async fn test_guard_rollback_consumes_guard() {
        let executor = MockExecutor::new();
        let guard = TransactionGuard::begin(&executor, false).await.unwrap();

        assert!(!executor.was_rolled_back());
        guard.rollback().await.unwrap();
        assert!(executor.was_rolled_back());
        // guard is now consumed - can't use it anymore (compile-time enforced)
    }

    #[tokio::test]
    async fn test_guard_multiple_executes_before_commit() {
        let executor = MockExecutor::new();
        let guard = TransactionGuard::begin(&executor, false).await.unwrap();

        // Execute multiple queries in the same transaction
        for _ in 0..3 {
            let request = crate::QueryRequest::new("{ test }");
            let response = guard.execute(request).await;
            assert!(!response.has_errors());
        }

        guard.commit().await.unwrap();
        assert!(executor.was_committed());
    }

    #[tokio::test]
    async fn test_guard_drop_without_finalization_does_not_commit_or_rollback() {
        let executor = MockExecutor::new();

        // Create a guard but don't call commit() or rollback()
        {
            let _guard = TransactionGuard::begin(&executor, false).await.unwrap();
            // Guard is dropped here without finalization
        }

        // Verify that neither commit nor rollback was called
        assert!(
            !executor.was_committed(),
            "Dropping guard should not commit the transaction"
        );
        assert!(
            !executor.was_rolled_back(),
            "Dropping guard should not rollback (async not possible in Drop)"
        );
        // Note: The drop will log an error, but we can't easily verify that in tests
    }

    #[tokio::test]
    async fn test_guard_execute_after_commit_returns_error() {
        let executor = MockExecutor::new();
        let mut guard = TransactionGuard::begin(&executor, false).await.unwrap();

        // Take the handle to simulate commit having consumed it
        let handle = guard.handle.take();
        assert!(handle.is_some());

        // Now execute should return an error response
        let request = crate::QueryRequest::new("{ test }");
        let response = guard.execute(request).await;
        assert!(response.has_errors());
    }
}
