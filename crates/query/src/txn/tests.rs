//! Unit tests for the transaction layer: `TransactionGuard` lifecycle and the
//! plan-layer transaction primitives.

use super::*;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

// Mock executor for testing TransactionGuard
use crate::error::TransactionError;

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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

/// Mock executor that fails on commit (for testing error handling)
struct FailingCommitExecutor {
    txn_counter: AtomicU64,
}

impl FailingCommitExecutor {
    fn new() -> Self {
        Self {
            txn_counter: AtomicU64::new(0),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl crate::QueryExecutor for FailingCommitExecutor {
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
        Ok(TransactionHandle::new(format!("failing-txn-{}", id)))
    }

    async fn commit_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Err(TransactionError::execution("simulated storage failure"))
    }

    async fn rollback_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
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
}

#[tokio::test]
async fn test_guard_rollback_consumes_guard() {
    let executor = MockExecutor::new();
    let guard = TransactionGuard::begin(&executor, false).await.unwrap();
    assert!(!executor.was_rolled_back());
    guard.rollback().await.unwrap();
    assert!(executor.was_rolled_back());
}

#[tokio::test]
async fn test_guard_multiple_executes_before_commit() {
    let executor = MockExecutor::new();
    let guard = TransactionGuard::begin(&executor, false).await.unwrap();

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
    {
        let _guard = TransactionGuard::begin(&executor, false).await.unwrap();
    }
    assert!(
        !executor.was_committed(),
        "Dropping guard should not commit the transaction"
    );
    assert!(
        !executor.was_rolled_back(),
        "Dropping guard should not rollback (async not possible in Drop)"
    );
}

#[tokio::test]
async fn test_guard_execute_after_commit_returns_error() {
    let executor = MockExecutor::new();
    let mut guard = TransactionGuard::begin(&executor, false).await.unwrap();
    let handle = guard.handle.take();
    assert!(handle.is_some());

    let request = crate::QueryRequest::new("{ test }");
    let response = guard.execute(request).await;
    assert!(response.has_errors());
}

#[tokio::test]
async fn test_guard_commit_failure_returns_error_and_consumes_handle() {
    let executor = FailingCommitExecutor::new();
    let guard = TransactionGuard::begin(&executor, false).await.unwrap();
    assert!(guard.handle().is_some());

    let result = guard.commit().await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("simulated storage failure"));
}

#[tokio::test]
async fn test_guard_commit_failure_is_retryable() {
    let executor = FailingCommitExecutor::new();
    let guard = TransactionGuard::begin(&executor, false).await.unwrap();

    let result = guard.commit().await;
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert!(
        err.is_retryable(),
        "Storage execution errors should be retryable with a new transaction"
    );
}

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
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl TransactionContext for MockCtx {
        fn id(&self) -> &str {
            "test"
        }
        fn is_readonly(&self) -> bool {
            false
        }
        fn doc_fetcher(&self) -> Arc<dyn crate::fetcher::DocFetcher> {
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
