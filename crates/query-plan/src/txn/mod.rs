//! Transaction management for query execution.
//!
//! This module provides traits and types for executing queries within
//! transaction contexts, enabling ACID guarantees for multi-query operations.
//!
//! The `TransactionGuard` high-level API lives in the `query` crate because
//! it depends on the `QueryExecutor` trait which is defined at the runner
//! layer. Everything here is plan-layer primitives that are safe to expose
//! to plan nodes without creating a cycle with the runner.

mod context;
mod handle;
mod registry;
mod result;

// Re-export all public types
pub use context::{
    check_doc_access_with_overlay, current_deferred_acp_mutations, is_doc_registered_with_overlay,
    scope_deferred_acp_mutations, DeferredAcpMutations, TransactionContext,
};
pub use handle::TransactionHandle;
pub use registry::{NoOpTransactionRegistry, TransactionRegistry};
pub use result::GetTransactionResult;

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

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
}
