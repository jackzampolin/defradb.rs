//! Integration tests for transaction support in the query module.
//!
//! These tests verify the full transaction flow from QueryRunner through
//! DbTransactionRegistry to real storage.

use std::sync::Arc;

use db::{DbTransactionRegistry, DB};
use query::executor::{QueryExecutor, QueryRequest};
use query::runner::QueryRunner;
use query::txn::TransactionGuard;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;

/// Create a test schema with a Users collection.
fn test_schema() -> Vec<CollectionVersion> {
    vec![CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    )]
}

#[tokio::test]
async fn test_transaction_begin_commit_flow() {
    // Setup: create DB, registry, and query runner
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());

    // For this test we need a fetcher that can read from the DB.
    // Since we're testing transactions, we'll use the registry's transaction-scoped fetcher.
    // Create a no-data fetcher for non-transactional queries (our tests use transactions).
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    // Begin a transaction
    let txn_handle = runner.begin_txn(false).await.unwrap();
    assert!(txn_handle.as_str().starts_with("txn-"));

    // Commit the transaction
    let commit_result = runner.commit_txn(&txn_handle).await;
    assert!(commit_result.is_ok());
}

#[tokio::test]
async fn test_transaction_begin_rollback_flow() {
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    // Begin a transaction
    let txn_handle = runner.begin_txn(false).await.unwrap();

    // Rollback the transaction
    let rollback_result = runner.rollback_txn(&txn_handle).await;
    assert!(rollback_result.is_ok());
}

#[tokio::test]
async fn test_transaction_double_commit_fails() {
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    let txn_handle = runner.begin_txn(false).await.unwrap();

    // First commit should succeed
    runner.commit_txn(&txn_handle).await.unwrap();

    // Second commit should fail - transaction no longer exists
    let second_commit = runner.commit_txn(&txn_handle).await;
    assert!(second_commit.is_err());
    assert!(second_commit.unwrap_err().to_string().contains("not found"));
}

#[tokio::test]
async fn test_transaction_commit_after_rollback_fails() {
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    let txn_handle = runner.begin_txn(false).await.unwrap();

    // Rollback
    runner.rollback_txn(&txn_handle).await.unwrap();

    // Commit should fail
    let commit_result = runner.commit_txn(&txn_handle).await;
    assert!(commit_result.is_err());
}

#[tokio::test]
async fn test_execute_query_in_transaction() {
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    let txn_handle = runner.begin_txn(false).await.unwrap();

    // Execute a query in the transaction
    // Even though there's no data, the query should succeed (return empty array)
    let request = QueryRequest::new("{ Users { name age } }");
    let response = runner.execute_in_txn(request, &txn_handle).await;

    assert!(
        !response.has_errors(),
        "Query should succeed, got errors: {:?}",
        response.errors
    );
    assert!(response.data.is_some());

    let data = response.data.unwrap();
    let users = data.get("Users").unwrap();
    assert!(users.is_array());
    assert_eq!(users.as_array().unwrap().len(), 0);

    runner.commit_txn(&txn_handle).await.unwrap();
}

#[tokio::test]
async fn test_execute_multiple_queries_in_transaction() {
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    let txn_handle = runner.begin_txn(false).await.unwrap();

    // Execute multiple queries in the same transaction
    for i in 0..3 {
        let request = QueryRequest::new("{ Users { name } }");
        let response = runner.execute_in_txn(request, &txn_handle).await;
        assert!(!response.has_errors(), "Query {} should succeed", i);
    }

    runner.commit_txn(&txn_handle).await.unwrap();
}

#[tokio::test]
async fn test_query_on_nonexistent_transaction_fails() {
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    let fake_handle: query::txn::TransactionHandle = "fake-txn-123".parse().unwrap();

    let request = QueryRequest::new("{ Users { name } }");
    let response = runner.execute_in_txn(request, &fake_handle).await;

    assert!(response.has_errors());
    assert!(response.errors[0].message.contains("not found"));
}

#[tokio::test]
async fn test_query_after_commit_fails() {
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    let txn_handle = runner.begin_txn(false).await.unwrap();
    runner.commit_txn(&txn_handle).await.unwrap();

    // Query should fail - transaction is committed
    let request = QueryRequest::new("{ Users { name } }");
    let response = runner.execute_in_txn(request, &txn_handle).await;

    assert!(response.has_errors());
    assert!(response.errors[0].message.contains("not found"));
}

#[tokio::test]
async fn test_query_after_rollback_fails() {
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    let txn_handle = runner.begin_txn(false).await.unwrap();
    runner.rollback_txn(&txn_handle).await.unwrap();

    // Query should fail - transaction is rolled back
    let request = QueryRequest::new("{ Users { name } }");
    let response = runner.execute_in_txn(request, &txn_handle).await;

    assert!(response.has_errors());
    assert!(response.errors[0].message.contains("not found"));
}

#[tokio::test]
async fn test_readonly_transaction_flag() {
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    // Create readonly transaction
    let readonly_handle = runner.begin_txn(true).await.unwrap();

    // Execute a query (read operations should work)
    let request = QueryRequest::new("{ Users { name } }");
    let response = runner.execute_in_txn(request, &readonly_handle).await;
    assert!(!response.has_errors());

    runner.commit_txn(&readonly_handle).await.unwrap();
}

#[tokio::test]
async fn test_transaction_guard_commit() {
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    // Use TransactionGuard for safe transaction management
    let guard = TransactionGuard::begin(&runner, false).await.unwrap();

    // Execute a query
    let request = QueryRequest::new("{ Users { name } }");
    let response = guard.execute(request).await;
    assert!(!response.has_errors());

    // Commit via guard (consumes the guard)
    guard.commit().await.unwrap();
}

#[tokio::test]
async fn test_transaction_guard_rollback() {
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    let guard = TransactionGuard::begin(&runner, false).await.unwrap();

    let request = QueryRequest::new("{ Users { name } }");
    let response = guard.execute(request).await;
    assert!(!response.has_errors());

    // Rollback via guard
    guard.rollback().await.unwrap();
}

#[tokio::test]
async fn test_concurrent_transactions() {
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    // Begin multiple transactions
    let txn1 = runner.begin_txn(false).await.unwrap();
    let txn2 = runner.begin_txn(false).await.unwrap();
    let txn3 = runner.begin_txn(true).await.unwrap();

    // All should be distinct
    assert_ne!(txn1.as_str(), txn2.as_str());
    assert_ne!(txn2.as_str(), txn3.as_str());
    assert_ne!(txn1.as_str(), txn3.as_str());

    // All should be usable
    let req = QueryRequest::new("{ Users { name } }");
    assert!(!runner.execute_in_txn(req.clone(), &txn1).await.has_errors());
    assert!(!runner.execute_in_txn(req.clone(), &txn2).await.has_errors());
    assert!(!runner.execute_in_txn(req.clone(), &txn3).await.has_errors());

    // Clean up
    runner.commit_txn(&txn1).await.unwrap();
    runner.rollback_txn(&txn2).await.unwrap();
    runner.commit_txn(&txn3).await.unwrap();
}

#[tokio::test]
async fn test_query_error_does_not_invalidate_transaction() {
    let db = Arc::new(DB::new(MemoryStore::new()));
    let registry = DbTransactionRegistry::new(db.clone(), test_schema());
    let fetcher = query::test_utils::MockFetcher::new();
    let runner = QueryRunner::with_registry(fetcher, test_schema(), registry);

    let txn_handle = runner.begin_txn(false).await.unwrap();

    // Execute an invalid query (unknown collection)
    let bad_request = QueryRequest::new("{ NonExistentCollection { name } }");
    let bad_response = runner.execute_in_txn(bad_request, &txn_handle).await;
    assert!(bad_response.has_errors());

    // Transaction should still be valid - execute a good query
    let good_request = QueryRequest::new("{ Users { name } }");
    let good_response = runner.execute_in_txn(good_request, &txn_handle).await;
    assert!(
        !good_response.has_errors(),
        "Transaction should still be valid after query error"
    );

    // Should be able to commit
    runner.commit_txn(&txn_handle).await.unwrap();
}
