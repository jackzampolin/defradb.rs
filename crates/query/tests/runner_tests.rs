//! Tests for the QueryRunner module

use acp::DocumentACP;
use async_trait::async_trait;
use document::Document;
use identity::Did;
use query::error::{QueryError, Result};
use query::executor::{QueryExecutor, QueryRequest};
use query::mutator::{CreateResult, DeleteResult, DocMutator, UpdateResult};
use query::runner::{DocFetcher, FetchByIdsResult, QueryRunner};
use query::test_utils::{MockFetcher, MockTxnRegistry};
use query::txn::TransactionHandle;
use schema::{CollectionVersion, FieldDescription, FieldKind, PolicyDescription};
use std::sync::{Arc, Mutex};

// =============================================================================
// Test Utilities
// =============================================================================

/// Mock mutator for testing
struct MockMutator {
    docs: Mutex<Vec<(String, Document)>>,
}

impl MockMutator {
    fn new() -> Self {
        Self {
            docs: Mutex::new(Vec::new()),
        }
    }

    fn created_docs(&self) -> Vec<(String, Document)> {
        self.docs.lock().unwrap().clone()
    }

    fn add_doc(&self, collection: &str, doc: Document) {
        self.docs
            .lock()
            .unwrap()
            .push((collection.to_string(), doc));
    }
}

#[async_trait]
impl DocMutator for MockMutator {
    async fn create(&self, collection_name: &str, mut doc: Document) -> Result<CreateResult> {
        doc.generate_and_set_doc_id()
            .map_err(|e| QueryError::execution(format!("Failed to generate DocID: {}", e)))?;

        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| QueryError::execution("Document should have ID after generation"))?;

        self.docs
            .lock()
            .unwrap()
            .push((collection_name.to_string(), doc.clone()));

        Ok(CreateResult::new(doc_id, doc))
    }

    async fn update(&self, _collection_name: &str, doc: Document) -> Result<UpdateResult> {
        let modified = doc.values().len();
        Ok(UpdateResult::new(doc, modified))
    }

    async fn delete(
        &self,
        _collection_name: &str,
        doc_id: &document::DocID,
    ) -> Result<DeleteResult> {
        let mut docs = self.docs.lock().unwrap();
        let existed = docs
            .iter()
            .position(|(_, d)| d.id().map(|id| id.to_string()) == Some(doc_id.to_string()))
            .map(|i| docs.remove(i))
            .is_some();
        Ok(DeleteResult::new(doc_id.clone(), existed))
    }

    async fn exists(&self, _collection_name: &str, doc_id: &document::DocID) -> Result<bool> {
        let docs = self.docs.lock().unwrap();
        Ok(docs
            .iter()
            .any(|(_, d)| d.id().map(|id| id.to_string()) == Some(doc_id.to_string())))
    }

    async fn get_for_update(
        &self,
        _collection_name: &str,
        doc_id: &document::DocID,
    ) -> Result<Option<Document>> {
        let docs = self.docs.lock().unwrap();
        Ok(docs
            .iter()
            .find(|(_, d)| d.id().map(|id| id.to_string()) == Some(doc_id.to_string()))
            .map(|(_, d)| d.clone()))
    }
}

/// Mock fetcher that returns errors
struct FailingFetcher;

#[async_trait]
impl DocFetcher for FailingFetcher {
    async fn get_all(&self, _collection_name: &str) -> Result<Vec<Document>> {
        Err(QueryError::execution("storage failure"))
    }

    async fn get_by_ids(
        &self,
        _collection_name: &str,
        _doc_ids: &[String],
    ) -> Result<FetchByIdsResult> {
        Err(QueryError::execution("storage failure"))
    }

    async fn get_by_field_value(
        &self,
        _collection_name: &str,
        _field_name: &str,
        _value: &str,
    ) -> Result<Vec<Document>> {
        Err(QueryError::execution("storage failure"))
    }
}

fn make_test_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v1",
        "coll-1",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    )
}

fn make_acp_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v1",
        "coll-acp",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    )
    .with_policy(PolicyDescription::new("policy-acp", "Users"))
}

fn test_acp_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}

fn test_acp_did2() -> Did {
    Did::new("did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR").unwrap()
}

// =============================================================================
// Basic Query Tests
// =============================================================================

#[tokio::test]
async fn test_execute_simple_query() {
    let fetcher = MockFetcher::new();

    let mut doc = Document::new();
    doc.set("name", "Alice");
    doc.set("age", 30i64);
    doc.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query("{ Users { name age } }")
        .await
        .unwrap();

    assert!(result.is_object());
    let users = result.get("Users").unwrap();
    assert!(users.is_array());
    let arr = users.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0].get("name").unwrap(), "Alice");
    assert_eq!(arr[0].get("age").unwrap(), 30);
}

#[tokio::test]
async fn test_execute_query_with_docid() {
    let fetcher = MockFetcher::new();

    let mut doc = Document::new();
    doc.set("name", "Bob");
    doc.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query("{ Users { _docID name } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert!(users[0].get("_docID").unwrap().is_string());
    assert_eq!(users[0].get("name").unwrap(), "Bob");
}

#[tokio::test]
async fn test_execute_empty_collection() {
    let fetcher = MockFetcher::new();
    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner.execute_query("{ Users { name } }").await.unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert!(users.is_empty());
}

#[tokio::test]
async fn test_execute_unknown_collection() {
    let fetcher = MockFetcher::new();
    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner.execute_query("{ Posts { title } }").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_execute_with_limit() {
    let fetcher = MockFetcher::new();

    for i in 0..5 {
        let mut doc = Document::new();
        doc.set("name", format!("User{}", i));
        doc.set("age", i as i64);
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);
    }

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query("{ Users(limit: 2) { name } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 2);
}

#[tokio::test]
async fn test_query_executor_trait() {
    let fetcher = MockFetcher::new();

    let mut doc = Document::new();
    doc.set("name", "Alice");
    doc.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let request = QueryRequest::new("{ Users { name } }");

    let response = runner.execute(request).await;

    assert!(response.errors.is_empty());
    assert!(response.data.is_some());
}

#[tokio::test]
async fn test_fetcher_error_propagates() {
    let runner = QueryRunner::new(FailingFetcher, vec![make_test_collection()]);

    let result = runner.execute_query("{ Users { name } }").await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("storage failure"));
}

#[tokio::test]
async fn test_query_executor_error_response_format() {
    let fetcher = MockFetcher::new();
    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let request = QueryRequest::new("{ InvalidCollection { name } }");

    let response = runner.execute(request).await;

    assert!(response.data.is_none());
    assert_eq!(response.errors.len(), 1);
    assert!(response.errors[0].message.contains("collection not found"));
}

#[tokio::test]
async fn test_execute_with_offset() {
    let fetcher = MockFetcher::new();

    for i in 0..5 {
        let mut doc = Document::new();
        doc.set("name", format!("User{}", i));
        doc.set("age", i as i64);
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);
    }

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query("{ Users(offset: 2) { name } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 3);
}

#[tokio::test]
async fn test_execute_with_limit_and_offset() {
    let fetcher = MockFetcher::new();

    for i in 0..10 {
        let mut doc = Document::new();
        doc.set("name", format!("User{}", i));
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);
    }

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query("{ Users(limit: 3, offset: 2) { name } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 3);
}

#[tokio::test]
async fn test_execute_query_with_doc_ids() {
    let fetcher = MockFetcher::new();

    let mut doc1 = Document::new();
    doc1.set("name", "Alice");
    doc1.generate_and_set_doc_id().unwrap();
    let doc1_id = doc1.id().unwrap().to_string();
    fetcher.add_doc("Users", doc1);

    let mut doc2 = Document::new();
    doc2.set("name", "Bob");
    doc2.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc2);

    let mut doc3 = Document::new();
    doc3.set("name", "Charlie");
    doc3.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc3);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let query = format!(r#"{{ Users(docIDs: ["{}"]) {{ name }} }}"#, doc1_id);
    let result = runner.execute_query(&query).await.unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].get("name").unwrap(), "Alice");
}

#[tokio::test]
async fn test_unknown_collection_error() {
    let fetcher = MockFetcher::new();
    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner.execute_query("{ Posts { title } }").await;

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("collection not found: Posts"));
}

// =============================================================================
// Filter Tests
// =============================================================================

#[tokio::test]
async fn test_execute_query_with_filter() {
    let fetcher = MockFetcher::new();

    let mut doc1 = Document::new();
    doc1.set("name", "Alice");
    doc1.set("age", 30i64);
    doc1.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc1);

    let mut doc2 = Document::new();
    doc2.set("name", "Bob");
    doc2.set("age", 25i64);
    doc2.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc2);

    let mut doc3 = Document::new();
    doc3.set("name", "Charlie");
    doc3.set("age", 35i64);
    doc3.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc3);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query(r#"{ Users(filter: {age: {_gte: 30}}) { name age } }"#)
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 2);

    let names: Vec<&str> = users
        .iter()
        .map(|u| u.get("name").unwrap().as_str().unwrap())
        .collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Charlie"));
    assert!(!names.contains(&"Bob"));
}

// =============================================================================
// Order By Tests
// =============================================================================

#[tokio::test]
async fn test_order_by_single_field_asc() {
    let fetcher = MockFetcher::new();

    let mut doc1 = Document::new();
    doc1.set("name", "Charlie");
    doc1.set("age", 35i64);
    doc1.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc1);

    let mut doc2 = Document::new();
    doc2.set("name", "Alice");
    doc2.set("age", 25i64);
    doc2.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc2);

    let mut doc3 = Document::new();
    doc3.set("name", "Bob");
    doc3.set("age", 30i64);
    doc3.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc3);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query("{ Users(order: {name: ASC}) { name } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 3);
    assert_eq!(users[0].get("name").unwrap(), "Alice");
    assert_eq!(users[1].get("name").unwrap(), "Bob");
    assert_eq!(users[2].get("name").unwrap(), "Charlie");
}

#[tokio::test]
async fn test_order_by_single_field_desc() {
    let fetcher = MockFetcher::new();

    let mut doc1 = Document::new();
    doc1.set("name", "Alice");
    doc1.set("age", 25i64);
    doc1.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc1);

    let mut doc2 = Document::new();
    doc2.set("name", "Charlie");
    doc2.set("age", 35i64);
    doc2.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc2);

    let mut doc3 = Document::new();
    doc3.set("name", "Bob");
    doc3.set("age", 30i64);
    doc3.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc3);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query("{ Users(order: {name: DESC}) { name } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 3);
    assert_eq!(users[0].get("name").unwrap(), "Charlie");
    assert_eq!(users[1].get("name").unwrap(), "Bob");
    assert_eq!(users[2].get("name").unwrap(), "Alice");
}

#[tokio::test]
async fn test_order_by_numeric_field() {
    let fetcher = MockFetcher::new();

    let mut doc1 = Document::new();
    doc1.set("name", "Alice");
    doc1.set("age", 30i64);
    doc1.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc1);

    let mut doc2 = Document::new();
    doc2.set("name", "Bob");
    doc2.set("age", 25i64);
    doc2.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc2);

    let mut doc3 = Document::new();
    doc3.set("name", "Charlie");
    doc3.set("age", 35i64);
    doc3.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc3);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query("{ Users(order: {age: ASC}) { name age } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 3);
    assert_eq!(users[0].get("name").unwrap(), "Bob");
    assert_eq!(users[1].get("name").unwrap(), "Alice");
    assert_eq!(users[2].get("name").unwrap(), "Charlie");
}

#[tokio::test]
async fn test_order_by_with_limit() {
    let fetcher = MockFetcher::new();

    for i in 0..10 {
        let mut doc = Document::new();
        doc.set("name", format!("User{}", i));
        doc.set("age", (100 - i) as i64);
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);
    }

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query("{ Users(order: {age: ASC}, limit: 3) { name age } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 3);
    assert_eq!(users[0].get("age").unwrap(), 91);
    assert_eq!(users[1].get("age").unwrap(), 92);
    assert_eq!(users[2].get("age").unwrap(), 93);
}

#[tokio::test]
async fn test_order_by_with_filter() {
    let fetcher = MockFetcher::new();

    let mut doc1 = Document::new();
    doc1.set("name", "Alice");
    doc1.set("age", 25i64);
    doc1.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc1);

    let mut doc2 = Document::new();
    doc2.set("name", "Bob");
    doc2.set("age", 35i64);
    doc2.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc2);

    let mut doc3 = Document::new();
    doc3.set("name", "Charlie");
    doc3.set("age", 30i64);
    doc3.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc3);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query(r#"{ Users(filter: {age: {_gte: 30}}, order: {name: ASC}) { name age } }"#)
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0].get("name").unwrap(), "Bob");
    assert_eq!(users[1].get("name").unwrap(), "Charlie");
}

// =============================================================================
// Group By Tests
// =============================================================================

#[tokio::test]
async fn test_group_by_single_field() {
    let fetcher = MockFetcher::new();

    let mut doc1 = Document::new();
    doc1.set("name", "Alice");
    doc1.set("age", 30i64);
    doc1.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc1);

    let mut doc2 = Document::new();
    doc2.set("name", "Bob");
    doc2.set("age", 25i64);
    doc2.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc2);

    let mut doc3 = Document::new();
    doc3.set("name", "Alice");
    doc3.set("age", 35i64);
    doc3.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc3);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query("{ Users(groupBy: [name]) { name } }")
        .await
        .unwrap();

    let users = result["Users"].as_array().unwrap();
    assert_eq!(users.len(), 2);

    let names: Vec<&str> = users.iter().map(|u| u["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
}

#[tokio::test]
async fn test_group_by_with_count() {
    let fetcher = MockFetcher::new();

    let mut doc1 = Document::new();
    doc1.set("name", "Alice");
    doc1.set("age", 30i64);
    doc1.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc1);

    let mut doc2 = Document::new();
    doc2.set("name", "Bob");
    doc2.set("age", 25i64);
    doc2.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc2);

    let mut doc3 = Document::new();
    doc3.set("name", "Alice");
    doc3.set("age", 35i64);
    doc3.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc3);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query("{ Users(groupBy: [name]) { name _count } }")
        .await
        .unwrap();

    let users = result["Users"].as_array().unwrap();
    assert_eq!(users.len(), 2);

    let alice = users
        .iter()
        .find(|u| u["name"].as_str() == Some("Alice"))
        .unwrap();
    assert_eq!(alice["_count"].as_i64(), Some(2));

    let bob = users
        .iter()
        .find(|u| u["name"].as_str() == Some("Bob"))
        .unwrap();
    assert_eq!(bob["_count"].as_i64(), Some(1));
}

// =============================================================================
// Alias Tests
// =============================================================================

#[tokio::test]
async fn test_field_alias_in_output() {
    let fetcher = MockFetcher::new();

    let mut doc = Document::new();
    doc.set("name", "Alice");
    doc.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query("{ Users { userName: name } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert!(users[0].get("userName").is_some());
    assert!(users[0].get("name").is_none());
    assert_eq!(users[0].get("userName").unwrap(), "Alice");
}

#[tokio::test]
async fn test_collection_alias_in_output() {
    let fetcher = MockFetcher::new();

    let mut doc = Document::new();
    doc.set("name", "Alice");
    doc.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query("{ allUsers: Users { name } }")
        .await
        .unwrap();

    assert!(result.get("allUsers").is_some());
    assert!(result.get("Users").is_none());
    let users = result.get("allUsers").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
}

#[tokio::test]
async fn test_schema_generation() {
    let fetcher = MockFetcher::new();
    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let schema = runner.schema().await.unwrap();

    assert!(schema.contains("type Users"));
    assert!(schema.contains("_docID: ID"));
    assert!(schema.contains("name: String"));
    assert!(schema.contains("age: Int"));
}

// =============================================================================
// Transaction Tests
// =============================================================================

#[tokio::test]
async fn test_begin_txn() {
    let fetcher = MockFetcher::new();
    let registry = MockTxnRegistry::new(MockFetcher::new());
    let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

    let txn_id = runner.begin_txn(false).await.unwrap();
    assert!(txn_id.starts_with("txn-"));
}

#[tokio::test]
async fn test_begin_and_commit_txn() {
    let fetcher = MockFetcher::new();
    let registry = MockTxnRegistry::new(MockFetcher::new());
    let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

    let txn_id = runner.begin_txn(false).await.unwrap();
    let result = runner.commit_txn(&txn_id).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_begin_and_rollback_txn() {
    let fetcher = MockFetcher::new();
    let registry = MockTxnRegistry::new(MockFetcher::new());
    let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

    let txn_id = runner.begin_txn(false).await.unwrap();
    let result = runner.rollback_txn(&txn_id).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_commit_nonexistent_txn_returns_error() {
    let fetcher = MockFetcher::new();
    let registry = MockTxnRegistry::new(MockFetcher::new());
    let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

    let nonexistent_handle: TransactionHandle = "nonexistent-txn".parse().unwrap();
    let result = runner.commit_txn(&nonexistent_handle).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_execute_in_txn_success() {
    let fetcher = MockFetcher::new();

    let registry_fetcher = MockFetcher::new();
    let mut doc = Document::new();
    doc.set("name", "TxnAlice");
    doc.set("age", 40i64);
    doc.generate_and_set_doc_id().unwrap();
    registry_fetcher.add_doc("Users", doc);

    let registry = MockTxnRegistry::new(registry_fetcher);
    let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

    let txn_id = runner.begin_txn(false).await.unwrap();

    let request = QueryRequest::new("{ Users { name age } }");
    let response = runner.execute_in_txn(request, &txn_id).await;

    assert!(!response.has_errors());
    let data = response.data.unwrap();
    let users = data.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].get("name").unwrap(), "TxnAlice");
    assert_eq!(users[0].get("age").unwrap(), 40);

    runner.commit_txn(&txn_id).await.unwrap();
}

#[tokio::test]
async fn test_execute_in_txn_after_commit_fails() {
    let fetcher = MockFetcher::new();
    let registry = MockTxnRegistry::new(MockFetcher::new());
    let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

    let txn_id = runner.begin_txn(false).await.unwrap();
    runner.commit_txn(&txn_id).await.unwrap();

    let request = QueryRequest::new("{ Users { name } }");
    let response = runner.execute_in_txn(request, &txn_id).await;

    assert!(response.has_errors());
    assert!(response.errors[0].message.contains("not found"));
}

#[tokio::test]
async fn test_begin_txn_without_registry_returns_error() {
    let fetcher = MockFetcher::new();
    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner.begin_txn(false).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not supported"));
}

// =============================================================================
// Mutation Tests
// =============================================================================

#[tokio::test]
async fn test_execute_mutation_without_mutator_returns_error() {
    let fetcher = MockFetcher::new();
    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_mutation(r#"mutation { create_Users(input: [{name: "Alice"}]) { _docID } }"#)
        .await;

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("mutations require a mutator"));
}

#[tokio::test]
async fn test_execute_create_mutation() {
    let fetcher = MockFetcher::new();
    let mutator = Arc::new(MockMutator::new());
    let runner =
        QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

    let result = runner
        .execute_mutation(
            r#"mutation { create_Users(input: [{name: "Alice", age: 30}]) { _docID name } }"#,
        )
        .await
        .unwrap();

    assert!(result.is_object());
    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert!(users[0].get("_docID").is_some());
    assert_eq!(users[0].get("name").unwrap(), "Alice");

    let created = mutator.created_docs();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].0, "Users");
}

#[tokio::test]
async fn test_execute_create_multiple_documents() {
    let fetcher = MockFetcher::new();
    let mutator = Arc::new(MockMutator::new());
    let runner =
        QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

    let result = runner
        .execute_mutation(
            r#"mutation {
                create_Users(input: [
                    {name: "Alice", age: 30},
                    {name: "Bob", age: 25}
                ]) { _docID name }
            }"#,
        )
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 2);

    let names: Vec<&str> = users
        .iter()
        .map(|u| u.get("name").unwrap().as_str().unwrap())
        .collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));

    let created = mutator.created_docs();
    assert_eq!(created.len(), 2);
}

#[tokio::test]
async fn test_execute_delete_mutation() {
    let fetcher = MockFetcher::new();
    let mutator = Arc::new(MockMutator::new());

    let mut doc = Document::new();
    doc.set("name", "Alice");
    doc.set("age", 30i64);
    doc.generate_and_set_doc_id().unwrap();
    let doc_id = doc.id().unwrap().to_string();
    mutator.add_doc("Users", doc);

    let runner =
        QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

    let mutation = format!(
        r#"mutation {{ delete_Users(docIDs: ["{}"]) {{ _docID }} }}"#,
        doc_id
    );
    let result = runner.execute_mutation(&mutation).await.unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].get("_docID").unwrap().as_str().unwrap(), doc_id);

    assert!(mutator.created_docs().is_empty());
}

#[tokio::test]
async fn test_execute_update_mutation() {
    let fetcher = MockFetcher::new();
    let mutator = Arc::new(MockMutator::new());

    let mut doc = Document::new();
    doc.set("name", "Alice");
    doc.set("age", 25i64);
    doc.generate_and_set_doc_id().unwrap();
    let doc_id = doc.id().unwrap().to_string();
    mutator.add_doc("Users", doc);

    let runner =
        QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

    let mutation = format!(
        r#"mutation {{ update_Users(docIDs: ["{}"], input: {{name: "Alice Updated", age: 30}}) {{ _docID name age }} }}"#,
        doc_id
    );
    let result = runner.execute_mutation(&mutation).await.unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].get("_docID").unwrap().as_str().unwrap(), doc_id);
    assert_eq!(users[0].get("name").unwrap(), "Alice Updated");
    assert_eq!(users[0].get("age").unwrap(), 30);
}

// =============================================================================
// Aggregation Tests
// =============================================================================

#[tokio::test]
async fn test_count_all_documents() {
    let fetcher = MockFetcher::new();

    for name in ["Alice", "Bob", "Charlie"] {
        let mut doc = Document::new();
        doc.set("name", name);
        doc.set("age", 30i64);
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);
    }

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner.execute_query("{ Users { _count } }").await.unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].get("_count").unwrap(), 3);
}

#[tokio::test]
async fn test_sum_aggregate() {
    let fetcher = MockFetcher::new();

    let ages = [30i64, 25, 35];
    for (i, age) in ages.iter().enumerate() {
        let mut doc = Document::new();
        doc.set("name", format!("User{}", i));
        doc.set("age", *age);
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);
    }

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query(r#"{ Users { _sum(field: "age") } }"#)
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].get("_sum").unwrap(), 90);
}

#[tokio::test]
async fn test_avg_aggregate() {
    let fetcher = MockFetcher::new();

    let ages = [30i64, 20, 40];
    for (i, age) in ages.iter().enumerate() {
        let mut doc = Document::new();
        doc.set("name", format!("User{}", i));
        doc.set("age", *age);
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);
    }

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query(r#"{ Users { _avg(field: "age") } }"#)
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
    let avg = users[0].get("_avg").unwrap().as_f64().unwrap();
    assert!((avg - 30.0).abs() < 0.001);
}

#[tokio::test]
async fn test_min_max_aggregate() {
    let fetcher = MockFetcher::new();

    let ages = [30i64, 25, 35];
    for (i, age) in ages.iter().enumerate() {
        let mut doc = Document::new();
        doc.set("name", format!("User{}", i));
        doc.set("age", *age);
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);
    }

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner
        .execute_query(r#"{ Users { _min(field: "age") } }"#)
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users[0].get("_min").unwrap(), 25);

    let result = runner
        .execute_query(r#"{ Users { _max(field: "age") } }"#)
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users[0].get("_max").unwrap(), 35);
}

#[tokio::test]
async fn test_count_empty_collection() {
    let fetcher = MockFetcher::new();
    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    let result = runner.execute_query("{ Users { _count } }").await.unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].get("_count").unwrap(), 0);
}

// =============================================================================
// ACP Integration Tests
// =============================================================================

#[tokio::test]
async fn test_acp_owner_sees_registered_docs() {
    use acp::{LocalDocumentACP, MemoryAcpStore};

    let fetcher = MockFetcher::new();
    let store = Arc::new(MemoryAcpStore::new());
    let acp = Arc::new(LocalDocumentACP::new(store));

    let owner = test_acp_did();

    let mut doc1 = Document::new();
    doc1.set("_docID", "doc1");
    doc1.set("name", "Alice");
    doc1.set("age", 30i64);
    fetcher.add_doc("Users", doc1);

    let mut doc2 = Document::new();
    doc2.set("_docID", "doc2");
    doc2.set("name", "Bob");
    doc2.set("age", 25i64);
    fetcher.add_doc("Users", doc2);

    acp.register_doc_object(&owner, "policy-acp", "Users", "doc1")
        .await
        .unwrap();
    acp.register_doc_object(&owner, "policy-acp", "Users", "doc2")
        .await
        .unwrap();

    let runner = QueryRunner::new(fetcher, vec![make_acp_collection()]).with_acp(acp);

    let result = runner
        .execute_query_with_identity("{ Users { _docID name } }", Some(owner))
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 2, "owner should see all registered docs");
}

#[tokio::test]
async fn test_acp_non_owner_sees_nothing() {
    use acp::{LocalDocumentACP, MemoryAcpStore};

    let fetcher = MockFetcher::new();
    let store = Arc::new(MemoryAcpStore::new());
    let acp = Arc::new(LocalDocumentACP::new(store));

    let owner = test_acp_did();
    let other = test_acp_did2();

    let mut doc1 = Document::new();
    doc1.set("_docID", "doc1");
    doc1.set("name", "Alice");
    fetcher.add_doc("Users", doc1);

    acp.register_doc_object(&owner, "policy-acp", "Users", "doc1")
        .await
        .unwrap();

    let runner = QueryRunner::new(fetcher, vec![make_acp_collection()]).with_acp(acp);

    let result = runner
        .execute_query_with_identity("{ Users { _docID name } }", Some(other))
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(
        users.len(),
        0,
        "non-owner without permissions should see no docs"
    );
}

#[tokio::test]
async fn test_acp_reader_sees_shared_doc() {
    use acp::{LocalDocumentACP, MemoryAcpStore, READER_RELATION};

    let fetcher = MockFetcher::new();
    let store = Arc::new(MemoryAcpStore::new());
    let acp = Arc::new(LocalDocumentACP::new(store));

    let owner = test_acp_did();
    let reader = test_acp_did2();

    let mut doc1 = Document::new();
    doc1.set("_docID", "doc1");
    doc1.set("name", "Alice");
    fetcher.add_doc("Users", doc1);

    acp.register_doc_object(&owner, "policy-acp", "Users", "doc1")
        .await
        .unwrap();
    acp.add_actor_relationship(&owner, &reader, "Users", "doc1", READER_RELATION)
        .await
        .unwrap();

    let runner = QueryRunner::new(fetcher, vec![make_acp_collection()]).with_acp(acp);

    let result = runner
        .execute_query_with_identity("{ Users { _docID name } }", Some(reader))
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1, "reader should see shared doc");
    assert_eq!(users[0].get("name").unwrap(), "Alice");
}
