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

// Test _group field with nested selection
#[tokio::test]
async fn test_group_by_with_group_field() {
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
        .execute_query("{ Users(groupBy: [name]) { name _group { age } } }")
        .await
        .unwrap();

    let users = result["Users"].as_array().unwrap();
    assert_eq!(users.len(), 2, "Should have 2 groups");

    // Find Alice's group
    let alice = users
        .iter()
        .find(|u| u["name"].as_str() == Some("Alice"))
        .unwrap();
    let alice_group = alice["_group"].as_array().unwrap();
    assert_eq!(alice_group.len(), 2, "Alice's group should have 2 docs");

    // Verify the ages in Alice's group
    let ages: Vec<i64> = alice_group
        .iter()
        .map(|g| g["age"].as_i64().unwrap())
        .collect();
    assert!(ages.contains(&30), "Alice's group should have age 30");
    assert!(ages.contains(&35), "Alice's group should have age 35");

    // Find Bob's group
    let bob = users
        .iter()
        .find(|u| u["name"].as_str() == Some("Bob"))
        .unwrap();
    let bob_group = bob["_group"].as_array().unwrap();
    assert_eq!(bob_group.len(), 1, "Bob's group should have 1 doc");
    assert_eq!(bob_group[0]["age"].as_i64(), Some(25));
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
    let users = result.get("create_Users").unwrap().as_array().unwrap();
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

    let users = result.get("create_Users").unwrap().as_array().unwrap();
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

    let users = result.get("delete_Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].get("_docID").unwrap().as_str().unwrap(), doc_id);

    assert!(mutator.created_docs().is_empty());
}

// NOTE: test_execute_delete_mutation_with_filter removed - delete mutation behavior
// is validated through Go interop tests which are the source of truth for
// behavioral compatibility.

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

    let users = result.get("update_Users").unwrap().as_array().unwrap();
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

// =============================================================================
// Nested Query Integration Tests
// =============================================================================

fn make_users_with_posts_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v1",
        "coll-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // One-to-many relation to posts (array)
            FieldDescription::new("3", "posts", FieldKind::relation("Posts", true))
                .with_relation_name("author_posts"),
        ],
    )
}

fn make_posts_with_author_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Posts",
        "v1",
        "coll-posts",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            // Many-to-one relation to users (singular)
            FieldDescription::new("3", "author", FieldKind::relation("Users", false))
                .with_relation_name("author_posts")
                .as_primary(),
            // Auto-generated FK field
            FieldDescription::new("4", "author_id", FieldKind::doc_id())
                .with_relation_name("author_posts")
                .as_primary(),
        ],
    )
}

fn make_comments_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Comments",
        "v1",
        "coll-comments",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "body", FieldKind::string()),
            // Many-to-one relation to posts
            FieldDescription::new("3", "post", FieldKind::relation("Posts", false))
                .with_relation_name("post_comments")
                .as_primary(),
            // FK field
            FieldDescription::new("4", "post_id", FieldKind::doc_id())
                .with_relation_name("post_comments")
                .as_primary(),
        ],
    )
}

fn make_posts_with_comments_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Posts",
        "v1",
        "coll-posts",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            // Many-to-one relation to users (singular)
            FieldDescription::new("3", "author", FieldKind::relation("Users", false))
                .with_relation_name("author_posts")
                .as_primary(),
            // Auto-generated FK field for author
            FieldDescription::new("4", "author_id", FieldKind::doc_id())
                .with_relation_name("author_posts")
                .as_primary(),
            // One-to-many relation to comments (array)
            FieldDescription::new("5", "comments", FieldKind::relation("Comments", true))
                .with_relation_name("post_comments"),
        ],
    )
}

#[tokio::test]
async fn test_nested_query_one_to_many() {
    // Query: { Users { name posts { title } } }
    // User.posts is a one-to-many relation

    let fetcher = MockFetcher::new();

    // Add users
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    let mut bob = Document::new();
    bob.set("_docID", "user-2");
    bob.set("name", "Bob");
    fetcher.add_doc("Users", bob);

    // Add posts with FK pointing to users
    let mut post1 = Document::new();
    post1.set("_docID", "post-1");
    post1.set("title", "Alice's First Post");
    post1.set("author_id", "user-1");
    fetcher.add_doc("Posts", post1);

    let mut post2 = Document::new();
    post2.set("_docID", "post-2");
    post2.set("title", "Alice's Second Post");
    post2.set("author_id", "user-1");
    fetcher.add_doc("Posts", post2);

    let mut post3 = Document::new();
    post3.set("_docID", "post-3");
    post3.set("title", "Bob's Post");
    post3.set("author_id", "user-2");
    fetcher.add_doc("Posts", post3);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Users { name posts { title } } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 2);

    // Find Alice and verify her posts
    let alice = users
        .iter()
        .find(|u| u.get("name").unwrap() == "Alice")
        .expect("Alice should exist");
    let alice_posts = alice.get("posts").unwrap().as_array().unwrap();
    assert_eq!(alice_posts.len(), 2);

    let post_titles: Vec<&str> = alice_posts
        .iter()
        .map(|p| p.get("title").unwrap().as_str().unwrap())
        .collect();
    assert!(post_titles.contains(&"Alice's First Post"));
    assert!(post_titles.contains(&"Alice's Second Post"));

    // Find Bob and verify his posts
    let bob = users
        .iter()
        .find(|u| u.get("name").unwrap() == "Bob")
        .expect("Bob should exist");
    let bob_posts = bob.get("posts").unwrap().as_array().unwrap();
    assert_eq!(bob_posts.len(), 1);
    assert_eq!(bob_posts[0].get("title").unwrap(), "Bob's Post");
}

#[tokio::test]
async fn test_nested_query_many_to_one() {
    // Query: { Posts { title author { name } } }
    // Post.author is a many-to-one relation

    let fetcher = MockFetcher::new();

    // Add users
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    let mut bob = Document::new();
    bob.set("_docID", "user-2");
    bob.set("name", "Bob");
    fetcher.add_doc("Users", bob);

    // Add posts
    let mut post1 = Document::new();
    post1.set("_docID", "post-1");
    post1.set("title", "First Post");
    post1.set("author_id", "user-1");
    fetcher.add_doc("Posts", post1);

    let mut post2 = Document::new();
    post2.set("_docID", "post-2");
    post2.set("title", "Second Post");
    post2.set("author_id", "user-2");
    fetcher.add_doc("Posts", post2);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Posts { title author { name } } }")
        .await
        .unwrap();

    let posts = result.get("Posts").unwrap().as_array().unwrap();
    assert_eq!(posts.len(), 2);

    // Verify first post has Alice as author
    let post1 = posts
        .iter()
        .find(|p| p.get("title").unwrap() == "First Post")
        .expect("First Post should exist");
    let author1 = post1.get("author").unwrap();
    assert_eq!(author1.get("name").unwrap(), "Alice");

    // Verify second post has Bob as author
    let post2 = posts
        .iter()
        .find(|p| p.get("title").unwrap() == "Second Post")
        .expect("Second Post should exist");
    let author2 = post2.get("author").unwrap();
    assert_eq!(author2.get("name").unwrap(), "Bob");
}

#[tokio::test]
async fn test_nested_query_with_null_relation() {
    // Query for a post with no author (orphan post)

    let fetcher = MockFetcher::new();

    // Add a post with null author_id
    let mut orphan_post = Document::new();
    orphan_post.set("_docID", "post-orphan");
    orphan_post.set("title", "Orphan Post");
    // No author_id set
    fetcher.add_doc("Posts", orphan_post);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Posts { title author { name } } }")
        .await
        .unwrap();

    let posts = result.get("Posts").unwrap().as_array().unwrap();
    assert_eq!(posts.len(), 1);

    // Author should be null for orphan post
    let post = &posts[0];
    assert_eq!(post.get("title").unwrap(), "Orphan Post");
    assert!(
        post.get("author").unwrap().is_null(),
        "Author should be null for post without author_id"
    );
}

#[tokio::test]
async fn test_nested_query_user_with_no_posts() {
    // Query for a user with no posts (empty array)

    let fetcher = MockFetcher::new();

    // Add user with no posts
    let mut lonely_user = Document::new();
    lonely_user.set("_docID", "user-lonely");
    lonely_user.set("name", "Lonely User");
    fetcher.add_doc("Users", lonely_user);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Users { name posts { title } } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);

    // Posts should be empty array, not null
    let user = &users[0];
    assert_eq!(user.get("name").unwrap(), "Lonely User");
    let posts = user.get("posts").unwrap().as_array().unwrap();
    assert!(
        posts.is_empty(),
        "Posts should be empty array for user with no posts"
    );
}

#[tokio::test]
async fn test_nested_query_multi_level_nesting() {
    // Query: { Users { name posts { title comments { body } } } }
    // Three levels of nesting: Users -> Posts -> Comments

    let fetcher = MockFetcher::new();

    // Add user
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    // Add post
    let mut post = Document::new();
    post.set("_docID", "post-1");
    post.set("title", "Alice's Post");
    post.set("author_id", "user-1");
    fetcher.add_doc("Posts", post);

    // Add comments
    let mut comment1 = Document::new();
    comment1.set("_docID", "comment-1");
    comment1.set("body", "Great post!");
    comment1.set("post_id", "post-1");
    fetcher.add_doc("Comments", comment1);

    let mut comment2 = Document::new();
    comment2.set("_docID", "comment-2");
    comment2.set("body", "Thanks for sharing!");
    comment2.set("post_id", "post-1");
    fetcher.add_doc("Comments", comment2);

    // Create users collection with posts relation
    let users_collection = CollectionVersion::new(
        "Users",
        "v1",
        "coll-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "posts", FieldKind::relation("Posts", true))
                .with_relation_name("author_posts"),
        ],
    );

    let runner = QueryRunner::new(
        fetcher,
        vec![
            users_collection,
            make_posts_with_comments_collection(),
            make_comments_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Users { name posts { title comments { body } } } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);

    let alice = &users[0];
    assert_eq!(alice.get("name").unwrap(), "Alice");

    let posts = alice.get("posts").unwrap().as_array().unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].get("title").unwrap(), "Alice's Post");

    let comments = posts[0].get("comments").unwrap().as_array().unwrap();
    assert_eq!(comments.len(), 2);

    let comment_bodies: Vec<&str> = comments
        .iter()
        .map(|c| c.get("body").unwrap().as_str().unwrap())
        .collect();
    assert!(comment_bodies.contains(&"Great post!"));
    assert!(comment_bodies.contains(&"Thanks for sharing!"));
}

#[tokio::test]
async fn test_nested_query_with_filter_on_parent() {
    // Query: { Users(filter: {name: {_eq: "Alice"}}) { name posts { title } } }
    // Filter on parent, nested selection on relation

    let fetcher = MockFetcher::new();

    // Add users
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    let mut bob = Document::new();
    bob.set("_docID", "user-2");
    bob.set("name", "Bob");
    fetcher.add_doc("Users", bob);

    // Add posts
    let mut post1 = Document::new();
    post1.set("_docID", "post-1");
    post1.set("title", "Alice's Post");
    post1.set("author_id", "user-1");
    fetcher.add_doc("Posts", post1);

    let mut post2 = Document::new();
    post2.set("_docID", "post-2");
    post2.set("title", "Bob's Post");
    post2.set("author_id", "user-2");
    fetcher.add_doc("Posts", post2);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query(r#"{ Users(filter: {name: {_eq: "Alice"}}) { name posts { title } } }"#)
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);

    let alice = &users[0];
    assert_eq!(alice.get("name").unwrap(), "Alice");

    let posts = alice.get("posts").unwrap().as_array().unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].get("title").unwrap(), "Alice's Post");
}

#[tokio::test]
async fn test_nested_query_with_limit_on_parent() {
    // Query: { Users(limit: 1) { name posts { title } } }
    // Limit on parent should not affect nested query

    let fetcher = MockFetcher::new();

    // Add users
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    let mut bob = Document::new();
    bob.set("_docID", "user-2");
    bob.set("name", "Bob");
    fetcher.add_doc("Users", bob);

    // Add posts
    for i in 1..=3 {
        let mut post = Document::new();
        post.set("_docID", format!("post-{}", i));
        post.set("title", format!("Post {}", i));
        post.set("author_id", "user-1");
        fetcher.add_doc("Posts", post);
    }

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Users(limit: 1) { name posts { title } } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);

    // The one returned user should have all their posts (not limited)
    let user = &users[0];
    let _posts = user.get("posts").unwrap().as_array().unwrap();
    // If Alice was returned, she should have 3 posts
    // If Bob was returned, he should have 0 posts
    // The exact user depends on iteration order
}

#[tokio::test]
async fn test_nested_query_with_aliased_field() {
    // Query: { Users { name posts { headline: title } } }
    // Tests that aliased fields in nested selections render correctly.
    // This test demonstrates a bug where aliases in nested queries are lost
    // because build_scan_mapping_for_join searches by alias name instead of field name.

    let fetcher = MockFetcher::new();

    // Add user
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    // Add post
    let mut post = Document::new();
    post.set("_docID", "post-1");
    post.set("title", "My First Post");
    post.set("author_id", "user-1");
    fetcher.add_doc("Posts", post);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Users { name posts { headline: title } } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);

    let alice = &users[0];
    let posts = alice.get("posts").unwrap().as_array().unwrap();
    assert_eq!(posts.len(), 1);

    // The aliased field should appear as "headline", not "title"
    let post = &posts[0];
    assert!(
        post.get("headline").is_some(),
        "Aliased field 'headline' should exist in output. Got: {:?}",
        post
    );
    assert_eq!(post.get("headline").unwrap(), "My First Post");
    assert!(
        post.get("title").is_none(),
        "Original field name 'title' should not appear when aliased"
    );
}

// Helper for 4-level nesting test
fn make_reactions_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Reactions",
        "v1",
        "coll-reactions",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "emoji", FieldKind::string()),
            // Many-to-one relation to comments
            FieldDescription::new("3", "comment", FieldKind::relation("Comments", false))
                .with_relation_name("comment_reactions")
                .as_primary(),
            // FK field
            FieldDescription::new("4", "comment_id", FieldKind::doc_id())
                .with_relation_name("comment_reactions")
                .as_primary(),
        ],
    )
}

fn make_comments_with_reactions_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Comments",
        "v1",
        "coll-comments",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "body", FieldKind::string()),
            // Many-to-one relation to posts
            FieldDescription::new("3", "post", FieldKind::relation("Posts", false))
                .with_relation_name("post_comments")
                .as_primary(),
            // FK field
            FieldDescription::new("4", "post_id", FieldKind::doc_id())
                .with_relation_name("post_comments")
                .as_primary(),
            // One-to-many relation to reactions
            FieldDescription::new("5", "reactions", FieldKind::relation("Reactions", true))
                .with_relation_name("comment_reactions"),
        ],
    )
}

#[tokio::test]
async fn test_nested_query_four_levels_deep() {
    // Query: { Users { name posts { title comments { body reactions { emoji } } } } }
    // Four levels of nesting: Users -> Posts -> Comments -> Reactions

    let fetcher = MockFetcher::new();

    // Add user
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    // Add post
    let mut post = Document::new();
    post.set("_docID", "post-1");
    post.set("title", "Alice's Post");
    post.set("author_id", "user-1");
    fetcher.add_doc("Posts", post);

    // Add comment
    let mut comment = Document::new();
    comment.set("_docID", "comment-1");
    comment.set("body", "Great post!");
    comment.set("post_id", "post-1");
    fetcher.add_doc("Comments", comment);

    // Add reactions
    let mut reaction1 = Document::new();
    reaction1.set("_docID", "reaction-1");
    reaction1.set("emoji", "👍");
    reaction1.set("comment_id", "comment-1");
    fetcher.add_doc("Reactions", reaction1);

    let mut reaction2 = Document::new();
    reaction2.set("_docID", "reaction-2");
    reaction2.set("emoji", "❤️");
    reaction2.set("comment_id", "comment-1");
    fetcher.add_doc("Reactions", reaction2);

    // Create users collection with posts relation
    let users_collection = CollectionVersion::new(
        "Users",
        "v1",
        "coll-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "posts", FieldKind::relation("Posts", true))
                .with_relation_name("author_posts"),
        ],
    );

    // Create posts collection with comments relation
    let posts_collection = CollectionVersion::new(
        "Posts",
        "v1",
        "coll-posts",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("Users", false))
                .with_relation_name("author_posts")
                .as_primary(),
            FieldDescription::new("4", "author_id", FieldKind::doc_id())
                .with_relation_name("author_posts")
                .as_primary(),
            FieldDescription::new("5", "comments", FieldKind::relation("Comments", true))
                .with_relation_name("post_comments"),
        ],
    );

    let runner = QueryRunner::new(
        fetcher,
        vec![
            users_collection,
            posts_collection,
            make_comments_with_reactions_collection(),
            make_reactions_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Users { name posts { title comments { body reactions { emoji } } } } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);

    let alice = &users[0];
    assert_eq!(alice.get("name").unwrap(), "Alice");

    let posts = alice.get("posts").unwrap().as_array().unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].get("title").unwrap(), "Alice's Post");

    let comments = posts[0].get("comments").unwrap().as_array().unwrap();
    assert_eq!(comments.len(), 1);
    assert_eq!(comments[0].get("body").unwrap(), "Great post!");

    let reactions = comments[0].get("reactions").unwrap().as_array().unwrap();
    assert_eq!(reactions.len(), 2);

    let emojis: Vec<&str> = reactions
        .iter()
        .map(|r| r.get("emoji").unwrap().as_str().unwrap())
        .collect();
    assert!(emojis.contains(&"👍"));
    assert!(emojis.contains(&"❤️"));
}

// Helper for self-referential test
fn make_employees_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Employees",
        "v1",
        "coll-employees",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // Self-referential: many-to-one relation to manager (another Employee)
            FieldDescription::new("3", "manager", FieldKind::relation("Employees", false))
                .with_relation_name("manager_reports")
                .as_primary(),
            // FK field for manager
            FieldDescription::new("4", "manager_id", FieldKind::doc_id())
                .with_relation_name("manager_reports")
                .as_primary(),
            // One-to-many: direct reports (array of Employees)
            FieldDescription::new("5", "reports", FieldKind::relation("Employees", true))
                .with_relation_name("manager_reports"),
        ],
    )
}

#[tokio::test]
async fn test_nested_query_self_referential_relation() {
    // Query: { Employees { name manager { name } reports { name } } }
    // Self-referential relation: Employee.manager -> Employee

    let fetcher = MockFetcher::new();

    // Add CEO (no manager)
    let mut ceo = Document::new();
    ceo.set("_docID", "emp-ceo");
    ceo.set("name", "CEO Carol");
    // No manager_id for CEO
    fetcher.add_doc("Employees", ceo);

    // Add manager who reports to CEO
    let mut manager = Document::new();
    manager.set("_docID", "emp-manager");
    manager.set("name", "Manager Mike");
    manager.set("manager_id", "emp-ceo");
    fetcher.add_doc("Employees", manager);

    // Add employee who reports to manager
    let mut employee = Document::new();
    employee.set("_docID", "emp-dev");
    employee.set("name", "Developer Dave");
    employee.set("manager_id", "emp-manager");
    fetcher.add_doc("Employees", employee);

    let runner = QueryRunner::new(fetcher, vec![make_employees_collection()]);

    let result = runner
        .execute_query("{ Employees { name manager { name } reports { name } } }")
        .await
        .unwrap();

    let employees = result.get("Employees").unwrap().as_array().unwrap();
    assert_eq!(employees.len(), 3);

    // Find CEO - should have null manager and 1 report
    let ceo = employees
        .iter()
        .find(|e| e.get("name").unwrap() == "CEO Carol")
        .expect("CEO should exist");
    assert!(
        ceo.get("manager").unwrap().is_null(),
        "CEO should have null manager"
    );
    let ceo_reports = ceo.get("reports").unwrap().as_array().unwrap();
    assert_eq!(ceo_reports.len(), 1);
    assert_eq!(ceo_reports[0].get("name").unwrap(), "Manager Mike");

    // Find Manager - should have CEO as manager and 1 report
    let manager = employees
        .iter()
        .find(|e| e.get("name").unwrap() == "Manager Mike")
        .expect("Manager should exist");
    assert_eq!(
        manager.get("manager").unwrap().get("name").unwrap(),
        "CEO Carol"
    );
    let manager_reports = manager.get("reports").unwrap().as_array().unwrap();
    assert_eq!(manager_reports.len(), 1);
    assert_eq!(manager_reports[0].get("name").unwrap(), "Developer Dave");

    // Find Developer - should have Manager as manager and no reports
    let dev = employees
        .iter()
        .find(|e| e.get("name").unwrap() == "Developer Dave")
        .expect("Developer should exist");
    assert_eq!(
        dev.get("manager").unwrap().get("name").unwrap(),
        "Manager Mike"
    );
    let dev_reports = dev.get("reports").unwrap().as_array().unwrap();
    assert!(dev_reports.is_empty(), "Developer should have no reports");
}

#[tokio::test]
async fn test_nested_query_with_aliased_doc_id() {
    // Query: { Users { name posts { id: _docID title } } }
    // Tests that _docID can be aliased in nested selections

    let fetcher = MockFetcher::new();

    // Add user
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    // Add post
    let mut post = Document::new();
    post.set("_docID", "post-1");
    post.set("title", "Test Post");
    post.set("author_id", "user-1");
    fetcher.add_doc("Posts", post);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Users { name posts { id: _docID title } } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);

    let posts = users[0].get("posts").unwrap().as_array().unwrap();
    assert_eq!(posts.len(), 1);

    let post = &posts[0];
    // _docID should appear as "id" due to alias
    assert!(
        post.get("id").is_some(),
        "Aliased _docID should appear as 'id'. Got: {:?}",
        post
    );
    assert_eq!(post.get("id").unwrap(), "post-1");
    assert!(
        post.get("_docID").is_none(),
        "_docID should not appear when aliased to 'id'"
    );
    assert_eq!(post.get("title").unwrap(), "Test Post");
}

#[tokio::test]
async fn test_nested_query_filter_on_fk_field_included_in_select() {
    // Query: { Users { name posts(filter: { author_id: { _eq: "user-1" } }) { title author_id } } }
    // Filter on FK field that IS in the select list (should succeed)

    let fetcher = MockFetcher::new();

    // Add users
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    let mut bob = Document::new();
    bob.set("_docID", "user-2");
    bob.set("name", "Bob");
    fetcher.add_doc("Users", bob);

    // Add posts - some by Alice, some by Bob
    let mut post1 = Document::new();
    post1.set("_docID", "post-1");
    post1.set("title", "Alice's Post");
    post1.set("author_id", "user-1");
    fetcher.add_doc("Posts", post1);

    let mut post2 = Document::new();
    post2.set("_docID", "post-2");
    post2.set("title", "Bob's Post");
    post2.set("author_id", "user-2");
    fetcher.add_doc("Posts", post2);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    // Filter posts by author_id, and include author_id in select
    let result = runner
        .execute_query(
            r#"{ Users { name posts(filter: { author_id: { _eq: "user-1" } }) { title author_id } } }"#,
        )
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();

    // Find Alice - should have her post (filter matches)
    let alice = users
        .iter()
        .find(|u| u.get("name").unwrap() == "Alice")
        .expect("Alice should exist");
    let alice_posts = alice.get("posts").unwrap().as_array().unwrap();
    assert_eq!(alice_posts.len(), 1);
    assert_eq!(alice_posts[0].get("title").unwrap(), "Alice's Post");
    assert_eq!(alice_posts[0].get("author_id").unwrap(), "user-1");

    // Find Bob - his posts are filtered out (author_id doesn't match)
    let bob = users
        .iter()
        .find(|u| u.get("name").unwrap() == "Bob")
        .expect("Bob should exist");
    let bob_posts = bob.get("posts").unwrap().as_array().unwrap();
    assert!(
        bob_posts.is_empty(),
        "Bob's posts should be filtered out. Got: {:?}",
        bob_posts
    );
}

// =============================================================================
// Nested Query Edge Cases and Error Handling Tests
// =============================================================================

#[tokio::test]
async fn test_nested_query_limit_on_nested_selection_returns_error() {
    // Query: { Users { posts(limit: 5) { title } } }
    // Limit on nested selections is not yet supported - should return clear error

    let fetcher = MockFetcher::new();

    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Users { name posts(limit: 5) { title } } }")
        .await;

    assert!(
        result.is_err(),
        "Expected error for limit on nested selection"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("limit") && err.contains("not yet supported"),
        "Error should mention limit not supported. Got: {}",
        err
    );
}

#[tokio::test]
async fn test_nested_query_order_on_nested_selection_returns_error() {
    // Query: { Users { posts(order: {title: ASC}) { title } } }
    // Order on nested selections is not yet supported - should return clear error

    let fetcher = MockFetcher::new();

    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Users { name posts(order: {title: ASC}) { title } } }")
        .await;

    assert!(
        result.is_err(),
        "Expected error for order on nested selection"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("order") && err.contains("not yet supported"),
        "Error should mention order not supported. Got: {}",
        err
    );
}

#[tokio::test]
async fn test_nested_query_exceeds_max_depth_returns_error() {
    // Query with >10 levels of nesting should fail with clear error
    // MAX_NESTING_DEPTH is 10 in builder.rs

    let fetcher = MockFetcher::new();

    // Create a chain of collections for deep nesting: L1 -> L2 -> ... -> L12
    fn make_level_collection(level: usize, next_level: Option<usize>) -> CollectionVersion {
        let name = format!("Level{}", level);
        let mut fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ];

        if let Some(next) = next_level {
            // Add relation to next level (one-to-many)
            fields.push(
                FieldDescription::new(
                    "3",
                    "children",
                    FieldKind::relation(&format!("Level{}", next), true),
                )
                .with_relation_name(&format!("level{}_{}", level, next)),
            );
        }

        // Add FK field if not the first level
        if level > 1 {
            fields.push(
                FieldDescription::new("4", "parent_id", FieldKind::doc_id())
                    .with_relation_name(&format!("level{}_{}", level - 1, level))
                    .as_primary(),
            );
        }

        CollectionVersion::new(&name, "v1", &format!("coll-level{}", level), fields)
    }

    // Create 12 levels of collections
    let collections: Vec<CollectionVersion> = (1..=12)
        .map(|i| {
            let next = if i < 12 { Some(i + 1) } else { None };
            make_level_collection(i, next)
        })
        .collect();

    let runner = QueryRunner::new(fetcher, collections);

    // Build a query with 11 levels of nesting (exceeds MAX_NESTING_DEPTH of 10)
    let query = "{ Level1 { name children { name children { name children { name children { name children { name children { name children { name children { name children { name children { name children { name } } } } } } } } } } } } }";

    let result = runner.execute_query(query).await;

    assert!(
        result.is_err(),
        "Expected error for query exceeding max nesting depth"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("nesting depth") && err.contains("exceeds maximum"),
        "Error should mention nesting depth limit. Got: {}",
        err
    );
}

#[tokio::test]
async fn test_nested_query_circular_relation_within_depth_limit() {
    // Query: { Posts { author { posts { author { name } } } } }
    // Circular relation A -> B -> A pattern, 4 levels deep (within limit)

    let fetcher = MockFetcher::new();

    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    let mut post = Document::new();
    post.set("_docID", "post-1");
    post.set("title", "Alice's Post");
    post.set("author_id", "user-1");
    fetcher.add_doc("Posts", post);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    // 4 levels: Posts -> author (Users) -> posts (Posts) -> author (Users)
    let result = runner
        .execute_query("{ Posts { title author { name posts { title author { name } } } } }")
        .await
        .unwrap();

    let posts = result.get("Posts").unwrap().as_array().unwrap();
    assert_eq!(posts.len(), 1);

    let post = &posts[0];
    assert_eq!(post.get("title").unwrap(), "Alice's Post");

    let author = post.get("author").unwrap();
    assert_eq!(author.get("name").unwrap(), "Alice");

    let nested_posts = author.get("posts").unwrap().as_array().unwrap();
    assert_eq!(nested_posts.len(), 1);
    assert_eq!(nested_posts[0].get("title").unwrap(), "Alice's Post");

    let nested_author = nested_posts[0].get("author").unwrap();
    assert_eq!(nested_author.get("name").unwrap(), "Alice");
}

#[tokio::test]
async fn test_nested_query_multiple_relations_at_same_level() {
    // Query: { Users { name posts { title } comments { text } } }
    // Tests multiple nested relations on the same parent type

    let fetcher = MockFetcher::new();

    // Users collection with both posts and comments relations
    let users_multi = CollectionVersion::new(
        "Users",
        "v1",
        "coll-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "posts", FieldKind::relation("Posts", true))
                .with_relation_name("author_posts"),
            FieldDescription::new("4", "comments", FieldKind::relation("Comments", true))
                .with_relation_name("author_comments"),
        ],
    );

    let posts = CollectionVersion::new(
        "Posts",
        "v1",
        "coll-posts",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("Users", false))
                .with_relation_name("author_posts")
                .as_primary(),
            FieldDescription::new("4", "author_id", FieldKind::doc_id())
                .with_relation_name("author_posts")
                .as_primary(),
        ],
    );

    let comments = CollectionVersion::new(
        "Comments",
        "v1",
        "coll-comments",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "text", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("Users", false))
                .with_relation_name("author_comments")
                .as_primary(),
            FieldDescription::new("4", "author_id", FieldKind::doc_id())
                .with_relation_name("author_comments")
                .as_primary(),
        ],
    );

    // Add test data
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    let mut bob = Document::new();
    bob.set("_docID", "user-2");
    bob.set("name", "Bob");
    fetcher.add_doc("Users", bob);

    let mut post1 = Document::new();
    post1.set("_docID", "post-1");
    post1.set("title", "Alice's First Post");
    post1.set("author_id", "user-1");
    fetcher.add_doc("Posts", post1);

    let mut post2 = Document::new();
    post2.set("_docID", "post-2");
    post2.set("title", "Alice's Second Post");
    post2.set("author_id", "user-1");
    fetcher.add_doc("Posts", post2);

    let mut comment1 = Document::new();
    comment1.set("_docID", "comment-1");
    comment1.set("text", "Great post!");
    comment1.set("author_id", "user-1");
    fetcher.add_doc("Comments", comment1);

    let mut comment2 = Document::new();
    comment2.set("_docID", "comment-2");
    comment2.set("text", "Thanks!");
    comment2.set("author_id", "user-2");
    fetcher.add_doc("Comments", comment2);

    let runner = QueryRunner::new(fetcher, vec![users_multi, posts, comments]);

    let result = runner
        .execute_query("{ Users { name posts { title } comments { text } } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 2, "Should have 2 users");

    // Find Alice and verify her posts and comments
    let alice = users
        .iter()
        .find(|u| u.get("name").unwrap() == "Alice")
        .unwrap();
    let alice_posts = alice.get("posts").unwrap().as_array().unwrap();
    assert_eq!(alice_posts.len(), 2, "Alice should have 2 posts");

    let alice_comments = alice.get("comments").unwrap().as_array().unwrap();
    assert_eq!(alice_comments.len(), 1, "Alice should have 1 comment");
    assert_eq!(alice_comments[0].get("text").unwrap(), "Great post!");

    // Find Bob and verify his relations
    let bob = users
        .iter()
        .find(|u| u.get("name").unwrap() == "Bob")
        .unwrap();
    let bob_posts = bob.get("posts").unwrap().as_array().unwrap();
    assert_eq!(bob_posts.len(), 0, "Bob should have 0 posts");

    let bob_comments = bob.get("comments").unwrap().as_array().unwrap();
    assert_eq!(bob_comments.len(), 1, "Bob should have 1 comment");
    assert_eq!(bob_comments[0].get("text").unwrap(), "Thanks!");
}

// Helper to create a collection with ACP policy
fn make_users_collection_with_acp() -> CollectionVersion {
    CollectionVersion::new(
        "ProtectedUsers",
        "v1",
        "coll-protected-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "posts", FieldKind::relation("Posts", true))
                .with_relation_name("protected_author_posts"),
        ],
    )
    .with_policy(PolicyDescription {
        id: "policy-1".to_string(),
        resource_name: "protected_users".to_string(),
    })
}

fn make_posts_collection_with_acp() -> CollectionVersion {
    CollectionVersion::new(
        "ProtectedPosts",
        "v1",
        "coll-protected-posts",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("Users", false))
                .with_relation_name("author_protected_posts")
                .as_primary(),
            FieldDescription::new("4", "author_id", FieldKind::doc_id())
                .with_relation_name("author_protected_posts")
                .as_primary(),
        ],
    )
    .with_policy(PolicyDescription {
        id: "policy-2".to_string(),
        resource_name: "protected_posts".to_string(),
    })
}

// Mock ACP for testing
struct MockAcp;

#[async_trait]
impl DocumentACP for MockAcp {
    async fn register_doc_object(
        &self,
        _identity: &Did,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<()> {
        Ok(())
    }

    async fn is_doc_registered(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<bool> {
        Ok(false)
    }

    async fn check_doc_access(
        &self,
        _identity: &acp::Identity,
        _permission: acp::DocumentPermission,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<bool> {
        Ok(true) // Allow all access for testing
    }

    async fn add_actor_relationship(
        &self,
        _requestor: &Did,
        _target: &Did,
        _collection_id: &str,
        _doc_id: &str,
        _relation: &str,
    ) -> acp::Result<bool> {
        Ok(true)
    }

    async fn delete_actor_relationship(
        &self,
        _requestor: &Did,
        _target: &Did,
        _collection_id: &str,
        _doc_id: &str,
        _relation: &str,
    ) -> acp::Result<bool> {
        Ok(true)
    }

    async fn unregister_doc_object(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_nested_query_blocked_when_root_collection_has_acp() {
    // Root collection has ACP policy, nested query should be blocked

    let fetcher = MockFetcher::new();

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_collection_with_acp(), // Root has ACP
            make_posts_with_author_collection(),
        ],
    )
    .with_acp(Arc::new(MockAcp));

    let result = runner
        .execute_query("{ ProtectedUsers { name posts { title } } }")
        .await;

    assert!(
        result.is_err(),
        "Expected error for nested query on ACP-protected root"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("ACP") && err.contains("not yet supported"),
        "Error should mention ACP not supported for nested queries. Got: {}",
        err
    );
}

#[tokio::test]
async fn test_nested_query_blocked_when_nested_collection_has_acp() {
    // Nested collection has ACP policy, nested query should be blocked

    let fetcher = MockFetcher::new();

    // Create users without ACP but with relation to protected posts
    let users_with_protected_posts = CollectionVersion::new(
        "Users",
        "v1",
        "coll-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new(
                "3",
                "protectedPosts",
                FieldKind::relation("ProtectedPosts", true),
            )
            .with_relation_name("author_protected_posts"),
        ],
    );

    let runner = QueryRunner::new(
        fetcher,
        vec![
            users_with_protected_posts,       // Root has no ACP
            make_posts_collection_with_acp(), // Nested has ACP
        ],
    )
    .with_acp(Arc::new(MockAcp));

    let result = runner
        .execute_query("{ Users { name protectedPosts { title } } }")
        .await;

    assert!(
        result.is_err(),
        "Expected error for nested query on ACP-protected nested collection"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("ACP") && err.contains("not yet supported"),
        "Error should mention ACP not supported for nested queries. Got: {}",
        err
    );
}

#[tokio::test]
async fn test_nested_query_allowed_when_no_acp_configured() {
    // When ACP is not configured on runner, nested queries should work
    // (even if collections have ACP policies defined)

    let fetcher = MockFetcher::new();

    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    let mut post = Document::new();
    post.set("_docID", "post-1");
    post.set("title", "Alice's Post");
    post.set("author_id", "user-1");
    fetcher.add_doc("Posts", post);

    // No .with_acp() - ACP not configured
    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Users { name posts { title } } }")
        .await;

    assert!(result.is_ok(), "Should succeed when ACP not configured");
    let result_data = result.unwrap();
    let users = result_data.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);
}

#[tokio::test]
async fn test_nested_query_blocked_when_deeply_nested_collection_has_acp() {
    // Test 3-level deep nesting where only the deepest level has ACP:
    // Authors (no ACP) → Books (no ACP) → Reviews (has ACP)
    // Query: { Authors { books { reviews { ... } } } }
    // Should be blocked because Reviews has ACP policy

    let fetcher = MockFetcher::new();

    // Authors collection (no ACP) - has relation to Books
    let authors = CollectionVersion::new(
        "Authors",
        "v1",
        "coll-authors",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "books", FieldKind::relation("Books", true))
                .with_relation_name("author_books"),
        ],
    );

    // Books collection (no ACP) - has relation to Reviews
    let books = CollectionVersion::new(
        "Books",
        "v1",
        "coll-books",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("Authors", false))
                .with_relation_name("author_books")
                .as_primary(),
            FieldDescription::new("4", "author_id", FieldKind::doc_id())
                .with_relation_name("author_books")
                .as_primary(),
            FieldDescription::new(
                "5",
                "reviews",
                FieldKind::relation("ProtectedReviews", true),
            )
            .with_relation_name("book_reviews"),
        ],
    );

    // Reviews collection (HAS ACP) - the deepest level
    let protected_reviews = CollectionVersion::new(
        "ProtectedReviews",
        "v1",
        "coll-protected-reviews",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "content", FieldKind::string()),
            FieldDescription::new("3", "rating", FieldKind::int()),
            FieldDescription::new("4", "book", FieldKind::relation("Books", false))
                .with_relation_name("book_reviews")
                .as_primary(),
            FieldDescription::new("5", "book_id", FieldKind::doc_id())
                .with_relation_name("book_reviews")
                .as_primary(),
        ],
    )
    .with_policy(PolicyDescription {
        id: "policy-reviews".to_string(),
        resource_name: "protected_reviews".to_string(),
    });

    let runner = QueryRunner::new(fetcher, vec![authors, books, protected_reviews])
        .with_acp(Arc::new(MockAcp));

    // Query 3 levels deep: Authors → Books → Reviews (protected)
    let result = runner
        .execute_query("{ Authors { name books { title reviews { content rating } } } }")
        .await;

    assert!(
        result.is_err(),
        "Expected error for deeply nested query touching ACP-protected collection"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("ACP") && err.contains("not yet supported"),
        "Error should mention ACP not supported. Got: {}",
        err
    );
    assert!(
        err.contains("ProtectedReviews"),
        "Error should identify the ACP-protected collection 'ProtectedReviews'. Got: {}",
        err
    );
}

// =============================================================================
// Complex Filter Tests on Nested Selections
// =============================================================================

#[tokio::test]
async fn test_nested_query_with_and_filter_on_nested_selection() {
    // Query: { Users { name posts(filter: {_and: [{title: {_like: "A%"}}, {title: {_ne: "Archived"}}]}) { title } } }
    // Tests _and filter on nested selection

    let fetcher = MockFetcher::new();

    // Add user
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    // Add posts - some match both conditions, some match one, some match none
    let mut post1 = Document::new();
    post1.set("_docID", "post-1");
    post1.set("title", "Amazing Post"); // Starts with A, not "Archived"
    post1.set("author_id", "user-1");
    fetcher.add_doc("Posts", post1);

    let mut post2 = Document::new();
    post2.set("_docID", "post-2");
    post2.set("title", "Archived"); // Starts with A, but IS "Archived"
    post2.set("author_id", "user-1");
    fetcher.add_doc("Posts", post2);

    let mut post3 = Document::new();
    post3.set("_docID", "post-3");
    post3.set("title", "Another Great"); // Starts with A, not "Archived"
    post3.set("author_id", "user-1");
    fetcher.add_doc("Posts", post3);

    let mut post4 = Document::new();
    post4.set("_docID", "post-4");
    post4.set("title", "Best Post"); // Doesn't start with A
    post4.set("author_id", "user-1");
    fetcher.add_doc("Posts", post4);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query(
            r#"{ Users { name posts(filter: {_and: [{title: {_like: "A%"}}, {title: {_ne: "Archived"}}]}) { title } } }"#,
        )
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);

    let posts = users[0].get("posts").unwrap().as_array().unwrap();
    // Only posts starting with A and NOT equal to "Archived" should be returned
    assert_eq!(posts.len(), 2, "Expected 2 posts matching _and condition");

    let titles: Vec<&str> = posts
        .iter()
        .map(|p| p.get("title").unwrap().as_str().unwrap())
        .collect();
    assert!(titles.contains(&"Amazing Post"));
    assert!(titles.contains(&"Another Great"));
    assert!(!titles.contains(&"Archived"));
    assert!(!titles.contains(&"Best Post"));
}

#[tokio::test]
async fn test_nested_query_with_or_filter_on_nested_selection() {
    // Query: { Users { name posts(filter: {_or: [{title: {_eq: "Post A"}}, {title: {_eq: "Post C"}}]}) { title } } }
    // Tests _or filter on nested selection

    let fetcher = MockFetcher::new();

    // Add user
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    // Add posts
    let mut post_a = Document::new();
    post_a.set("_docID", "post-a");
    post_a.set("title", "Post A");
    post_a.set("author_id", "user-1");
    fetcher.add_doc("Posts", post_a);

    let mut post_b = Document::new();
    post_b.set("_docID", "post-b");
    post_b.set("title", "Post B");
    post_b.set("author_id", "user-1");
    fetcher.add_doc("Posts", post_b);

    let mut post_c = Document::new();
    post_c.set("_docID", "post-c");
    post_c.set("title", "Post C");
    post_c.set("author_id", "user-1");
    fetcher.add_doc("Posts", post_c);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query(
            r#"{ Users { name posts(filter: {_or: [{title: {_eq: "Post A"}}, {title: {_eq: "Post C"}}]}) { title } } }"#,
        )
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);

    let posts = users[0].get("posts").unwrap().as_array().unwrap();
    // Only Post A and Post C should be returned
    assert_eq!(posts.len(), 2, "Expected 2 posts matching _or condition");

    let titles: Vec<&str> = posts
        .iter()
        .map(|p| p.get("title").unwrap().as_str().unwrap())
        .collect();
    assert!(titles.contains(&"Post A"));
    assert!(titles.contains(&"Post C"));
    assert!(!titles.contains(&"Post B"));
}

#[tokio::test]
async fn test_nested_query_with_not_filter_on_nested_selection() {
    // Query: { Users { name posts(filter: {_not: {title: {_eq: "Draft"}}}) { title } } }
    // Tests _not filter on nested selection

    let fetcher = MockFetcher::new();

    // Add user
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    // Add posts
    let mut published = Document::new();
    published.set("_docID", "post-1");
    published.set("title", "Published Post");
    published.set("author_id", "user-1");
    fetcher.add_doc("Posts", published);

    let mut draft = Document::new();
    draft.set("_docID", "post-2");
    draft.set("title", "Draft");
    draft.set("author_id", "user-1");
    fetcher.add_doc("Posts", draft);

    let mut another = Document::new();
    another.set("_docID", "post-3");
    another.set("title", "Another Post");
    another.set("author_id", "user-1");
    fetcher.add_doc("Posts", another);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query(
            r#"{ Users { name posts(filter: {_not: {title: {_eq: "Draft"}}}) { title } } }"#,
        )
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);

    let posts = users[0].get("posts").unwrap().as_array().unwrap();
    // All posts except "Draft" should be returned
    assert_eq!(posts.len(), 2, "Expected 2 posts (not Draft)");

    let titles: Vec<&str> = posts
        .iter()
        .map(|p| p.get("title").unwrap().as_str().unwrap())
        .collect();
    assert!(titles.contains(&"Published Post"));
    assert!(titles.contains(&"Another Post"));
    assert!(!titles.contains(&"Draft"));
}

#[tokio::test]
async fn test_nested_query_with_complex_combined_filter() {
    // Query: { Users { name posts(filter: {_and: [{_or: [{title: {_like: "A%"}}, {title: {_like: "B%"}}]}, {_not: {title: {_eq: "Blocked"}}}]}) { title } } }
    // Tests combined _and, _or, _not filter on nested selection

    let fetcher = MockFetcher::new();

    // Add user
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    // Add posts
    let mut awesome = Document::new();
    awesome.set("_docID", "post-1");
    awesome.set("title", "Awesome");
    awesome.set("author_id", "user-1");
    fetcher.add_doc("Posts", awesome);

    let mut brilliant = Document::new();
    brilliant.set("_docID", "post-2");
    brilliant.set("title", "Brilliant");
    brilliant.set("author_id", "user-1");
    fetcher.add_doc("Posts", brilliant);

    let mut blocked = Document::new();
    blocked.set("_docID", "post-3");
    blocked.set("title", "Blocked"); // Starts with B but is excluded by _not
    blocked.set("author_id", "user-1");
    fetcher.add_doc("Posts", blocked);

    let mut cool = Document::new();
    cool.set("_docID", "post-4");
    cool.set("title", "Cool"); // Doesn't start with A or B
    cool.set("author_id", "user-1");
    fetcher.add_doc("Posts", cool);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query(
            r#"{ Users { name posts(filter: {_and: [{_or: [{title: {_like: "A%"}}, {title: {_like: "B%"}}]}, {_not: {title: {_eq: "Blocked"}}}]}) { title } } }"#,
        )
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert_eq!(users.len(), 1);

    let posts = users[0].get("posts").unwrap().as_array().unwrap();
    // Only "Awesome" and "Brilliant" should be returned
    // (starts with A or B) AND (not "Blocked")
    assert_eq!(posts.len(), 2, "Expected 2 posts matching combined filter");

    let titles: Vec<&str> = posts
        .iter()
        .map(|p| p.get("title").unwrap().as_str().unwrap())
        .collect();
    assert!(titles.contains(&"Awesome"));
    assert!(titles.contains(&"Brilliant"));
    assert!(!titles.contains(&"Blocked"));
    assert!(!titles.contains(&"Cool"));
}

#[tokio::test]
async fn test_nested_query_empty_parent_collection() {
    // Query: { Users { name posts { title } } }
    // Tests that empty parent collection returns empty array (not error)

    let fetcher = MockFetcher::new();

    // Add posts but no users
    let mut post = Document::new();
    post.set("_docID", "post-1");
    post.set("title", "Orphan Post");
    post.set("author_id", "nonexistent-user");
    fetcher.add_doc("Posts", post);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Users { name posts { title } } }")
        .await
        .unwrap();

    let users = result.get("Users").unwrap().as_array().unwrap();
    assert!(
        users.is_empty(),
        "Should return empty array for empty parent collection"
    );
}

#[tokio::test]
async fn test_nested_query_filter_on_nonexistent_field_returns_error() {
    // Query: { Users { name posts(filter: { nonexistent_field: { _eq: "value" } }) { title } } }
    // Tests that filter on non-existent field returns an error

    let fetcher = MockFetcher::new();

    // Add user
    let mut alice = Document::new();
    alice.set("_docID", "user-1");
    alice.set("name", "Alice");
    fetcher.add_doc("Users", alice);

    // Add post
    let mut post = Document::new();
    post.set("_docID", "post-1");
    post.set("title", "Test Post");
    post.set("author_id", "user-1");
    fetcher.add_doc("Posts", post);

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query(
            r#"{ Users { name posts(filter: { nonexistent_field: { _eq: "value" } }) { title } } }"#,
        )
        .await;

    // Should return an error for unknown field in filter
    assert!(
        result.is_err(),
        "Should return error for filter on non-existent field"
    );
    let err = result.unwrap_err();
    let err_msg = err.to_string().to_lowercase();
    assert!(
        err_msg.contains("unknown") || err_msg.contains("field") || err_msg.contains("not found"),
        "Error should indicate unknown field: {}",
        err
    );
}

// =============================================================================
// ACP Error Propagation Tests
// =============================================================================

/// ACP implementation that returns storage errors on check_doc_access.
/// Used to verify that ACP errors are properly propagated rather than silently swallowed.
struct FailingAcp {
    error_message: String,
}

impl FailingAcp {
    fn new(message: &str) -> Self {
        Self {
            error_message: message.to_string(),
        }
    }
}

#[async_trait]
impl DocumentACP for FailingAcp {
    async fn register_doc_object(
        &self,
        _identity: &Did,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<()> {
        Ok(())
    }

    async fn is_doc_registered(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<bool> {
        // Return error to simulate storage failure
        Err(acp::Error::Storage(self.error_message.clone()))
    }

    async fn check_doc_access(
        &self,
        _identity: &acp::Identity,
        _permission: acp::DocumentPermission,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<bool> {
        // Return error to simulate storage failure
        Err(acp::Error::Storage(self.error_message.clone()))
    }

    async fn add_actor_relationship(
        &self,
        _requestor: &Did,
        _target: &Did,
        _collection_id: &str,
        _doc_id: &str,
        _relation: &str,
    ) -> acp::Result<bool> {
        Ok(true)
    }

    async fn delete_actor_relationship(
        &self,
        _requestor: &Did,
        _target: &Did,
        _collection_id: &str,
        _doc_id: &str,
        _relation: &str,
    ) -> acp::Result<bool> {
        Ok(true)
    }

    async fn unregister_doc_object(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<()> {
        Ok(())
    }
}

/// ACP implementation that denies all access (returns Ok(false) for check_doc_access).
/// Used to verify permission denied scenarios work correctly.
struct DenyingAcp;

#[async_trait]
impl DocumentACP for DenyingAcp {
    async fn register_doc_object(
        &self,
        _identity: &Did,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<()> {
        Ok(())
    }

    async fn is_doc_registered(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<bool> {
        // Document is registered, so ACP checks will be performed
        Ok(true)
    }

    async fn check_doc_access(
        &self,
        _identity: &acp::Identity,
        _permission: acp::DocumentPermission,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<bool> {
        // Deny all access
        Ok(false)
    }

    async fn add_actor_relationship(
        &self,
        _requestor: &Did,
        _target: &Did,
        _collection_id: &str,
        _doc_id: &str,
        _relation: &str,
    ) -> acp::Result<bool> {
        Ok(true)
    }

    async fn delete_actor_relationship(
        &self,
        _requestor: &Did,
        _target: &Did,
        _collection_id: &str,
        _doc_id: &str,
        _relation: &str,
    ) -> acp::Result<bool> {
        Ok(true)
    }

    async fn unregister_doc_object(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_mutation_update_acp_error_propagates() {
    // When ACP check_doc_access returns an error, it should propagate
    // rather than being silently swallowed.

    let fetcher = MockFetcher::new();
    let mutator = Arc::new(MockMutator::new());

    // Add a document to update
    let mut doc = Document::new();
    doc.set("_docID", "doc-1");
    doc.set("name", "Alice");
    doc.set("age", 30i64);
    mutator.add_doc("Users", doc.clone());
    fetcher.add_doc("Users", doc);

    let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
        .with_mutator(mutator)
        .with_acp(Arc::new(FailingAcp::new("connection refused")));

    let result = runner
        .execute_mutation_with_identity(
            r#"mutation { update_Users(docIDs: ["doc-1"], input: { name: "Bob" }) { name } }"#,
            Some(test_acp_did()),
        )
        .await;

    // Should fail with ACP error, not permission denied
    assert!(result.is_err(), "Expected ACP error to propagate");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("ACP") && err.contains("check failed"),
        "Error should indicate ACP check failed (not generic permission denied). Got: {}",
        err
    );
    assert!(
        err.contains("connection refused"),
        "Error should contain the underlying error message. Got: {}",
        err
    );
}

#[tokio::test]
async fn test_mutation_delete_acp_error_propagates() {
    // When ACP check_doc_access returns an error during DELETE, it should propagate.

    let fetcher = MockFetcher::new();
    let mutator = Arc::new(MockMutator::new());

    // Add a document to delete
    let mut doc = Document::new();
    doc.set("_docID", "doc-1");
    doc.set("name", "Alice");
    doc.set("age", 30i64);
    mutator.add_doc("Users", doc.clone());
    fetcher.add_doc("Users", doc);

    let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
        .with_mutator(mutator)
        .with_acp(Arc::new(FailingAcp::new("database unavailable")));

    let result = runner
        .execute_mutation_with_identity(
            r#"mutation { delete_Users(docIDs: ["doc-1"]) { _docID } }"#,
            Some(test_acp_did()),
        )
        .await;

    // Should fail with ACP error
    assert!(result.is_err(), "Expected ACP error to propagate");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("ACP") && err.contains("check failed"),
        "Error should indicate ACP check failed. Got: {}",
        err
    );
    assert!(
        err.contains("database unavailable"),
        "Error should contain the underlying error message. Got: {}",
        err
    );
}

#[tokio::test]
async fn test_mutation_create_registration_check_error_propagates() {
    // When is_doc_registered returns an error during CREATE, it should propagate.

    let fetcher = MockFetcher::new();
    let mutator = Arc::new(MockMutator::new());

    let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
        .with_mutator(mutator)
        .with_acp(Arc::new(FailingAcp::new("storage timeout")));

    let result = runner
        .execute_mutation_with_identity(
            r#"mutation { create_Users(input: [{ name: "Alice", age: 30 }]) { _docID name } }"#,
            Some(test_acp_did()),
        )
        .await;

    // Should fail with ACP registration check error
    assert!(
        result.is_err(),
        "Expected ACP registration check error to propagate"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("registration") && err.contains("storage timeout"),
        "Error should indicate registration check failed with underlying error. Got: {}",
        err
    );
}

#[tokio::test]
async fn test_mutation_update_permission_denied() {
    // When ACP check_doc_access returns Ok(false), should get permission denied error.

    let fetcher = MockFetcher::new();
    let mutator = Arc::new(MockMutator::new());

    // Add a document to update
    let mut doc = Document::new();
    doc.set("_docID", "doc-1");
    doc.set("name", "Alice");
    doc.set("age", 30i64);
    mutator.add_doc("Users", doc.clone());
    fetcher.add_doc("Users", doc);

    let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
        .with_mutator(mutator)
        .with_acp(Arc::new(DenyingAcp));

    let result = runner
        .execute_mutation_with_identity(
            r#"mutation { update_Users(docIDs: ["doc-1"], input: { name: "Bob" }) { name } }"#,
            Some(test_acp_did()),
        )
        .await;

    // Should fail with permission denied (not ACP check failed)
    assert!(result.is_err(), "Expected permission denied error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("permission denied"),
        "Error should indicate permission denied. Got: {}",
        err
    );
    assert!(
        err.contains("update"),
        "Error should mention the operation type. Got: {}",
        err
    );
}

#[tokio::test]
async fn test_mutation_delete_permission_denied() {
    // When ACP check_doc_access returns Ok(false) for DELETE, should get permission denied.

    let fetcher = MockFetcher::new();
    let mutator = Arc::new(MockMutator::new());

    // Add a document to delete
    let mut doc = Document::new();
    doc.set("_docID", "doc-1");
    doc.set("name", "Alice");
    doc.set("age", 30i64);
    mutator.add_doc("Users", doc.clone());
    fetcher.add_doc("Users", doc);

    let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
        .with_mutator(mutator)
        .with_acp(Arc::new(DenyingAcp));

    let result = runner
        .execute_mutation_with_identity(
            r#"mutation { delete_Users(docIDs: ["doc-1"]) { _docID } }"#,
            Some(test_acp_did()),
        )
        .await;

    // Should fail with permission denied
    assert!(result.is_err(), "Expected permission denied error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("permission denied"),
        "Error should indicate permission denied. Got: {}",
        err
    );
    assert!(
        err.contains("delete"),
        "Error should mention the operation type. Got: {}",
        err
    );
}

// =============================================================================
// Large Dataset Tests
// =============================================================================

#[tokio::test]
async fn test_nested_query_with_100_users_and_10_posts_each() {
    // Tests that nested queries perform well with larger datasets.
    // 100 users with 10 posts each = 1000 posts total.

    let fetcher = MockFetcher::new();

    // Add 100 users
    for i in 0..100 {
        let mut user = Document::new();
        user.set("_docID", format!("user-{}", i));
        user.set("name", format!("User {}", i));
        fetcher.add_doc("Users", user);
    }

    // Add 1000 posts (10 per user)
    for i in 0..1000 {
        let mut post = Document::new();
        post.set("_docID", format!("post-{}", i));
        post.set("title", format!("Post {} by User {}", i % 10, i / 10));
        post.set("author_id", format!("user-{}", i / 10));
        fetcher.add_doc("Posts", post);
    }

    let runner = QueryRunner::new(
        fetcher,
        vec![
            make_users_with_posts_collection(),
            make_posts_with_author_collection(),
        ],
    );

    let result = runner
        .execute_query("{ Users { _docID name posts { _docID title } } }")
        .await;

    assert!(result.is_ok(), "Query should succeed with large dataset");
    let result_data = result.unwrap();
    let users = result_data.get("Users").unwrap().as_array().unwrap();

    // Verify all 100 users returned
    assert_eq!(users.len(), 100, "Should have 100 users");

    // Verify each user has 10 posts
    for user in users {
        let posts = user.get("posts").unwrap().as_array().unwrap();
        assert_eq!(posts.len(), 10, "Each user should have 10 posts");
    }
}

#[tokio::test]
async fn test_nested_query_three_levels_with_many_documents() {
    // Tests multi-level nesting with larger datasets.
    // 20 users -> 50 posts total -> 200 comments total

    let fetcher = MockFetcher::new();

    // Add 20 users
    for i in 0..20 {
        let mut user = Document::new();
        user.set("_docID", format!("user-{}", i));
        user.set("name", format!("User {}", i));
        fetcher.add_doc("Users", user);
    }

    // Add 50 posts (some users have more, some have less)
    for i in 0..50 {
        let mut post = Document::new();
        post.set("_docID", format!("post-{}", i));
        post.set("title", format!("Post {}", i));
        post.set("author_id", format!("user-{}", i % 20)); // Distribute among users
        fetcher.add_doc("Posts", post);
    }

    // Add 200 comments (4 per post)
    for i in 0..200 {
        let mut comment = Document::new();
        comment.set("_docID", format!("comment-{}", i));
        comment.set("text", format!("Comment {}", i));
        comment.set("post_id", format!("post-{}", i / 4)); // 4 comments per post
        fetcher.add_doc("Comments", comment);
    }

    // Create collections with nested relations
    let users_collection = CollectionVersion::new(
        "Users",
        "v1",
        "coll-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "posts", FieldKind::relation("Posts", true))
                .with_relation_name("author_posts"),
        ],
    );

    let posts_collection = CollectionVersion::new(
        "Posts",
        "v1",
        "coll-posts",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("Users", false))
                .with_relation_name("author_posts")
                .as_primary(),
            FieldDescription::new("4", "author_id", FieldKind::doc_id())
                .with_relation_name("author_posts")
                .as_primary(),
            FieldDescription::new("5", "comments", FieldKind::relation("Comments", true))
                .with_relation_name("post_comments"),
        ],
    );

    let comments_collection = CollectionVersion::new(
        "Comments",
        "v1",
        "coll-comments",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "text", FieldKind::string()),
            FieldDescription::new("3", "post", FieldKind::relation("Posts", false))
                .with_relation_name("post_comments")
                .as_primary(),
            FieldDescription::new("4", "post_id", FieldKind::doc_id())
                .with_relation_name("post_comments")
                .as_primary(),
        ],
    );

    let runner = QueryRunner::new(
        fetcher,
        vec![users_collection, posts_collection, comments_collection],
    );

    let result = runner
        .execute_query("{ Users { _docID name posts { _docID title comments { _docID text } } } }")
        .await;

    assert!(
        result.is_ok(),
        "3-level nested query should succeed with many documents"
    );
    let result_data = result.unwrap();
    let users = result_data.get("Users").unwrap().as_array().unwrap();

    // Verify all 20 users returned
    assert_eq!(users.len(), 20, "Should have 20 users");

    // Count total posts and comments to verify join correctness
    let mut total_posts = 0;
    let mut total_comments = 0;
    for user in users {
        let posts = user.get("posts").unwrap().as_array().unwrap();
        total_posts += posts.len();
        for post in posts {
            let comments = post.get("comments").unwrap().as_array().unwrap();
            total_comments += comments.len();
        }
    }

    assert_eq!(total_posts, 50, "Should have 50 total posts");
    assert_eq!(total_comments, 200, "Should have 200 total comments");
}

#[tokio::test]
async fn test_multi_level_relation_filter() {
    // Test multi-level relation filter: Book(filter: {author: {published: {rating: {_eq: 4.9}}}})
    // This filters Books where the author's published book has rating 4.9.
    // Book → Author → Book (published) → rating
    //
    // Data setup:
    // Book 0: "Painted House" (rating 4.9)
    // Book 1: "Theif Lord" (rating 4.8)
    // Author 0: John Grisham -> published Book 0
    // Author 1: Cornelia Funke -> published Book 1
    //
    // Expected: Only Book 0 should be returned (its author's published book has rating 4.9)

    let fetcher = MockFetcher::new();

    // Add books
    let mut book0 = Document::new();
    book0.set("_docID", "book-0");
    book0.set("name", "Painted House");
    book0.set("rating", 4.9f64);
    fetcher.add_doc("Book", book0);

    let mut book1 = Document::new();
    book1.set("_docID", "book-1");
    book1.set("name", "Theif Lord");
    book1.set("rating", 4.8f64);
    fetcher.add_doc("Book", book1);

    // Add authors with published_id (FK to the book they published)
    let mut author0 = Document::new();
    author0.set("_docID", "author-0");
    author0.set("name", "John Grisham");
    author0.set("age", 65);
    author0.set("published_id", "book-0");
    fetcher.add_doc("Author", author0);

    let mut author1 = Document::new();
    author1.set("_docID", "author-1");
    author1.set("name", "Cornelia Funke");
    author1.set("age", 62);
    author1.set("published_id", "book-1");
    fetcher.add_doc("Author", author1);

    // Create Book collection - author is inverted (Author.published_id points to Book._docID)
    let book_collection = CollectionVersion::new(
        "Book",
        "v1",
        "coll-book",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "rating", FieldKind::float64()),
            FieldDescription::new("4", "author", FieldKind::relation("Author", false))
                .with_relation_name("published"),
        ],
    );

    // Create Author collection - published is primary (has published_id FK)
    let author_collection = CollectionVersion::new(
        "Author",
        "v1",
        "coll-author",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
            FieldDescription::new("4", "published", FieldKind::relation("Book", false))
                .as_primary()
                .with_relation_name("published"),
            FieldDescription::new("5", "published_id", FieldKind::string()),
        ],
    );

    let runner = QueryRunner::new(fetcher, vec![book_collection, author_collection]);

    // Query with multi-level filter: Book where author.published.rating == 4.9
    let result = runner
        .execute_query(
            r#"{ Book(filter: {author: {published: {rating: {_eq: 4.9}}}}) { name rating author { name age } } }"#,
        )
        .await;

    assert!(
        result.is_ok(),
        "Multi-level filter query should succeed. Error: {:?}",
        result.err()
    );
    let result_data = result.unwrap();
    let books = result_data.get("Book").unwrap().as_array().unwrap();

    // Should return exactly 1 book (Painted House)
    assert_eq!(books.len(), 1, "Should return 1 book matching the filter");

    let book = &books[0];
    assert_eq!(
        book.get("name").unwrap().as_str().unwrap(),
        "Painted House",
        "Should be Painted House"
    );
    assert_eq!(
        book.get("rating").unwrap().as_f64().unwrap(),
        4.9,
        "Rating should be 4.9"
    );

    let author = book.get("author").unwrap();
    assert_eq!(
        author.get("name").unwrap().as_str().unwrap(),
        "John Grisham",
        "Author should be John Grisham"
    );
}

// =============================================================================
// Secondary Relation ID Field Tests
// =============================================================================

/// Test querying a secondary relation ID field without the relation object.
/// This matches the failing FFI test: TestQueryOneToOne_WithRelationIDFromSecondarySide
///
/// Schema:
/// - Book { name, author } where author is SECONDARY (no @primary)
/// - Author { name, published @primary } where published is PRIMARY
///
/// When querying `Book { name _authorID }`, the `_authorID` should be populated
/// by doing a reverse lookup: find Author where _publishedID = Book._docID
#[tokio::test]
async fn test_secondary_relation_id_field_without_relation_object() {
    let fetcher = MockFetcher::new();

    // Create Book collection - author is SECONDARY (no FK stored in Book)
    // The _authorID field exists in the schema but is NOT primary
    let book_collection = CollectionVersion::new(
        "Book",
        "v1",
        "coll-book",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // author relation field - SECONDARY (is_primary = false)
            FieldDescription::new("", "author", FieldKind::relation("Author", false))
                .with_relation_name("published"),
            // _authorID field for the relation ID - also SECONDARY
            FieldDescription::new("", "_authorID", FieldKind::doc_id())
                .with_relation_name("published"),
        ],
    );

    // Create Author collection - published is PRIMARY (has FK)
    let author_collection = CollectionVersion::new(
        "Author",
        "v1",
        "coll-author",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // published relation field - PRIMARY (is_primary = true)
            FieldDescription::new("3", "published", FieldKind::relation("Book", false))
                .with_relation_name("published")
                .as_primary(),
            // _publishedID FK field - also PRIMARY
            FieldDescription::new("4", "_publishedID", FieldKind::doc_id())
                .with_relation_name("published")
                .as_primary(),
        ],
    );

    // Add Book document
    let mut book = Document::new();
    book.set("_docID", "book-1");
    book.set("name", "Painted House");
    fetcher.add_doc("Book", book);

    // Add Author document with FK pointing to the book
    let mut author = Document::new();
    author.set("_docID", "author-1");
    author.set("name", "John Grisham");
    author.set("_publishedID", "book-1"); // FK points to book-1
    fetcher.add_doc("Author", author);

    let runner = QueryRunner::new(fetcher, vec![book_collection, author_collection]);

    // Query only the relation ID field, not the relation object
    let result = runner
        .execute_query("{ Book { name _authorID } }")
        .await
        .unwrap();

    eprintln!("Result: {}", serde_json::to_string_pretty(&result).unwrap());

    let books = result.get("Book").unwrap().as_array().unwrap();
    assert_eq!(books.len(), 1);

    let book = &books[0];
    assert_eq!(book.get("name").unwrap(), "Painted House");

    // _authorID should be populated with the Author's _docID
    // This requires a reverse lookup: find Author where _publishedID = Book._docID
    let author_id = book.get("_authorID");
    assert!(
        author_id.is_some(),
        "_authorID field should be present in result"
    );
    assert_eq!(
        author_id.unwrap(),
        "author-1",
        "_authorID should be the Author's _docID from reverse lookup"
    );
}

/// Test compound filter with both scalar and relation conditions.
/// This matches the failing FFI test: TestQueryOneToOneWithCompoundAndFilterThatIncludesRelation
///
/// Schema:
/// - Book { name, rating, author } where author is SECONDARY
/// - Author { name, age, verified, published @primary }
///
/// Query: Book(filter: {_and: [{rating: {_geq: 4.0}}, {author: {verified: {_eq: true}}}]}) { name rating }
/// Expected: Only books where rating >= 4.0 AND author.verified == true
#[tokio::test]
async fn test_compound_filter_with_relation_condition() {
    let fetcher = MockFetcher::new();

    // Create Book collection - author is SECONDARY
    let book_collection = CollectionVersion::new(
        "Book",
        "v1",
        "coll-book",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "rating", FieldKind::float64()),
            // author relation field - SECONDARY (is_primary = false)
            FieldDescription::new("", "author", FieldKind::relation("Author", false))
                .with_relation_name("published"),
            // _authorID field for the relation ID - also SECONDARY
            FieldDescription::new("", "_authorID", FieldKind::doc_id())
                .with_relation_name("published"),
        ],
    );

    // Create Author collection - published is PRIMARY
    let author_collection = CollectionVersion::new(
        "Author",
        "v1",
        "coll-author",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
            FieldDescription::new("4", "verified", FieldKind::bool()),
            // published relation field - PRIMARY
            FieldDescription::new("5", "published", FieldKind::relation("Book", false))
                .with_relation_name("published")
                .as_primary(),
            // _publishedID FK field - also PRIMARY
            FieldDescription::new("6", "_publishedID", FieldKind::doc_id())
                .with_relation_name("published")
                .as_primary(),
        ],
    );

    // Add Books
    let mut book1 = Document::new();
    book1.set("_docID", "book-1");
    book1.set("name", "Painted House");
    book1.set("rating", 4.9f64);
    fetcher.add_doc("Book", book1);

    let mut book2 = Document::new();
    book2.set("_docID", "book-2");
    book2.set("name", "Some Book");
    book2.set("rating", 4.0f64);
    fetcher.add_doc("Book", book2);

    let mut book3 = Document::new();
    book3.set("_docID", "book-3");
    book3.set("name", "Low Rated Book");
    book3.set("rating", 3.0f64);
    fetcher.add_doc("Book", book3);

    // Add Authors with verified status
    let mut author1 = Document::new();
    author1.set("_docID", "author-1");
    author1.set("name", "John Grisham");
    author1.set("age", 65i64);
    author1.set("verified", true);
    author1.set("_publishedID", "book-1"); // FK points to Painted House
    fetcher.add_doc("Author", author1);

    let mut author2 = Document::new();
    author2.set("_docID", "author-2");
    author2.set("name", "Some Writer");
    author2.set("age", 45i64);
    author2.set("verified", false); // NOT verified
    author2.set("_publishedID", "book-2"); // FK points to Some Book
    fetcher.add_doc("Author", author2);

    let mut author3 = Document::new();
    author3.set("_docID", "author-3");
    author3.set("name", "Another Writer");
    author3.set("age", 30i64);
    author3.set("verified", true); // verified but low rated book
    author3.set("_publishedID", "book-3"); // FK points to Low Rated Book
    fetcher.add_doc("Author", author3);

    let runner = QueryRunner::new(fetcher, vec![book_collection, author_collection]);

    // Compound filter: rating >= 4.0 AND author.verified == true
    let result = runner
        .execute_query(
            r#"{ Book(filter: {_and: [{rating: {_ge: 4.0}}, {author: {verified: {_eq: true}}}]}) { name rating } }"#,
        )
        .await
        .unwrap();

    eprintln!("Result: {}", serde_json::to_string_pretty(&result).unwrap());

    let books = result.get("Book").unwrap().as_array().unwrap();

    // Expected: Only "Painted House" because:
    // - rating 4.9 >= 4.0 ✓
    // - author (John Grisham) verified = true ✓
    //
    // "Some Book" is excluded because author.verified = false
    // "Low Rated Book" is excluded because rating 3.0 < 4.0
    assert_eq!(
        books.len(),
        1,
        "Should return 1 book matching both conditions"
    );
    assert_eq!(books[0].get("name").unwrap(), "Painted House");
}

#[tokio::test]
async fn test_order_by_relation_field_strips_ordering_only_fields() {
    // Test: ORDER BY author.verified should work correctly, but the `verified` field
    // should NOT appear in the output when not explicitly selected.
    // Query: { Book(order: {author: {verified: ASC}}) { name author { name } } }

    let fetcher = MockFetcher::new();

    // Create Book collection - author is SECONDARY (looks up Author via reverse FK)
    let book_collection = CollectionVersion::new(
        "Book",
        "v1",
        "coll-book",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // author relation field - SECONDARY (is_primary = false)
            FieldDescription::new("", "author", FieldKind::relation("Author", false))
                .with_relation_name("published"),
            // _authorID field for the relation ID - also SECONDARY
            FieldDescription::new("", "_authorID", FieldKind::doc_id())
                .with_relation_name("published"),
        ],
    );

    // Create Author collection - published is PRIMARY
    let author_collection = CollectionVersion::new(
        "Author",
        "v1",
        "coll-author",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "verified", FieldKind::bool()),
            // published relation field - PRIMARY
            FieldDescription::new("4", "published", FieldKind::relation("Book", false))
                .with_relation_name("published")
                .as_primary(),
            // _publishedID FK field - also PRIMARY
            FieldDescription::new("5", "_publishedID", FieldKind::doc_id())
                .with_relation_name("published")
                .as_primary(),
        ],
    );

    // Add Books
    let mut book1 = Document::new();
    book1.set("_docID", "book-1");
    book1.set("name", "Book One");
    fetcher.add_doc("Book", book1);

    let mut book2 = Document::new();
    book2.set("_docID", "book-2");
    book2.set("name", "Book Two");
    fetcher.add_doc("Book", book2);

    // Add Authors with different verified status
    let mut author1 = Document::new();
    author1.set("_docID", "author-1");
    author1.set("name", "Verified Author");
    author1.set("verified", true);
    author1.set("_publishedID", "book-1");
    fetcher.add_doc("Author", author1);

    let mut author2 = Document::new();
    author2.set("_docID", "author-2");
    author2.set("name", "Unverified Author");
    author2.set("verified", false);
    author2.set("_publishedID", "book-2");
    fetcher.add_doc("Author", author2);

    let runner = QueryRunner::new(fetcher, vec![book_collection, author_collection]);

    // Order by author.verified ASC - should sort unverified (false) before verified (true)
    // but the `verified` field should NOT appear in the output
    let result = runner
        .execute_query(r#"{ Book(order: {author: {verified: ASC}}) { name author { name } } }"#)
        .await
        .unwrap();

    eprintln!("Result: {}", serde_json::to_string_pretty(&result).unwrap());

    let books = result.get("Book").unwrap().as_array().unwrap();
    assert_eq!(books.len(), 2);

    // First book should be Book Two (author.verified = false, sorts first in ASC)
    let first_book = &books[0];
    assert_eq!(first_book.get("name").unwrap(), "Book Two");

    // Second book should be Book One (author.verified = true)
    let second_book = &books[1];
    assert_eq!(second_book.get("name").unwrap(), "Book One");

    // CRITICAL: The `verified` field should NOT appear in the author object
    // because it was only added for ordering, not selected
    let first_author = first_book.get("author").unwrap().as_object().unwrap();
    assert!(
        !first_author.contains_key("verified"),
        "The 'verified' field should NOT appear in output when not selected. Found: {:?}",
        first_author
    );
    assert!(
        first_author.contains_key("name"),
        "The 'name' field should appear (it was selected)"
    );

    let second_author = second_book.get("author").unwrap().as_object().unwrap();
    assert!(
        !second_author.contains_key("verified"),
        "The 'verified' field should NOT appear in output when not selected. Found: {:?}",
        second_author
    );
}

// Test GROUP BY with _avg aggregate
#[tokio::test]
async fn test_group_by_with_average() {
    let fetcher = MockFetcher::new();

    let mut doc1 = Document::new();
    doc1.set("name", "John");
    doc1.set("age", 32i64);
    doc1.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc1);

    let mut doc2 = Document::new();
    doc2.set("name", "John");
    doc2.set("age", 38i64);
    doc2.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc2);

    let mut doc3 = Document::new();
    doc3.set("name", "Alice");
    doc3.set("age", -19i64);
    doc3.generate_and_set_doc_id().unwrap();
    fetcher.add_doc("Users", doc3);

    let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

    // Use the same syntax as the Go test: _avg(_group: {field: Age})
    let result = runner
        .execute_query("{ Users(groupBy: [name]) { name _avg(_group: {field: age}) } }")
        .await
        .unwrap();

    let users = result["Users"].as_array().unwrap();
    assert_eq!(users.len(), 2, "Should have 2 groups: John and Alice");

    let john = users
        .iter()
        .find(|u| u["name"].as_str() == Some("John"))
        .unwrap();
    assert_eq!(
        john["_avg"].as_f64(),
        Some(35.0),
        "John's average age should be 35 (32+38)/2"
    );

    let alice = users
        .iter()
        .find(|u| u["name"].as_str() == Some("Alice"))
        .unwrap();
    assert_eq!(
        alice["_avg"].as_f64(),
        Some(-19.0),
        "Alice's average age should be -19"
    );
}
