//! Mock executors for testing HTTP routes.

use async_trait::async_trait;
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use query::error::{QueryError, Result, TransactionError};
use query::executor::{QueryExecutor, QueryRequest, QueryResponse};
use query::rest::{RestError, RestOperations, RestResult};
use query::TransactionHandle;

/// Mock executor for testing HTTP routes with pattern-matched query responses.
#[derive(Debug, Clone)]
pub struct MockQueryExecutor {
    schema_sdl: String,
}

/// Mock executor for testing error paths. Schema errors are optional.
#[derive(Debug, Clone)]
pub struct FailingMockExecutor {
    schema_error: Option<String>,
}

impl FailingMockExecutor {
    /// Create a mock that fails schema() calls.
    pub fn with_schema_error(msg: impl Into<String>) -> Self {
        Self {
            schema_error: Some(msg.into()),
        }
    }
}

#[async_trait]
impl QueryExecutor for FailingMockExecutor {
    async fn execute(&self, _request: QueryRequest) -> QueryResponse {
        QueryResponse::error("execution failed")
    }

    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        _handle: &TransactionHandle,
    ) -> QueryResponse {
        self.execute(request).await
    }

    async fn begin_txn(
        &self,
        _readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        Err(TransactionError::not_supported(
            "mock executor does not support transactions",
        ))
    }

    async fn commit_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Err(TransactionError::not_supported(
            "mock executor does not support transactions",
        ))
    }

    async fn rollback_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Err(TransactionError::not_supported(
            "mock executor does not support transactions",
        ))
    }

    async fn schema(&self) -> Result<String> {
        match &self.schema_error {
            Some(msg) => Err(QueryError::internal(msg.clone())),
            None => Ok("type Query { ping: String }".to_string()),
        }
    }
}

impl MockQueryExecutor {
    /// Create a mock executor with default schema.
    pub fn new() -> Self {
        Self {
            schema_sdl: default_mock_schema(),
        }
    }

    /// Create with custom schema.
    pub fn with_schema(schema: impl Into<String>) -> Self {
        Self {
            schema_sdl: schema.into(),
        }
    }
}

impl Default for MockQueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QueryExecutor for MockQueryExecutor {
    async fn execute(&self, request: QueryRequest) -> QueryResponse {
        let query = request.query.to_lowercase();

        if query.contains("users") {
            QueryResponse::success(json!({
                "users": [
                    {"_docID": "bae-123", "name": "Alice", "age": 30},
                    {"_docID": "bae-456", "name": "Bob", "age": 25}
                ]
            }))
        } else if query.contains("__schema") || query.contains("__type") {
            QueryResponse::success(json!({
                "__schema": {
                    "types": [],
                    "queryType": {"name": "Query"},
                    "mutationType": {"name": "Mutation"}
                }
            }))
        } else if query.contains("create") || query.contains("update") || query.contains("delete") {
            QueryResponse::success(json!({
                "_docID": "bae-new-789"
            }))
        } else {
            QueryResponse::success(json!({
                "data": null,
                "message": "Mock response"
            }))
        }
    }

    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        _handle: &TransactionHandle,
    ) -> QueryResponse {
        self.execute(request).await
    }

    async fn begin_txn(
        &self,
        _readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        Ok(TransactionHandle::new("mock-txn-001".to_string()))
    }

    async fn commit_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Ok(())
    }

    async fn rollback_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Ok(())
    }

    async fn schema(&self) -> Result<String> {
        Ok(self.schema_sdl.clone())
    }
}

fn default_mock_schema() -> String {
    r#"
type User {
    _docID: ID!
    name: String!
    age: Int
    email: String
}

type Query {
    users: [User!]!
    user(_docID: ID!): User
}

type Mutation {
    create_User(input: UserInput!): User!
    update_User(_docID: ID!, input: UserInput!): User!
    delete_User(_docID: ID!): User!
}

input UserInput {
    name: String!
    age: Int
    email: String
}
"#
    .to_string()
}

// ============================================================================
// Mock REST Operations
// ============================================================================

/// Internal document storage for mock REST operations.
#[derive(Debug, Clone, Default)]
struct MockDocument {
    doc_id: String,
    data: JsonValue,
}

/// Mock REST operations for testing collection and document handlers.
#[derive(Debug)]
pub struct MockRestOperations {
    /// Collections with their documents.
    collections: Arc<RwLock<HashMap<String, Vec<MockDocument>>>>,
    /// Counter for generating unique document IDs.
    next_id: Arc<RwLock<u64>>,
}

impl Clone for MockRestOperations {
    fn clone(&self) -> Self {
        Self {
            collections: Arc::clone(&self.collections),
            next_id: Arc::clone(&self.next_id),
        }
    }
}

impl Default for MockRestOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl MockRestOperations {
    /// Create a new mock REST operations instance with default collections.
    pub fn new() -> Self {
        let mut collections = HashMap::new();

        // Add default Users collection with sample data
        collections.insert(
            "Users".to_string(),
            vec![
                MockDocument {
                    doc_id: "bae-123".to_string(),
                    data: json!({"name": "Alice", "age": 30}),
                },
                MockDocument {
                    doc_id: "bae-456".to_string(),
                    data: json!({"name": "Bob", "age": 25}),
                },
            ],
        );

        // Add empty Books collection
        collections.insert("Books".to_string(), vec![]);

        Self {
            collections: Arc::new(RwLock::new(collections)),
            next_id: Arc::new(RwLock::new(1000)),
        }
    }

    /// Create an empty mock REST operations instance.
    pub fn empty() -> Self {
        Self {
            collections: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Add a collection (for test setup).
    pub fn with_collection(self, name: &str) -> Self {
        self.collections
            .write()
            .unwrap()
            .insert(name.to_string(), vec![]);
        self
    }

    /// Generate a new unique document ID.
    fn generate_doc_id(&self) -> String {
        let mut id = self.next_id.write().unwrap();
        *id += 1;
        format!("bae-{:08x}", *id)
    }
}

#[async_trait]
impl RestOperations for MockRestOperations {
    async fn list_collections(&self) -> RestResult<Vec<String>> {
        let collections = self.collections.read().unwrap();
        let mut names: Vec<String> = collections.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    async fn get_collection_doc_ids(&self, collection: &str) -> RestResult<Vec<String>> {
        let collections = self.collections.read().unwrap();
        match collections.get(collection) {
            Some(docs) => Ok(docs.iter().map(|d| d.doc_id.clone()).collect()),
            None => Err(RestError::collection_not_found(collection)),
        }
    }

    async fn get_document(&self, collection: &str, doc_id: &str) -> RestResult<Option<JsonValue>> {
        let collections = self.collections.read().unwrap();
        match collections.get(collection) {
            Some(docs) => {
                let doc = docs.iter().find(|d| d.doc_id == doc_id);
                match doc {
                    Some(d) => {
                        let mut result = d.data.clone();
                        if let Some(obj) = result.as_object_mut() {
                            obj.insert("_docID".to_string(), json!(d.doc_id));
                        }
                        Ok(Some(result))
                    }
                    None => Ok(None),
                }
            }
            None => Err(RestError::collection_not_found(collection)),
        }
    }

    async fn create_document(&self, collection: &str, data: JsonValue) -> RestResult<JsonValue> {
        let mut collections = self.collections.write().unwrap();
        match collections.get_mut(collection) {
            Some(docs) => {
                let doc_id = self.generate_doc_id();
                let doc = MockDocument {
                    doc_id: doc_id.clone(),
                    data: data.clone(),
                };
                docs.push(doc);

                let mut result = data;
                if let Some(obj) = result.as_object_mut() {
                    obj.insert("_docID".to_string(), json!(doc_id));
                }
                Ok(result)
            }
            None => Err(RestError::collection_not_found(collection)),
        }
    }

    async fn create_documents(
        &self,
        collection: &str,
        data: Vec<JsonValue>,
    ) -> RestResult<Vec<JsonValue>> {
        let mut results = Vec::with_capacity(data.len());
        for item in data {
            let result = self.create_document(collection, item).await?;
            results.push(result);
        }
        Ok(results)
    }

    async fn update_document(
        &self,
        collection: &str,
        doc_id: &str,
        patch: JsonValue,
    ) -> RestResult<JsonValue> {
        let mut collections = self.collections.write().unwrap();
        match collections.get_mut(collection) {
            Some(docs) => {
                let doc = docs.iter_mut().find(|d| d.doc_id == doc_id);
                match doc {
                    Some(d) => {
                        // Merge patch into existing data
                        if let (Some(existing), Some(updates)) =
                            (d.data.as_object_mut(), patch.as_object())
                        {
                            for (key, value) in updates {
                                existing.insert(key.clone(), value.clone());
                            }
                        }

                        let mut result = d.data.clone();
                        if let Some(obj) = result.as_object_mut() {
                            obj.insert("_docID".to_string(), json!(d.doc_id));
                        }
                        Ok(result)
                    }
                    None => Err(RestError::document_not_found(doc_id)),
                }
            }
            None => Err(RestError::collection_not_found(collection)),
        }
    }

    async fn delete_document(&self, collection: &str, doc_id: &str) -> RestResult<bool> {
        let mut collections = self.collections.write().unwrap();
        match collections.get_mut(collection) {
            Some(docs) => {
                let initial_len = docs.len();
                docs.retain(|d| d.doc_id != doc_id);
                Ok(docs.len() < initial_len)
            }
            None => Err(RestError::collection_not_found(collection)),
        }
    }
}

/// Mock REST operations that always fails (for error path testing).
#[derive(Debug, Clone)]
pub struct FailingMockRestOperations {
    error_message: String,
}

impl FailingMockRestOperations {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            error_message: message.into(),
        }
    }
}

#[async_trait]
impl RestOperations for FailingMockRestOperations {
    async fn list_collections(&self) -> RestResult<Vec<String>> {
        Err(RestError::internal(&self.error_message))
    }

    async fn get_collection_doc_ids(&self, _collection: &str) -> RestResult<Vec<String>> {
        Err(RestError::internal(&self.error_message))
    }

    async fn get_document(
        &self,
        _collection: &str,
        _doc_id: &str,
    ) -> RestResult<Option<JsonValue>> {
        Err(RestError::internal(&self.error_message))
    }

    async fn create_document(&self, _collection: &str, _data: JsonValue) -> RestResult<JsonValue> {
        Err(RestError::internal(&self.error_message))
    }

    async fn create_documents(
        &self,
        _collection: &str,
        _data: Vec<JsonValue>,
    ) -> RestResult<Vec<JsonValue>> {
        Err(RestError::internal(&self.error_message))
    }

    async fn update_document(
        &self,
        _collection: &str,
        _doc_id: &str,
        _patch: JsonValue,
    ) -> RestResult<JsonValue> {
        Err(RestError::internal(&self.error_message))
    }

    async fn delete_document(&self, _collection: &str, _doc_id: &str) -> RestResult<bool> {
        Err(RestError::internal(&self.error_message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_users_query() {
        let executor = MockQueryExecutor::new();
        let request = QueryRequest::new("{ users { name } }");

        let response = executor.execute(request).await;
        assert!(!response.has_errors());

        let data = response.data.unwrap();
        let users = data.get("users").unwrap();
        assert!(users.is_array());
    }

    #[tokio::test]
    async fn test_mock_schema() {
        let executor = MockQueryExecutor::new();
        let schema = executor.schema().await.unwrap();
        assert!(schema.contains("type User"));
        assert!(schema.contains("type Query"));
    }

    #[tokio::test]
    async fn test_mock_introspection() {
        let executor = MockQueryExecutor::new();
        let request = QueryRequest::new("{ __schema { types { name } } }");

        let response = executor.execute(request).await;
        assert!(!response.has_errors());
    }

    // ========================================================================
    // REST Operations tests
    // ========================================================================

    #[tokio::test]
    async fn test_mock_rest_list_collections() {
        let rest = MockRestOperations::new();
        let collections = rest.list_collections().await.unwrap();
        assert!(collections.contains(&"Users".to_string()));
        assert!(collections.contains(&"Books".to_string()));
    }

    #[tokio::test]
    async fn test_mock_rest_get_collection_doc_ids() {
        let rest = MockRestOperations::new();
        let ids = rest.get_collection_doc_ids("Users").await.unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"bae-123".to_string()));
        assert!(ids.contains(&"bae-456".to_string()));
    }

    #[tokio::test]
    async fn test_mock_rest_get_collection_not_found() {
        let rest = MockRestOperations::new();
        let result = rest.get_collection_doc_ids("NonExistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_rest_get_document() {
        let rest = MockRestOperations::new();
        let doc = rest.get_document("Users", "bae-123").await.unwrap();
        assert!(doc.is_some());
        let doc = doc.unwrap();
        assert_eq!(doc.get("_docID").unwrap(), "bae-123");
        assert_eq!(doc.get("name").unwrap(), "Alice");
    }

    #[tokio::test]
    async fn test_mock_rest_get_document_not_found() {
        let rest = MockRestOperations::new();
        let doc = rest.get_document("Users", "bae-nonexistent").await.unwrap();
        assert!(doc.is_none());
    }

    #[tokio::test]
    async fn test_mock_rest_create_document() {
        let rest = MockRestOperations::new();
        let doc = rest
            .create_document("Users", json!({"name": "Charlie", "age": 35}))
            .await
            .unwrap();
        assert!(doc.get("_docID").is_some());
        assert_eq!(doc.get("name").unwrap(), "Charlie");
    }

    #[tokio::test]
    async fn test_mock_rest_create_documents() {
        let rest = MockRestOperations::new();
        let docs = rest
            .create_documents(
                "Users",
                vec![
                    json!({"name": "Dave", "age": 40}),
                    json!({"name": "Eve", "age": 28}),
                ],
            )
            .await
            .unwrap();
        assert_eq!(docs.len(), 2);
        assert!(docs[0].get("_docID").is_some());
        assert!(docs[1].get("_docID").is_some());
    }

    #[tokio::test]
    async fn test_mock_rest_update_document() {
        let rest = MockRestOperations::new();
        let doc = rest
            .update_document("Users", "bae-123", json!({"age": 31}))
            .await
            .unwrap();
        assert_eq!(doc.get("_docID").unwrap(), "bae-123");
        assert_eq!(doc.get("name").unwrap(), "Alice");
        assert_eq!(doc.get("age").unwrap(), 31);
    }

    #[tokio::test]
    async fn test_mock_rest_update_document_not_found() {
        let rest = MockRestOperations::new();
        let result = rest
            .update_document("Users", "bae-nonexistent", json!({"age": 31}))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_rest_delete_document() {
        let rest = MockRestOperations::new();
        let deleted = rest.delete_document("Users", "bae-123").await.unwrap();
        assert!(deleted);

        // Verify it's gone
        let doc = rest.get_document("Users", "bae-123").await.unwrap();
        assert!(doc.is_none());
    }

    #[tokio::test]
    async fn test_mock_rest_delete_document_not_found() {
        let rest = MockRestOperations::new();
        let deleted = rest
            .delete_document("Users", "bae-nonexistent")
            .await
            .unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_failing_mock_rest() {
        let rest = FailingMockRestOperations::new("test error");
        assert!(rest.list_collections().await.is_err());
        assert!(rest.get_collection_doc_ids("Users").await.is_err());
        assert!(rest.get_document("Users", "bae-123").await.is_err());
        assert!(rest.create_document("Users", json!({})).await.is_err());
        assert!(rest.create_documents("Users", vec![]).await.is_err());
        assert!(rest
            .update_document("Users", "bae-123", json!({}))
            .await
            .is_err());
        assert!(rest.delete_document("Users", "bae-123").await.is_err());
    }
}
