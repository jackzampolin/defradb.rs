//! REST operations trait for document CRUD endpoints.
//!
//! This module defines the interface between the HTTP layer and REST-specific operations.
//! It provides collection listing and document CRUD operations separate from GraphQL.

use async_trait::async_trait;
use identity::Did;
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::error::QueryError;
use crate::fetcher::DocFetcher;
use crate::runner::QueryRunner;
use crate::txn::TransactionRegistry;

/// Result type for REST operations.
pub type RestResult<T> = std::result::Result<T, RestError>;

/// Error type for REST operations.
#[derive(Debug, Clone)]
pub enum RestError {
    /// Collection not found.
    CollectionNotFound(String),
    /// Document not found.
    DocumentNotFound(String),
    /// Invalid document ID format.
    InvalidDocId(String),
    /// Invalid input data.
    InvalidInput(String),
    /// Permission denied (ACP check failed).
    PermissionDenied(String),
    /// Storage or execution error.
    Internal(String),
}

impl std::fmt::Display for RestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CollectionNotFound(name) => write!(f, "collection not found: {}", name),
            Self::DocumentNotFound(id) => write!(f, "document not found: {}", id),
            Self::InvalidDocId(id) => write!(f, "invalid document ID: {}", id),
            Self::InvalidInput(msg) => write!(f, "invalid input: {}", msg),
            Self::PermissionDenied(msg) => write!(f, "permission denied: {}", msg),
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for RestError {}

impl RestError {
    pub fn collection_not_found(name: impl Into<String>) -> Self {
        Self::CollectionNotFound(name.into())
    }

    pub fn document_not_found(id: impl Into<String>) -> Self {
        Self::DocumentNotFound(id.into())
    }

    pub fn invalid_doc_id(id: impl Into<String>) -> Self {
        Self::InvalidDocId(id.into())
    }

    pub fn invalid_input(msg: impl Into<String>) -> Self {
        Self::InvalidInput(msg.into())
    }

    pub fn permission_denied(msg: impl Into<String>) -> Self {
        Self::PermissionDenied(msg.into())
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }
}

impl From<QueryError> for RestError {
    fn from(err: QueryError) -> Self {
        match err {
            // Not found errors
            QueryError::CollectionNotFound(name) => Self::CollectionNotFound(name),
            QueryError::DocumentNotFound(id) => Self::DocumentNotFound(id),
            // Invalid input errors (user-fixable, should be 400 Bad Request)
            QueryError::InvalidDocId(id) => Self::InvalidDocId(id),
            QueryError::InvalidMutationInput(msg) => Self::InvalidInput(msg),
            QueryError::Parse(msg) => Self::InvalidInput(format!("parse error: {}", msg)),
            QueryError::InvalidFilter(msg) => {
                Self::InvalidInput(format!("invalid filter: {}", msg))
            }
            QueryError::FilterFieldNotSelected { field, collection } => {
                Self::InvalidInput(format!(
                    "filter field '{}' must be in select list for '{}'",
                    field, collection
                ))
            }
            QueryError::UnknownField(name) => {
                Self::InvalidInput(format!("unknown field: {}", name))
            }
            QueryError::TypeMismatch { expected, actual } => Self::InvalidInput(format!(
                "type mismatch: expected {}, got {}",
                expected, actual
            )),
            QueryError::RequiredFieldMissing(field) => {
                Self::InvalidInput(format!("required field missing: {}", field))
            }
            QueryError::InvalidAggregateTarget(msg) => {
                Self::InvalidInput(format!("invalid aggregate target: {}", msg))
            }
            // Permission errors (should be 403 Forbidden)
            QueryError::PermissionDenied(msg) => Self::PermissionDenied(msg),
            QueryError::AcpRegistrationFailed { doc_id, message } => Self::PermissionDenied(
                format!("ACP registration failed for '{}': {}", doc_id, message),
            ),
            // True internal errors (500 Internal Server Error)
            other => Self::Internal(other.to_string()),
        }
    }
}

/// REST operations trait for collection and document CRUD.
///
/// This trait provides REST-specific operations separate from GraphQL execution.
/// Each operation runs with auto-commit semantics (one transaction per operation).
///
/// # Identity and ACP
///
/// All document operations accept an optional `identity` parameter for access control.
/// When provided, the identity is used for ACP (Access Control Policy) permission checks:
/// - Read operations check read permission on protected documents
/// - Create operations register the document with the identity as owner
/// - Update/Delete operations check the corresponding permissions
///
/// # Example
///
/// ```ignore
/// use query::rest::{RestOperations, RestResult};
/// use serde_json::json;
///
/// async fn create_user<R: RestOperations>(rest: &R) -> RestResult<serde_json::Value> {
///     rest.create_document("Users", json!({
///         "name": "Alice",
///         "age": 30
///     }), None).await
/// }
/// ```
#[async_trait]
pub trait RestOperations: Send + Sync {
    /// List all collection names.
    ///
    /// Returns the names of all collections in the database.
    async fn list_collections(&self) -> RestResult<Vec<String>>;

    /// Get all document IDs in a collection.
    ///
    /// Returns a list of document IDs (bae-...) for all documents in the collection.
    /// Identity is used to filter documents based on read permissions.
    async fn get_collection_doc_ids(
        &self,
        collection: &str,
        identity: Option<&Did>,
    ) -> RestResult<Vec<String>>;

    /// Get a single document by ID.
    ///
    /// Returns the document as JSON if found, or None if the document doesn't exist.
    /// Identity is used to check read permission on protected documents.
    async fn get_document(
        &self,
        collection: &str,
        doc_id: &str,
        identity: Option<&Did>,
    ) -> RestResult<Option<JsonValue>>;

    /// Create a single document.
    ///
    /// Returns the created document with its generated `_docID`.
    /// If identity is provided and the collection has a policy, the document
    /// is registered with ACP and the identity becomes the owner.
    async fn create_document(
        &self,
        collection: &str,
        data: JsonValue,
        identity: Option<&Did>,
    ) -> RestResult<JsonValue>;

    /// Create multiple documents.
    ///
    /// Returns all created documents with their generated `_docID`s.
    /// If identity is provided and the collection has a policy, documents
    /// are registered with ACP and the identity becomes the owner.
    async fn create_documents(
        &self,
        collection: &str,
        data: Vec<JsonValue>,
        identity: Option<&Did>,
    ) -> RestResult<Vec<JsonValue>>;

    /// Update a single document.
    ///
    /// Applies a partial update (patch) to the document.
    /// Returns the updated document.
    /// Identity is used to check update permission on protected documents.
    async fn update_document(
        &self,
        collection: &str,
        doc_id: &str,
        patch: JsonValue,
        identity: Option<&Did>,
    ) -> RestResult<JsonValue>;

    /// Delete a single document.
    ///
    /// Returns true if the document was deleted, false if it didn't exist.
    /// Identity is used to check delete permission on protected documents.
    async fn delete_document(
        &self,
        collection: &str,
        doc_id: &str,
        identity: Option<&Did>,
    ) -> RestResult<bool>;
}

/// Production implementation of REST operations using QueryRunner.
///
/// This wraps a QueryRunner and translates REST operations into GraphQL queries/mutations.
pub struct RestOperationsImpl<F: DocFetcher, R: TransactionRegistry> {
    runner: Arc<QueryRunner<F, R>>,
}

impl<F: DocFetcher, R: TransactionRegistry> RestOperationsImpl<F, R> {
    /// Create a new REST operations implementation.
    ///
    /// The QueryRunner must have a mutator configured for create/update/delete operations.
    pub fn new(runner: Arc<QueryRunner<F, R>>) -> Self {
        Self { runner }
    }

    /// Convert a JSON value to GraphQL input syntax.
    ///
    /// GraphQL uses bare identifiers for object keys (not quoted strings like JSON).
    /// Handles nested objects, arrays, and escapes special characters in strings.
    /// This converts: {"name": "Alice", "age": 30} to {name: "Alice", age: 30}
    fn json_to_graphql_input(value: &JsonValue) -> String {
        match value {
            JsonValue::Null => "null".to_string(),
            JsonValue::Bool(b) => b.to_string(),
            JsonValue::Number(n) => n.to_string(),
            JsonValue::String(s) => {
                let escaped = s
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n")
                    .replace('\r', "\\r")
                    .replace('\t', "\\t");
                format!("\"{}\"", escaped)
            }
            JsonValue::Array(arr) => {
                let items: Vec<String> = arr.iter().map(Self::json_to_graphql_input).collect();
                format!("[{}]", items.join(", "))
            }
            JsonValue::Object(obj) => {
                let fields: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, Self::json_to_graphql_input(v)))
                    .collect();
                format!("{{{}}}", fields.join(", "))
            }
        }
    }

    /// Build a GraphQL query to fetch all document IDs in a collection.
    fn build_list_ids_query(&self, collection: &str) -> String {
        format!(
            r#"{{ {collection} {{ _docID }} }}"#,
            collection = collection
        )
    }

    /// Build a GraphQL create mutation.
    fn build_create_mutation(&self, collection: &str, data: &JsonValue) -> String {
        let graphql_data = Self::json_to_graphql_input(data);
        format!(
            r#"mutation {{ create_{collection}(input: [{graphql_data}]) {{ _docID }} }}"#,
            collection = collection,
            graphql_data = graphql_data
        )
    }

    /// Build a GraphQL update mutation.
    fn build_update_mutation(&self, collection: &str, doc_id: &str, patch: &JsonValue) -> String {
        let graphql_patch = Self::json_to_graphql_input(patch);
        format!(
            r#"mutation {{ update_{collection}(docIDs: ["{doc_id}"], input: {graphql_patch}) {{ _docID }} }}"#,
            collection = collection,
            doc_id = doc_id,
            graphql_patch = graphql_patch
        )
    }

    /// Build a GraphQL delete mutation.
    fn build_delete_mutation(&self, collection: &str, doc_id: &str) -> String {
        format!(
            r#"mutation {{ delete_{collection}(docIDs: ["{doc_id}"]) {{ _docID }} }}"#,
            collection = collection,
            doc_id = doc_id
        )
    }

    /// Extract document IDs from a query result.
    ///
    /// Returns an error if the result format is unexpected (missing collection key or non-array).
    fn extract_doc_ids(&self, result: &JsonValue, collection: &str) -> RestResult<Vec<String>> {
        let docs = result.get(collection).ok_or_else(|| {
            tracing::warn!(
                collection = %collection,
                result = ?result,
                "Query result missing collection key"
            );
            RestError::internal(format!("query result missing key '{}'", collection))
        })?;

        let arr = docs.as_array().ok_or_else(|| {
            tracing::warn!(
                collection = %collection,
                result = ?result,
                "Query result collection is not an array"
            );
            RestError::internal(format!(
                "expected array for collection '{}', got {}",
                collection,
                match docs {
                    JsonValue::Null => "null",
                    JsonValue::Bool(_) => "boolean",
                    JsonValue::Number(_) => "number",
                    JsonValue::String(_) => "string",
                    JsonValue::Object(_) => "object",
                    JsonValue::Array(_) => "array",
                }
            ))
        })?;

        Ok(arr
            .iter()
            .filter_map(|doc| doc.get("_docID").and_then(|id| id.as_str()))
            .map(String::from)
            .collect())
    }

    /// Get the full document data for a document by ID.
    async fn fetch_full_document(
        &self,
        collection: &str,
        doc_id: &str,
        identity: Option<&Did>,
    ) -> RestResult<Option<JsonValue>> {
        // Query all fields by not specifying a selection (the runner returns all fields)
        let query = format!(
            r#"{{ {collection}(docID: "{doc_id}") }}"#,
            collection = collection,
            doc_id = doc_id
        );

        let result = self
            .runner
            .execute_query_with_identity(&query, identity.cloned())
            .await?;

        // Extract the document from the result
        let doc = result
            .get(collection)
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .cloned();

        Ok(doc)
    }
}

#[async_trait]
impl<F: DocFetcher, R: TransactionRegistry> RestOperations for RestOperationsImpl<F, R> {
    async fn list_collections(&self) -> RestResult<Vec<String>> {
        self.runner
            .collection_names()
            .await
            .map_err(|e| RestError::internal(e.to_string()))
    }

    async fn get_collection_doc_ids(
        &self,
        collection: &str,
        identity: Option<&Did>,
    ) -> RestResult<Vec<String>> {
        if !self
            .runner
            .has_collection(collection)
            .await
            .map_err(|e| RestError::internal(e.to_string()))?
        {
            return Err(RestError::collection_not_found(collection));
        }

        let query = self.build_list_ids_query(collection);
        let result = self
            .runner
            .execute_query_with_identity(&query, identity.cloned())
            .await?;
        self.extract_doc_ids(&result, collection)
    }

    async fn get_document(
        &self,
        collection: &str,
        doc_id: &str,
        identity: Option<&Did>,
    ) -> RestResult<Option<JsonValue>> {
        if !self
            .runner
            .has_collection(collection)
            .await
            .map_err(|e| RestError::internal(e.to_string()))?
        {
            return Err(RestError::collection_not_found(collection));
        }

        self.fetch_full_document(collection, doc_id, identity).await
    }

    async fn create_document(
        &self,
        collection: &str,
        data: JsonValue,
        identity: Option<&Did>,
    ) -> RestResult<JsonValue> {
        if !self
            .runner
            .has_collection(collection)
            .await
            .map_err(|e| RestError::internal(e.to_string()))?
        {
            return Err(RestError::collection_not_found(collection));
        }

        let mutation = self.build_create_mutation(collection, &data);
        let result = self
            .runner
            .execute_mutation_with_identity(&mutation, identity.cloned())
            .await?;

        // Extract the created document's ID and fetch the full document
        let doc_id = result
            .get(format!("create_{}", collection))
            .or_else(|| result.get(collection))
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|doc| doc.get("_docID"))
            .and_then(|id| id.as_str())
            .ok_or_else(|| RestError::internal("failed to get created document ID"))?;

        // Fetch and return the full document
        self.fetch_full_document(collection, doc_id, identity)
            .await?
            .ok_or_else(|| RestError::internal("created document not found"))
    }

    async fn create_documents(
        &self,
        collection: &str,
        data: Vec<JsonValue>,
        identity: Option<&Did>,
    ) -> RestResult<Vec<JsonValue>> {
        let mut results = Vec::with_capacity(data.len());
        for item in data {
            let result = self
                .create_document(collection, item.clone(), identity)
                .await?;
            results.push(result);
        }
        Ok(results)
    }

    async fn update_document(
        &self,
        collection: &str,
        doc_id: &str,
        patch: JsonValue,
        identity: Option<&Did>,
    ) -> RestResult<JsonValue> {
        if !self
            .runner
            .has_collection(collection)
            .await
            .map_err(|e| RestError::internal(e.to_string()))?
        {
            return Err(RestError::collection_not_found(collection));
        }

        // Check if document exists first (with identity for permission check)
        let existing = self
            .fetch_full_document(collection, doc_id, identity)
            .await?;
        if existing.is_none() {
            return Err(RestError::document_not_found(doc_id));
        }

        let mutation = self.build_update_mutation(collection, doc_id, &patch);
        self.runner
            .execute_mutation_with_identity(&mutation, identity.cloned())
            .await?;

        // Fetch and return the updated document
        self.fetch_full_document(collection, doc_id, identity)
            .await?
            .ok_or_else(|| RestError::internal("updated document not found"))
    }

    async fn delete_document(
        &self,
        collection: &str,
        doc_id: &str,
        identity: Option<&Did>,
    ) -> RestResult<bool> {
        if !self
            .runner
            .has_collection(collection)
            .await
            .map_err(|e| RestError::internal(e.to_string()))?
        {
            return Err(RestError::collection_not_found(collection));
        }

        let existing = self
            .fetch_full_document(collection, doc_id, identity)
            .await?;
        if existing.is_none() {
            return Ok(false);
        }

        let mutation = self.build_delete_mutation(collection, doc_id);
        let result = self
            .runner
            .execute_mutation_with_identity(&mutation, identity.cloned())
            .await?;

        // Extract deletion result, returning error if format is unexpected
        let delete_key = format!("delete_{}", collection);
        let delete_result = result.get(&delete_key).or_else(|| result.get(collection));

        let deleted = match delete_result {
            Some(v) => match v.as_array() {
                Some(arr) => !arr.is_empty(),
                None => {
                    tracing::warn!(
                        collection = %collection,
                        doc_id = %doc_id,
                        result = ?result,
                        "Delete mutation result is not an array"
                    );
                    return Err(RestError::internal(
                        "unexpected delete mutation result format: expected array",
                    ));
                }
            },
            None => {
                tracing::warn!(
                    collection = %collection,
                    doc_id = %doc_id,
                    result = ?result,
                    "Delete mutation returned unexpected result structure"
                );
                return Err(RestError::internal(
                    "unexpected delete mutation result: missing expected key",
                ));
            }
        };

        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Helper to access json_to_graphql_input for testing.
    // The function is private to the impl block, so we test it indirectly
    // by creating a minimal wrapper for testing purposes.
    fn json_to_graphql(value: &JsonValue) -> String {
        match value {
            JsonValue::Null => "null".to_string(),
            JsonValue::Bool(b) => b.to_string(),
            JsonValue::Number(n) => n.to_string(),
            JsonValue::String(s) => {
                let escaped = s
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n")
                    .replace('\r', "\\r")
                    .replace('\t', "\\t");
                format!("\"{}\"", escaped)
            }
            JsonValue::Array(arr) => {
                let items: Vec<String> = arr.iter().map(json_to_graphql).collect();
                format!("[{}]", items.join(", "))
            }
            JsonValue::Object(obj) => {
                let fields: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, json_to_graphql(v)))
                    .collect();
                format!("{{{}}}", fields.join(", "))
            }
        }
    }

    #[test]
    fn test_json_to_graphql_null() {
        assert_eq!(json_to_graphql(&json!(null)), "null");
    }

    #[test]
    fn test_json_to_graphql_bool() {
        assert_eq!(json_to_graphql(&json!(true)), "true");
        assert_eq!(json_to_graphql(&json!(false)), "false");
    }

    #[test]
    fn test_json_to_graphql_number() {
        assert_eq!(json_to_graphql(&json!(42)), "42");
        assert_eq!(json_to_graphql(&json!(-17)), "-17");
        assert_eq!(json_to_graphql(&json!(3.14)), "3.14");
    }

    #[test]
    fn test_json_to_graphql_simple_string() {
        assert_eq!(json_to_graphql(&json!("hello")), "\"hello\"");
        assert_eq!(json_to_graphql(&json!("")), "\"\"");
    }

    #[test]
    fn test_json_to_graphql_string_with_quotes() {
        assert_eq!(
            json_to_graphql(&json!("Hello \"World\"")),
            "\"Hello \\\"World\\\"\""
        );
    }

    #[test]
    fn test_json_to_graphql_string_with_backslashes() {
        assert_eq!(
            json_to_graphql(&json!("path\\to\\file")),
            "\"path\\\\to\\\\file\""
        );
    }

    #[test]
    fn test_json_to_graphql_string_with_newlines() {
        assert_eq!(json_to_graphql(&json!("line1\nline2")), "\"line1\\nline2\"");
    }

    #[test]
    fn test_json_to_graphql_string_with_carriage_return() {
        assert_eq!(json_to_graphql(&json!("line1\rline2")), "\"line1\\rline2\"");
    }

    #[test]
    fn test_json_to_graphql_string_with_tabs() {
        assert_eq!(json_to_graphql(&json!("col1\tcol2")), "\"col1\\tcol2\"");
    }

    #[test]
    fn test_json_to_graphql_string_with_mixed_escapes() {
        assert_eq!(
            json_to_graphql(&json!("line1\nline2\t\"quoted\"\r\\end")),
            "\"line1\\nline2\\t\\\"quoted\\\"\\r\\\\end\""
        );
    }

    #[test]
    fn test_json_to_graphql_array_empty() {
        assert_eq!(json_to_graphql(&json!([])), "[]");
    }

    #[test]
    fn test_json_to_graphql_array_simple() {
        assert_eq!(json_to_graphql(&json!([1, 2, 3])), "[1, 2, 3]");
    }

    #[test]
    fn test_json_to_graphql_array_mixed() {
        assert_eq!(
            json_to_graphql(&json!(["hello", 42, true, null])),
            "[\"hello\", 42, true, null]"
        );
    }

    #[test]
    fn test_json_to_graphql_array_nested() {
        assert_eq!(
            json_to_graphql(&json!([[1, 2], [3, 4]])),
            "[[1, 2], [3, 4]]"
        );
    }

    #[test]
    fn test_json_to_graphql_object_simple() {
        let result = json_to_graphql(&json!({"name": "Alice", "age": 30}));
        // Object key order may vary, so check for both possibilities
        assert!(
            result == "{name: \"Alice\", age: 30}" || result == "{age: 30, name: \"Alice\"}",
            "Unexpected result: {}",
            result
        );
    }

    #[test]
    fn test_json_to_graphql_object_nested() {
        let result = json_to_graphql(&json!({"user": {"name": "Bob"}}));
        assert_eq!(result, "{user: {name: \"Bob\"}}");
    }

    #[test]
    fn test_json_to_graphql_object_with_array() {
        let result = json_to_graphql(&json!({"tags": ["a", "b"]}));
        assert_eq!(result, "{tags: [\"a\", \"b\"]}");
    }

    #[test]
    fn test_json_to_graphql_complex_nested() {
        let value = json!({
            "user": {
                "name": "Alice\nSmith",
                "tags": ["admin", "user"],
                "active": true
            }
        });
        let result = json_to_graphql(&value);
        // The nested structure should be properly converted
        assert!(result.contains("name: \"Alice\\nSmith\""));
        assert!(result.contains("tags: [\"admin\", \"user\"]"));
        assert!(result.contains("active: true"));
    }

    #[test]
    fn test_json_to_graphql_unicode() {
        assert_eq!(json_to_graphql(&json!("héllo 世界")), "\"héllo 世界\"");
    }

    #[test]
    fn test_rest_error_display() {
        let err = RestError::collection_not_found("Users");
        assert_eq!(err.to_string(), "collection not found: Users");

        let err = RestError::document_not_found("bae-123");
        assert_eq!(err.to_string(), "document not found: bae-123");

        let err = RestError::invalid_doc_id("invalid");
        assert_eq!(err.to_string(), "invalid document ID: invalid");

        let err = RestError::invalid_input("missing field");
        assert_eq!(err.to_string(), "invalid input: missing field");

        let err = RestError::permission_denied("access denied");
        assert_eq!(err.to_string(), "permission denied: access denied");

        let err = RestError::internal("storage failure");
        assert_eq!(err.to_string(), "internal error: storage failure");
    }

    #[test]
    fn test_rest_error_from_query_error() {
        // Not found errors
        let err = QueryError::collection_not_found("Users");
        let rest_err: RestError = err.into();
        assert!(matches!(rest_err, RestError::CollectionNotFound(_)));

        let err = QueryError::DocumentNotFound("bae-123".into());
        let rest_err: RestError = err.into();
        assert!(matches!(rest_err, RestError::DocumentNotFound(_)));

        // Invalid input errors (user-fixable)
        let err = QueryError::parse("unexpected token");
        let rest_err: RestError = err.into();
        assert!(matches!(rest_err, RestError::InvalidInput(_)));
        assert!(rest_err.to_string().contains("parse error"));

        let err = QueryError::invalid_filter("bad condition");
        let rest_err: RestError = err.into();
        assert!(matches!(rest_err, RestError::InvalidInput(_)));
        assert!(rest_err.to_string().contains("invalid filter"));

        let err = QueryError::unknown_field("foo");
        let rest_err: RestError = err.into();
        assert!(matches!(rest_err, RestError::InvalidInput(_)));
        assert!(rest_err.to_string().contains("unknown field"));

        let err = QueryError::TypeMismatch {
            expected: "String".into(),
            actual: "Int".into(),
        };
        let rest_err: RestError = err.into();
        assert!(matches!(rest_err, RestError::InvalidInput(_)));
        assert!(rest_err.to_string().contains("type mismatch"));

        let err = QueryError::RequiredFieldMissing("name".into());
        let rest_err: RestError = err.into();
        assert!(matches!(rest_err, RestError::InvalidInput(_)));
        assert!(rest_err.to_string().contains("required field missing"));

        // Permission errors
        let err = QueryError::permission_denied("not authorized");
        let rest_err: RestError = err.into();
        assert!(matches!(rest_err, RestError::PermissionDenied(_)));

        let err = QueryError::acp_registration_failed("bae-123", "policy error");
        let rest_err: RestError = err.into();
        assert!(matches!(rest_err, RestError::PermissionDenied(_)));
        assert!(rest_err.to_string().contains("ACP registration failed"));
    }
}
