//! REST operations trait for document CRUD endpoints.
//!
//! This module defines the interface between the HTTP layer and REST-specific operations.
//! It provides collection listing and document CRUD operations separate from GraphQL.

use async_trait::async_trait;
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
///     })).await
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
    async fn get_collection_doc_ids(&self, collection: &str) -> RestResult<Vec<String>>;

    /// Get a single document by ID.
    ///
    /// Returns the document as JSON if found, or None if the document doesn't exist.
    async fn get_document(&self, collection: &str, doc_id: &str) -> RestResult<Option<JsonValue>>;

    /// Create a single document.
    ///
    /// Returns the created document with its generated `_docID`.
    async fn create_document(&self, collection: &str, data: JsonValue) -> RestResult<JsonValue>;

    /// Create multiple documents.
    ///
    /// Returns all created documents with their generated `_docID`s.
    async fn create_documents(
        &self,
        collection: &str,
        data: Vec<JsonValue>,
    ) -> RestResult<Vec<JsonValue>>;

    /// Update a single document.
    ///
    /// Applies a partial update (patch) to the document.
    /// Returns the updated document.
    async fn update_document(
        &self,
        collection: &str,
        doc_id: &str,
        patch: JsonValue,
    ) -> RestResult<JsonValue>;

    /// Delete a single document.
    ///
    /// Returns true if the document was deleted, false if it didn't exist.
    async fn delete_document(&self, collection: &str, doc_id: &str) -> RestResult<bool>;
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

    /// Build a GraphQL query to fetch all document IDs in a collection.
    fn build_list_ids_query(&self, collection: &str) -> String {
        format!(
            r#"{{ {collection} {{ _docID }} }}"#,
            collection = collection
        )
    }

    /// Build a GraphQL create mutation.
    fn build_create_mutation(&self, collection: &str, data: &JsonValue) -> String {
        format!(
            r#"mutation {{ create_{collection}(input: [{data}]) {{ _docID }} }}"#,
            collection = collection,
            data = data
        )
    }

    /// Build a GraphQL update mutation.
    fn build_update_mutation(&self, collection: &str, doc_id: &str, patch: &JsonValue) -> String {
        format!(
            r#"mutation {{ update_{collection}(docID: "{doc_id}", input: {patch}) {{ _docID }} }}"#,
            collection = collection,
            doc_id = doc_id,
            patch = patch
        )
    }

    /// Build a GraphQL delete mutation.
    fn build_delete_mutation(&self, collection: &str, doc_id: &str) -> String {
        format!(
            r#"mutation {{ delete_{collection}(docID: "{doc_id}") {{ _docID }} }}"#,
            collection = collection,
            doc_id = doc_id
        )
    }

    /// Extract document IDs from a query result.
    fn extract_doc_ids(&self, result: &JsonValue, collection: &str) -> Vec<String> {
        result
            .get(collection)
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|doc| doc.get("_docID").and_then(|id| id.as_str()))
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the full document data for a document by ID.
    async fn fetch_full_document(
        &self,
        collection: &str,
        doc_id: &str,
    ) -> RestResult<Option<JsonValue>> {
        // Query all fields by not specifying a selection (the runner returns all fields)
        let query = format!(
            r#"{{ {collection}(docID: "{doc_id}") }}"#,
            collection = collection,
            doc_id = doc_id
        );

        let result = self.runner.execute_query(&query).await?;

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
        Ok(self.runner.collection_names())
    }

    async fn get_collection_doc_ids(&self, collection: &str) -> RestResult<Vec<String>> {
        if !self.runner.has_collection(collection) {
            return Err(RestError::collection_not_found(collection));
        }

        let query = self.build_list_ids_query(collection);
        let result = self.runner.execute_query(&query).await?;
        Ok(self.extract_doc_ids(&result, collection))
    }

    async fn get_document(&self, collection: &str, doc_id: &str) -> RestResult<Option<JsonValue>> {
        if !self.runner.has_collection(collection) {
            return Err(RestError::collection_not_found(collection));
        }

        self.fetch_full_document(collection, doc_id).await
    }

    async fn create_document(&self, collection: &str, data: JsonValue) -> RestResult<JsonValue> {
        if !self.runner.has_collection(collection) {
            return Err(RestError::collection_not_found(collection));
        }

        let mutation = self.build_create_mutation(collection, &data);
        let result = self.runner.execute_mutation(&mutation).await?;

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
        self.fetch_full_document(collection, doc_id)
            .await?
            .ok_or_else(|| RestError::internal("created document not found"))
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
        if !self.runner.has_collection(collection) {
            return Err(RestError::collection_not_found(collection));
        }

        // Check if document exists first
        let existing = self.fetch_full_document(collection, doc_id).await?;
        if existing.is_none() {
            return Err(RestError::document_not_found(doc_id));
        }

        let mutation = self.build_update_mutation(collection, doc_id, &patch);
        self.runner.execute_mutation(&mutation).await?;

        // Fetch and return the updated document
        self.fetch_full_document(collection, doc_id)
            .await?
            .ok_or_else(|| RestError::internal("updated document not found"))
    }

    async fn delete_document(&self, collection: &str, doc_id: &str) -> RestResult<bool> {
        if !self.runner.has_collection(collection) {
            return Err(RestError::collection_not_found(collection));
        }

        // Check if document exists first
        let existing = self.fetch_full_document(collection, doc_id).await?;
        if existing.is_none() {
            return Ok(false);
        }

        let mutation = self.build_delete_mutation(collection, doc_id);
        let result = self.runner.execute_mutation(&mutation).await?;

        // Check if deletion was successful by looking at the result
        let deleted = result
            .get(format!("delete_{}", collection))
            .or_else(|| result.get(collection))
            .and_then(|v| v.as_array())
            .map(|arr| !arr.is_empty())
            .unwrap_or(false);

        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
