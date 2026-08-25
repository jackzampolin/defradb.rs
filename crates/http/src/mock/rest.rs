//! Mock REST operations for testing collection and document handlers.

use async_trait::async_trait;
use identity::Did;
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use query::rest::{RestError, RestOperations, RestResult};

/// Internal document storage for mock REST operations.
#[derive(Debug, Clone, Default)]
struct MockDocument {
    doc_id: String,
    data: JsonValue,
}

/// Whether `filter` selects `data`, using the production filter engine.
///
/// Not a hand-rolled matcher: one that understood a different filter language
/// than the server would make every test here green on requests the real path
/// rejects, which is the whole hazard on a route that deletes documents.
fn matches_filter(data: &JsonValue, filter: &JsonValue) -> RestResult<bool> {
    let conditions = filter
        .as_object()
        .ok_or_else(|| RestError::invalid_input("filter must be an object"))?;
    query::Filter::from_conditions(conditions.clone())
        .matches_json_object(data)
        .map_err(|e| RestError::invalid_input(e.to_string()))
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

    async fn get_collection_doc_ids(
        &self,
        collection: &str,
        _identity: Option<&Did>,
    ) -> RestResult<Vec<String>> {
        let collections = self.collections.read().unwrap();
        match collections.get(collection) {
            Some(docs) => Ok(docs.iter().map(|d| d.doc_id.clone()).collect()),
            None => Err(RestError::collection_not_found(collection)),
        }
    }

    async fn get_document(
        &self,
        collection: &str,
        doc_id: &str,
        _identity: Option<&Did>,
    ) -> RestResult<Option<JsonValue>> {
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

    async fn create_document(
        &self,
        collection: &str,
        data: JsonValue,
        _identity: Option<&Did>,
    ) -> RestResult<JsonValue> {
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
        identity: Option<&Did>,
    ) -> RestResult<Vec<JsonValue>> {
        let mut results = Vec::with_capacity(data.len());
        for item in data {
            let result = self.create_document(collection, item, identity).await?;
            results.push(result);
        }
        Ok(results)
    }

    async fn update_document(
        &self,
        collection: &str,
        doc_id: &str,
        patch: JsonValue,
        _identity: Option<&Did>,
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

    async fn delete_document(
        &self,
        collection: &str,
        doc_id: &str,
        _identity: Option<&Did>,
    ) -> RestResult<bool> {
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

    /// Matches on equality of each filter key against the stored document,
    /// which is enough to tell "the filter was applied" from "it was ignored".
    async fn delete_documents_with_filter(
        &self,
        collection: &str,
        filter: &JsonValue,
        _identity: Option<&Did>,
    ) -> RestResult<Vec<String>> {
        let mut collections = self.collections.write().unwrap();
        let docs = collections
            .get_mut(collection)
            .ok_or_else(|| RestError::collection_not_found(collection))?;

        let mut matched = Vec::new();
        for doc in docs.iter() {
            if matches_filter(&doc.data, filter)? {
                matched.push(doc.doc_id.clone());
            }
        }
        docs.retain(|doc| !matched.contains(&doc.doc_id));
        Ok(matched)
    }
}

/// Mock REST operations that always fails (for error path testing).
///
/// Supports configurable error types for testing different error paths.
#[derive(Debug, Clone)]
pub struct FailingMockRestOperations {
    error: RestError,
}

impl FailingMockRestOperations {
    /// Create a mock that always returns an internal error with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            error: RestError::internal(message),
        }
    }

    /// Create a mock that always returns the specified error.
    pub fn with_error(error: RestError) -> Self {
        Self { error }
    }

    /// Create a mock that returns InvalidDocId errors.
    pub fn with_invalid_doc_id(id: impl Into<String>) -> Self {
        Self {
            error: RestError::invalid_doc_id(id),
        }
    }

    /// Create a mock that returns InvalidInput errors.
    pub fn with_invalid_input(msg: impl Into<String>) -> Self {
        Self {
            error: RestError::invalid_input(msg),
        }
    }

    /// Create a mock that returns PermissionDenied errors.
    pub fn with_permission_denied(msg: impl Into<String>) -> Self {
        Self {
            error: RestError::permission_denied(msg),
        }
    }
}

#[async_trait]
impl RestOperations for FailingMockRestOperations {
    async fn list_collections(&self) -> RestResult<Vec<String>> {
        Err(self.error.clone())
    }

    async fn get_collection_doc_ids(
        &self,
        _collection: &str,
        _identity: Option<&Did>,
    ) -> RestResult<Vec<String>> {
        Err(self.error.clone())
    }

    async fn get_document(
        &self,
        _collection: &str,
        _doc_id: &str,
        _identity: Option<&Did>,
    ) -> RestResult<Option<JsonValue>> {
        Err(self.error.clone())
    }

    async fn create_document(
        &self,
        _collection: &str,
        _data: JsonValue,
        _identity: Option<&Did>,
    ) -> RestResult<JsonValue> {
        Err(self.error.clone())
    }

    async fn create_documents(
        &self,
        _collection: &str,
        _data: Vec<JsonValue>,
        _identity: Option<&Did>,
    ) -> RestResult<Vec<JsonValue>> {
        Err(self.error.clone())
    }

    async fn update_document(
        &self,
        _collection: &str,
        _doc_id: &str,
        _patch: JsonValue,
        _identity: Option<&Did>,
    ) -> RestResult<JsonValue> {
        Err(self.error.clone())
    }

    async fn delete_document(
        &self,
        _collection: &str,
        _doc_id: &str,
        _identity: Option<&Did>,
    ) -> RestResult<bool> {
        Err(self.error.clone())
    }

    async fn delete_documents_with_filter(
        &self,
        _collection: &str,
        _filter: &JsonValue,
        _identity: Option<&Did>,
    ) -> RestResult<Vec<String>> {
        Err(self.error.clone())
    }
}
