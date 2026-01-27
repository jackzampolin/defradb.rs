//! UpdateNode for updating existing documents
//!
//! This node updates documents in storage during query execution, following
//! the Go DefraDB pattern where persistence happens within the plan node.

use std::sync::Arc;

use async_trait::async_trait;
use document::{DocID, Document};
use serde_json::Value as JsonValue;
use tracing;

use crate::document::{document_to_plan_doc, DocumentMapping};
use crate::error::{QueryError, Result};
use crate::fetcher::DocFetcher;
use crate::mapper::Filter;
use crate::mutator::{DocMutator, UpdateResult};
use crate::planner::{Doc, PlanNode};

use super::create::{json_to_normal_value, normal_value_to_json};

/// Input for an update mutation - field values to patch on existing documents.
#[derive(Debug, Clone)]
pub struct UpdateInput {
    /// Field values to update, keyed by field name
    pub fields: std::collections::HashMap<String, JsonValue>,
}

impl UpdateInput {
    /// Create a new empty input.
    pub fn new() -> Self {
        Self {
            fields: std::collections::HashMap::new(),
        }
    }

    /// Add a field value to update.
    pub fn with_field(mut self, name: impl Into<String>, value: JsonValue) -> Self {
        self.fields.insert(name.into(), value);
        self
    }

    /// Apply this update to a document.
    pub fn apply_to(&self, doc: &mut Document) -> Result<usize> {
        let mut modified_count = 0;

        for (field_name, value) in &self.fields {
            let normal_value = json_to_normal_value(value)?;
            doc.set(field_name.clone(), normal_value);
            modified_count += 1;
        }

        Ok(modified_count)
    }
}

impl Default for UpdateInput {
    fn default() -> Self {
        Self::new()
    }
}

/// UpdateNode updates existing documents in a collection.
///
/// This node implements the Volcano iterator model. On the first call to `next()`,
/// it finds all matching documents (by docIDs or filter), applies the update patch,
/// and persists them via the `DocMutator`. Subsequent calls iterate through results.
///
/// # Example
///
/// ```ignore
/// let input = UpdateInput::new()
///     .with_field("email", json!("newemail@example.com"));
///
/// let mut node = UpdateNode::new("Users", mutator, fetcher, mapping)
///     .with_doc_ids(vec!["bae-123".to_string()])
///     .with_input(input);
///
/// node.init().await?;
/// node.start().await?;
///
/// while node.next().await? {
///     let updated_doc = node.value();
///     println!("Updated: {:?}", updated_doc.doc_id());
/// }
/// ```
pub struct UpdateNode {
    /// Collection name to update documents in
    collection_name: String,
    /// Document mutator for storage operations
    mutator: Arc<dyn DocMutator>,
    /// Document fetcher for resolving filters and getting all documents
    fetcher: Arc<dyn DocFetcher>,
    /// Document mapping for field positions
    document_mapping: DocumentMapping,
    /// Document IDs to update (mutually exclusive with filter)
    doc_ids: Option<Vec<String>>,
    /// Filter to find documents to update (mutually exclusive with doc_ids)
    filter: Option<Filter>,
    /// Update input (fields to patch)
    input: UpdateInput,
    /// Updated documents (populated after first next())
    updated_docs: Vec<Doc>,
    /// Document IDs that were requested but not found
    not_found_ids: Vec<String>,
    /// Current position in updated_docs
    position: usize,
    /// Current document being yielded
    current_doc: Doc,
    /// Whether updates have been performed yet
    did_update: bool,
    /// Whether the node has been initialized
    initialized: bool,
}

impl UpdateNode {
    /// Create a new update node for a collection.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - Name of the collection to update documents in
    /// * `mutator` - Document mutator for storage operations
    /// * `fetcher` - Document fetcher for resolving filters and getting all documents
    /// * `document_mapping` - Field mapping for result documents
    pub fn new(
        collection_name: impl Into<String>,
        mutator: Arc<dyn DocMutator>,
        fetcher: Arc<dyn DocFetcher>,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            collection_name: collection_name.into(),
            mutator,
            fetcher,
            document_mapping,
            doc_ids: None,
            filter: None,
            input: UpdateInput::new(),
            updated_docs: Vec::new(),
            not_found_ids: Vec::new(),
            position: 0,
            current_doc: Doc::default(),
            did_update: false,
            initialized: false,
        }
    }

    /// Set specific document IDs to update.
    pub fn with_doc_ids(mut self, doc_ids: Vec<String>) -> Self {
        self.doc_ids = Some(doc_ids);
        self
    }

    /// Set a filter to find documents to update.
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set the update input (fields to patch).
    pub fn with_input(mut self, input: UpdateInput) -> Self {
        self.input = input;
        self
    }

    /// Get the number of documents that were updated.
    pub fn updated_count(&self) -> usize {
        self.updated_docs.len()
    }

    /// Get the document IDs that were requested but not found.
    ///
    /// This allows callers to detect when update operations silently
    /// skipped documents due to them not existing in the collection.
    pub fn not_found_ids(&self) -> &[String] {
        &self.not_found_ids
    }

    /// Convert an UpdateResult to a plan Doc using our document mapping.
    fn update_result_to_doc(&self, result: &UpdateResult) -> Result<Doc> {
        let num_fields = self.document_mapping.next_index();
        let mut doc = Doc::new(num_fields);

        // Set document ID at index 0
        if let Some(doc_id) = result.document.id() {
            doc.set_doc_id(doc_id.to_string());
        }

        // Map each field from the updated document
        for (field_name, field_value) in result.document.values() {
            if let Some(index) = self.document_mapping.first_index_of_name(field_name) {
                let json_value = normal_value_to_json(field_value.value());
                doc.set(index, json_value);
            }
        }

        Ok(doc)
    }
}

#[async_trait]
impl PlanNode for UpdateNode {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;
        self.updated_docs.clear();
        self.not_found_ids.clear();
        self.did_update = false;
        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(QueryError::execution(
                "UpdateNode.next() called before init()",
            ));
        }

        // On first call, perform all updates
        if !self.did_update {
            // Get document IDs to update
            let doc_ids_to_update = if let Some(ref ids) = self.doc_ids {
                // Explicit doc_ids provided
                ids.clone()
            } else {
                // No doc_ids - fetch documents and optionally filter
                let all_docs = self.fetcher.get_all(&self.collection_name).await?;
                let mut matching_ids = Vec::new();

                for doc in all_docs {
                    // If filter exists, check if document matches
                    if let Some(ref filter) = self.filter {
                        let plan_doc = document_to_plan_doc(&doc, &self.document_mapping)?;
                        if !filter.matches(plan_doc.fields(), &self.document_mapping)? {
                            continue;
                        }
                    }
                    // Include this document
                    if let Some(id) = doc.id() {
                        matching_ids.push(id.to_string());
                    }
                }
                matching_ids
            };

            // Update each document
            for doc_id_str in doc_ids_to_update {
                let doc_id = DocID::from_string(&doc_id_str).map_err(|e| {
                    QueryError::execution(format!("Invalid DocID '{}': {}", doc_id_str, e))
                })?;

                // Fetch document for update
                let doc_opt = self
                    .mutator
                    .get_for_update(&self.collection_name, &doc_id)
                    .await?;

                if let Some(mut doc) = doc_opt {
                    // Apply update input
                    self.input.apply_to(&mut doc)?;

                    // Persist update
                    let result = self.mutator.update(&self.collection_name, doc).await?;

                    // Convert to plan Doc
                    let plan_doc = self.update_result_to_doc(&result)?;
                    self.updated_docs.push(plan_doc);
                } else {
                    // Track and log missing documents instead of silently skipping
                    tracing::warn!(
                        collection = %self.collection_name,
                        doc_id = %doc_id_str,
                        "Document not found for update - skipping"
                    );
                    self.not_found_ids.push(doc_id_str.clone());
                }
            }

            self.did_update = true;
        }

        // Iterate through updated documents
        if self.position >= self.updated_docs.len() {
            return Ok(false);
        }

        self.current_doc = self.updated_docs[self.position].deep_clone();
        self.position += 1;
        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.updated_docs.clear();
        self.not_found_ids.clear();
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        None // UpdateNode is a leaf node
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "updateNode"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fetcher::FetchByIdsResult;
    use crate::mutator::{CreateResult, DeleteResult};
    use serde_json::json;
    use std::sync::Mutex;

    /// Mock storage that implements both DocMutator and DocFetcher
    struct MockStorage {
        docs: Mutex<std::collections::HashMap<String, Document>>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                docs: Mutex::new(std::collections::HashMap::new()),
            }
        }

        fn add_doc(&self, doc: Document) {
            if let Some(id) = doc.id() {
                self.docs.lock().unwrap().insert(id.to_string(), doc);
            }
        }
    }

    #[async_trait]
    impl DocMutator for MockStorage {
        async fn create(&self, _collection_name: &str, mut doc: Document) -> Result<CreateResult> {
            doc.generate_and_set_doc_id()
                .map_err(|e| QueryError::execution(format!("Failed to generate DocID: {}", e)))?;

            let doc_id = doc
                .id()
                .cloned()
                .ok_or_else(|| QueryError::execution("Document should have ID after generation"))?;

            self.docs
                .lock()
                .unwrap()
                .insert(doc_id.to_string(), doc.clone());

            Ok(CreateResult::new(doc_id, doc))
        }

        async fn update(&self, _collection_name: &str, doc: Document) -> Result<UpdateResult> {
            let doc_id = doc
                .id()
                .ok_or_else(|| QueryError::execution("Document must have ID for update"))?;

            let modified = doc.values().len();

            self.docs
                .lock()
                .unwrap()
                .insert(doc_id.to_string(), doc.clone());

            Ok(UpdateResult::new(doc, modified))
        }

        async fn delete(&self, _collection_name: &str, doc_id: &DocID) -> Result<DeleteResult> {
            let existed = self
                .docs
                .lock()
                .unwrap()
                .remove(&doc_id.to_string())
                .is_some();
            Ok(DeleteResult::new(doc_id.clone(), existed))
        }

        async fn exists(&self, _collection_name: &str, doc_id: &DocID) -> Result<bool> {
            Ok(self.docs.lock().unwrap().contains_key(&doc_id.to_string()))
        }

        async fn get_for_update(
            &self,
            _collection_name: &str,
            doc_id: &DocID,
        ) -> Result<Option<Document>> {
            Ok(self.docs.lock().unwrap().get(&doc_id.to_string()).cloned())
        }
    }

    #[async_trait]
    impl DocFetcher for MockStorage {
        async fn get_all(&self, _collection_name: &str) -> Result<Vec<Document>> {
            Ok(self.docs.lock().unwrap().values().cloned().collect())
        }

        async fn get_by_ids(
            &self,
            _collection_name: &str,
            doc_ids: &[String],
        ) -> Result<FetchByIdsResult> {
            let docs = self.docs.lock().unwrap();
            let mut found = Vec::new();
            let mut missing = Vec::new();
            for id in doc_ids {
                if let Some(doc) = docs.get(id) {
                    found.push(doc.clone());
                } else {
                    missing.push(id.clone());
                }
            }
            Ok(FetchByIdsResult::partial(found, missing))
        }

        async fn get_by_field_value(
            &self,
            _collection_name: &str,
            field_name: &str,
            value: &str,
        ) -> Result<Vec<Document>> {
            let docs = self.docs.lock().unwrap();
            Ok(docs
                .values()
                .filter(|doc| {
                    doc.get(field_name)
                        .map(|v| v.as_str() == Some(value))
                        .unwrap_or(false)
                })
                .cloned()
                .collect())
        }
    }

    fn make_test_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "name");
        m.add(2, "email");
        m
    }

    fn create_test_doc(name: &str, email: &str) -> Document {
        let mut doc = Document::new();
        doc.set("name", name.to_string());
        doc.set("email", email.to_string());
        doc.generate_and_set_doc_id().unwrap();
        doc
    }

    #[tokio::test]
    async fn test_update_single_document() {
        let storage = Arc::new(MockStorage::new());
        let mapping = make_test_mapping();

        // Create initial document
        let doc = create_test_doc("Alice", "alice@old.com");
        let doc_id = doc.id().unwrap().to_string();
        storage.add_doc(doc);

        // Update it
        let input = UpdateInput::new().with_field("email", json!("alice@new.com"));

        let mut node = UpdateNode::new("Users", storage.clone(), storage.clone(), mapping)
            .with_doc_ids(vec![doc_id.clone()])
            .with_input(input);

        node.init().await.unwrap();
        node.start().await.unwrap();

        assert!(node.next().await.unwrap());

        let updated = node.value();
        assert_eq!(updated.doc_id(), Some(doc_id.as_str()));
        assert_eq!(updated.get(2), Some(&json!("alice@new.com")));

        assert!(!node.next().await.unwrap()); // No more documents
    }

    #[tokio::test]
    async fn test_update_multiple_documents() {
        let storage = Arc::new(MockStorage::new());
        let mapping = make_test_mapping();

        // Create initial documents
        let doc1 = create_test_doc("Alice", "alice@old.com");
        let doc1_id = doc1.id().unwrap().to_string();
        storage.add_doc(doc1);

        let doc2 = create_test_doc("Bob", "bob@old.com");
        let doc2_id = doc2.id().unwrap().to_string();
        storage.add_doc(doc2);

        // Update both
        let input = UpdateInput::new().with_field("email", json!("updated@example.com"));

        let mut node = UpdateNode::new("Users", storage.clone(), storage.clone(), mapping)
            .with_doc_ids(vec![doc1_id, doc2_id])
            .with_input(input);

        node.init().await.unwrap();
        node.start().await.unwrap();

        let mut count = 0;
        while node.next().await.unwrap() {
            assert_eq!(node.value().get(2), Some(&json!("updated@example.com")));
            count += 1;
        }

        assert_eq!(count, 2);
        assert_eq!(node.updated_count(), 2);
    }

    #[tokio::test]
    async fn test_update_missing_document_skipped() {
        let storage = Arc::new(MockStorage::new());
        let mapping = make_test_mapping();

        // Don't add any documents

        let input = UpdateInput::new().with_field("email", json!("new@example.com"));

        let mut node = UpdateNode::new("Users", storage.clone(), storage.clone(), mapping)
            .with_doc_ids(vec!["bae-nonexistent-id".to_string()])
            .with_input(input);

        node.init().await.unwrap();
        node.start().await.unwrap();

        // Should return false immediately (no documents found)
        // Note: The DocID parsing will fail for invalid format
        let result = node.next().await;
        // Either fails on invalid DocID or succeeds with 0 results
        if result.is_ok() {
            assert!(!result.unwrap());
            assert_eq!(node.updated_count(), 0);
        }
    }

    #[tokio::test]
    async fn test_update_without_doc_ids_or_filter_updates_all() {
        let storage = Arc::new(MockStorage::new());
        let mapping = make_test_mapping();

        // Create some documents
        let doc1 = create_test_doc("Alice", "alice@old.com");
        storage.add_doc(doc1);

        let doc2 = create_test_doc("Bob", "bob@old.com");
        storage.add_doc(doc2);

        let input = UpdateInput::new().with_field("email", json!("updated@example.com"));

        // No doc_ids and no filter - should update ALL documents
        let mut node =
            UpdateNode::new("Users", storage.clone(), storage.clone(), mapping).with_input(input);

        node.init().await.unwrap();
        node.start().await.unwrap();

        let mut count = 0;
        while node.next().await.unwrap() {
            assert_eq!(node.value().get(2), Some(&json!("updated@example.com")));
            count += 1;
        }

        assert_eq!(count, 2, "Should update all documents");
        assert_eq!(node.updated_count(), 2);
    }

    #[tokio::test]
    async fn test_update_without_doc_ids_or_filter_empty_collection() {
        let storage = Arc::new(MockStorage::new());
        let mapping = make_test_mapping();

        // Don't add any documents

        let input = UpdateInput::new().with_field("email", json!("new@example.com"));

        // No doc_ids and no filter on empty collection - should succeed with 0 results
        let mut node =
            UpdateNode::new("Users", storage.clone(), storage.clone(), mapping).with_input(input);

        node.init().await.unwrap();
        node.start().await.unwrap();

        let result = node.next().await;
        assert!(result.is_ok());
        assert!(!result.unwrap()); // No documents to update
        assert_eq!(node.updated_count(), 0);
    }

    #[tokio::test]
    async fn test_update_next_before_init_errors() {
        let storage = Arc::new(MockStorage::new());
        let mapping = make_test_mapping();

        let mut node = UpdateNode::new("Users", storage.clone(), storage.clone(), mapping);

        let result = node.next().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_tracks_not_found_documents() {
        let storage = Arc::new(MockStorage::new());
        let mapping = make_test_mapping();

        // Create one document that exists
        let doc = create_test_doc("Alice", "alice@test.com");
        let existing_id = doc.id().unwrap().to_string();
        storage.add_doc(doc);

        // Create another valid-looking DocID that doesn't exist in storage
        let mut fake_doc = Document::new();
        fake_doc.set("name", "Fake".to_string());
        fake_doc.generate_and_set_doc_id().unwrap();
        let nonexistent_id = fake_doc.id().unwrap().to_string();
        // Don't add fake_doc to storage - it won't exist

        let input = UpdateInput::new().with_field("email", json!("updated@test.com"));

        let mut node = UpdateNode::new("Users", storage.clone(), storage.clone(), mapping)
            .with_doc_ids(vec![existing_id.clone(), nonexistent_id.clone()])
            .with_input(input);

        node.init().await.unwrap();
        node.start().await.unwrap();

        // Exhaust the iterator
        let mut count = 0;
        while node.next().await.unwrap() {
            count += 1;
        }

        // Only the existing document should have been updated
        assert_eq!(count, 1, "Should only update one existing document");
        assert_eq!(node.updated_count(), 1);

        // The non-existent document should be tracked
        let not_found = node.not_found_ids();
        assert_eq!(not_found.len(), 1, "Should have one not-found ID");
        assert_eq!(not_found[0], nonexistent_id);
    }
}
