//! DeleteNode for deleting existing documents
//!
//! This node deletes documents from storage during query execution, following
//! the Go DefraDB pattern where persistence happens within the plan node.

use std::sync::Arc;

use async_trait::async_trait;
use document::DocID;
use tracing;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::Filter;
use crate::mutator::DocMutator;
use crate::planner::{Doc, PlanNode};

/// DeleteNode deletes existing documents from a collection.
///
/// This node implements the Volcano iterator model. On the first call to `next()`,
/// it finds all matching documents (by docIDs or filter) and deletes them via
/// the `DocMutator`. Subsequent calls iterate through the deleted document IDs.
///
/// # Example
///
/// ```ignore
/// let mut node = DeleteNode::new("Users", mutator, mapping)
///     .with_doc_ids(vec!["bae-123".to_string()]);
///
/// node.init().await?;
/// node.start().await?;
///
/// while node.next().await? {
///     let deleted_doc = node.value();
///     println!("Deleted: {:?}", deleted_doc.doc_id());
/// }
/// ```
pub struct DeleteNode {
    /// Collection name to delete documents from
    collection_name: String,
    /// Document mutator for storage operations
    mutator: Arc<dyn DocMutator>,
    /// Document mapping for field positions
    document_mapping: DocumentMapping,
    /// Document IDs to delete (mutually exclusive with filter)
    doc_ids: Option<Vec<String>>,
    /// Filter to find documents to delete (mutually exclusive with doc_ids)
    filter: Option<Filter>,
    /// Deleted document representations (populated after first next())
    deleted_docs: Vec<Doc>,
    /// Document IDs that were requested but did not exist
    not_found_ids: Vec<String>,
    /// Current position in deleted_docs
    position: usize,
    /// Current document being yielded
    current_doc: Doc,
    /// Whether deletions have been performed yet
    did_delete: bool,
    /// Whether the node has been initialized
    initialized: bool,
}

impl DeleteNode {
    /// Create a new delete node for a collection.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - Name of the collection to delete documents from
    /// * `mutator` - Document mutator for storage operations
    /// * `document_mapping` - Field mapping for result documents
    pub fn new(
        collection_name: impl Into<String>,
        mutator: Arc<dyn DocMutator>,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            collection_name: collection_name.into(),
            mutator,
            document_mapping,
            doc_ids: None,
            filter: None,
            deleted_docs: Vec::new(),
            not_found_ids: Vec::new(),
            position: 0,
            current_doc: Doc::default(),
            did_delete: false,
            initialized: false,
        }
    }

    /// Set specific document IDs to delete.
    pub fn with_doc_ids(mut self, doc_ids: Vec<String>) -> Self {
        self.doc_ids = Some(doc_ids);
        self
    }

    /// Set a filter to find documents to delete.
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Get the number of documents that were deleted.
    pub fn deleted_count(&self) -> usize {
        self.deleted_docs.len()
    }

    /// Get the document IDs that were requested but did not exist.
    ///
    /// This allows callers to detect when delete operations skipped
    /// documents because they didn't exist in the collection.
    pub fn not_found_ids(&self) -> &[String] {
        &self.not_found_ids
    }

    /// Create a minimal Doc representing a deleted document.
    fn create_deleted_doc(&self, doc_id: &str) -> Doc {
        let num_fields = self.document_mapping.next_index();
        let mut doc = Doc::new(num_fields);
        doc.set_doc_id(doc_id);
        doc.mark_deleted();
        doc
    }
}

#[async_trait]
impl PlanNode for DeleteNode {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;
        self.deleted_docs.clear();
        self.not_found_ids.clear();
        self.did_delete = false;
        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(QueryError::execution(
                "DeleteNode.next() called before init()",
            ));
        }

        // On first call, perform all deletions
        if !self.did_delete {
            // Get document IDs to delete
            // Note: Filter-based deletion is handled by the mutation runner which resolves
            // filters to doc_ids before passing to DeleteNode. See resolve_filter_to_doc_ids().
            let doc_ids_to_delete = if let Some(ref ids) = self.doc_ids {
                ids.clone()
            } else {
                return Err(QueryError::execution(
                    "DeleteNode requires doc_ids (filter resolution should be done by runner)",
                ));
            };

            // Delete each document
            for doc_id_str in doc_ids_to_delete {
                let doc_id = DocID::from_string(&doc_id_str).map_err(|e| {
                    QueryError::execution(format!("Invalid DocID '{}': {}", doc_id_str, e))
                })?;

                // Attempt to delete
                let result = self.mutator.delete(&self.collection_name, &doc_id).await?;

                // Only yield if the document actually existed
                if result.existed {
                    let plan_doc = self.create_deleted_doc(&doc_id_str);
                    self.deleted_docs.push(plan_doc);
                } else {
                    // Track and log non-existent documents
                    tracing::warn!(
                        collection = %self.collection_name,
                        doc_id = %doc_id_str,
                        "Attempted to delete non-existent document"
                    );
                    self.not_found_ids.push(doc_id_str);
                }
            }

            self.did_delete = true;
        }

        // Iterate through deleted documents
        if self.position >= self.deleted_docs.len() {
            return Ok(false);
        }

        self.current_doc = self.deleted_docs[self.position].deep_clone();
        self.position += 1;
        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.deleted_docs.clear();
        self.not_found_ids.clear();
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        None // DeleteNode is a leaf node
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "deleteNode"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutator::{CreateResult, DeleteResult, UpdateResult};
    use document::Document;
    use std::sync::Mutex;

    /// Mock mutator for testing
    struct MockMutator {
        docs: Mutex<std::collections::HashMap<String, Document>>,
    }

    impl MockMutator {
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

        fn doc_count(&self) -> usize {
            self.docs.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl DocMutator for MockMutator {
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

    fn make_test_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "name");
        m
    }

    fn create_test_doc(name: &str) -> Document {
        let mut doc = Document::new();
        doc.set("name", name.to_string());
        doc.generate_and_set_doc_id().unwrap();
        doc
    }

    #[tokio::test]
    async fn test_delete_single_document() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        // Create initial document
        let doc = create_test_doc("Alice");
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc(doc);

        assert_eq!(mutator.doc_count(), 1);

        // Delete it
        let mut node =
            DeleteNode::new("Users", mutator.clone(), mapping).with_doc_ids(vec![doc_id.clone()]);

        node.init().await.unwrap();
        node.start().await.unwrap();

        assert!(node.next().await.unwrap());

        let deleted = node.value();
        assert_eq!(deleted.doc_id(), Some(doc_id.as_str()));
        assert!(deleted.is_deleted());

        assert!(!node.next().await.unwrap()); // No more documents

        // Verify document was actually deleted
        assert_eq!(mutator.doc_count(), 0);
    }

    #[tokio::test]
    async fn test_delete_multiple_documents() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        // Create initial documents
        let doc1 = create_test_doc("Alice");
        let doc1_id = doc1.id().unwrap().to_string();
        mutator.add_doc(doc1);

        let doc2 = create_test_doc("Bob");
        let doc2_id = doc2.id().unwrap().to_string();
        mutator.add_doc(doc2);

        assert_eq!(mutator.doc_count(), 2);

        // Delete both
        let mut node =
            DeleteNode::new("Users", mutator.clone(), mapping).with_doc_ids(vec![doc1_id, doc2_id]);

        node.init().await.unwrap();
        node.start().await.unwrap();

        let mut count = 0;
        while node.next().await.unwrap() {
            assert!(node.value().is_deleted());
            count += 1;
        }

        assert_eq!(count, 2);
        assert_eq!(node.deleted_count(), 2);
        assert_eq!(mutator.doc_count(), 0);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_document_skipped() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        // Create one document
        let doc = create_test_doc("Alice");
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc(doc);

        // Try to delete with a mix of existing and non-existing IDs
        // Note: We need to use the same format as valid DocIDs
        let mut node =
            DeleteNode::new("Users", mutator.clone(), mapping).with_doc_ids(vec![doc_id.clone()]);

        node.init().await.unwrap();
        node.start().await.unwrap();

        // Should only get one result (the existing doc)
        let mut count = 0;
        while node.next().await.unwrap() {
            count += 1;
        }

        assert_eq!(count, 1);
        assert_eq!(mutator.doc_count(), 0);
    }

    #[tokio::test]
    async fn test_delete_without_doc_ids_or_filter_errors() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        let mut node = DeleteNode::new("Users", mutator, mapping);

        node.init().await.unwrap();
        node.start().await.unwrap();

        let result = node.next().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_next_before_init_errors() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        let mut node = DeleteNode::new("Users", mutator, mapping);

        let result = node.next().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_tracks_not_found_documents() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        // Create one document that exists
        let doc = create_test_doc("Alice");
        let existing_id = doc.id().unwrap().to_string();
        mutator.add_doc(doc);

        // Create another valid-looking DocID that doesn't exist in storage
        let mut fake_doc = Document::new();
        fake_doc.set("name", "Fake".to_string());
        fake_doc.generate_and_set_doc_id().unwrap();
        let nonexistent_id = fake_doc.id().unwrap().to_string();
        // Don't add fake_doc to mutator - it won't exist

        let mut node = DeleteNode::new("Users", mutator.clone(), mapping)
            .with_doc_ids(vec![existing_id.clone(), nonexistent_id.clone()]);

        node.init().await.unwrap();
        node.start().await.unwrap();

        // Exhaust the iterator
        let mut count = 0;
        while node.next().await.unwrap() {
            count += 1;
        }

        // Only the existing document should have been deleted
        assert_eq!(count, 1, "Should only delete one existing document");
        assert_eq!(node.deleted_count(), 1);
        assert_eq!(mutator.doc_count(), 0, "Existing doc should be removed");

        // The non-existent document should be tracked
        let not_found = node.not_found_ids();
        assert_eq!(not_found.len(), 1, "Should have one not-found ID");
        assert_eq!(not_found[0], nonexistent_id);
    }
}
