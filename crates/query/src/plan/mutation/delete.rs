//! DeleteNode for deleting existing documents
//!
//! This node deletes documents from storage during query execution, following
//! the Go DefraDB pattern where persistence happens within the plan node.

use std::sync::Arc;

use async_trait::async_trait;
use document::DocID;
use tracing;

use crate::fetcher::DocFetcher;
use crate::mutator::DocMutator;
use crate::planner::{Doc, PlanNode};
use query_types::document::{document_to_plan_doc, DocumentMapping};
use query_types::error::{QueryError, Result};
use query_types::mapper::Filter;

/// DeleteNode deletes existing documents from a collection.
///
/// This node implements the Volcano iterator model. On the first call to `next()`,
/// it finds all matching documents (by docIDs or filter) and deletes them via
/// the `DocMutator`. Subsequent calls iterate through the deleted document IDs.
///
/// # Example
///
/// ```ignore
/// let mut node = DeleteNode::new("Users", mutator, fetcher, mapping)
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
    /// Document fetcher for resolving filters and getting all documents
    fetcher: Arc<dyn DocFetcher>,
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

    /// Create a Doc from a fetched document, then mark it deleted.
    fn create_deleted_doc_from_document(&self, storage_doc: &document::Document) -> Result<Doc> {
        let mut plan_doc = document_to_plan_doc(storage_doc, &self.document_mapping)?;
        plan_doc.mark_deleted();
        Ok(plan_doc)
    }

    /// Create a minimal Doc for a deleted document (fallback when doc was not fetched).
    fn create_deleted_doc(&self, doc_id: &str) -> Doc {
        let num_fields = self.document_mapping.next_index();
        let mut doc = Doc::new(num_fields);
        doc.set_doc_id(doc_id);
        doc.mark_deleted();
        doc
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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
            if let Some(ref ids) = self.doc_ids {
                let mut parsed_ids = Vec::with_capacity(ids.len());
                for doc_id_str in ids {
                    match DocID::from_string(doc_id_str) {
                        Ok(doc_id) => parsed_ids.push((doc_id_str.clone(), doc_id)),
                        Err(_) => {
                            tracing::warn!(
                                collection = %self.collection_name,
                                doc_id = %doc_id_str,
                                "Invalid DocID format - skipping"
                            );
                            self.not_found_ids.push(doc_id_str.clone());
                        }
                    }
                }

                // Explicit doc_ids: fetch documents first to populate result fields
                let valid_ids = parsed_ids
                    .iter()
                    .map(|(doc_id, _)| doc_id.clone())
                    .collect::<Vec<_>>();
                let fetch_result = self
                    .fetcher
                    .get_by_ids(&self.collection_name, &valid_ids)
                    .await?;

                // Build a map from doc_id -> Document for lookup
                let mut doc_map: std::collections::HashMap<String, document::Document> =
                    std::collections::HashMap::new();
                for doc in fetch_result.into_docs() {
                    if let Some(id) = doc.id() {
                        doc_map.insert(id.to_string(), doc);
                    }
                }

                for (doc_id_str, doc_id) in parsed_ids {
                    let result = self.mutator.delete(&self.collection_name, &doc_id).await?;

                    if result.existed {
                        let plan_doc = if let Some(storage_doc) = doc_map.get(&doc_id_str) {
                            self.create_deleted_doc_from_document(storage_doc)?
                        } else {
                            self.create_deleted_doc(&doc_id_str)
                        };
                        self.deleted_docs.push(plan_doc);
                    } else {
                        tracing::warn!(
                            collection = %self.collection_name,
                            doc_id = %doc_id_str,
                            "Attempted to delete non-existent document"
                        );
                        self.not_found_ids.push(doc_id_str.clone());
                    }
                }
            } else {
                // No doc_ids - fetch documents and optionally filter
                let all_docs = self.fetcher.get_all(&self.collection_name).await?;
                let mut docs_to_delete: Vec<document::Document> = Vec::new();

                for doc in all_docs {
                    if let Some(ref filter) = self.filter {
                        let plan_doc = document_to_plan_doc(&doc, &self.document_mapping)?;
                        if !filter.matches(plan_doc.fields(), &self.document_mapping)? {
                            continue;
                        }
                    }
                    docs_to_delete.push(doc);
                }

                for storage_doc in &docs_to_delete {
                    if let Some(id) = storage_doc.id() {
                        let doc_id = DocID::from_string(&id.to_string()).map_err(|e| {
                            QueryError::execution(format!("Invalid DocID '{}': {}", id, e))
                        })?;

                        let result = self.mutator.delete(&self.collection_name, &doc_id).await?;

                        if result.existed {
                            let plan_doc = self.create_deleted_doc_from_document(storage_doc)?;
                            self.deleted_docs.push(plan_doc);
                        }
                    }
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

    fn explain_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        // docID: array of doc IDs to delete (null if using filter)
        if let Some(ref doc_ids) = self.doc_ids {
            obj.insert(
                "docID".to_string(),
                serde_json::Value::Array(
                    doc_ids
                        .iter()
                        .map(|id| serde_json::Value::String(id.clone()))
                        .collect(),
                ),
            );
        } else {
            obj.insert("docID".to_string(), serde_json::Value::Null);
        }

        // filter: the filter expression (null if using doc IDs)
        if let Some(ref filter) = self.filter {
            obj.insert("filter".to_string(), serde_json::json!(filter.conditions()));
        } else {
            obj.insert("filter".to_string(), serde_json::Value::Null);
        }

        // Include child node if present
        if let Some(source) = self.source() {
            let child_explain = source.explain();
            if let Some(child_obj) = child_explain.as_object() {
                for (key, value) in child_obj {
                    obj.insert(key.clone(), value.clone());
                }
            }
        }

        serde_json::Value::Object(obj)
    }
}
