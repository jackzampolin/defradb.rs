//! ScanNode for scanning collection documents

use async_trait::async_trait;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use schema::CollectionVersion;

use crate::document::{documents_to_plan_docs, documents_with_status_to_plan_docs, DocumentMapping};
use crate::error::Result;
use crate::fetcher::DocFetcher;
use crate::mapper::Filter;
use crate::planner::{Doc, PlanNode};

/// Derive a short u32 ID from a collection_id string.
/// Uses the same hash as db::collection_short_id for consistency.
fn collection_short_id(collection_id: &str) -> u32 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    collection_id.hash(&mut hasher);
    hasher.finish() as u32
}

/// ScanNode scans documents from a collection.
///
/// This is the primary data source node in query plans.
/// It reads documents from storage and yields them to parent nodes.
///
/// # Data Loading
///
/// ScanNode can obtain documents in two ways:
/// 1. Pre-loaded via `with_docs()` - for testing or when data is already available
/// 2. On-demand via a `DocFetcher` - fetches during `init()` if docs are empty
///
/// When a fetcher is provided and no docs are pre-loaded, the node will
/// automatically fetch all documents from the collection during initialization.
pub struct ScanNode {
    /// Collection schema
    collection: CollectionVersion,
    /// Document mapping for field positions
    document_mapping: DocumentMapping,
    /// Optional filter to apply during scan
    filter: Option<Filter>,
    /// Whether to show deleted documents
    show_deleted: bool,
    /// Current document
    current_doc: Doc,
    /// Iterator state (simulated for now)
    docs: Vec<Doc>,
    /// Current position in docs
    position: usize,
    /// Whether the node has been initialized
    initialized: bool,
    /// Optional fetcher for loading documents on-demand
    fetcher: Option<Arc<dyn DocFetcher>>,
    /// Whether docs were explicitly provided (even if empty)
    docs_provided: bool,
}

impl ScanNode {
    /// Create a new scan node for a collection
    pub fn new(collection: CollectionVersion, document_mapping: DocumentMapping) -> Self {
        Self {
            collection,
            document_mapping,
            filter: None,
            show_deleted: false,
            current_doc: Doc::default(),
            docs: Vec::new(),
            position: 0,
            initialized: false,
            fetcher: None,
            docs_provided: false,
        }
    }

    /// Set the filter for this scan
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set whether to include deleted documents
    pub fn with_show_deleted(mut self, show_deleted: bool) -> Self {
        self.show_deleted = show_deleted;
        self
    }

    /// Set documents directly (for testing or in-memory operations).
    ///
    /// Providing an empty vector is valid and represents an empty collection.
    pub fn with_docs(mut self, docs: Vec<Doc>) -> Self {
        self.docs = docs;
        self.docs_provided = true;
        self
    }

    /// Set a document fetcher for on-demand data loading.
    ///
    /// When set, the node will fetch documents from storage during `init()`
    /// if no documents were pre-loaded via `with_docs()`.
    pub fn with_fetcher(mut self, fetcher: Arc<dyn DocFetcher>) -> Self {
        self.fetcher = Some(fetcher);
        self
    }

    /// Get the collection
    pub fn collection(&self) -> &CollectionVersion {
        &self.collection
    }

    /// Get the collection name
    pub fn collection_name(&self) -> &str {
        &self.collection.name
    }

    /// Get the storage prefix for this collection.
    ///
    /// Go uses a sequential monotonic counter stored in the system store.
    /// Rust uses a hash-based approach for consistency across the codebase.
    /// Note: This means Rust and Go will produce different prefix values.
    fn collection_prefix(&self) -> u32 {
        collection_short_id(&self.collection.collection_id)
    }
}

#[async_trait]
impl PlanNode for ScanNode {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;

        // If docs weren't provided and we have a fetcher, load documents from storage
        if !self.docs_provided {
            if let Some(ref fetcher) = self.fetcher {
                // Use get_all_with_deleted to get documents with their deletion status.
                // When show_deleted is true, we get all documents including deleted ones.
                // The deletion status is used to:
                // 1. Set DocStatus on the plan Doc for filtering in next()
                // 2. Populate the _deleted field if it's in the document mapping
                let docs_with_status = fetcher
                    .get_all_with_deleted(&self.collection.name, self.show_deleted)
                    .await?;
                self.docs =
                    documents_with_status_to_plan_docs(&docs_with_status, &self.document_mapping)?;
            } else {
                // No docs provided and no fetcher - this is a programming error.
                // Either pre-load docs with with_docs() or attach a fetcher with with_fetcher().
                return Err(crate::error::QueryError::internal(format!(
                    "ScanNode for collection '{}' has no documents and no fetcher - \
                     this indicates a bug in query planning or test setup",
                    self.collection.name
                )));
            }
        }

        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(crate::error::QueryError::execution(
                "ScanNode.next() called before init()",
            ));
        }

        loop {
            if self.position >= self.docs.len() {
                return Ok(false);
            }

            let doc = &self.docs[self.position];
            self.position += 1;

            // Skip deleted docs if not showing deleted
            if !self.show_deleted && doc.is_deleted() {
                continue;
            }

            // Apply filter if present
            if let Some(ref filter) = self.filter {
                if !filter.matches(doc.fields(), &self.document_mapping)? {
                    continue;
                }
            }

            self.current_doc = doc.deep_clone();
            return Ok(true);
        }
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.docs.clear();
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        None // ScanNode is a leaf node
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "scanNode"
    }

    fn explain_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        // Go DefraDB format: always include filter (null if none)
        if let Some(ref filter) = self.filter {
            obj.insert("filter".to_string(), serde_json::json!(filter.conditions()));
        } else {
            obj.insert("filter".to_string(), serde_json::Value::Null);
        }

        // Go DefraDB uses "collectionName" and "collectionID"
        // Note: Go's explain uses VersionID (not CollectionID) for the collectionID field
        obj.insert(
            "collectionName".to_string(),
            serde_json::Value::String(self.collection.name.clone()),
        );
        obj.insert(
            "collectionID".to_string(),
            serde_json::Value::String(self.collection.version_id.clone()),
        );

        // Go DefraDB format: always include prefixes
        // Prefix format is "/<collection_root_id>" which is a unique identifier for the collection's data
        // The collection_id is a CID but the prefix uses a shorter index - for now use collection_id's suffix
        // This will be refined when we have proper storage key integration
        obj.insert(
            "prefixes".to_string(),
            serde_json::json!([format!("/{}", self.collection_prefix())]),
        );

        if self.show_deleted {
            obj.insert("showDeleted".to_string(), serde_json::Value::Bool(true));
        }

        serde_json::Value::Object(obj)
    }
}
