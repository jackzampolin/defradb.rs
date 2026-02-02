//! ScanNode for scanning collection documents

use async_trait::async_trait;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use schema::CollectionVersion;

use crate::document::{
    documents_to_plan_docs, documents_with_status_to_plan_docs, DocumentMapping,
};
use crate::error::Result;
use crate::fetcher::DocFetcher;
use crate::mapper::Filter;
use crate::planner::{Doc, ExecInfo, PlanNode};

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
    /// Optional document IDs to scan (for explain prefixes)
    doc_ids: Option<Vec<String>>,
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
    /// Execution statistics for explain execute mode
    exec_info: ExecInfo,
    /// Number of fields per document (for fieldFetches calculation)
    fields_per_doc: usize,
}

impl ScanNode {
    /// Create a new scan node for a collection
    pub fn new(collection: CollectionVersion, document_mapping: DocumentMapping) -> Self {
        // Count storable fields from the collection schema (matches Go's field fetch counting).
        // Go counts KV pairs from storage, which corresponds to fields with a non-empty FieldID.
        // This excludes virtual relation objects (no FieldID) and system fields.
        let fields_per_doc = collection
            .fields
            .iter()
            .filter(|f| !f.id.is_empty())
            .count();
        Self {
            collection,
            document_mapping,
            filter: None,
            doc_ids: None,
            show_deleted: false,
            current_doc: Doc::default(),
            docs: Vec::new(),
            position: 0,
            initialized: false,
            fetcher: None,
            docs_provided: false,
            exec_info: ExecInfo::default(),
            fields_per_doc,
        }
    }

    /// Set the filter for this scan
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set the document IDs for this scan (used in explain prefixes)
    pub fn with_doc_ids(mut self, doc_ids: Vec<String>) -> Self {
        // Only set if non-empty; empty means scan entire collection
        if !doc_ids.is_empty() {
            self.doc_ids = Some(doc_ids);
        }
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
    /// Uses the sequential root_id if available (assigned during collection creation),
    /// falling back to hash-based short_id for backwards compatibility.
    fn collection_prefix(&self) -> u32 {
        if self.collection.root_id > 0 {
            self.collection.root_id
        } else {
            collection_short_id(&self.collection.collection_id)
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for ScanNode {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;
        // Reset execution stats
        self.exec_info = ExecInfo::default();

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

        // Track iteration (Go counts each call to next, including final false)
        self.exec_info.iterations += 1;

        loop {
            if self.position >= self.docs.len() {
                return Ok(false);
            }

            let doc = &self.docs[self.position];
            self.position += 1;

            // Track document fetch
            self.exec_info.docs_fetched += 1;
            // Track field fetches (actual stored fields in this document)
            self.exec_info.fields_fetched += doc.stored_field_count as u64;

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

        // Go DefraDB format: always include filter (null if none or empty)
        // Only strip _docID conditions when doc_ids are provided as a query argument
        // (Go converts those to prefix scans). When _docID is a regular filter condition,
        // keep it in the filter output.
        if let Some(ref filter) = self.filter {
            let conditions = filter.conditions();
            if self.doc_ids.is_some() {
                // doc_ids provided → strip _docID (it's shown in prefixes)
                let stripped: std::collections::BTreeMap<_, _> = conditions
                    .into_iter()
                    .filter(|(k, _)| k.as_str() != "_docID")
                    .collect();
                if stripped.is_empty() {
                    obj.insert("filter".to_string(), serde_json::Value::Null);
                } else {
                    obj.insert("filter".to_string(), serde_json::json!(stripped));
                }
            } else if conditions.is_empty() {
                obj.insert("filter".to_string(), serde_json::Value::Null);
            } else {
                obj.insert("filter".to_string(), serde_json::json!(conditions));
            }
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
        // When docIDs are provided, each prefix is "/<collection_prefix>/<docID>"
        // Otherwise just "/<collection_prefix>"
        let prefixes: Vec<String> = if let Some(ref doc_ids) = self.doc_ids {
            doc_ids
                .iter()
                .map(|id| format!("/{}/{}", self.collection_prefix(), id))
                .collect()
        } else {
            vec![format!("/{}", self.collection_prefix())]
        };
        obj.insert("prefixes".to_string(), serde_json::json!(prefixes));

        if self.show_deleted {
            obj.insert("showDeleted".to_string(), serde_json::Value::Bool(true));
        }

        serde_json::Value::Object(obj)
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }

    fn explain_execute_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations as u64),
        );
        obj.insert(
            "docFetches".to_string(),
            serde_json::json!(self.exec_info.docs_fetched as u64),
        );
        obj.insert(
            "fieldFetches".to_string(),
            serde_json::json!(self.exec_info.fields_fetched as u64),
        );
        obj.insert(
            "indexFetches".to_string(),
            serde_json::json!(self.exec_info.indexes_fetched as u64),
        );

        serde_json::Value::Object(obj)
    }
}
