//! IndexScanNode for index-driven document scanning
//!
//! This node represents an index-based scan in the query plan.
//! It uses documents retrieved via index lookup, providing better performance
//! than full collection scans when filters match indexed fields.
//!
//! # Data Loading
//!
//! IndexScanNode can obtain documents in two ways:
//! 1. Pre-loaded via `with_docs()` - for testing or when data is already available
//! 2. On-demand via a `DocFetcher` - fetches during `init()` using index scan params

use async_trait::async_trait;
use schema::CollectionVersion;
use std::sync::Arc;

use crate::document::{documents_to_plan_docs, DocumentMapping};
use crate::error::Result;
use crate::fetcher::DocFetcher;
use crate::mapper::Filter;
use crate::planner::index_selection::IndexScanParams;
use crate::planner::{Doc, ExecInfo, PlanNode};

/// IndexScanNode scans documents retrieved via index lookup.
///
/// Similar to ScanNode, but uses index-based document fetching for better
/// performance when filters match indexed fields. The index scan parameters
/// are stored for query explanation and optimization analysis.
pub struct IndexScanNode {
    /// Collection schema
    collection: CollectionVersion,
    /// Document mapping for field positions
    document_mapping: DocumentMapping,
    /// Index scan parameters that were used
    index_params: IndexScanParams,
    /// Residual filter for conditions not covered by index
    residual_filter: Option<Filter>,
    /// Whether to show deleted documents
    show_deleted: bool,
    /// Current document
    current_doc: Doc,
    /// Documents fetched via index
    docs: Vec<Doc>,
    /// Current position in docs
    position: usize,
    /// Whether the node has been initialized
    initialized: bool,
    /// Optional fetcher for loading documents on-demand
    fetcher: Option<Arc<dyn DocFetcher>>,
    /// Whether docs were explicitly provided (even if empty)
    docs_provided: bool,
    /// Number of index key lookups performed (for explain output)
    index_fetches: u64,
    /// Execution statistics for explain execute mode
    exec_info: ExecInfo,
    /// Number of fields per document (for fieldFetches calculation)
    fields_per_doc: usize,
}

impl IndexScanNode {
    /// Create a new index scan node
    pub fn new(
        collection: CollectionVersion,
        document_mapping: DocumentMapping,
        index_params: IndexScanParams,
    ) -> Self {
        // Count storable fields from the collection schema (matches Go's field fetch counting).
        let fields_per_doc = collection
            .fields
            .iter()
            .filter(|f| !f.id.is_empty())
            .count();
        Self {
            collection,
            document_mapping,
            index_params,
            residual_filter: None,
            show_deleted: false,
            current_doc: Doc::default(),
            docs: Vec::new(),
            position: 0,
            initialized: false,
            fetcher: None,
            docs_provided: false,
            index_fetches: 0,
            exec_info: ExecInfo::default(),
            fields_per_doc,
        }
    }

    /// Set a residual filter for conditions not covered by the index.
    ///
    /// When a filter has multiple conditions but only some are covered by the index,
    /// the remaining conditions become the residual filter applied after index lookup.
    pub fn with_residual_filter(mut self, filter: Filter) -> Self {
        self.residual_filter = Some(filter);
        self
    }

    /// Set whether to include deleted documents
    pub fn with_show_deleted(mut self, show_deleted: bool) -> Self {
        self.show_deleted = show_deleted;
        self
    }

    /// Set documents directly (retrieved via index lookup).
    ///
    /// Providing an empty vector is valid and represents an empty result set.
    pub fn with_docs(mut self, docs: Vec<Doc>) -> Self {
        self.index_fetches = docs.len() as u64;
        self.docs = docs;
        self.docs_provided = true;
        self
    }

    /// Set a document fetcher for on-demand data loading.
    ///
    /// When set, the node will fetch documents from storage during `init()`
    /// using the index scan parameters if no documents were pre-loaded.
    pub fn with_fetcher(mut self, fetcher: Arc<dyn DocFetcher>) -> Self {
        self.fetcher = Some(fetcher);
        self
    }

    /// Get the index scan parameters
    pub fn index_params(&self) -> &IndexScanParams {
        &self.index_params
    }

    /// Get the collection
    pub fn collection(&self) -> &CollectionVersion {
        &self.collection
    }

    /// Get the index name being used
    pub fn index_name(&self) -> &str {
        &self.index_params.index_name
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for IndexScanNode {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;
        // Reset execution stats
        self.exec_info = ExecInfo::default();

        // If docs weren't provided and we have a fetcher, load documents via index
        if !self.docs_provided {
            if let Some(ref fetcher) = self.fetcher {
                // Use index scan to get document IDs
                let doc_ids = fetcher
                    .get_by_index_scan(&self.collection.name, &self.index_params)
                    .await?;

                // Track number of index key lookups (matches Go's IndexesFetched)
                self.index_fetches = doc_ids.len() as u64;

                if !doc_ids.is_empty() {
                    // Fetch the actual documents by their IDs
                    let result = fetcher.get_by_ids(&self.collection.name, &doc_ids).await?;
                    self.docs = documents_to_plan_docs(result.docs(), &self.document_mapping)?;
                }
            }
            // Note: If no fetcher and no docs, we have an empty result set.
            // This is valid for index scans that match no documents.
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
                "IndexScanNode.next() called before init()",
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
            // Track field fetches (each field in the document)
            self.exec_info.fields_fetched += self.fields_per_doc as u64;

            // Skip deleted docs if not showing deleted
            if !self.show_deleted && doc.is_deleted() {
                continue;
            }

            // Apply residual filter if present
            if let Some(ref filter) = self.residual_filter {
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
        None // IndexScanNode is a leaf node
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        // Use "scanNode" for explain compatibility with Go DefraDB asserter.
        // The index-specific info (indexName) distinguishes this from a regular scan.
        "scanNode"
    }

    fn explain_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        // Go DefraDB uses "collectionName" and "collectionID"
        obj.insert(
            "collectionName".to_string(),
            serde_json::Value::String(self.collection.name.clone()),
        );
        obj.insert(
            "collectionID".to_string(),
            serde_json::Value::String(self.collection.collection_id.clone()),
        );

        // Index-specific info - presence of indexName indicates this is an index scan
        obj.insert(
            "indexName".to_string(),
            serde_json::Value::String(self.index_params.index_name.clone()),
        );

        // Index fetch count (set during init, used by explain metrics)
        obj.insert(
            "indexFetches".to_string(),
            serde_json::json!(self.index_fetches),
        );

        if let Some(ref filter) = self.residual_filter {
            obj.insert("filter".to_string(), serde_json::json!(filter.conditions()));
        }

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
            serde_json::json!(self.index_fetches),
        );

        serde_json::Value::Object(obj)
    }
}
