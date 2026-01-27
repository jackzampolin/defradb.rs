//! IndexScanNode for index-driven document scanning
//!
//! This node represents an index-based scan in the query plan.
//! It uses pre-fetched documents that were retrieved via index lookup,
//! providing better performance than full collection scans when filters
//! match indexed fields.

use async_trait::async_trait;
use schema::CollectionVersion;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::Filter;
use crate::planner::index_selection::IndexScanParams;
use crate::planner::{Doc, PlanNode};

/// IndexScanNode scans documents retrieved via index lookup.
///
/// Similar to ScanNode, but indicates that documents were fetched
/// using an index for better performance. The index scan parameters
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
}

impl IndexScanNode {
    /// Create a new index scan node
    pub fn new(
        collection: CollectionVersion,
        document_mapping: DocumentMapping,
        index_params: IndexScanParams,
    ) -> Self {
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

    /// Set documents directly (retrieved via index lookup)
    pub fn with_docs(mut self, docs: Vec<Doc>) -> Self {
        self.docs = docs;
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

#[async_trait]
impl PlanNode for IndexScanNode {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;
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
        "indexScanNode"
    }
}
