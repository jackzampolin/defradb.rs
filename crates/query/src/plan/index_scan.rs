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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::index_selection::IndexScanType;
    use document::NormalValue;
    use schema::{FieldDescription, FieldKind};
    use serde_json::json;

    fn make_test_collection() -> CollectionVersion {
        CollectionVersion::new(
            "users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        )
    }

    fn make_test_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "name");
        m.add(2, "age");
        m
    }

    fn make_test_docs() -> Vec<Doc> {
        vec![
            Doc::with_fields(vec![
                Some(json!("doc1")),
                Some(json!("Alice")),
                Some(json!(30)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc2")),
                Some(json!("Bob")),
                Some(json!(25)),
            ]),
        ]
    }

    fn make_index_params() -> IndexScanParams {
        IndexScanParams {
            index_name: "name_idx".to_string(),
            scan_type: IndexScanType::ExactMatch {
                values: vec![NormalValue::String("Alice".to_string())],
            },
        }
    }

    #[tokio::test]
    async fn test_index_scan_all_docs() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs();
        let params = make_index_params();

        let mut scan = IndexScanNode::new(collection, mapping, params).with_docs(docs);
        scan.init().await.unwrap();
        scan.start().await.unwrap();

        let mut count = 0;
        while scan.next().await.unwrap() {
            count += 1;
        }

        assert_eq!(count, 2);
        scan.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_index_scan_with_residual_filter() {
        use std::collections::HashMap;

        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs();
        let params = make_index_params();

        // Residual filter for age >= 28
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gte": 28}))]));

        let mut scan = IndexScanNode::new(collection, mapping, params)
            .with_docs(docs)
            .with_residual_filter(filter);

        scan.init().await.unwrap();
        scan.start().await.unwrap();

        let mut results = Vec::new();
        while scan.next().await.unwrap() {
            results.push(scan.value().doc_id().map(String::from));
        }

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], Some("doc1".to_string())); // Alice, age 30
    }

    #[tokio::test]
    async fn test_index_scan_skip_deleted() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let params = make_index_params();

        let mut docs = make_test_docs();
        docs[1].mark_deleted(); // Bob is deleted

        let mut scan = IndexScanNode::new(collection, mapping, params).with_docs(docs);
        scan.init().await.unwrap();
        scan.start().await.unwrap();

        let mut count = 0;
        while scan.next().await.unwrap() {
            count += 1;
        }

        assert_eq!(count, 1); // Only Alice
    }

    #[tokio::test]
    async fn test_index_scan_kind() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let params = make_index_params();

        let scan = IndexScanNode::new(collection, mapping, params);
        assert_eq!(scan.kind(), "indexScanNode");
    }

    #[tokio::test]
    async fn test_index_scan_index_name() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let params = make_index_params();

        let scan = IndexScanNode::new(collection, mapping, params);
        assert_eq!(scan.index_name(), "name_idx");
    }

    #[tokio::test]
    async fn test_index_scan_next_before_init_errors() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let params = make_index_params();
        let docs = make_test_docs();

        let mut scan = IndexScanNode::new(collection, mapping, params).with_docs(docs);
        // Intentionally not calling init()

        let result = scan.next().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("called before init"));
    }
}
