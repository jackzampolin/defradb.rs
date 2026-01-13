//! ScanNode for scanning collection documents

use async_trait::async_trait;

use schema::CollectionVersion;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::Filter;
use crate::planner::{Doc, PlanNode};

/// ScanNode scans documents from a collection.
///
/// This is the primary data source node in query plans.
/// It reads documents from storage and yields them to parent nodes.
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

    /// Set documents directly (for testing or in-memory operations)
    pub fn with_docs(mut self, docs: Vec<Doc>) -> Self {
        self.docs = docs;
        self
    }

    /// Get the collection
    pub fn collection(&self) -> &CollectionVersion {
        &self.collection
    }
}

#[async_trait]
impl PlanNode for ScanNode {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{CollectionVersion, FieldDescription, FieldKind};
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
            Doc::with_fields(vec![
                Some(json!("doc3")),
                Some(json!("Charlie")),
                Some(json!(35)),
            ]),
        ]
    }

    #[tokio::test]
    async fn test_scan_all_docs() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs();

        let mut scan = ScanNode::new(collection, mapping).with_docs(docs);
        scan.init().await.unwrap();
        scan.start().await.unwrap();

        let mut count = 0;
        while scan.next().await.unwrap() {
            count += 1;
        }

        assert_eq!(count, 3);
        scan.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_scan_with_filter() {
        use std::collections::HashMap;

        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs();

        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gte": 30}))]));

        let mut scan = ScanNode::new(collection, mapping)
            .with_docs(docs)
            .with_filter(filter);

        scan.init().await.unwrap();
        scan.start().await.unwrap();

        let mut results = Vec::new();
        while scan.next().await.unwrap() {
            results.push(scan.value().doc_id().map(String::from));
        }

        assert_eq!(results.len(), 2);
        assert!(results.contains(&Some("doc1".to_string()))); // Alice, age 30
        assert!(results.contains(&Some("doc3".to_string()))); // Charlie, age 35
    }

    #[tokio::test]
    async fn test_scan_skip_deleted() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let mut docs = make_test_docs();
        docs[1].mark_deleted(); // Bob is deleted

        let mut scan = ScanNode::new(collection, mapping).with_docs(docs);
        scan.init().await.unwrap();
        scan.start().await.unwrap();

        let mut count = 0;
        while scan.next().await.unwrap() {
            count += 1;
        }

        assert_eq!(count, 2); // Alice and Charlie only
    }

    #[tokio::test]
    async fn test_scan_show_deleted() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let mut docs = make_test_docs();
        docs[1].mark_deleted();

        let mut scan = ScanNode::new(collection, mapping)
            .with_docs(docs)
            .with_show_deleted(true);

        scan.init().await.unwrap();
        scan.start().await.unwrap();

        let mut count = 0;
        while scan.next().await.unwrap() {
            count += 1;
        }

        assert_eq!(count, 3); // All three including deleted
    }

    #[tokio::test]
    async fn test_scan_next_before_init_errors() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs();

        let mut scan = ScanNode::new(collection, mapping).with_docs(docs);
        // Intentionally not calling init()

        let result = scan.next().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("called before init"));
    }

    #[tokio::test]
    async fn test_scan_filter_error_propagation() {
        use std::collections::HashMap;

        let collection = make_test_collection();
        let mapping = make_test_mapping();

        // Create docs with null age field
        let docs = vec![Doc::with_fields(vec![
            Some(json!("doc1")),
            Some(json!("Alice")),
            None, // age is null
        ])];

        // Filter with _gt on null will error
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gt": 25}))]));

        let mut scan = ScanNode::new(collection, mapping)
            .with_docs(docs)
            .with_filter(filter);

        scan.init().await.unwrap();
        scan.start().await.unwrap();

        let result = scan.next().await;
        assert!(
            result.is_err(),
            "Filter error should propagate through scan"
        );
    }
}
