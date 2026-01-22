//! LimitNode for applying limit and offset to query results

use async_trait::async_trait;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{Doc, PlanNode};

/// LimitNode applies limit and offset to query results.
///
/// This node wraps another plan node and:
/// - Skips the first `offset` documents
/// - Returns at most `limit` documents
pub struct LimitNode {
    /// Source plan node
    source: Box<dyn PlanNode>,
    /// Maximum number of documents to return (None = unlimited)
    limit: Option<u64>,
    /// Number of documents to skip
    offset: u64,
    /// Current row index (how many docs have been processed)
    row_index: u64,
    /// Number of documents returned
    docs_returned: u64,
    /// Current document
    current_doc: Doc,
}

impl LimitNode {
    /// Create a new limit node wrapping a source
    pub fn new(source: Box<dyn PlanNode>, limit: Option<u64>, offset: u64) -> Self {
        Self {
            source,
            limit,
            offset,
            row_index: 0,
            docs_returned: 0,
            current_doc: Doc::default(),
        }
    }

    /// Create a limit node with only a limit (no offset)
    pub fn limit_only(source: Box<dyn PlanNode>, limit: u64) -> Self {
        Self::new(source, Some(limit), 0)
    }

    /// Create a limit node with only an offset (no limit)
    pub fn offset_only(source: Box<dyn PlanNode>, offset: u64) -> Self {
        Self::new(source, None, offset)
    }
}

#[async_trait]
impl PlanNode for LimitNode {
    async fn init(&mut self) -> Result<()> {
        self.row_index = 0;
        self.docs_returned = 0;
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        // Check if we've already returned enough documents
        if let Some(limit) = self.limit {
            if self.docs_returned >= limit {
                return Ok(false);
            }
        }

        loop {
            // Get next document from source
            if !self.source.next().await? {
                return Ok(false);
            }

            self.row_index += 1;

            // Skip documents until we've passed the offset
            if self.row_index <= self.offset {
                continue;
            }

            // We have a document to return
            self.current_doc = self.source.value().deep_clone();
            self.docs_returned += 1;
            return Ok(true);
        }
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.source.close().await
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.source.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        self.source.document_map()
    }

    fn kind(&self) -> &'static str {
        "limitNode"
    }

    fn explain(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "node".to_string(),
            serde_json::Value::String(self.kind().to_string()),
        );

        if let Some(limit) = self.limit {
            obj.insert("limit".to_string(), serde_json::Value::Number(limit.into()));
        }

        if self.offset > 0 {
            obj.insert(
                "offset".to_string(),
                serde_json::Value::Number(self.offset.into()),
            );
        }

        // Recursively explain child node
        obj.insert("source".to_string(), self.source.explain());

        serde_json::Value::Object(obj)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::DocumentMapping;
    use crate::plan::ScanNode;
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
            ],
        )
    }

    fn make_test_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "name");
        m
    }

    fn make_test_docs(count: usize) -> Vec<Doc> {
        (0..count)
            .map(|i| {
                Doc::with_fields(vec![
                    Some(json!(format!("doc{}", i))),
                    Some(json!(format!("User{}", i))),
                ])
            })
            .collect()
    }

    #[tokio::test]
    async fn test_limit_only() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs(10);

        let scan = ScanNode::new(collection, mapping).with_docs(docs);
        let mut limit = LimitNode::limit_only(Box::new(scan), 3);

        limit.init().await.unwrap();
        limit.start().await.unwrap();

        let mut results = Vec::new();
        while limit.next().await.unwrap() {
            results.push(limit.value().doc_id().map(String::from));
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some("doc0".to_string()));
        assert_eq!(results[1], Some("doc1".to_string()));
        assert_eq!(results[2], Some("doc2".to_string()));
    }

    #[tokio::test]
    async fn test_offset_only() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs(5);

        let scan = ScanNode::new(collection, mapping).with_docs(docs);
        let mut limit = LimitNode::offset_only(Box::new(scan), 2);

        limit.init().await.unwrap();
        limit.start().await.unwrap();

        let mut results = Vec::new();
        while limit.next().await.unwrap() {
            results.push(limit.value().doc_id().map(String::from));
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some("doc2".to_string()));
        assert_eq!(results[1], Some("doc3".to_string()));
        assert_eq!(results[2], Some("doc4".to_string()));
    }

    #[tokio::test]
    async fn test_limit_and_offset() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs(10);

        let scan = ScanNode::new(collection, mapping).with_docs(docs);
        let mut limit = LimitNode::new(Box::new(scan), Some(3), 2);

        limit.init().await.unwrap();
        limit.start().await.unwrap();

        let mut results = Vec::new();
        while limit.next().await.unwrap() {
            results.push(limit.value().doc_id().map(String::from));
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some("doc2".to_string()));
        assert_eq!(results[1], Some("doc3".to_string()));
        assert_eq!(results[2], Some("doc4".to_string()));
    }

    #[tokio::test]
    async fn test_limit_exceeds_available() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs(3);

        let scan = ScanNode::new(collection, mapping).with_docs(docs);
        let mut limit = LimitNode::limit_only(Box::new(scan), 10);

        limit.init().await.unwrap();
        limit.start().await.unwrap();

        let mut count = 0;
        while limit.next().await.unwrap() {
            count += 1;
        }

        assert_eq!(count, 3); // Only 3 available
    }

    #[tokio::test]
    async fn test_offset_exceeds_available() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs(3);

        let scan = ScanNode::new(collection, mapping).with_docs(docs);
        let mut limit = LimitNode::offset_only(Box::new(scan), 10);

        limit.init().await.unwrap();
        limit.start().await.unwrap();

        let mut count = 0;
        while limit.next().await.unwrap() {
            count += 1;
        }

        assert_eq!(count, 0); // All skipped
    }

    #[tokio::test]
    async fn test_limit_zero_returns_nothing() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs(5);

        let scan = ScanNode::new(collection, mapping).with_docs(docs);
        let mut limit = LimitNode::limit_only(Box::new(scan), 0);

        limit.init().await.unwrap();
        limit.start().await.unwrap();

        // limit=0 should return no documents
        assert!(!limit.next().await.unwrap());
    }
}
