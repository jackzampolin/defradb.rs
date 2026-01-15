//! AllDocsNode for providing all documents as a single group
//!
//! Used to enable multiple aggregates without GROUP BY by buffering
//! all documents and making them available via `current_group_docs()`.

use async_trait::async_trait;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{Doc, PlanNode};

/// AllDocsNode buffers all documents and yields them once as a single group.
///
/// This node enables multiple aggregates to work correctly without GROUP BY:
/// - During `start()`, it buffers all documents from the source
/// - It yields a single "group" containing all documents
/// - `current_group_docs()` returns all buffered documents
///
/// Without this node, chained aggregates would each consume the previous
/// aggregate's result instead of sharing access to the original documents.
pub struct AllDocsNode {
    source: Box<dyn PlanNode>,
    document_mapping: DocumentMapping,
    /// All documents buffered from source
    docs: Vec<Doc>,
    /// Current representative document
    current_doc: Doc,
    /// Whether start() has been called
    started: bool,
    /// Whether we've yielded the single result
    done: bool,
}

impl AllDocsNode {
    /// Create a new AllDocsNode wrapping a source
    pub fn new(source: Box<dyn PlanNode>, document_mapping: DocumentMapping) -> Self {
        Self {
            source,
            document_mapping,
            docs: Vec::new(),
            current_doc: Doc::default(),
            started: false,
            done: false,
        }
    }
}

#[async_trait]
impl PlanNode for AllDocsNode {
    async fn init(&mut self) -> Result<()> {
        self.docs.clear();
        self.done = false;
        self.started = false;
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await?;
        self.started = true;

        // Buffer all documents from source
        while self.source.next().await? {
            self.docs.push(self.source.value().deep_clone());
        }

        // Set representative document (first doc or empty)
        if !self.docs.is_empty() {
            self.current_doc = self.docs[0].deep_clone();
        }

        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.started {
            self.start().await?;
        }

        if self.done {
            return Ok(false);
        }

        self.done = true;
        Ok(true)
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
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "allDocsNode"
    }

    fn current_group_docs(&self) -> Option<&[Doc]> {
        Some(&self.docs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::ScanNode;
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use serde_json::json;

    fn make_test_collection() -> CollectionVersion {
        CollectionVersion::new(
            "Users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        )
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

    fn make_test_mapping() -> DocumentMapping {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");
        mapping.add(2, "age");
        mapping
    }

    #[tokio::test]
    async fn test_alldocs_buffers_all_documents() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs();

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut node = AllDocsNode::new(Box::new(scan), mapping);

        node.init().await.unwrap();

        // Should yield exactly once
        assert!(node.next().await.unwrap());

        // current_group_docs should return all 3 docs
        let group_docs = node.current_group_docs().unwrap();
        assert_eq!(group_docs.len(), 3);

        // Should not yield again
        assert!(!node.next().await.unwrap());

        node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_alldocs_empty_source() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(vec![]);
        let mut node = AllDocsNode::new(Box::new(scan), mapping);

        node.init().await.unwrap();

        // Should yield once with empty group
        assert!(node.next().await.unwrap());

        let group_docs = node.current_group_docs().unwrap();
        assert_eq!(group_docs.len(), 0);

        assert!(!node.next().await.unwrap());

        node.close().await.unwrap();
    }
}
