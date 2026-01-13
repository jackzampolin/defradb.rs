//! SelectNode for selecting fields from documents

use async_trait::async_trait;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::Filter;
use crate::planner::{Doc, PlanNode};

/// SelectNode selects specific fields from documents.
///
/// This node wraps another plan node and applies field selection,
/// optional filtering, and prepares documents for rendering.
pub struct SelectNode {
    /// Source plan node
    source: Box<dyn PlanNode>,
    /// Document mapping for this select
    document_mapping: DocumentMapping,
    /// Optional additional filter
    filter: Option<Filter>,
    /// Current document
    current_doc: Doc,
}

impl SelectNode {
    /// Create a new select node wrapping a source
    pub fn new(source: Box<dyn PlanNode>, document_mapping: DocumentMapping) -> Self {
        Self {
            source,
            document_mapping,
            filter: None,
            current_doc: Doc::default(),
        }
    }

    /// Set an additional filter
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }
}

#[async_trait]
impl PlanNode for SelectNode {
    async fn init(&mut self) -> Result<()> {
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        loop {
            if !self.source.next().await? {
                return Ok(false);
            }

            let doc = self.source.value();

            // Apply filter if present
            if let Some(ref filter) = self.filter {
                if !filter.matches(doc.fields(), &self.document_mapping)? {
                    continue;
                }
            }

            // Copy the document (field projection happens at render time)
            self.current_doc = doc.deep_clone();
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
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "selectNode"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::RenderKey;
    use crate::plan::ScanNode;
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use serde_json::json;
    use std::collections::HashMap;

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
        // Add render keys for name and age only
        m.render_keys.push(RenderKey::new(1, "name"));
        m.render_keys.push(RenderKey::new(2, "age"));
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

    #[tokio::test]
    async fn test_select_passthrough() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs();

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut select = SelectNode::new(Box::new(scan), mapping);

        select.init().await.unwrap();
        select.start().await.unwrap();

        let mut count = 0;
        while select.next().await.unwrap() {
            count += 1;
        }

        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_select_with_filter() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs();

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_eq": "Alice"}),
        )]));

        let mut select = SelectNode::new(Box::new(scan), mapping).with_filter(filter);

        select.init().await.unwrap();
        select.start().await.unwrap();

        let mut results = Vec::new();
        while select.next().await.unwrap() {
            results.push(select.value().doc_id().map(String::from));
        }

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], Some("doc1".to_string()));
    }

    #[tokio::test]
    async fn test_select_source_error_propagation() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        // Create docs with null age field
        let docs = vec![Doc::with_fields(vec![
            Some(json!("doc1")),
            Some(json!("Alice")),
            None, // age is null
        ])];

        // Add filter on scan that will error
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gt": 25}))]));

        let scan = ScanNode::new(collection, mapping.clone())
            .with_docs(docs)
            .with_filter(filter);

        let mut select = SelectNode::new(Box::new(scan), mapping);

        select.init().await.unwrap();
        select.start().await.unwrap();

        let result = select.next().await;
        assert!(
            result.is_err(),
            "Source error should propagate through select"
        );
    }
}
