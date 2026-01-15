//! CountNode for computing COUNT aggregate

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{Doc, PlanNode};

/// CountNode computes the count of documents from its source.
///
/// Operates in two modes:
/// - Without GROUP BY: Counts all documents and yields a single result
/// - With GROUP BY: For each group, adds the group count to the document
///
/// When the source is a GroupByNode, CountNode operates in pass-through mode:
/// it iterates through groups and adds the count for each group's documents.
pub struct CountNode {
    source: Box<dyn PlanNode>,
    document_mapping: DocumentMapping,
    /// Index in the document where count result should be stored
    aggregate_index: usize,
    /// The computed count value (for non-grouped mode)
    count: i64,
    /// Current document with count result
    current_doc: Doc,
    /// Whether we've already yielded the result (for non-grouped mode)
    done: bool,
    /// Whether start() has been called
    started: bool,
    /// Whether we're in grouped mode (source provides group docs)
    grouped_mode: bool,
}

impl CountNode {
    /// Create a new CountNode wrapping a source
    pub fn new(
        source: Box<dyn PlanNode>,
        document_mapping: DocumentMapping,
        aggregate_index: usize,
    ) -> Self {
        Self {
            source,
            document_mapping,
            aggregate_index,
            count: 0,
            current_doc: Doc::default(),
            done: false,
            started: false,
            grouped_mode: false,
        }
    }
}

#[async_trait]
impl PlanNode for CountNode {
    async fn init(&mut self) -> Result<()> {
        self.count = 0;
        self.done = false;
        self.started = false;
        self.grouped_mode = false;
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await?;
        self.started = true;

        // Check if we're in grouped mode by testing if source provides group docs
        // We can't detect this until we call next() on the source, so we'll check later
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.started {
            self.start().await?;
        }

        // Try to get next from source
        if !self.source.next().await? {
            // No more source documents
            if !self.grouped_mode && !self.done {
                // Non-grouped mode: We counted during iteration, return the single result
                self.done = true;
                let num_fields = self
                    .document_mapping
                    .next_index()
                    .max(self.aggregate_index + 1);
                let mut doc = Doc::new(num_fields);
                doc.set(self.aggregate_index, JsonValue::Number(self.count.into()));
                self.current_doc = doc;
                return Ok(true);
            }
            return Ok(false);
        }

        // Check if source provides group docs
        if let Some(group_docs) = self.source.current_group_docs() {
            // Grouped mode: count docs in this group
            self.grouped_mode = true;
            let group_count = group_docs.iter().filter(|d| !d.hidden).count() as i64;

            // Clone the current doc from source and add the count
            let mut doc = self.source.value().deep_clone();
            // Ensure doc has enough fields
            if doc.num_fields() <= self.aggregate_index {
                doc.set(self.aggregate_index, JsonValue::Null);
            }
            doc.set(self.aggregate_index, JsonValue::Number(group_count.into()));
            self.current_doc = doc;
            return Ok(true);
        }

        // Non-grouped mode: count this doc
        let doc = self.source.value();
        if !doc.hidden {
            self.count += 1;
        }

        // Continue iterating to count all docs
        // We'll return the result after all docs are counted (when source returns false)
        Box::pin(self.next()).await
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
        "countNode"
    }

    fn current_group_docs(&self) -> Option<&[Doc]> {
        // Pass through from source for stacked aggregates
        self.source.current_group_docs()
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
        mapping.add(3, "_count");
        mapping.add_render_key(3, "_count");
        mapping
    }

    #[tokio::test]
    async fn test_count_all_documents() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs();

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut count_node = CountNode::new(Box::new(scan), mapping, 3);

        count_node.init().await.unwrap();

        // Should return exactly one result
        assert!(count_node.next().await.unwrap());
        let result = count_node.value();
        assert_eq!(result.get(3), Some(&json!(3)));

        // Should return false on second call
        assert!(!count_node.next().await.unwrap());

        count_node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_count_empty_source() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(vec![]);
        let mut count_node = CountNode::new(Box::new(scan), mapping, 3);

        count_node.init().await.unwrap();

        assert!(count_node.next().await.unwrap());
        let result = count_node.value();
        assert_eq!(result.get(3), Some(&json!(0)));

        count_node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_count_skips_hidden_docs() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        // Create docs with one hidden
        let mut docs = make_test_docs();
        docs[1].hidden = true;

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut count_node = CountNode::new(Box::new(scan), mapping, 3);

        count_node.init().await.unwrap();

        assert!(count_node.next().await.unwrap());
        let result = count_node.value();
        assert_eq!(result.get(3), Some(&json!(2))); // 3 docs - 1 hidden = 2

        count_node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_count_with_groupby() {
        use crate::mapper::GroupBy;
        use crate::plan::GroupByNode;

        let collection = CollectionVersion::new(
            "Users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "department", FieldKind::string()),
            ],
        );

        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");
        mapping.add(2, "department");
        mapping.add(3, "_count");
        mapping.add_render_key(0, "_docID");
        mapping.add_render_key(1, "name");
        mapping.add_render_key(2, "department");
        mapping.add_render_key(3, "_count");

        let docs = vec![
            Doc::with_fields(vec![
                Some(json!("doc1")),
                Some(json!("Alice")),
                Some(json!("Engineering")),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc2")),
                Some(json!("Bob")),
                Some(json!("Sales")),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc3")),
                Some(json!("Charlie")),
                Some(json!("Engineering")),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc4")),
                Some(json!("Diana")),
                Some(json!("Sales")),
            ]),
        ];

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let group_by = GroupBy::new(vec!["department".to_string()]);
        let groupby_node = GroupByNode::new(Box::new(scan), group_by, mapping.clone());
        let mut count_node = CountNode::new(Box::new(groupby_node), mapping, 3);

        count_node.init().await.unwrap();

        // Should get 2 groups
        let mut results = Vec::new();
        while count_node.next().await.unwrap() {
            results.push(count_node.value().deep_clone());
        }

        assert_eq!(results.len(), 2);

        // Each group should have count 2
        for result in &results {
            assert_eq!(result.get(3), Some(&json!(2)));
        }

        count_node.close().await.unwrap();
    }
}
