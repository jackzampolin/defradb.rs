//! SumNode for computing SUM aggregate

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{Doc, PlanNode};

/// SumNode computes the sum of a numeric field from its source.
///
/// Operates in two modes:
/// - Without GROUP BY: Sums all documents and yields a single result
/// - With GROUP BY: For each group, adds the sum to the document
///
/// Null values are skipped. Returns 0 if no values to sum.
/// Returns f64 if any values are floats, i64 if all integers.
pub struct SumNode {
    source: Box<dyn PlanNode>,
    document_mapping: DocumentMapping,
    /// Index of the field to sum
    field_index: usize,
    /// Index in the document where sum result should be stored
    aggregate_index: usize,
    /// The computed sum value as float (for non-grouped mode)
    sum: f64,
    /// Whether we've seen any float values
    has_float: bool,
    /// Current document with sum result
    current_doc: Doc,
    /// Whether we've already yielded the result (for non-grouped mode)
    done: bool,
    /// Whether start() has been called
    started: bool,
    /// Whether we're in grouped mode (source provides group docs)
    grouped_mode: bool,
}

impl SumNode {
    /// Create a new SumNode wrapping a source
    pub fn new(
        source: Box<dyn PlanNode>,
        document_mapping: DocumentMapping,
        field_index: usize,
        aggregate_index: usize,
    ) -> Self {
        Self {
            source,
            document_mapping,
            field_index,
            aggregate_index,
            sum: 0.0,
            has_float: false,
            current_doc: Doc::default(),
            done: false,
            started: false,
            grouped_mode: false,
        }
    }

    /// Extract numeric value from JSON, returning None for nulls
    fn extract_numeric(value: Option<&JsonValue>) -> Option<(f64, bool)> {
        match value {
            Some(JsonValue::Number(n)) => n
                .as_i64()
                .map(|i| (i as f64, false))
                .or_else(|| n.as_f64().map(|f| (f, true))),
            _ => None,
        }
    }

    /// Compute sum of a slice of documents
    fn compute_sum(&self, docs: &[Doc]) -> (f64, bool) {
        let mut sum = 0.0;
        let mut has_float = false;

        for doc in docs {
            if doc.hidden {
                continue;
            }
            if let Some((val, is_float)) = Self::extract_numeric(doc.get(self.field_index)) {
                sum += val;
                has_float = has_float || is_float;
            }
        }

        (sum, has_float)
    }

    /// Convert sum to JSON value (int if no floats, float otherwise)
    fn sum_to_json(sum: f64, has_float: bool) -> JsonValue {
        if has_float {
            JsonValue::Number(serde_json::Number::from_f64(sum).unwrap_or_else(|| 0.into()))
        } else {
            JsonValue::Number((sum as i64).into())
        }
    }
}

#[async_trait]
impl PlanNode for SumNode {
    async fn init(&mut self) -> Result<()> {
        self.sum = 0.0;
        self.has_float = false;
        self.done = false;
        self.started = false;
        self.grouped_mode = false;
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await?;
        self.started = true;
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
                // Non-grouped mode: Return the single result
                self.done = true;
                let num_fields = self
                    .document_mapping
                    .next_index()
                    .max(self.aggregate_index + 1);
                let mut doc = Doc::new(num_fields);
                doc.set(
                    self.aggregate_index,
                    Self::sum_to_json(self.sum, self.has_float),
                );
                self.current_doc = doc;
                return Ok(true);
            }
            return Ok(false);
        }

        // Check if source provides group docs
        if let Some(group_docs) = self.source.current_group_docs() {
            // Grouped mode: sum field values in this group
            self.grouped_mode = true;
            let (group_sum, group_has_float) = self.compute_sum(group_docs);

            // Clone the current doc from source and add the sum
            let mut doc = self.source.value().deep_clone();
            if doc.num_fields() <= self.aggregate_index {
                doc.set(self.aggregate_index, JsonValue::Null);
            }
            doc.set(
                self.aggregate_index,
                Self::sum_to_json(group_sum, group_has_float),
            );
            self.current_doc = doc;
            return Ok(true);
        }

        // Non-grouped mode: accumulate sum
        let doc = self.source.value();
        if !doc.hidden {
            if let Some((val, is_float)) = Self::extract_numeric(doc.get(self.field_index)) {
                self.sum += val;
                self.has_float = self.has_float || is_float;
            }
        }

        // Continue iterating to sum all docs
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
        "sumNode"
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
        mapping.add(3, "_sum");
        mapping.add_render_key(3, "_sum");
        mapping
    }

    #[tokio::test]
    async fn test_sum_integer_field() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs();

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        // field_index=2 is "age", aggregate_index=3 is where result goes
        let mut sum_node = SumNode::new(Box::new(scan), mapping, 2, 3);

        sum_node.init().await.unwrap();

        assert!(sum_node.next().await.unwrap());
        let result = sum_node.value();
        // 30 + 25 + 35 = 90
        assert_eq!(result.get(3), Some(&json!(90)));

        assert!(!sum_node.next().await.unwrap());
        sum_node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sum_empty_source() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(vec![]);
        let mut sum_node = SumNode::new(Box::new(scan), mapping, 2, 3);

        sum_node.init().await.unwrap();

        assert!(sum_node.next().await.unwrap());
        let result = sum_node.value();
        assert_eq!(result.get(3), Some(&json!(0)));

        sum_node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sum_with_nulls() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let docs = vec![
            Doc::with_fields(vec![
                Some(json!("doc1")),
                Some(json!("Alice")),
                Some(json!(30)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc2")),
                Some(json!("Bob")),
                None, // null age
            ]),
            Doc::with_fields(vec![
                Some(json!("doc3")),
                Some(json!("Charlie")),
                Some(json!(35)),
            ]),
        ];

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut sum_node = SumNode::new(Box::new(scan), mapping, 2, 3);

        sum_node.init().await.unwrap();

        assert!(sum_node.next().await.unwrap());
        let result = sum_node.value();
        // 30 + 35 = 65 (null skipped)
        assert_eq!(result.get(3), Some(&json!(65)));

        sum_node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sum_float_field() {
        let collection = CollectionVersion::new(
            "Products",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "price", FieldKind::float64()),
            ],
        );

        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");
        mapping.add(2, "price");
        mapping.add(3, "_sum");
        mapping.add_render_key(3, "_sum");

        let docs = vec![
            Doc::with_fields(vec![
                Some(json!("doc1")),
                Some(json!("Item A")),
                Some(json!(10.5)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc2")),
                Some(json!("Item B")),
                Some(json!(20.25)),
            ]),
        ];

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut sum_node = SumNode::new(Box::new(scan), mapping, 2, 3);

        sum_node.init().await.unwrap();

        assert!(sum_node.next().await.unwrap());
        let result = sum_node.value();
        // 10.5 + 20.25 = 30.75
        let sum_val = result.get(3).unwrap().as_f64().unwrap();
        assert!((sum_val - 30.75).abs() < 0.001);

        sum_node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sum_skips_hidden_docs() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let mut docs = make_test_docs();
        docs[1].hidden = true; // Hide Bob (age 25)

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut sum_node = SumNode::new(Box::new(scan), mapping, 2, 3);

        sum_node.init().await.unwrap();

        assert!(sum_node.next().await.unwrap());
        let result = sum_node.value();
        // 30 + 35 = 65 (25 skipped)
        assert_eq!(result.get(3), Some(&json!(65)));

        sum_node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sum_with_groupby() {
        use crate::mapper::GroupBy;
        use crate::plan::GroupByNode;

        let collection = CollectionVersion::new(
            "Sales",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "department", FieldKind::string()),
                FieldDescription::new("3", "amount", FieldKind::int()),
            ],
        );

        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "department");
        mapping.add(2, "amount");
        mapping.add(3, "_sum");
        mapping.add_render_key(1, "department");
        mapping.add_render_key(3, "_sum");

        let docs = vec![
            Doc::with_fields(vec![
                Some(json!("doc1")),
                Some(json!("Engineering")),
                Some(json!(100)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc2")),
                Some(json!("Sales")),
                Some(json!(200)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc3")),
                Some(json!("Engineering")),
                Some(json!(150)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc4")),
                Some(json!("Sales")),
                Some(json!(300)),
            ]),
        ];

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let group_by = GroupBy::new(vec!["department".to_string()]);
        let groupby_node = GroupByNode::new(Box::new(scan), group_by, mapping.clone());
        let mut sum_node = SumNode::new(Box::new(groupby_node), mapping, 2, 3);

        sum_node.init().await.unwrap();

        let mut results = Vec::new();
        while sum_node.next().await.unwrap() {
            results.push(sum_node.value().deep_clone());
        }

        assert_eq!(results.len(), 2);

        // Find Engineering group (100 + 150 = 250)
        let eng = results
            .iter()
            .find(|d| d.get(1).and_then(|v| v.as_str()) == Some("Engineering"))
            .unwrap();
        assert_eq!(eng.get(3), Some(&json!(250)));

        // Find Sales group (200 + 300 = 500)
        let sales = results
            .iter()
            .find(|d| d.get(1).and_then(|v| v.as_str()) == Some("Sales"))
            .unwrap();
        assert_eq!(sales.get(3), Some(&json!(500)));

        sum_node.close().await.unwrap();
    }
}
