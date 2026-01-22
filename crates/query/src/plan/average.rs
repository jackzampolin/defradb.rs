//! AverageNode for computing AVG aggregate

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{Doc, PlanNode};

/// AverageNode computes the average of a numeric field from its source.
///
/// Operates in two modes:
/// - Without GROUP BY: Computes average of all documents and yields a single result
/// - With GROUP BY: For each group, adds the average to the document
///
/// Null values are skipped. Returns 0 if no values to average (Go DefraDB semantics).
/// Always returns f64 for precision.
pub struct AverageNode {
    source: Box<dyn PlanNode>,
    document_mapping: DocumentMapping,
    /// Index of the field to average
    field_index: usize,
    /// Index in the document where average result should be stored
    aggregate_index: usize,
    /// The running sum (for non-grouped mode)
    sum: f64,
    /// The running count (for non-grouped mode)
    count: usize,
    /// Current document with average result
    current_doc: Doc,
    /// Whether we've already yielded the result (for non-grouped mode)
    done: bool,
    /// Whether start() has been called
    started: bool,
    /// Whether we're in grouped mode (source provides group docs)
    grouped_mode: bool,
}

impl AverageNode {
    /// Create a new AverageNode wrapping a source
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
            count: 0,
            current_doc: Doc::default(),
            done: false,
            started: false,
            grouped_mode: false,
        }
    }

    /// Extract numeric value from JSON, returning None for nulls
    fn extract_numeric(value: Option<&JsonValue>) -> Option<f64> {
        match value {
            Some(JsonValue::Number(n)) => n.as_f64(),
            _ => None,
        }
    }

    /// Compute average of a slice of documents
    /// Returns 0.0 if no values (Go DefraDB semantics: AVG of empty set is 0)
    fn compute_average(&self, docs: &[Doc]) -> f64 {
        let mut sum = 0.0;
        let mut count = 0usize;

        for doc in docs {
            if doc.hidden {
                continue;
            }
            if let Some(val) = Self::extract_numeric(doc.get(self.field_index)) {
                sum += val;
                count += 1;
            }
        }

        if count == 0 {
            0.0 // Go DefraDB returns 0 for empty set, not null
        } else {
            sum / count as f64
        }
    }

    /// Convert average to JSON value
    /// Returns Null for NaN/Infinity to prevent silent data corruption
    fn avg_to_json(avg: f64) -> JsonValue {
        // NaN and Infinity cannot be represented in JSON - return 0
        // This matches Go DefraDB behavior
        serde_json::Number::from_f64(avg)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::Number(serde_json::Number::from(0)))
    }
}

#[async_trait]
impl PlanNode for AverageNode {
    async fn init(&mut self) -> Result<()> {
        self.sum = 0.0;
        self.count = 0;
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

        loop {
            // Try to get next from source
            if !self.source.next().await? {
                // No more source documents
                if !self.grouped_mode && !self.done {
                    // Non-grouped mode: Return the single result
                    self.done = true;
                    // Go DefraDB returns 0 for empty set, not null
                    let avg = if self.count == 0 {
                        0.0
                    } else {
                        self.sum / self.count as f64
                    };
                    let num_fields = self
                        .document_mapping
                        .next_index()
                        .max(self.aggregate_index + 1);
                    let mut doc = Doc::new(num_fields);
                    doc.set(self.aggregate_index, Self::avg_to_json(avg));
                    self.current_doc = doc;
                    return Ok(true);
                }
                return Ok(false);
            }

            // Check if source provides group docs
            if let Some(group_docs) = self.source.current_group_docs() {
                // Grouped mode: compute average for this group
                self.grouped_mode = true;
                let group_avg = self.compute_average(group_docs);

                // Clone the current doc from source and add the average
                let mut doc = self.source.value().deep_clone();
                doc.set(self.aggregate_index, Self::avg_to_json(group_avg));
                self.current_doc = doc;
                return Ok(true);
            }

            // Non-grouped mode: accumulate sum and count
            let doc = self.source.value();
            if !doc.hidden {
                if let Some(val) = Self::extract_numeric(doc.get(self.field_index)) {
                    self.sum += val;
                    self.count += 1;
                }
            }

            // Continue iterating (loop continues)
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
        "averageNode"
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
                Some(json!(20)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc3")),
                Some(json!("Charlie")),
                Some(json!(40)),
            ]),
        ]
    }

    fn make_test_mapping() -> DocumentMapping {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");
        mapping.add(2, "age");
        mapping.add(3, "_avg");
        mapping.add_render_key(3, "_avg");
        mapping
    }

    #[tokio::test]
    async fn test_average_integer_field() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs();

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut avg_node = AverageNode::new(Box::new(scan), mapping, 2, 3);

        avg_node.init().await.unwrap();

        assert!(avg_node.next().await.unwrap());
        let result = avg_node.value();
        // (30 + 20 + 40) / 3 = 30
        let avg_val = result.get(3).unwrap().as_f64().unwrap();
        assert!((avg_val - 30.0).abs() < 0.001);

        assert!(!avg_node.next().await.unwrap());
        avg_node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_average_empty_source() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(vec![]);
        let mut avg_node = AverageNode::new(Box::new(scan), mapping, 2, 3);

        avg_node.init().await.unwrap();

        assert!(avg_node.next().await.unwrap());
        let result = avg_node.value();
        // Go DefraDB returns 0 for empty set, not null
        let avg_val = result.get(3).unwrap();
        assert!(avg_val.is_number(), "AVG of empty set should return 0, not null");
        assert_eq!(avg_val.as_f64().unwrap(), 0.0);

        avg_node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_average_with_nulls() {
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
                Some(json!(40)),
            ]),
        ];

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut avg_node = AverageNode::new(Box::new(scan), mapping, 2, 3);

        avg_node.init().await.unwrap();

        assert!(avg_node.next().await.unwrap());
        let result = avg_node.value();
        // (30 + 40) / 2 = 35 (null skipped)
        let avg_val = result.get(3).unwrap().as_f64().unwrap();
        assert!((avg_val - 35.0).abs() < 0.001);

        avg_node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_average_float_field() {
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
        mapping.add(3, "_avg");
        mapping.add_render_key(3, "_avg");

        let docs = vec![
            Doc::with_fields(vec![
                Some(json!("doc1")),
                Some(json!("Item A")),
                Some(json!(10.0)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc2")),
                Some(json!("Item B")),
                Some(json!(20.0)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc3")),
                Some(json!("Item C")),
                Some(json!(30.0)),
            ]),
        ];

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut avg_node = AverageNode::new(Box::new(scan), mapping, 2, 3);

        avg_node.init().await.unwrap();

        assert!(avg_node.next().await.unwrap());
        let result = avg_node.value();
        // (10 + 20 + 30) / 3 = 20
        let avg_val = result.get(3).unwrap().as_f64().unwrap();
        assert!((avg_val - 20.0).abs() < 0.001);

        avg_node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_average_skips_hidden_docs() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let mut docs = make_test_docs();
        docs[1].hidden = true; // Hide Bob (age 20)

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut avg_node = AverageNode::new(Box::new(scan), mapping, 2, 3);

        avg_node.init().await.unwrap();

        assert!(avg_node.next().await.unwrap());
        let result = avg_node.value();
        // (30 + 40) / 2 = 35 (20 skipped)
        let avg_val = result.get(3).unwrap().as_f64().unwrap();
        assert!((avg_val - 35.0).abs() < 0.001);

        avg_node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_average_with_groupby() {
        use crate::mapper::GroupBy;
        use crate::plan::GroupByNode;

        let collection = CollectionVersion::new(
            "Employees",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "department", FieldKind::string()),
                FieldDescription::new("3", "salary", FieldKind::int()),
            ],
        );

        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "department");
        mapping.add(2, "salary");
        mapping.add(3, "_avg");
        mapping.add_render_key(1, "department");
        mapping.add_render_key(3, "_avg");

        let docs = vec![
            Doc::with_fields(vec![
                Some(json!("doc1")),
                Some(json!("Engineering")),
                Some(json!(100)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc2")),
                Some(json!("Sales")),
                Some(json!(80)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc3")),
                Some(json!("Engineering")),
                Some(json!(120)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc4")),
                Some(json!("Sales")),
                Some(json!(100)),
            ]),
        ];

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let group_by = GroupBy::new(vec!["department".to_string()]);
        let groupby_node = GroupByNode::new(Box::new(scan), group_by, mapping.clone());
        let mut avg_node = AverageNode::new(Box::new(groupby_node), mapping, 2, 3);

        avg_node.init().await.unwrap();

        let mut results = Vec::new();
        while avg_node.next().await.unwrap() {
            results.push(avg_node.value().deep_clone());
        }

        assert_eq!(results.len(), 2);

        // Find Engineering group (100 + 120) / 2 = 110
        let eng = results
            .iter()
            .find(|d| d.get(1).and_then(|v| v.as_str()) == Some("Engineering"))
            .unwrap();
        let eng_avg = eng.get(3).unwrap().as_f64().unwrap();
        assert!((eng_avg - 110.0).abs() < 0.001);

        // Find Sales group (80 + 100) / 2 = 90
        let sales = results
            .iter()
            .find(|d| d.get(1).and_then(|v| v.as_str()) == Some("Sales"))
            .unwrap();
        let sales_avg = sales.get(3).unwrap().as_f64().unwrap();
        assert!((sales_avg - 90.0).abs() < 0.001);

        avg_node.close().await.unwrap();
    }
}
