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
    /// Returns Null for NaN/Infinity to prevent silent data corruption
    fn sum_to_json(sum: f64, has_float: bool) -> JsonValue {
        if has_float {
            // NaN and Infinity cannot be represented in JSON - return null
            serde_json::Number::from_f64(sum)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
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

        loop {
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

            // Continue iterating to sum all docs (loop continues)
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
        "sumNode"
    }

    fn current_group_docs(&self) -> Option<&[Doc]> {
        // Pass through from source for stacked aggregates
        self.source.current_group_docs()
    }
}
