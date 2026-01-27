//! MaxNode for computing MAX aggregate

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{Doc, PlanNode};

/// MaxNode computes the maximum of a numeric field from its source.
///
/// Operates in two modes:
/// - Without GROUP BY: Finds max of all documents and yields a single result
/// - With GROUP BY: For each group, adds the max to the document
///
/// Null values are skipped. Returns null if no values found.
pub struct MaxNode {
    source: Box<dyn PlanNode>,
    document_mapping: DocumentMapping,
    /// Index of the field to find max
    field_index: usize,
    /// Index in the document where max result should be stored
    aggregate_index: usize,
    /// The current maximum value (for non-grouped mode)
    max: Option<f64>,
    /// Whether we've seen any float values
    has_float: bool,
    /// Current document with max result
    current_doc: Doc,
    /// Whether we've already yielded the result (for non-grouped mode)
    done: bool,
    /// Whether start() has been called
    started: bool,
    /// Whether we're in grouped mode (source provides group docs)
    grouped_mode: bool,
}

impl MaxNode {
    /// Create a new MaxNode wrapping a source
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
            max: None,
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

    /// Compute max of a slice of documents
    fn compute_max(&self, docs: &[Doc]) -> (Option<f64>, bool) {
        let mut max: Option<f64> = None;
        let mut has_float = false;

        for doc in docs {
            if doc.hidden {
                continue;
            }
            if let Some((val, is_float)) = Self::extract_numeric(doc.get(self.field_index)) {
                max = Some(match max {
                    None => val,
                    Some(current) => current.max(val),
                });
                has_float = has_float || is_float;
            }
        }

        (max, has_float)
    }

    /// Convert max to JSON value
    /// Returns Null for NaN/Infinity to prevent silent data corruption
    fn max_to_json(max: Option<f64>, has_float: bool) -> JsonValue {
        match max {
            None => JsonValue::Null,
            Some(val) if has_float => {
                // NaN and Infinity cannot be represented in JSON - return null
                serde_json::Number::from_f64(val)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null)
            }
            Some(val) => JsonValue::Number((val as i64).into()),
        }
    }
}

#[async_trait]
impl PlanNode for MaxNode {
    async fn init(&mut self) -> Result<()> {
        self.max = None;
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
                        Self::max_to_json(self.max, self.has_float),
                    );
                    self.current_doc = doc;
                    return Ok(true);
                }
                return Ok(false);
            }

            // Check if source provides group docs
            if let Some(group_docs) = self.source.current_group_docs() {
                // Grouped mode: find max in this group
                self.grouped_mode = true;
                let (group_max, group_has_float) = self.compute_max(group_docs);

                // Clone the current doc from source and add the max
                let mut doc = self.source.value().deep_clone();
                if doc.num_fields() <= self.aggregate_index {
                    doc.set(self.aggregate_index, JsonValue::Null);
                }
                doc.set(
                    self.aggregate_index,
                    Self::max_to_json(group_max, group_has_float),
                );
                self.current_doc = doc;
                return Ok(true);
            }

            // Non-grouped mode: track maximum
            let doc = self.source.value();
            if !doc.hidden {
                if let Some((val, is_float)) = Self::extract_numeric(doc.get(self.field_index)) {
                    self.max = Some(match self.max {
                        None => val,
                        Some(current) => current.max(val),
                    });
                    self.has_float = self.has_float || is_float;
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
        "maxNode"
    }

    fn current_group_docs(&self) -> Option<&[Doc]> {
        // Pass through from source for stacked aggregates
        self.source.current_group_docs()
    }
}
