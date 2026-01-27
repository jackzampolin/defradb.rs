//! AverageNode for computing AVG aggregate

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::{Filter, Limit};
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
    /// Optional filter to apply to group documents before averaging
    aggregate_filter: Option<Filter>,
    /// Optional limit/offset to apply to group documents before averaging
    aggregate_limit: Option<Limit>,
    /// If set, operate in "child aggregate" mode: read values from _group JSON array.
    child_aggregate_source: Option<(usize, String)>,
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
            aggregate_filter: None,
            aggregate_limit: None,
            child_aggregate_source: None,
        }
    }

    pub fn with_child_aggregate_source(mut self, group_index: usize, field_name: String) -> Self {
        self.child_aggregate_source = Some((group_index, field_name));
        self
    }

    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.aggregate_filter = Some(filter);
        self
    }

    pub fn with_limit(mut self, limit: Limit) -> Self {
        self.aggregate_limit = Some(limit);
        self
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

        let filtered: Vec<&Doc> = docs
            .iter()
            .filter(|d| !d.hidden)
            .filter(|d| {
                if let Some(ref filter) = self.aggregate_filter {
                    filter
                        .matches(d.fields(), &self.document_mapping)
                        .unwrap_or(false)
                } else {
                    true
                }
            })
            .collect();

        let limited: Box<dyn Iterator<Item = &&Doc>> = if let Some(ref limit) = self.aggregate_limit
        {
            let offset = limit.offset as usize;
            let effective_limit = limit.limit.map(|l| l as usize);
            match (effective_limit, offset) {
                (Some(0), _) => Box::new(filtered.iter()),
                (Some(l), o) => Box::new(filtered.iter().skip(o).take(l)),
                (None, o) if o > 0 => Box::new(filtered.iter().skip(o)),
                _ => Box::new(filtered.iter()),
            }
        } else {
            Box::new(filtered.iter())
        };

        for doc in limited {
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

        // Child aggregate mode: read from _group JSON array on each doc
        if let Some((group_index, ref field_name)) = self.child_aggregate_source {
            if !self.source.next().await? {
                return Ok(false);
            }
            let doc = self.source.value();
            let avg = if let Some(JsonValue::Array(items)) = doc.get(group_index) {
                let mut sum = 0.0;
                let mut count = 0usize;
                for item in items {
                    if let JsonValue::Object(obj) = item {
                        if let Some(val) = obj.get(field_name.as_str()) {
                            if let Some(f) = val.as_f64() {
                                sum += f;
                                count += 1;
                            }
                        }
                    }
                }
                if count == 0 {
                    0.0
                } else {
                    sum / count as f64
                }
            } else {
                0.0
            };
            let mut new_doc = doc.deep_clone();
            new_doc.set(self.aggregate_index, Self::avg_to_json(avg));
            self.current_doc = new_doc;
            return Ok(true);
        }

        loop {
            // Try to get next from source
            if !self.source.next().await? {
                // No more source documents
                if !self.grouped_mode && !self.done {
                    if self.source.is_grouped_source() {
                        return Ok(false);
                    }
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

    fn is_grouped_source(&self) -> bool {
        self.source.is_grouped_source()
    }
}
