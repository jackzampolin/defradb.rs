//! SumNode for computing SUM aggregate

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::{Filter, Limit};
use crate::planner::{Doc, ExecInfo, PlanNode};

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
    /// Optional filter to apply to group documents before summing
    aggregate_filter: Option<Filter>,
    /// Optional limit/offset to apply to group documents before summing
    aggregate_limit: Option<Limit>,
    /// If set, operate in "child aggregate" mode: read values from _group JSON array.
    child_aggregate_source: Option<(usize, String)>,
    /// Execution statistics for explain execute mode
    exec_info: ExecInfo,
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
            aggregate_filter: None,
            aggregate_limit: None,
            child_aggregate_source: None,
            exec_info: ExecInfo::default(),
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for SumNode {
    async fn init(&mut self) -> Result<()> {
        self.sum = 0.0;
        self.has_float = false;
        self.done = false;
        self.started = false;
        self.grouped_mode = false;
        self.exec_info = ExecInfo::default();
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

        // Track iterations (Go counts each call to next)
        self.exec_info.iterations += 1;

        // Child aggregate mode: read from _group JSON array on each doc
        if let Some((group_index, ref field_name)) = self.child_aggregate_source {
            if !self.source.next().await? {
                return Ok(false);
            }
            let doc = self.source.value();
            let (sum, has_float) = if let Some(JsonValue::Array(items)) = doc.get(group_index) {
                let mut s = 0.0;
                let mut hf = false;
                for item in items {
                    if let JsonValue::Object(obj) = item {
                        if let Some(val) = obj.get(field_name.as_str()) {
                            if let Some(i) = val.as_i64() {
                                s += i as f64;
                            } else if let Some(f) = val.as_f64() {
                                s += f;
                                hf = true;
                            }
                        }
                    }
                }
                (s, hf)
            } else {
                (0.0, false)
            };
            let mut new_doc = doc.deep_clone();
            new_doc.set(self.aggregate_index, Self::sum_to_json(sum, has_float));
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

    fn is_grouped_source(&self) -> bool {
        self.source.is_grouped_source()
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }

    fn explain_execute_inner(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();

        // Go DefraDB execute format: iterations
        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations),
        );

        // Recursively explain child node with execution info
        let child_explain = self.source.explain_execute();
        if let Some(child_obj) = child_explain.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(obj)
    }
}
