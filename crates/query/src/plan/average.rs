//! AverageNode for computing AVG aggregate

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::{Filter, Limit};
use crate::planner::{Doc, ExecInfo, PlanNode};

/// Source metadata for average explain output.
#[derive(Debug, Clone)]
pub struct AvgSourceMeta {
    /// Field name (collection name or relation field name)
    pub field_name: String,
    /// Optional child field name for field-level aggregates
    pub child_field_name: Option<String>,
    /// Optional filter on this source
    pub filter: Option<Filter>,
    /// Whether this is an inline array aggregate (emits {_neq: null} filter in explain)
    pub is_inline_array: bool,
}

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
    /// Execution statistics for explain execute mode
    exec_info: ExecInfo,
    /// Source metadata for explain output
    sources: Vec<AvgSourceMeta>,
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
            exec_info: ExecInfo::default(),
            sources: Vec::new(),
        }
    }

    pub fn with_sources(mut self, sources: Vec<AvgSourceMeta>) -> Self {
        self.sources = sources;
        self
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

    /// Build the filter JSON for an average source explain.
    /// Go adds {child_field_name: {_neq: null}} to both sumNode and countNode sources,
    /// but only for regular fields (not aggregate refs like _avg, _count, etc.).
    fn build_source_filter(source: &AvgSourceMeta) -> JsonValue {
        if source.is_inline_array {
            return serde_json::json!({"_neq": serde_json::Value::Null});
        }

        // Aggregate field refs (starting with _) don't get {_neq: null} filter
        let is_aggregate_ref = source
            .child_field_name
            .as_ref()
            .map(|n| n.starts_with('_'))
            .unwrap_or(false);

        match (&source.child_field_name, &source.filter) {
            (Some(cfn), Some(filter)) if !is_aggregate_ref => {
                let conditions = filter.conditions();
                if conditions.is_empty() {
                    serde_json::json!({cfn: {"_neq": serde_json::Value::Null}})
                } else {
                    // Merge {_neq: null} into existing conditions on the same field
                    let mut merged = serde_json::Map::new();
                    let mut added_neq = false;
                    for (key, val) in conditions {
                        if key == cfn {
                            if let JsonValue::Object(existing_ops) = val {
                                let mut ops = existing_ops.clone();
                                ops.insert("_neq".to_string(), serde_json::Value::Null);
                                merged.insert(key.clone(), JsonValue::Object(ops));
                            } else {
                                merged.insert(key.clone(), val.clone());
                            }
                            added_neq = true;
                        } else {
                            merged.insert(key.clone(), val.clone());
                        }
                    }
                    if !added_neq {
                        merged.insert(
                            cfn.clone(),
                            serde_json::json!({"_neq": serde_json::Value::Null}),
                        );
                    }
                    JsonValue::Object(merged)
                }
            }
            (Some(cfn), None) if !is_aggregate_ref => {
                serde_json::json!({cfn: {"_neq": serde_json::Value::Null}})
            }
            (_, Some(filter)) => {
                let conditions = filter.conditions();
                if conditions.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(conditions)
                }
            }
            _ => serde_json::Value::Null,
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for AverageNode {
    async fn init(&mut self) -> Result<()> {
        self.sum = 0.0;
        self.count = 0;
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

        // Early return when already done (Go checks isCompleted before calling source.next)
        if self.done {
            return Ok(false);
        }

        // Track iterations (Go counts each call to next)
        self.exec_info.iterations += 1;

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

    fn explain_inner(&self) -> JsonValue {
        // Go decomposes average into: averageNode → countNode → sumNode → source
        // Get the source explain output
        let source_explain = self.source.explain();

        // Build sumNode sources (includes childFieldName)
        let sum_sources: Vec<JsonValue> = self
            .sources
            .iter()
            .map(|s| {
                let mut source_obj = serde_json::Map::new();
                source_obj.insert(
                    "fieldName".to_string(),
                    JsonValue::String(s.field_name.clone()),
                );
                match &s.child_field_name {
                    Some(child_name) => {
                        source_obj.insert(
                            "childFieldName".to_string(),
                            JsonValue::String(child_name.clone()),
                        );
                    }
                    None => {
                        source_obj.insert(
                            "childFieldName".to_string(),
                            serde_json::Value::Null,
                        );
                    }
                }
                source_obj.insert("filter".to_string(), Self::build_source_filter(s));
                JsonValue::Object(source_obj)
            })
            .collect();

        // Build countNode sources (NO childFieldName)
        let count_sources: Vec<JsonValue> = self
            .sources
            .iter()
            .map(|s| {
                let mut source_obj = serde_json::Map::new();
                source_obj.insert(
                    "fieldName".to_string(),
                    JsonValue::String(s.field_name.clone()),
                );
                source_obj.insert("filter".to_string(), Self::build_source_filter(s));
                JsonValue::Object(source_obj)
            })
            .collect();

        // Wrap in: countNode { sources: [...], sumNode { sources: [...], ...source... } }
        let mut sum_inner = serde_json::Map::new();
        sum_inner.insert("sources".to_string(), JsonValue::Array(sum_sources));
        if let Some(source_obj) = source_explain.as_object() {
            for (key, value) in source_obj {
                sum_inner.insert(key.clone(), value.clone());
            }
        }

        let mut count_inner = serde_json::Map::new();
        count_inner.insert("sources".to_string(), JsonValue::Array(count_sources));
        count_inner.insert("sumNode".to_string(), JsonValue::Object(sum_inner));

        let mut obj = serde_json::Map::new();
        obj.insert("countNode".to_string(), JsonValue::Object(count_inner));

        JsonValue::Object(obj)
    }

    fn explain_debug_inner(&self) -> JsonValue {
        // Same structure for debug: averageNode → countNode → sumNode → source
        let source_explain = self.source.explain_debug();

        // Build sumNode sources (includes childFieldName)
        let sum_sources: Vec<JsonValue> = self
            .sources
            .iter()
            .map(|s| {
                let mut source_obj = serde_json::Map::new();
                source_obj.insert(
                    "fieldName".to_string(),
                    JsonValue::String(s.field_name.clone()),
                );
                match &s.child_field_name {
                    Some(child_name) => {
                        source_obj.insert(
                            "childFieldName".to_string(),
                            JsonValue::String(child_name.clone()),
                        );
                    }
                    None => {
                        source_obj.insert(
                            "childFieldName".to_string(),
                            serde_json::Value::Null,
                        );
                    }
                }
                source_obj.insert("filter".to_string(), Self::build_source_filter(s));
                JsonValue::Object(source_obj)
            })
            .collect();

        // Build countNode sources (NO childFieldName)
        let count_sources: Vec<JsonValue> = self
            .sources
            .iter()
            .map(|s| {
                let mut source_obj = serde_json::Map::new();
                source_obj.insert(
                    "fieldName".to_string(),
                    JsonValue::String(s.field_name.clone()),
                );
                source_obj.insert("filter".to_string(), Self::build_source_filter(s));
                JsonValue::Object(source_obj)
            })
            .collect();

        let mut sum_inner = serde_json::Map::new();
        sum_inner.insert("sources".to_string(), JsonValue::Array(sum_sources));
        if let Some(source_obj) = source_explain.as_object() {
            for (key, value) in source_obj {
                sum_inner.insert(key.clone(), value.clone());
            }
        }

        let mut count_inner = serde_json::Map::new();
        count_inner.insert("sources".to_string(), JsonValue::Array(count_sources));
        count_inner.insert("sumNode".to_string(), JsonValue::Object(sum_inner));

        let mut obj = serde_json::Map::new();
        obj.insert("countNode".to_string(), JsonValue::Object(count_inner));

        JsonValue::Object(obj)
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
            serde_json::json!(self.exec_info.iterations as u64),
        );

        // Go decomposes average execute into: averageNode → countNode → sumNode → source
        let source_explain = self.source.explain_execute();

        // Wrap in: countNode { iterations: N, sumNode { iterations: N, ...source... } }
        // Go's decomposed countNode/sumNode process the same documents as averageNode,
        // so their iteration counts match.
        let iterations = self.exec_info.iterations as u64;
        let mut sum_inner = serde_json::Map::new();
        sum_inner.insert("iterations".to_string(), serde_json::json!(iterations));
        if let Some(source_obj) = source_explain.as_object() {
            for (key, value) in source_obj {
                sum_inner.insert(key.clone(), value.clone());
            }
        }

        let mut count_inner = serde_json::Map::new();
        count_inner.insert("iterations".to_string(), serde_json::json!(iterations));
        count_inner.insert("sumNode".to_string(), JsonValue::Object(sum_inner));

        obj.insert("countNode".to_string(), JsonValue::Object(count_inner));

        serde_json::Value::Object(obj)
    }
}
