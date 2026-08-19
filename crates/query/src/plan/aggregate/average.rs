//! AVG aggregate - special implementation due to unique explain structure
//!
//! AVG has custom explain output that decomposes into countNode → sumNode → source,
//! which differs from other aggregates. This file provides a standalone implementation
//! rather than using the generic AggregateNode.

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::planner::{index_selection::CursorSeek, Doc, ExecInfo, PlanNode};
use query_types::document::DocumentMapping;
use query_types::error::Result;
use query_types::mapper::{Filter, Limit};

use super::NumericSourceMeta;

/// Source metadata alias for AVG
pub type AvgSourceMeta = NumericSourceMeta;

/// Marker type for AVG (for type aliases)
pub struct AvgOp;

/// AverageNode computes the average of a numeric field from its source.
///
/// Has custom explain output that decomposes into countNode → sumNode → source
/// to match Go DefraDB's structure.
pub struct AverageNode {
    source: Box<dyn PlanNode>,
    document_mapping: DocumentMapping,
    field_index: usize,
    aggregate_index: usize,
    sum: f64,
    count: usize,
    current_doc: Doc,
    done: bool,
    started: bool,
    grouped_mode: bool,
    aggregate_filter: Option<Filter>,
    aggregate_limit: Option<Limit>,
    child_aggregate_source: Option<(usize, String)>,
    exec_info: ExecInfo,
    sources: Vec<AvgSourceMeta>,
}

impl AverageNode {
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

    fn extract_numeric(value: Option<&JsonValue>) -> Option<f64> {
        match value {
            Some(JsonValue::Number(n)) => n.as_f64(),
            _ => None,
        }
    }

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
            0.0
        } else {
            sum / count as f64
        }
    }

    fn build_source_filter(source: &AvgSourceMeta) -> JsonValue {
        if source.is_inline_array {
            return serde_json::json!({"_neq": serde_json::Value::Null});
        }

        let is_aggregate_ref = source
            .child_field_name
            .as_ref()
            .map(|n| {
                n.starts_with('_') || matches!(n.as_str(), "AVG" | "SUM" | "COUNT" | "MIN" | "MAX")
            })
            .unwrap_or(false);

        match (&source.child_field_name, &source.filter) {
            (Some(cfn), Some(filter)) if !is_aggregate_ref => {
                let conditions = filter.conditions();
                if conditions.is_empty() {
                    serde_json::json!({cfn: {"_neq": serde_json::Value::Null}})
                } else {
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

    fn avg_to_json(avg: f64) -> JsonValue {
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

        if self.done {
            return Ok(false);
        }

        self.exec_info.iterations += 1;

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
            if !self.source.next().await? {
                if !self.grouped_mode && !self.done {
                    if self.source.is_grouped_source() {
                        return Ok(false);
                    }
                    self.done = true;
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

            if let Some(group_docs) = self.source.current_group_docs() {
                self.grouped_mode = true;
                let group_avg = self.compute_average(group_docs);

                let mut doc = self.source.value().deep_clone();
                doc.set(self.aggregate_index, Self::avg_to_json(group_avg));
                self.current_doc = doc;
                return Ok(true);
            }

            let doc = self.source.value();
            if !doc.hidden {
                if let Some(val) = Self::extract_numeric(doc.get(self.field_index)) {
                    self.sum += val;
                    self.count += 1;
                }
            }
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

    fn set_cursor_seek(&mut self, seek: CursorSeek) -> bool {
        self.source.set_cursor_seek(seek)
    }

    fn set_cursor_fetch_limit(&mut self, _limit: u64) -> bool {
        // Average consumes all input rows; bounding the scan below would
        // produce a wrong aggregate. Do not forward.
        false
    }

    fn page_info(&self) -> Option<crate::plan::CursorPageInfo> {
        self.source.page_info()
    }

    fn kind(&self) -> &'static str {
        "averageNode"
    }

    fn explain_inner(&self) -> JsonValue {
        let source_explain = self.source.explain();

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
                        source_obj.insert("childFieldName".to_string(), serde_json::Value::Null);
                    }
                }
                source_obj.insert("filter".to_string(), Self::build_source_filter(s));
                JsonValue::Object(source_obj)
            })
            .collect();

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

    fn explain_debug_inner(&self) -> JsonValue {
        let source_explain = self.source.explain_debug();

        let mut sum_inner = serde_json::Map::new();
        if let Some(source_obj) = source_explain.as_object() {
            for (key, value) in source_obj {
                sum_inner.insert(key.clone(), value.clone());
            }
        }

        let mut count_inner = serde_json::Map::new();
        count_inner.insert("sumNode".to_string(), JsonValue::Object(sum_inner));

        let mut obj = serde_json::Map::new();
        obj.insert("countNode".to_string(), JsonValue::Object(count_inner));

        JsonValue::Object(obj)
    }

    fn current_group_docs(&self) -> Option<&[Doc]> {
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

        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations),
        );

        let source_explain = self.source.explain_execute();
        let iterations = self.exec_info.iterations;
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
