//! Generic aggregate node infrastructure

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::marker::PhantomData;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::{Filter, Limit};
use crate::planner::{index_selection::CursorSeek, Doc, ExecInfo, PlanNode};

/// Source metadata for aggregates that operate on numeric fields.
/// Used by SUM, MAX, MIN, AVG.
#[derive(Debug, Clone)]
pub struct NumericSourceMeta {
    /// Field name (collection name or relation field name)
    pub field_name: String,
    /// Optional child field name for field-level aggregates
    pub child_field_name: Option<String>,
    /// Optional filter on this source
    pub filter: Option<Filter>,
    /// Whether this is an inline array aggregate (emits {_neq: null} filter in explain)
    pub is_inline_array: bool,
}

/// Trait for aggregate operations (COUNT, SUM, MAX, MIN, AVG).
///
/// Each aggregate type implements this trait to define:
/// - The accumulator type and initialization
/// - How to accumulate values from documents
/// - How to finalize the result
/// - Explain output format
pub trait AggregateOp: Send + Sync + 'static {
    /// Accumulator state for this aggregate
    type Accumulator: Default + Clone + Send + Sync;

    /// Source metadata type for this aggregate
    type SourceMeta: Clone + Send + Sync;

    /// Whether this aggregate requires a field index (COUNT doesn't, others do)
    const REQUIRES_FIELD_INDEX: bool;

    /// Initialize accumulator state
    fn init_accumulator() -> Self::Accumulator;

    /// Accumulate a value from a document field
    fn accumulate(acc: &mut Self::Accumulator, value: Option<&JsonValue>);

    /// Accumulate from a child aggregate JSON array
    fn accumulate_from_group(acc: &mut Self::Accumulator, items: &[JsonValue], field_name: &str);

    /// Finalize the accumulator into a JSON result
    fn finalize(acc: &Self::Accumulator) -> JsonValue;

    /// The node kind name for explain output (e.g., "countNode", "sumNode")
    fn kind() -> &'static str;

    /// Build explain inner output for this aggregate's sources
    fn build_explain_sources(sources: &[Self::SourceMeta]) -> Vec<JsonValue>;

    /// Build explain sources for the count portion of average (no childFieldName)
    fn build_explain_sources_for_count(_sources: &[Self::SourceMeta]) -> Option<Vec<JsonValue>> {
        None
    }
}

/// Generic aggregate node that wraps any `AggregateOp`.
///
/// Handles the common iteration, grouping, filtering, and limiting logic
/// shared by all aggregate nodes. The specific accumulation logic is
/// delegated to the `Op` type parameter.
pub struct AggregateNode<Op: AggregateOp> {
    source: Box<dyn PlanNode>,
    document_mapping: DocumentMapping,
    /// Index of the field to aggregate (not used by COUNT)
    field_index: usize,
    /// Index in the document where result should be stored
    aggregate_index: usize,
    /// Accumulator state for non-grouped mode
    accumulator: Op::Accumulator,
    /// Current document with result
    current_doc: Doc,
    /// Whether we've already yielded the result (for non-grouped mode)
    done: bool,
    /// Whether start() has been called
    started: bool,
    /// Whether we're in grouped mode (source provides group docs)
    grouped_mode: bool,
    /// Optional filter to apply to group documents
    aggregate_filter: Option<Filter>,
    /// Optional limit/offset to apply to group documents
    aggregate_limit: Option<Limit>,
    /// If set, operate in "child aggregate" mode: read values from _group JSON array
    child_aggregate_source: Option<(usize, String)>,
    /// Execution statistics
    exec_info: ExecInfo,
    /// Source metadata for explain output
    sources: Vec<Op::SourceMeta>,
    /// Phantom marker for the operation type
    _op: PhantomData<Op>,
}

impl<Op: AggregateOp> AggregateNode<Op> {
    /// Create a new aggregate node with a field index (for SUM, MAX, MIN, AVG)
    pub fn new_with_field(
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
            accumulator: Op::init_accumulator(),
            current_doc: Doc::default(),
            done: false,
            started: false,
            grouped_mode: false,
            aggregate_filter: None,
            aggregate_limit: None,
            child_aggregate_source: None,
            exec_info: ExecInfo::default(),
            sources: Vec::new(),
            _op: PhantomData,
        }
    }

    /// Create a new aggregate node without a field index (for COUNT)
    pub fn new_without_field(
        source: Box<dyn PlanNode>,
        document_mapping: DocumentMapping,
        aggregate_index: usize,
    ) -> Self {
        Self::new_with_field(source, document_mapping, 0, aggregate_index)
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

    pub fn with_sources(mut self, sources: Vec<Op::SourceMeta>) -> Self {
        self.sources = sources;
        self
    }

    /// Filter and limit a slice of documents for grouped aggregation
    fn filter_and_limit_docs<'a>(&self, docs: &'a [Doc]) -> Vec<&'a Doc> {
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

        if let Some(ref limit) = self.aggregate_limit {
            let offset = limit.offset as usize;
            let effective_limit = limit.limit.map(|l| l as usize);
            match (effective_limit, offset) {
                (Some(0), _) => filtered,
                (Some(l), o) => filtered.into_iter().skip(o).take(l).collect(),
                (None, o) if o > 0 => filtered.into_iter().skip(o).collect(),
                _ => filtered,
            }
        } else {
            filtered
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Op: AggregateOp> PlanNode for AggregateNode<Op> {
    async fn init(&mut self) -> Result<()> {
        self.accumulator = Op::init_accumulator();
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

        // Child aggregate mode: read from _group JSON array on each doc
        if let Some((group_index, ref field_name)) = self.child_aggregate_source {
            if !self.source.next().await? {
                return Ok(false);
            }
            let doc = self.source.value();
            let mut acc = Op::init_accumulator();
            if let Some(JsonValue::Array(items)) = doc.get(group_index) {
                Op::accumulate_from_group(&mut acc, items, field_name);
            }
            let mut new_doc = doc.deep_clone();
            new_doc.set(self.aggregate_index, Op::finalize(&acc));
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
                    let num_fields = self
                        .document_mapping
                        .next_index()
                        .max(self.aggregate_index + 1);
                    let mut doc = Doc::new(num_fields);
                    doc.set(self.aggregate_index, Op::finalize(&self.accumulator));
                    self.current_doc = doc;
                    return Ok(true);
                }
                return Ok(false);
            }

            if let Some(group_docs) = self.source.current_group_docs() {
                self.grouped_mode = true;
                let filtered = self.filter_and_limit_docs(group_docs);
                let mut acc = Op::init_accumulator();
                for doc in &filtered {
                    if Op::REQUIRES_FIELD_INDEX {
                        Op::accumulate(&mut acc, doc.get(self.field_index));
                    } else {
                        // COUNT: just accumulate presence
                        Op::accumulate(&mut acc, Some(&JsonValue::Null));
                    }
                }
                let mut doc = self.source.value().deep_clone();
                if doc.num_fields() <= self.aggregate_index {
                    doc.set(self.aggregate_index, JsonValue::Null);
                }
                doc.set(self.aggregate_index, Op::finalize(&acc));
                self.current_doc = doc;
                return Ok(true);
            }

            // Non-grouped mode: accumulate
            let doc = self.source.value();
            if !doc.hidden {
                if Op::REQUIRES_FIELD_INDEX {
                    Op::accumulate(&mut self.accumulator, doc.get(self.field_index));
                } else {
                    Op::accumulate(&mut self.accumulator, Some(&JsonValue::Null));
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

    fn kind(&self) -> &'static str {
        Op::kind()
    }

    fn explain_inner(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();

        let sources = Op::build_explain_sources(&self.sources);
        obj.insert("sources".to_string(), JsonValue::Array(sources));

        if let Some(source) = self.source() {
            let child_explain = source.explain();
            if let Some(child_obj) = child_explain.as_object() {
                for (key, value) in child_obj {
                    obj.insert(key.clone(), value.clone());
                }
            }
        }

        serde_json::Value::Object(obj)
    }

    fn current_group_docs(&self) -> Option<&[Doc]> {
        self.source.current_group_docs()
    }

    fn set_cursor_seek(&mut self, seek: CursorSeek) -> bool {
        self.source.set_cursor_seek(seek)
    }

    fn set_cursor_fetch_limit(&mut self, _limit: u64) -> bool {
        // Aggregates consume all input rows; bounding the scan below would
        // produce a wrong aggregate. Do not forward.
        false
    }

    fn page_info(&self) -> Option<crate::plan::CursorPageInfo> {
        self.source.page_info()
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

        let child_explain = self.source.explain_execute();
        if let Some(child_obj) = child_explain.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(obj)
    }
}
