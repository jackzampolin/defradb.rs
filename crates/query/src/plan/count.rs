//! CountNode for computing COUNT aggregate

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::{Filter, Limit};
use crate::planner::{Doc, ExecInfo, PlanNode};

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
    /// Optional filter to apply to group documents before counting
    aggregate_filter: Option<Filter>,
    /// Optional limit/offset to apply to group documents before counting
    aggregate_limit: Option<Limit>,
    /// If set, operate in "child aggregate" mode: read values from _group JSON array.
    /// Tuple of (group_field_index, child_field_name).
    child_aggregate_source: Option<(usize, String)>,
    /// Execution statistics for explain execute mode
    exec_info: ExecInfo,
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
}

#[async_trait]
impl PlanNode for CountNode {
    async fn init(&mut self) -> Result<()> {
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

        // Check if we're in grouped mode by testing if source provides group docs
        // We can't detect this until we call next() on the source, so we'll check later
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.started {
            self.start().await?;
        }

        // Track iterations (Go counts each call to next)
        self.exec_info.iterations += 1;

        // Child aggregate mode: read from _group JSON array on each doc
        if let Some((group_index, ref _field_name)) = self.child_aggregate_source {
            if !self.source.next().await? {
                return Ok(false);
            }
            let doc = self.source.value();
            let group_count = if let Some(JsonValue::Array(items)) = doc.get(group_index) {
                items.len() as i64
            } else {
                0
            };
            let mut new_doc = doc.deep_clone();
            new_doc.set(self.aggregate_index, JsonValue::Number(group_count.into()));
            self.current_doc = new_doc;
            return Ok(true);
        }

        loop {
            // Try to get next from source
            if !self.source.next().await? {
                // No more source documents
                if !self.grouped_mode && !self.done {
                    // Empty collection with GroupBy: yield no results
                    if self.source.is_grouped_source() {
                        return Ok(false);
                    }
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
                let filtered: Vec<&Doc> = group_docs
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
                let group_count = if let Some(ref limit) = self.aggregate_limit {
                    let offset = limit.offset as usize;
                    let effective_limit = limit.limit.map(|l| l as usize);
                    match (effective_limit, offset) {
                        (Some(0), _) => filtered.len(),
                        (Some(l), o) => filtered.into_iter().skip(o).take(l).count(),
                        (None, o) if o > 0 => filtered.into_iter().skip(o).count(),
                        _ => filtered.len(),
                    }
                } else {
                    filtered.len()
                } as i64;

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

            // Continue iterating to count all docs (loop continues)
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
        "countNode"
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
