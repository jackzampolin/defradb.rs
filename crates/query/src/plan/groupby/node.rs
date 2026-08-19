use std::fmt::Write;

use serde_json::Value as JsonValue;

use crate::planner::{Doc, ExecInfo, PlanNode};
use query_types::document::DocumentMapping;
use query_types::error::{QueryError, Result};
use query_types::mapper::{Filter, GroupBy, OrderBy};

use super::types::{ChildSelectMeta, DocumentGroup, GroupAlias, InnerAggregateDef};

/// GroupByNode groups documents by specified fields.
///
/// This node buffers all documents from its source during `start()`,
/// groups them by the specified fields, then yields one document per group.
/// Each yielded document is the representative (first) document from each group.
///
/// Follows Go DefraDB pattern:
/// - Group key is generated from field values (format: `{index}_{value}_`)
/// - Groups are stored in insertion order (first group created is first yielded)
/// - Hidden documents are included in grouping
pub struct GroupByNode {
    pub(super) source: Box<dyn PlanNode>,
    pub(super) group_by: GroupBy,
    pub(super) document_mapping: DocumentMapping,
    /// Groups keyed by their group key string
    pub(super) groups: Vec<(String, DocumentGroup)>,
    /// Current position in groups
    pub(super) position: usize,
    /// Current document
    pub(super) current_doc: Doc,
    /// Whether start() has been called
    pub(super) started: bool,
    /// Group alias definitions - one per _group reference in the query
    pub(super) group_aliases: Vec<GroupAlias>,
    /// Inner aggregate definitions to compute during nested _group rendering
    pub(super) inner_aggregates: Vec<InnerAggregateDef>,
    /// Collection name (for __typename support in _group rendering)
    pub(super) collection_name: Option<String>,
    /// Inner group-by field names (from the nested _group Select's groupBy clause)
    pub(super) inner_group_by_fields: Vec<String>,
    /// Inner _group filter (for second-level nesting)
    pub(super) inner_group_filter: Option<Filter>,
    /// Inner _group order (for second-level nesting)
    pub(super) inner_group_order: Option<OrderBy>,
    /// Third-level group-by field names (from 3rd-level _group's groupBy clause)
    pub(super) third_level_group_by_fields: Vec<String>,
    /// Third-level aggregate definitions (from 3rd-level _group's aggregates)
    pub(super) third_level_aggregates: Vec<InnerAggregateDef>,
    /// Child select metadata for explain output
    pub(super) child_selects: Vec<ChildSelectMeta>,
    /// Execution statistics for explain execute mode
    pub(super) exec_info: ExecInfo,
}

impl GroupByNode {
    /// Create a new GroupByNode
    pub fn new(
        source: Box<dyn PlanNode>,
        group_by: GroupBy,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            source,
            group_by,
            document_mapping,
            groups: Vec::new(),
            position: 0,
            current_doc: Doc::default(),
            started: false,
            group_aliases: Vec::new(),
            inner_aggregates: Vec::new(),
            collection_name: None,
            inner_group_by_fields: Vec::new(),
            inner_group_filter: None,
            inner_group_order: None,
            third_level_group_by_fields: Vec::new(),
            third_level_aggregates: Vec::new(),
            exec_info: ExecInfo::default(),
            child_selects: Vec::new(),
        }
    }

    pub fn with_group_aliases(mut self, aliases: Vec<GroupAlias>) -> Self {
        self.group_aliases = aliases;
        self
    }

    pub fn with_child_selects(mut self, child_selects: Vec<ChildSelectMeta>) -> Self {
        self.child_selects = child_selects;
        self
    }

    pub fn with_inner_group_filter(mut self, filter: Filter) -> Self {
        self.inner_group_filter = Some(filter);
        self
    }

    pub fn with_inner_group_order(mut self, order: OrderBy) -> Self {
        self.inner_group_order = Some(order);
        self
    }

    pub fn with_inner_aggregates(mut self, inner_aggregates: Vec<InnerAggregateDef>) -> Self {
        self.inner_aggregates = inner_aggregates;
        self
    }

    pub fn with_collection_name(mut self, name: String) -> Self {
        self.collection_name = Some(name);
        self
    }

    pub fn with_inner_group_by_fields(mut self, fields: Vec<String>) -> Self {
        self.inner_group_by_fields = fields;
        self
    }

    pub fn with_third_level_group_by_fields(mut self, fields: Vec<String>) -> Self {
        self.third_level_group_by_fields = fields;
        self
    }

    pub fn with_third_level_aggregates(mut self, aggregates: Vec<InnerAggregateDef>) -> Self {
        self.third_level_aggregates = aggregates;
        self
    }

    /// Get the groups (for aggregation nodes to access)
    pub fn groups(&self) -> &[(String, DocumentGroup)] {
        &self.groups
    }

    /// Generate a group key from document field values
    /// Format: `{field_index}_{field_value}_` for each GROUP BY field
    /// Returns an error if any GROUP BY field is not found in the document mapping
    pub(super) fn generate_key(&self, doc: &Doc) -> Result<String> {
        let mut key = String::with_capacity(self.group_by.fields.len() * 16);
        for field_name in &self.group_by.fields {
            let index = self
                .document_mapping
                .first_index_of_name(field_name)
                .ok_or_else(|| {
                    QueryError::unknown_field(format!(
                        "GROUP BY field '{}' not found in document mapping",
                        field_name
                    ))
                })?;
            write!(key, "{index}_").unwrap();
            Self::write_value_key(&mut key, doc.get(index));
            key.push('_');
        }
        Ok(key)
    }

    /// Public wrapper for external benchmarking of the group-key hot path.
    pub fn generate_key_for_doc(&self, doc: &Doc) -> Result<String> {
        self.generate_key(doc)
    }

    /// Public wrapper for benchmarking the document-rendering hot path.
    #[doc(hidden)]
    pub fn render_docs_for_bench(
        docs: &[Doc],
        render_keys: &[query_types::document::RenderKey],
        type_name: Option<&str>,
    ) -> JsonValue {
        Self::render_docs_with_keys(docs.iter(), render_keys, type_name)
    }

    /// Write a JSON value directly into a key buffer without allocating
    /// an intermediate string representation.
    pub(super) fn write_value_key(buf: &mut String, value: Option<&JsonValue>) {
        match value {
            None | Some(JsonValue::Null) => buf.push_str("null"),
            Some(JsonValue::Bool(b)) => write!(buf, "{b}").unwrap(),
            Some(JsonValue::Number(n)) => write!(buf, "{n}").unwrap(),
            Some(JsonValue::String(s)) => buf.push_str(s),
            Some(JsonValue::Array(arr)) => {
                buf.push('[');
                for (index, value) in arr.iter().enumerate() {
                    if index > 0 {
                        buf.push(',');
                    }
                    Self::write_value_key(buf, Some(value));
                }
                buf.push(']');
            }
            Some(JsonValue::Object(obj)) => {
                buf.push('{');
                for (index, (key, value)) in obj.iter().enumerate() {
                    if index > 0 {
                        buf.push(',');
                    }
                    buf.push_str(key);
                    buf.push(':');
                    Self::write_value_key(buf, Some(value));
                }
                buf.push('}');
            }
        }
    }

    /// Increment scanNode.iterations by 1 in the explain JSON output.
    ///
    /// In Go's pipeNode architecture, when `_group` child selections exist,
    /// the childSource also iterates through the shared scanNode, causing
    /// one additional iteration beyond what the parent source consumes.
    pub(super) fn increment_scan_iterations(mut value: serde_json::Value) -> serde_json::Value {
        if let Some(obj) = value.as_object_mut() {
            if let Some(select_node) = obj.get_mut("selectNode") {
                if let Some(select_obj) = select_node.as_object_mut() {
                    if let Some(scan_node) = select_obj.get_mut("scanNode") {
                        if let Some(scan_obj) = scan_node.as_object_mut() {
                            if let Some(iterations) = scan_obj.get_mut("iterations") {
                                if let Some(n) = iterations.as_u64() {
                                    *iterations = serde_json::json!(n + 1);
                                }
                            }
                        }
                    }
                }
            }
        }
        value
    }

    /// Compare two field values for ordering.
    pub(super) fn compare_field_values(
        a: Option<&JsonValue>,
        b: Option<&JsonValue>,
    ) -> std::cmp::Ordering {
        match (a, b) {
            (None | Some(JsonValue::Null), None | Some(JsonValue::Null)) => {
                std::cmp::Ordering::Equal
            }
            (None | Some(JsonValue::Null), _) => std::cmp::Ordering::Less,
            (_, None | Some(JsonValue::Null)) => std::cmp::Ordering::Greater,
            (Some(JsonValue::Number(na)), Some(JsonValue::Number(nb))) => {
                let fa = na.as_f64().unwrap_or(0.0);
                let fb = nb.as_f64().unwrap_or(0.0);
                fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Some(JsonValue::String(sa)), Some(JsonValue::String(sb))) => sa.cmp(sb),
            (Some(JsonValue::Bool(ba)), Some(JsonValue::Bool(bb))) => ba.cmp(bb),
            _ => std::cmp::Ordering::Equal,
        }
    }
}
