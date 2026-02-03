//! TypeJoinMany - one-to-many relation joins

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::cmp::Ordering;
use std::collections::HashMap;
use tracing::warn;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{GroupBy, OrderBy, OrderDirection};
use crate::planner::{Doc, ExecInfo, PlanNode};

use super::{JoinSide, RelationFilter};

/// TypeJoinMany implements one-to-many relation joins.
///
/// The join flow:
/// 1. Parent plan yields a document (e.g., Author)
/// 2. Lookup all child docs where their FK matches parent's _docID
/// 3. Collect all matching child documents into an array
/// 4. Set the array on the parent document under the relation field key
///
/// # Optimization
///
/// Child documents are pre-loaded and indexed during `init()` to avoid
/// O(N * M) nested loop scans. Lookups are O(1) via HashMap.
///
/// # Memory Considerations
///
/// The child cache is unbounded - all child documents matching the query are loaded
/// into memory during `init()`. For collections with very large numbers of documents
/// (e.g., millions of posts for a popular author), this may cause significant memory
/// usage. Consider using pagination or separate queries for large datasets. Future
/// versions may implement LRU caching or streaming lookups to address this limitation.
pub struct TypeJoinMany {
    /// Parent side of the join (the "one" side)
    parent_side: JoinSide,
    /// Child side of the join (the "many" side)
    child_side: JoinSide,
    /// The parent plan node
    parent_plan: Box<dyn PlanNode>,
    /// The child plan node (scanned once during init)
    child_plan: Box<dyn PlanNode>,
    /// Document mapping for this join
    document_mapping: DocumentMapping,
    /// Current document (merged parent + children array)
    current_doc: Doc,
    /// The FK field index on the child side (validated at construction).
    /// Stored directly to avoid runtime option unwrapping.
    child_fk_index: usize,
    /// Whether initialized
    initialized: bool,
    /// Cached child documents indexed by FK field value.
    /// Key is the child's FK value (points to parent's _docID).
    child_cache: HashMap<String, Vec<Doc>>,
    /// Per-parent limit on children (None = no limit)
    child_limit: Option<u64>,
    /// Per-parent offset on children
    child_offset: u64,
    /// Order by specification for children
    child_order_by: Option<OrderBy>,
    /// Optional relation filter to apply during join.
    relation_filter: Option<RelationFilter>,
    /// Optional groupBy for nested grouping of children.
    child_group_by: Option<GroupBy>,
    /// Mapping for rendering documents inside the _group array.
    group_mapping: Option<DocumentMapping>,
    /// Execution statistics for this join node
    exec_info: ExecInfo,
    /// Cached child plan execution info (captured before child is closed)
    child_exec_info: ExecInfo,
    /// Simulated Go-compatible child metrics.
    /// Go re-initializes the child scan per parent, reading ALL children from
    /// the collection each time. Metrics accumulate across all parent scans.
    go_child_iterations: u64,
    go_child_doc_fetches: u64,
    go_child_field_fetches: u64,
    go_child_index_fetches: u64,
    /// Total children in the cache (docs per full collection scan)
    total_children_in_cache: u64,
    /// Total field fetches per full collection scan
    total_fields_per_scan: u64,
}

impl std::fmt::Debug for TypeJoinMany {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeJoinMany")
            .field("parent_side", &self.parent_side)
            .field("child_side", &self.child_side)
            .field(
                "parent_plan",
                &format_args!("<PlanNode: {}>", self.parent_plan.kind()),
            )
            .field(
                "child_plan",
                &format_args!("<PlanNode: {}>", self.child_plan.kind()),
            )
            .field("child_fk_index", &self.child_fk_index)
            .field("initialized", &self.initialized)
            .finish()
    }
}

impl TypeJoinMany {
    /// Create a new TypeJoinMany node.
    ///
    /// # Errors
    /// Returns an error if `child_side` does not have a `relation_id_field_index` (FK field).
    /// One-to-many joins require the child to have an FK field pointing to the parent.
    pub fn new(
        parent_plan: Box<dyn PlanNode>,
        child_plan: Box<dyn PlanNode>,
        parent_side: JoinSide,
        child_side: JoinSide,
        document_mapping: DocumentMapping,
    ) -> Result<Self> {
        // Validate and extract child FK field index - required for one-to-many joins
        let expected_fk_name = schema::CollectionVersion::relation_id_field_name(
            child_side.relation_field().name.as_str(),
        );
        let child_fk_index = child_side.relation_id_field_index().ok_or_else(|| {
            QueryError::internal(format!(
                "TypeJoinMany requires child side to have FK field. \
                 Child collection '{}' relation field '{}' is missing expected FK field '{}'. \
                 Ensure the schema includes a '{}: DocID' field on the 'many' side of the relation.",
                child_side.collection().name,
                child_side.relation_field().name,
                expected_fk_name,
                expected_fk_name
            ))
        })?;

        Ok(Self {
            parent_side,
            child_side,
            parent_plan,
            child_plan,
            document_mapping,
            current_doc: Doc::default(),
            child_fk_index,
            initialized: false,
            child_cache: HashMap::new(),
            child_limit: None,
            child_offset: 0,
            child_order_by: None,
            relation_filter: None,
            child_group_by: None,
            group_mapping: None,
            exec_info: ExecInfo::default(),
            child_exec_info: ExecInfo::default(),
            go_child_iterations: 0,
            go_child_doc_fetches: 0,
            go_child_field_fetches: 0,
            go_child_index_fetches: 0,
            total_children_in_cache: 0,
            total_fields_per_scan: 0,
        })
    }

    /// Set the per-parent limit on children.
    pub fn with_limit(mut self, limit: u64) -> Self {
        self.child_limit = Some(limit);
        self
    }

    /// Set the per-parent offset on children.
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.child_offset = offset;
        self
    }

    /// Set the order by specification for children.
    pub fn with_order_by(mut self, order_by: OrderBy) -> Self {
        self.child_order_by = Some(order_by);
        self
    }

    /// Set a relation filter to apply during the join.
    ///
    /// When set, parent documents will only be included if they have at least one
    /// child document that passes this filter. This is used for queries like
    /// `Author(filter: {published: {rating: {_gt: 4}}})` - only include authors
    /// who have published at least one book with rating > 4.
    pub fn with_relation_filter(mut self, filter: RelationFilter) -> Self {
        self.relation_filter = Some(filter);
        self
    }

    /// Set a groupBy specification for grouping children.
    ///
    /// When set, children will be grouped by the specified fields. The output
    /// will be an array of objects, each containing the groupBy field values
    /// and a `_group` array of documents in that group.
    ///
    /// Example: `published(groupBy: [rating]) { rating, _group { name } }`
    /// Groups books by rating, outputting: `[{rating: 4.9, _group: [{name: "..."}]}, ...]`
    pub fn with_group_by(mut self, group_by: GroupBy) -> Self {
        self.child_group_by = Some(group_by);
        self
    }

    /// Set the mapping for rendering documents inside the _group array.
    ///
    /// This mapping determines which fields are rendered for documents inside _group.
    /// Only used when child_group_by is set.
    pub fn with_group_mapping(mut self, mapping: DocumentMapping) -> Self {
        self.group_mapping = Some(mapping);
        self
    }

    /// Find all child documents that match the parent's _docID using the cache.
    /// Applies ordering, offset, and limit per-parent.
    /// Find all child docs for a parent, applying ordering but NOT limit/offset.
    /// Limit/offset are deferred to the runner's post-processing step so that
    /// relation aggregates (e.g., _count) can see ALL children.
    fn find_child_docs(&self, parent_doc_id: &str) -> Vec<Doc> {
        let Some(docs) = self.child_cache.get(parent_doc_id) else {
            return Vec::new();
        };

        let mut children: Vec<Doc> = docs.iter().map(|d| d.deep_clone()).collect();

        // Apply ordering if specified, otherwise default to _docID order
        // for deterministic results matching Go DefraDB's storage scan order.
        if let Some(ref order_by) = self.child_order_by {
            let child_mapping = self.child_plan.document_map();
            children.sort_by(|a, b| {
                for condition in &order_by.conditions {
                    let field_name = condition.fields.first().map(|s| s.as_str()).unwrap_or("");
                    let field_idx = child_mapping.first_index_of_name(field_name);

                    if let Some(idx) = field_idx {
                        let val_a = a.get(idx);
                        let val_b = b.get(idx);

                        // Resolve nested field paths (e.g., ["course", "name"])
                        let (resolved_a, resolved_b) = if condition.fields.len() > 1 {
                            let nested_path = &condition.fields[1..];
                            (
                                resolve_nested_field(val_a, nested_path),
                                resolve_nested_field(val_b, nested_path),
                            )
                        } else {
                            (val_a.cloned(), val_b.cloned())
                        };

                        let cmp = compare_json_values(resolved_a.as_ref(), resolved_b.as_ref());
                        let cmp = match condition.direction {
                            OrderDirection::Asc => cmp,
                            OrderDirection::Desc => cmp.reverse(),
                        };
                        if cmp != Ordering::Equal {
                            return cmp;
                        }
                    }
                }
                Ordering::Equal
            });
        } else {
            // Default: sort by _docID (index 0) for deterministic ordering
            children.sort_by(|a, b| compare_json_values(a.get(0), b.get(0)));
        }

        // NOTE: Limit/offset are NOT applied here. They are applied in the runner's
        // apply_relation_limits() after compute_relation_aggregates() has counted
        // all children. This ensures _count(published: {}) sees all children even
        // when published(limit: 1) limits the rendered output.

        children
    }

    /// Build the child cache by scanning child_plan once.
    /// Indexes children by their FK field value.
    async fn build_child_cache(&mut self) -> Result<()> {
        self.child_cache.clear();
        self.child_plan.init().await?;
        self.child_plan.start().await?;

        while self.child_plan.next().await? {
            let child_doc = self.child_plan.value();
            let child_fk_value = child_doc.get(self.child_fk_index);

            // Log type mismatch for non-null, non-string FK values
            if let Some(v) = child_fk_value {
                if !v.is_null() && !v.is_string() {
                    warn!(
                        child_collection = %self.child_side.collection().name,
                        relation_field = %self.child_side.relation_field().name,
                        fk_index = self.child_fk_index,
                        actual_type = ?v,
                        "Child FK field has unexpected type, expected string or null"
                    );
                }
            }

            // Index by FK value for O(1) lookup
            if let Some(fk) = child_fk_value.and_then(|v| v.as_str()) {
                self.child_cache
                    .entry(fk.to_string())
                    .or_default()
                    .push(child_doc.deep_clone());
            } else {
                warn!(
                    child_collection = %self.child_side.collection().name,
                    doc_id = ?child_doc.doc_id(),
                    fk_index = self.child_fk_index,
                    fk_value = ?child_fk_value,
                    "Child document skipped - FK field is null or not a string"
                );
            }
        }

        // Capture child plan's execution info before closing
        self.child_exec_info = self.child_plan.exec_info();
        self.go_child_index_fetches = self.child_exec_info.indexes_fetched;

        // Capture per-scan totals for Go-compatible metric simulation.
        // Go re-scans ALL children per parent, so we need these totals.
        self.total_children_in_cache =
            self.child_cache.values().map(|v| v.len() as u64).sum();
        self.total_fields_per_scan = self.child_exec_info.fields_fetched;

        self.child_plan.close().await?;

        // Debug: Log cache contents
        tracing::debug!(
            parent_side_index = self.parent_side.relation_field_index(),
            cache_keys = ?self.child_cache.keys().collect::<Vec<_>>(),
            total_children = self.child_cache.values().map(|v| v.len()).sum::<usize>(),
            "TypeJoinMany::build_child_cache complete"
        );

        Ok(())
    }

    /// Merge child documents into parent as an array.
    ///
    /// If `child_group_by` is set, groups children by the specified fields and
    /// outputs an array of group objects with `_group` arrays.
    /// Otherwise, outputs a simple array of child documents.
    fn merge_children(&self, parent_doc: &mut Doc, children: Vec<Doc>) {
        let array = if let Some(ref group_by) = self.child_group_by {
            self.build_grouped_array(&children, group_by)
        } else {
            self.build_simple_array(&children)
        };

        parent_doc.set(
            self.parent_side.relation_field_index(),
            JsonValue::Array(array),
        );
    }

    /// Build a simple array of child documents (no grouping).
    fn build_simple_array(&self, children: &[Doc]) -> Vec<JsonValue> {
        let child_mapping = self
            .document_mapping
            .child_at(self.parent_side.relation_field_index())
            .unwrap_or(self.child_plan.document_map());

        children
            .iter()
            .map(|doc| child_mapping.render_doc_to_json(doc))
            .collect()
    }

    /// Build a grouped array where children are grouped by the specified fields.
    ///
    /// Output format for each group:
    /// `{groupByField1: value1, groupByField2: value2, _group: [doc1, doc2, ...]}`
    fn build_grouped_array(&self, children: &[Doc], group_by: &GroupBy) -> Vec<JsonValue> {
        if children.is_empty() {
            return Vec::new();
        }

        // Get the child mapping for looking up field indices
        let child_mapping = self.child_plan.document_map();

        // Group children by the groupBy field values
        let mut groups: Vec<(String, Vec<&Doc>)> = Vec::new();
        let mut group_map: HashMap<String, usize> = HashMap::new();

        for child in children {
            let key = self.generate_group_key(child, group_by, child_mapping);
            if let Some(&idx) = group_map.get(&key) {
                groups[idx].1.push(child);
            } else {
                let idx = groups.len();
                group_map.insert(key.clone(), idx);
                groups.push((key, vec![child]));
            }
        }

        // Get the mapping for rendering. Use the child mapping from document_mapping
        // if available, otherwise fall back to child_plan's mapping.
        let render_mapping = self
            .document_mapping
            .child_at(self.parent_side.relation_field_index())
            .unwrap_or(child_mapping);

        // Build output array: one object per group
        let mut result = Vec::with_capacity(groups.len());
        for (_key, group_docs) in &groups {
            let mut obj = serde_json::Map::new();

            // Add groupBy field values from the first document in the group
            if let Some(first_doc) = group_docs.first() {
                for field_name in &group_by.fields {
                    if let Some(idx) = child_mapping.first_index_of_name(field_name) {
                        if let Some(value) = first_doc.get(idx) {
                            obj.insert(field_name.clone(), value.clone());
                        }
                    }
                }
            }

            // Build the _group array
            let group_array: Vec<JsonValue> = if let Some(ref group_mapping) = self.group_mapping {
                // Use explicit group mapping for rendering _group contents
                group_docs
                    .iter()
                    .map(|doc| group_mapping.render_doc_to_json(doc))
                    .collect()
            } else {
                // Fall back to render mapping, excluding groupBy fields
                group_docs
                    .iter()
                    .map(|doc| {
                        self.render_doc_excluding_fields(doc, render_mapping, &group_by.fields)
                    })
                    .collect()
            };

            obj.insert("_group".to_string(), JsonValue::Array(group_array));
            result.push(JsonValue::Object(obj));
        }

        result
    }

    /// Generate a group key from a document based on groupBy fields.
    fn generate_group_key(
        &self,
        doc: &Doc,
        group_by: &GroupBy,
        mapping: &DocumentMapping,
    ) -> String {
        let mut key = String::new();
        for field_name in &group_by.fields {
            if let Some(idx) = mapping.first_index_of_name(field_name) {
                key.push_str(&format!("{}_", idx));
                let value = doc.get(idx);
                key.push_str(&format!("{}_", Self::value_to_key(value)));
            }
        }
        key
    }

    /// Convert a JSON value to a string key component.
    fn value_to_key(value: Option<&JsonValue>) -> String {
        match value {
            None | Some(JsonValue::Null) => "null".to_string(),
            Some(JsonValue::Bool(b)) => b.to_string(),
            Some(JsonValue::Number(n)) => n.to_string(),
            Some(JsonValue::String(s)) => s.clone(),
            Some(JsonValue::Array(arr)) => {
                format!(
                    "[{}]",
                    arr.iter()
                        .map(|v| Self::value_to_key(Some(v)))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            Some(JsonValue::Object(obj)) => {
                format!(
                    "{{{}}}",
                    obj.iter()
                        .map(|(k, v)| format!("{}:{}", k, Self::value_to_key(Some(v))))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
        }
    }

    /// Render a document excluding the specified fields.
    fn render_doc_excluding_fields(
        &self,
        doc: &Doc,
        mapping: &DocumentMapping,
        exclude_fields: &[String],
    ) -> JsonValue {
        let mut obj = serde_json::Map::new();
        for rk in &mapping.render_keys {
            // Skip excluded fields (groupBy fields) and _group pseudo-field
            if exclude_fields.contains(&rk.key) || rk.key == "_group" {
                continue;
            }
            if let Some(value) = doc.get(rk.index) {
                obj.insert(rk.key.clone(), value.clone());
            }
        }
        JsonValue::Object(obj)
    }

    /// Check if at least one child document passes the relation filter.
    ///
    /// Returns true if:
    /// - There's no filter (always pass)
    /// - At least one child document passes the filter conditions
    ///
    /// Returns false if:
    /// - There are no child documents (empty relation can't pass any filter)
    /// - No child document passes the filter conditions
    fn check_relation_filter(&self, children: &[Doc], rel_filter: &RelationFilter) -> Result<bool> {
        if children.is_empty() {
            return Ok(false);
        }

        // Check if any child passes the filter
        let child_mapping = self.child_plan.document_map();
        for child in children {
            if rel_filter
                .conditions
                .matches(child.fields(), child_mapping)?
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get all children for a parent (unfiltered, for filter checking).
    /// This returns all children before limit/offset is applied.
    fn get_all_children(&self, parent_doc_id: &str) -> Vec<Doc> {
        self.child_cache
            .get(parent_doc_id)
            .map(|docs| docs.iter().map(|d| d.deep_clone()).collect())
            .unwrap_or_default()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for TypeJoinMany {
    async fn init(&mut self) -> Result<()> {
        // Reset execution stats
        self.exec_info = ExecInfo::default();
        self.child_exec_info = ExecInfo::default();
        self.go_child_iterations = 0;
        self.go_child_doc_fetches = 0;
        self.go_child_field_fetches = 0;
        self.go_child_index_fetches = 0;
        self.total_children_in_cache = 0;
        self.total_fields_per_scan = 0;

        // Build child cache first (scans child_plan once)
        self.build_child_cache().await?;
        // Then init parent plan
        self.parent_plan.init().await?;
        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        self.parent_plan.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(QueryError::execution(
                "TypeJoinMany.next() called before init()",
            ));
        }

        // Loop to skip parents that don't pass relation filter
        loop {
            // Track iterations (Go counts each call to next, including final false)
            self.exec_info.iterations += 1;

            if !self.parent_plan.next().await? {
                return Ok(false);
            }

            let mut parent_doc = self.parent_plan.value().deep_clone();

            // Get parent's _docID for the lookup (O(1) cache lookup)
            let parent_doc_id = match parent_doc.doc_id() {
                Some(id) => id.to_string(),
                None => {
                    warn!(
                        parent_collection = %self.parent_side.collection().name,
                        relation_field = %self.parent_side.relation_field().name,
                        "Parent document missing _docID - returning empty children array. \
                         This may indicate data corruption or a schema mismatch."
                    );
                    // No docID means no children can match - skip if filter is present
                    if self.relation_filter.is_some() {
                        continue;
                    }
                    // No filter, return with empty children
                    self.merge_children(&mut parent_doc, Vec::new());
                    self.current_doc = parent_doc;
                    return Ok(true);
                }
            };

            // Apply relation filter if present (check against ALL children, not just limited)
            if let Some(ref rel_filter) = self.relation_filter {
                let all_children = self.get_all_children(&parent_doc_id);
                if !self.check_relation_filter(&all_children, rel_filter)? {
                    // No children pass the filter - skip this parent
                    continue;
                }
            }

            // Get children (with ordering, offset, limit applied)
            let children = self.find_child_docs(&parent_doc_id);

            // Simulate Go's per-parent child scan metrics.
            // In Go, fetchPrimaryDocsReferencingSecondaryDoc re-initializes the child
            // scan for each parent, reading ALL children from the collection. The scanNode
            // filters by FK, counting iterations for each outer Next() call (matching + 1 false).
            // docFetches/fieldFetches count ALL docs read from storage per scan.
            let matching_count = self
                .child_cache
                .get(&parent_doc_id)
                .map(|v| v.len() as u64)
                .unwrap_or(0);
            self.go_child_iterations += matching_count + 1; // matching children + 1 false
            self.go_child_doc_fetches += self.total_children_in_cache;
            self.go_child_field_fetches += self.total_fields_per_scan;

            // Merge children array into parent
            self.merge_children(&mut parent_doc, children);
            self.current_doc = parent_doc;

            return Ok(true);
        }
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.parent_plan.close().await?;
        // child_plan was already closed in build_child_cache()
        self.child_cache.clear();
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.parent_plan.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        // Go's explain uses "typeIndexJoin" as the wrapper node
        "typeIndexJoin"
    }

    fn explain_inner(&self) -> JsonValue {
        // Simple/Default mode: typeIndexJoin contains both attributes and tree structure
        let mut obj = serde_json::Map::new();

        // Note: Go only adds "direction" for typeJoinOne, not typeJoinMany

        // joinType: "typeJoinMany" for one-to-many joins
        obj.insert("joinType".to_string(), serde_json::json!("typeJoinMany"));

        // rootName: the child side's relation field name (points back to parent)
        // Go uses immutable.Option[string], but areResultOptionsEqual compares the inner value
        let root_name = self.child_side.relation_field().name.clone();
        obj.insert("rootName".to_string(), serde_json::json!(root_name));

        // subTypeName: the parent side's relation field name (e.g., "articles")
        obj.insert(
            "subTypeName".to_string(),
            serde_json::json!(self.parent_side.relation_field().name),
        );

        // root: the parent plan's explain (contains scanNode)
        let root_explain = self.parent_plan.explain();
        obj.insert("root".to_string(), root_explain);

        // subType: the child plan's explain wrapped in selectTopNode
        // Optionally includes orderNode and/or limitNode wrappers
        // selectNode must include docID and filter attributes (Go always includes these)
        let child_explain = self.child_plan.explain();
        let child_is_select = self.child_plan.kind() == "selectNode";

        // If the child plan is already a SelectNode, its explain output already contains
        // the selectNode wrapper with docID, filter, and inner scanNode. Use it directly
        // to avoid double-wrapping (selectNode → selectNode → scanNode).
        let select_node_content = if child_is_select {
            // Child explain is {"selectNode": {"docID": ..., "filter": ..., "scanNode": ...}}
            // Extract the selectNode's inner content
            child_explain
                .as_object()
                .and_then(|o| o.get("selectNode"))
                .cloned()
                .unwrap_or(child_explain.clone())
        } else {
            let mut select_node_inner = serde_json::Map::new();
            select_node_inner.insert("docID".to_string(), serde_json::Value::Null);
            select_node_inner.insert("filter".to_string(), serde_json::Value::Null);
            // Merge child explain (e.g., scanNode) into selectNode
            if let Some(child_obj) = child_explain.as_object() {
                for (key, value) in child_obj {
                    select_node_inner.insert(key.clone(), value.clone());
                }
            }
            serde_json::Value::Object(select_node_inner)
        };

        // Build the subType structure based on order/limit presence
        // Structure: selectTopNode > [orderNode >] [limitNode >] selectNode > scanNode
        let has_order = self.child_order_by.is_some();
        let has_limit = self.child_limit.is_some() || self.child_offset > 0;

        // Start with selectNode content, then wrap with limitNode, then orderNode
        let mut inner_content = select_node_content;

        if has_limit {
            // Wrap selectNode in limitNode
            let mut limit_node = serde_json::Map::new();
            // Go always includes limit field, even when null
            limit_node.insert(
                "limit".to_string(),
                match self.child_limit {
                    Some(limit) => serde_json::Value::Number(limit.into()),
                    None => serde_json::Value::Null,
                },
            );
            // Go always includes offset
            limit_node.insert(
                "offset".to_string(),
                serde_json::Value::Number(self.child_offset.into()),
            );
            limit_node.insert("selectNode".to_string(), inner_content);
            inner_content =
                serde_json::json!({ "limitNode": serde_json::Value::Object(limit_node) });
        } else {
            // No limit, wrap selectNode directly
            inner_content = serde_json::json!({ "selectNode": inner_content });
        }

        if has_order {
            // Wrap in orderNode
            let mut order_node = serde_json::Map::new();
            // Add order attributes from child_order_by
            if let Some(ref order_by) = self.child_order_by {
                let orderings: Vec<JsonValue> = order_by
                    .conditions
                    .iter()
                    .map(|cond| {
                        serde_json::json!({
                            "direction": match cond.direction {
                                OrderDirection::Asc => "ASC",
                                OrderDirection::Desc => "DESC",
                            },
                            "fields": cond.fields.clone()
                        })
                    })
                    .collect();
                order_node.insert("orderings".to_string(), serde_json::json!(orderings));
            }
            // Add the child (limitNode or selectNode)
            if let Some(inner_obj) = inner_content.as_object() {
                for (key, value) in inner_obj {
                    order_node.insert(key.clone(), value.clone());
                }
            }
            inner_content =
                serde_json::json!({ "orderNode": serde_json::Value::Object(order_node) });
        }

        // Wrap everything in selectTopNode
        let sub_type = serde_json::json!({ "selectTopNode": inner_content });
        obj.insert("subType".to_string(), sub_type);

        serde_json::Value::Object(obj)
    }

    fn explain_debug_inner(&self) -> JsonValue {
        // Debug mode: typeIndexJoin contains typeJoinMany wrapper with full tree structure
        let mut inner_obj = serde_json::Map::new();

        // root: the parent plan's explain_debug (contains scanNode)
        let root_explain = self.parent_plan.explain_debug();
        inner_obj.insert("root".to_string(), root_explain);

        // subType: the child plan's explain_debug wrapped in selectTopNode
        // Optionally includes orderNode and/or limitNode wrappers
        let child_explain = self.child_plan.explain_debug();
        let child_is_select = self.child_plan.kind() == "selectNode";

        let select_node_content = if child_is_select {
            // Child is SelectNode - extract inner content to avoid double wrapping
            child_explain
                .as_object()
                .and_then(|o| o.get("selectNode"))
                .cloned()
                .unwrap_or(child_explain.clone())
        } else {
            let mut select_node_inner = serde_json::Map::new();
            // Merge child explain into selectNode
            if let Some(child_obj) = child_explain.as_object() {
                for (key, value) in child_obj {
                    select_node_inner.insert(key.clone(), value.clone());
                }
            }
            serde_json::Value::Object(select_node_inner)
        };

        // Build the subType structure based on order/limit presence
        // Structure: selectTopNode > [orderNode >] [limitNode >] selectNode > scanNode
        let has_order = self.child_order_by.is_some();
        let has_limit = self.child_limit.is_some() || self.child_offset > 0;

        // Start with selectNode content, then wrap with limitNode, then orderNode
        let mut inner_content = select_node_content;

        if has_limit {
            // Wrap selectNode in limitNode (debug mode: no attributes, just structure)
            inner_content = serde_json::json!({
                "limitNode": {
                    "selectNode": inner_content
                }
            });
        } else {
            // No limit, wrap selectNode directly
            inner_content = serde_json::json!({ "selectNode": inner_content });
        }

        if has_order {
            // Wrap in orderNode (debug mode: no attributes, just structure)
            let mut order_node_content = serde_json::Map::new();
            // Add the child (limitNode or selectNode)
            if let Some(inner_obj) = inner_content.as_object() {
                for (key, value) in inner_obj {
                    order_node_content.insert(key.clone(), value.clone());
                }
            }
            inner_content =
                serde_json::json!({ "orderNode": serde_json::Value::Object(order_node_content) });
        }

        // Wrap everything in selectTopNode
        let sub_type = serde_json::json!({ "selectTopNode": inner_content });
        inner_obj.insert("subType".to_string(), sub_type);

        // Wrap in typeJoinMany
        let mut obj = serde_json::Map::new();
        obj.insert(
            "typeJoinMany".to_string(),
            serde_json::Value::Object(inner_obj),
        );

        serde_json::Value::Object(obj)
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }

    fn explain_execute_inner(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();

        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations as u64),
        );

        let parent_execute = self.parent_plan.explain_execute();
        if let Some(parent_obj) = parent_execute.as_object() {
            for (key, value) in parent_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        // Use simulated Go-compatible metrics for the child scan.
        // Go re-initializes the child scanNode per parent, reading ALL children
        // from the collection each time with an FK filter. Metrics accumulate.
        let mut sub_type_obj = serde_json::Map::new();
        sub_type_obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.go_child_iterations),
        );
        sub_type_obj.insert(
            "docFetches".to_string(),
            serde_json::json!(self.go_child_doc_fetches),
        );
        sub_type_obj.insert(
            "fieldFetches".to_string(),
            serde_json::json!(self.go_child_field_fetches),
        );
        sub_type_obj.insert(
            "indexFetches".to_string(),
            serde_json::json!(self.go_child_index_fetches),
        );
        obj.insert(
            "subTypeScanNode".to_string(),
            serde_json::Value::Object(sub_type_obj),
        );

        serde_json::Value::Object(obj)
    }
}

/// Resolve a nested field path within a JSON value.
/// For example, given a JSON object `{"name": "Math"}` and path `["name"]`,
/// returns `Some(JsonValue::String("Math"))`.
pub fn resolve_nested_field(value: Option<&JsonValue>, path: &[String]) -> Option<JsonValue> {
    let mut current = value?.clone();
    for key in path {
        match current {
            JsonValue::Object(ref obj) => {
                current = obj.get(key.as_str())?.clone();
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Compare two JSON values for ordering.
/// Follows SQL-like ordering: NULL < bool < number < string < array < object
pub fn compare_json_values(a: Option<&JsonValue>, b: Option<&JsonValue>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(JsonValue::Null), Some(JsonValue::Null)) => Ordering::Equal,
        (Some(JsonValue::Null), Some(_)) => Ordering::Less,
        (Some(_), Some(JsonValue::Null)) => Ordering::Greater,
        (Some(JsonValue::Bool(a)), Some(JsonValue::Bool(b))) => a.cmp(b),
        (Some(JsonValue::Number(a)), Some(JsonValue::Number(b))) => {
            // Compare as f64 for numeric ordering
            let fa = a.as_f64().unwrap_or(0.0);
            let fb = b.as_f64().unwrap_or(0.0);
            fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
        }
        (Some(JsonValue::String(a)), Some(JsonValue::String(b))) => a.cmp(b),
        // Different types: order by type precedence
        (Some(a), Some(b)) => type_precedence(a).cmp(&type_precedence(b)),
    }
}

/// Get type precedence for ordering (lower = comes first)
fn type_precedence(v: &JsonValue) -> u8 {
    match v {
        JsonValue::Null => 0,
        JsonValue::Bool(_) => 1,
        JsonValue::Number(_) => 2,
        JsonValue::String(_) => 3,
        JsonValue::Array(_) => 4,
        JsonValue::Object(_) => 5,
    }
}
