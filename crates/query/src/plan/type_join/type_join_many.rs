//! TypeJoinMany - one-to-many relation joins

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::cmp::Ordering;
use std::collections::HashMap;
use tracing::warn;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{GroupBy, OrderBy, OrderDirection};
use crate::planner::{Doc, PlanNode};

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
    /// When set, only include parents that have at least one child passing this filter.
    /// Example: `Author(filter: {published: {rating: {_gt: 4}}})` means only include
    /// authors who have at least one book with rating > 4.
    relation_filter: Option<RelationFilter>,
    /// Optional groupBy for nested grouping of children.
    /// When set, children are grouped by the specified fields and output includes
    /// a `_group` array containing the grouped documents.
    /// Example: `published(groupBy: [rating]) { rating, _group { name } }`
    child_group_by: Option<GroupBy>,
    /// Mapping for rendering documents inside the _group array.
    /// Only used when child_group_by is set.
    group_mapping: Option<DocumentMapping>,
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

        // Apply ordering if specified
        if let Some(ref order_by) = self.child_order_by {
            let child_mapping = self.child_plan.document_map();
            children.sort_by(|a, b| {
                for condition in &order_by.conditions {
                    let field_name = condition.fields.first().map(|s| s.as_str()).unwrap_or("");
                    let field_idx = child_mapping.first_index_of_name(field_name);

                    if let Some(idx) = field_idx {
                        let val_a = a.get(idx);
                        let val_b = b.get(idx);
                        let cmp = compare_json_values(val_a, val_b);
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
            // No children - relation filter cannot pass
            return Ok(false);
        }

        // Check if any child passes the filter
        let child_mapping = self.child_plan.document_map();
        for child in children {
            if rel_filter.conditions.matches(child.fields(), child_mapping)? {
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

#[async_trait]
impl PlanNode for TypeJoinMany {
    async fn init(&mut self) -> Result<()> {
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
        "typeJoinMany"
    }
}

/// Compare two JSON values for ordering.
/// Follows SQL-like ordering: NULL < bool < number < string < array < object
fn compare_json_values(a: Option<&JsonValue>, b: Option<&JsonValue>) -> Ordering {
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
