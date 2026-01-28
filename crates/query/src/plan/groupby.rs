//! GroupByNode for grouping query results

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{AggregateType, Filter, GroupBy, Limit, OrderBy, OrderDirection};
use crate::planner::{Doc, PlanNode};

/// Definition of an inner aggregate to compute during nested group rendering.
///
/// When a query has nested _group with aggregates (e.g., `_group(groupBy: [Verified]) { _avg(...) }`),
/// the GroupByNode computes these aggregates inline during _group array rendering,
/// so that outer aggregates (e.g., `_max(_group: {field: _avg})`) can read the values.
#[derive(Debug, Clone)]
pub struct InnerAggregateDef {
    pub aggregate_type: AggregateType,
    /// Render key name for the aggregate result (e.g., "_avg" or alias)
    pub output_key: String,
    /// Index of the target field in the parent mapping (e.g., Age field index)
    pub field_index: usize,
}

/// Definition of a _group alias with its specific rendering arguments.
///
/// Each _group reference in a query (including aliases like `G1: _group(limit: 1)`)
/// gets its own GroupAlias with its specific filter, limit, order, and docID filter.
#[derive(Debug, Clone)]
pub struct GroupAlias {
    /// Index in the document mapping where this alias's array should be stored
    pub index: usize,
    /// Optional filter for this alias
    pub filter: Option<Filter>,
    /// Optional limit for this alias
    pub limit: Option<Limit>,
    /// Optional order for this alias
    pub order: Option<OrderBy>,
    /// Optional docID filter for this alias
    pub doc_ids: Option<Vec<String>>,
}

/// A group of documents with the same group key
#[derive(Debug)]
pub struct DocumentGroup {
    /// The documents in this group
    pub docs: Vec<Doc>,
    /// The representative document (first doc) for this group
    pub representative: Doc,
}

impl DocumentGroup {
    fn new(first_doc: Doc) -> Self {
        Self {
            representative: first_doc.deep_clone(),
            docs: vec![first_doc],
        }
    }

    fn add(&mut self, doc: Doc) {
        self.docs.push(doc);
    }
}

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
    source: Box<dyn PlanNode>,
    group_by: GroupBy,
    document_mapping: DocumentMapping,
    /// Groups keyed by their group key string
    groups: Vec<(String, DocumentGroup)>,
    /// Current position in groups
    position: usize,
    /// Current document
    current_doc: Doc,
    /// Whether start() has been called
    started: bool,
    /// Group alias definitions - one per _group reference in the query
    group_aliases: Vec<GroupAlias>,
    /// Inner aggregate definitions to compute during nested _group rendering
    inner_aggregates: Vec<InnerAggregateDef>,
    /// Collection name (for __typename support in _group rendering)
    collection_name: Option<String>,
    /// Inner group-by field names (from the nested _group Select's groupBy clause)
    inner_group_by_fields: Vec<String>,
    /// Inner _group filter (for second-level nesting)
    inner_group_filter: Option<Filter>,
    /// Inner _group order (for second-level nesting)
    inner_group_order: Option<OrderBy>,
    /// Third-level group-by field names (from 3rd-level _group's groupBy clause)
    third_level_group_by_fields: Vec<String>,
    /// Third-level aggregate definitions (from 3rd-level _group's aggregates)
    third_level_aggregates: Vec<InnerAggregateDef>,
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
        }
    }

    pub fn with_group_aliases(mut self, aliases: Vec<GroupAlias>) -> Self {
        self.group_aliases = aliases;
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
    fn generate_key(&self, doc: &Doc) -> Result<String> {
        let mut key = String::new();
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
            key.push_str(&format!("{}_", index));
            let value = doc.get(index);
            key.push_str(&format!("{}_", Self::value_to_key(value)));
        }
        Ok(key)
    }

    /// Convert a JSON value to a string key component
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

    /// Compare two field values for ordering.
    fn compare_field_values(a: Option<&JsonValue>, b: Option<&JsonValue>) -> std::cmp::Ordering {
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

    /// Build a JSON array of documents for the _group field.
    ///
    /// Each alias can have its own filter, order, limit, and docID filter.
    fn build_group_array(
        &self,
        docs: &[Doc],
        group_index: usize,
        alias_filter: Option<&Filter>,
        alias_order: Option<&OrderBy>,
        alias_limit: Option<&Limit>,
        alias_doc_ids: Option<&[String]>,
    ) -> JsonValue {
        // Get the child mapping for _group to determine which fields to render
        let child_mapping = self.document_mapping.child_at(group_index);

        // Apply filter to docs if present
        let filtered_docs: Vec<&Doc> = if let Some(filter) = alias_filter {
            docs.iter()
                .filter(|d| {
                    filter
                        .matches(d.fields(), &self.document_mapping)
                        .unwrap_or(false)
                })
                .collect()
        } else {
            docs.iter().collect()
        };

        // Apply docID/docIDs filter if present
        let filtered_docs: Vec<&Doc> = if let Some(doc_ids) = alias_doc_ids {
            // _docID is at index 0 in the document mapping
            let docid_idx = self
                .document_mapping
                .first_index_of_name("_docID")
                .unwrap_or(0);
            filtered_docs
                .into_iter()
                .filter(|d| {
                    if let Some(Some(JsonValue::String(id))) = d.fields().get(docid_idx) {
                        doc_ids.contains(id)
                    } else {
                        false
                    }
                })
                .collect()
        } else {
            filtered_docs
        };

        // Apply order
        let ordered_docs: Vec<&Doc> = if let Some(order) = alias_order {
            let mut sorted = filtered_docs;
            sorted.sort_by(|a, b| {
                for cond in &order.conditions {
                    if let Some(field_name) = cond.fields.first() {
                        if let Some(idx) = self.document_mapping.first_index_of_name(field_name) {
                            let val_a = a.get(idx);
                            let val_b = b.get(idx);
                            let cmp = Self::compare_field_values(val_a, val_b);
                            let cmp = match cond.direction {
                                OrderDirection::Asc => cmp,
                                OrderDirection::Desc => cmp.reverse(),
                            };
                            if cmp != std::cmp::Ordering::Equal {
                                return cmp;
                            }
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
            sorted
        } else {
            filtered_docs
        };

        // Apply limit and offset
        let docs_to_render: Vec<&Doc> = if let Some(limit) = alias_limit {
            let offset = limit.offset as usize;
            let effective_limit = limit.limit.map(|l| l as usize);
            match (effective_limit, offset) {
                (Some(0), _) => ordered_docs, // limit=0 means no limit
                (Some(l), o) => ordered_docs.into_iter().skip(o).take(l).collect(),
                (None, o) if o > 0 => ordered_docs.into_iter().skip(o).collect(),
                _ => ordered_docs,
            }
        } else {
            ordered_docs
        };

        // Check if we need to sub-group docs (inner groupBy)
        if !self.inner_group_by_fields.is_empty() {
            return self.build_subgrouped_array(&docs_to_render, child_mapping);
        }

        // Check if child_mapping has a nested _group that requires sub-grouping
        if let Some(mapping) = child_mapping {
            let inner_group_info = mapping
                .render_keys
                .iter()
                .find(|rk| rk.key == "_group")
                .map(|rk| rk.index);

            if let Some(inner_group_index) = inner_group_info {
                // Nested _group: sub-group the docs and produce nested arrays
                return self.build_nested_group_array(&docs_to_render, mapping, inner_group_index);
            }
        }

        // Simple case: no nested _group, just render fields
        let render_keys = if let Some(mapping) = child_mapping {
            &mapping.render_keys
        } else {
            &self.document_mapping.render_keys
        };
        Self::render_docs_with_keys(
            &docs_to_render,
            render_keys,
            self.collection_name.as_deref(),
        )
    }

    /// Build a sub-grouped _group array using inner_group_by_fields.
    ///
    /// Sub-groups docs by the inner groupBy fields, then for each sub-group:
    /// - Outputs the groupBy field values
    /// - Computes inner aggregates
    /// - Optionally includes an inner _group array of the remaining docs
    fn build_subgrouped_array(
        &self,
        docs: &[&Doc],
        child_mapping: Option<&DocumentMapping>,
    ) -> JsonValue {
        // Sub-group documents by the inner groupBy field values
        let mut sub_groups: Vec<(String, Vec<&Doc>)> = Vec::new();
        let mut sub_group_map: HashMap<String, usize> = HashMap::new();

        for doc in docs {
            let mut key = String::new();
            for field_name in &self.inner_group_by_fields {
                if let Some(idx) = self.document_mapping.first_index_of_name(field_name) {
                    key.push_str(&format!("{}_", idx));
                    let value = doc.get(idx);
                    key.push_str(&format!("{}_", Self::value_to_key(value)));
                }
            }

            if let Some(&idx) = sub_group_map.get(&key) {
                sub_groups[idx].1.push(doc);
            } else {
                let idx = sub_groups.len();
                sub_group_map.insert(key.clone(), idx);
                sub_groups.push((key, vec![doc]));
            }
        }

        // Build JSON array: one object per sub-group
        let mut array = Vec::with_capacity(sub_groups.len());
        for (_key, sub_group_docs) in &sub_groups {
            let mut obj = serde_json::Map::new();

            // Add groupBy field values from the first doc
            if let Some(first_doc) = sub_group_docs.first() {
                // Render fields from the child mapping's render keys
                if let Some(mapping) = child_mapping {
                    for render_key in &mapping.render_keys {
                        if render_key.key == "_group" || render_key.key == "__typename" {
                            if render_key.key == "__typename" {
                                if let Some(ref name) = self.collection_name {
                                    obj.insert(
                                        render_key.key.clone(),
                                        JsonValue::String(name.clone()),
                                    );
                                }
                            }
                            continue;
                        }
                        let value = first_doc
                            .get(render_key.index)
                            .cloned()
                            .unwrap_or(JsonValue::Null);
                        obj.insert(render_key.key.clone(), value);
                    }
                } else {
                    // Fallback: just render the groupBy fields
                    for field_name in &self.inner_group_by_fields {
                        if let Some(idx) = self.document_mapping.first_index_of_name(field_name) {
                            let value = first_doc.get(idx).cloned().unwrap_or(JsonValue::Null);
                            obj.insert(field_name.clone(), value);
                        }
                    }
                }
            }

            // Compute inner aggregates for this sub-group
            for agg_def in &self.inner_aggregates {
                let value = Self::compute_inline_aggregate(agg_def, sub_group_docs);
                obj.insert(agg_def.output_key.clone(), value);
            }

            // Check if child mapping has inner _group (for deeply nested _group)
            if let Some(mapping) = child_mapping {
                if let Some(inner_group_rk) =
                    mapping.render_keys.iter().find(|rk| rk.key == "_group")
                {
                    let inner_child_mapping = mapping.child_at(inner_group_rk.index);
                    let inner_render_keys = if let Some(inner_mapping) = inner_child_mapping {
                        &inner_mapping.render_keys
                    } else {
                        &mapping.render_keys
                    };

                    // Apply inner _group filter
                    let inner_filtered: Vec<&Doc> =
                        if let Some(ref filter) = self.inner_group_filter {
                            sub_group_docs
                                .iter()
                                .filter(|d| {
                                    filter
                                        .matches(d.fields(), &self.document_mapping)
                                        .unwrap_or(false)
                                })
                                .copied()
                                .collect()
                        } else {
                            sub_group_docs.clone()
                        };

                    // Apply inner _group order
                    let inner_ordered: Vec<&Doc> = if let Some(ref order) = self.inner_group_order {
                        let mut sorted = inner_filtered;
                        sorted.sort_by(|a, b| {
                            for cond in &order.conditions {
                                if let Some(field_name) = cond.fields.first() {
                                    if let Some(idx) =
                                        self.document_mapping.first_index_of_name(field_name)
                                    {
                                        let cmp =
                                            Self::compare_field_values(a.get(idx), b.get(idx));
                                        let cmp = match cond.direction {
                                            OrderDirection::Asc => cmp,
                                            OrderDirection::Desc => cmp.reverse(),
                                        };
                                        if cmp != std::cmp::Ordering::Equal {
                                            return cmp;
                                        }
                                    }
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                        sorted
                    } else {
                        inner_filtered
                    };

                    let inner_array = if !self.third_level_group_by_fields.is_empty() {
                        self.build_innermost_group_array(&inner_ordered, inner_child_mapping)
                    } else {
                        Self::render_docs_with_keys(
                            &inner_ordered,
                            inner_render_keys,
                            self.collection_name.as_deref(),
                        )
                    };
                    obj.insert("_group".to_string(), inner_array);
                }
            }

            array.push(JsonValue::Object(obj));
        }

        JsonValue::Array(array)
    }

    /// Build the innermost (3rd-level) _group array with sub-grouping and aggregates.
    ///
    /// Sub-groups documents by `third_level_group_by_fields`, computes
    /// `third_level_aggregates` per sub-group, and renders the result.
    fn build_innermost_group_array(
        &self,
        docs: &[&Doc],
        child_mapping: Option<&DocumentMapping>,
    ) -> JsonValue {
        // Sub-group documents by the third-level groupBy fields
        let mut sub_groups: Vec<(String, Vec<&Doc>)> = Vec::new();
        let mut sub_group_map: HashMap<String, usize> = HashMap::new();

        for doc in docs {
            let mut key = String::new();
            for field_name in &self.third_level_group_by_fields {
                if let Some(idx) = self.document_mapping.first_index_of_name(field_name) {
                    key.push_str(&format!("{}_", idx));
                    let value = doc.get(idx);
                    key.push_str(&format!("{}_", Self::value_to_key(value)));
                }
            }

            if let Some(&idx) = sub_group_map.get(&key) {
                sub_groups[idx].1.push(doc);
            } else {
                let idx = sub_groups.len();
                sub_group_map.insert(key.clone(), idx);
                sub_groups.push((key, vec![doc]));
            }
        }

        let render_keys = child_mapping.map(|m| &m.render_keys);

        let mut array = Vec::with_capacity(sub_groups.len());
        for (_key, sub_group_docs) in &sub_groups {
            let mut obj = serde_json::Map::new();

            // Render field values from the first doc in the sub-group
            if let Some(first_doc) = sub_group_docs.first() {
                if let Some(rks) = render_keys {
                    for rk in rks {
                        if rk.key == "_group" || rk.key == "__typename" {
                            continue;
                        }
                        let value = first_doc.get(rk.index).cloned().unwrap_or(JsonValue::Null);
                        obj.insert(rk.key.clone(), value);
                    }
                } else {
                    // Fallback: render groupBy fields
                    for field_name in &self.third_level_group_by_fields {
                        if let Some(idx) = self.document_mapping.first_index_of_name(field_name) {
                            let value = first_doc.get(idx).cloned().unwrap_or(JsonValue::Null);
                            obj.insert(field_name.clone(), value);
                        }
                    }
                }
            }

            // Compute 3rd-level aggregates for this sub-group
            for agg_def in &self.third_level_aggregates {
                let value = Self::compute_inline_aggregate(agg_def, sub_group_docs);
                obj.insert(agg_def.output_key.clone(), value);
            }

            array.push(JsonValue::Object(obj));
        }

        JsonValue::Array(array)
    }

    /// Build a nested _group array by sub-grouping documents.
    ///
    /// The sub-group fields are the non-_group render_keys from the child mapping.
    /// Documents are grouped by these field values, and each sub-group produces
    /// a JSON object with the grouping field values + a `_group` array.
    fn build_nested_group_array(
        &self,
        docs: &[&Doc],
        child_mapping: &DocumentMapping,
        inner_group_index: usize,
    ) -> JsonValue {
        // Sub-group fields: all render_keys except _group
        let sub_group_fields: Vec<_> = child_mapping
            .render_keys
            .iter()
            .filter(|rk| rk.key != "_group")
            .collect();

        // Sub-group documents by the sub-grouping field values
        let mut sub_groups: Vec<(String, Vec<&Doc>)> = Vec::new();
        let mut sub_group_map: HashMap<String, usize> = HashMap::new();

        for doc in docs {
            let mut key = String::new();
            for field in &sub_group_fields {
                key.push_str(&format!("{}_", field.index));
                let value = doc.get(field.index);
                key.push_str(&format!("{}_", Self::value_to_key(value)));
            }

            if let Some(&idx) = sub_group_map.get(&key) {
                sub_groups[idx].1.push(doc);
            } else {
                let idx = sub_groups.len();
                sub_group_map.insert(key.clone(), idx);
                sub_groups.push((key, vec![doc]));
            }
        }

        // Get the inner child mapping for the nested _group
        let inner_child_mapping = child_mapping.child_at(inner_group_index);

        // Build the JSON array: one object per sub-group
        let mut array = Vec::with_capacity(sub_groups.len());
        for (_key, sub_group_docs) in &sub_groups {
            let mut obj = serde_json::Map::new();

            // Add sub-grouping field values from the first doc in the sub-group
            if let Some(first_doc) = sub_group_docs.first() {
                for field in &sub_group_fields {
                    let value = first_doc
                        .get(field.index)
                        .cloned()
                        .unwrap_or(JsonValue::Null);
                    obj.insert(field.key.clone(), value);
                }
            }

            // Build the inner _group array
            let inner_group_array = if let Some(inner_mapping) = inner_child_mapping {
                // Check if the inner mapping has its own nested _group (recursive)
                let inner_nested_group = inner_mapping
                    .render_keys
                    .iter()
                    .find(|rk| rk.key == "_group")
                    .map(|rk| rk.index);

                if let Some(inner_inner_group_index) = inner_nested_group {
                    self.build_nested_group_array(
                        sub_group_docs,
                        inner_mapping,
                        inner_inner_group_index,
                    )
                } else {
                    Self::render_docs_with_keys(
                        sub_group_docs,
                        &inner_mapping.render_keys,
                        self.collection_name.as_deref(),
                    )
                }
            } else {
                // No inner child mapping: render docs with all non-_group parent render keys
                Self::render_docs_with_keys(
                    sub_group_docs,
                    &child_mapping.render_keys,
                    self.collection_name.as_deref(),
                )
            };

            obj.insert("_group".to_string(), inner_group_array);

            // Compute inner aggregates for this sub-group
            for agg_def in &self.inner_aggregates {
                let value = Self::compute_inline_aggregate(agg_def, sub_group_docs);
                obj.insert(agg_def.output_key.clone(), value);
            }

            array.push(JsonValue::Object(obj));
        }

        JsonValue::Array(array)
    }

    /// Compute an aggregate value inline for a sub-group of documents.
    fn compute_inline_aggregate(agg_def: &InnerAggregateDef, docs: &[&Doc]) -> JsonValue {
        let visible_docs: Vec<&&Doc> = docs.iter().filter(|d| !d.hidden).collect();

        match agg_def.aggregate_type {
            AggregateType::Count => JsonValue::Number((visible_docs.len() as i64).into()),
            AggregateType::Sum => {
                let mut sum = 0.0;
                let mut has_float = false;
                for doc in &visible_docs {
                    if let Some(JsonValue::Number(n)) = doc.get(agg_def.field_index) {
                        if let Some(i) = n.as_i64() {
                            sum += i as f64;
                        } else if let Some(f) = n.as_f64() {
                            sum += f;
                            has_float = true;
                        }
                    }
                }
                if has_float {
                    serde_json::Number::from_f64(sum)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::Null)
                } else {
                    JsonValue::Number((sum as i64).into())
                }
            }
            AggregateType::Average => {
                let mut sum = 0.0;
                let mut count = 0usize;
                for doc in &visible_docs {
                    if let Some(JsonValue::Number(n)) = doc.get(agg_def.field_index) {
                        if let Some(f) = n.as_f64() {
                            sum += f;
                            count += 1;
                        }
                    }
                }
                let avg = if count == 0 { 0.0 } else { sum / count as f64 };
                serde_json::Number::from_f64(avg)
                    .map(JsonValue::Number)
                    .unwrap_or_else(|| JsonValue::Number(serde_json::Number::from(0)))
            }
            AggregateType::Min => {
                let mut min: Option<f64> = None;
                let mut has_float = false;
                for doc in &visible_docs {
                    if let Some(JsonValue::Number(n)) = doc.get(agg_def.field_index) {
                        if let Some(i) = n.as_i64() {
                            let v = i as f64;
                            min = Some(min.map_or(v, |m: f64| m.min(v)));
                        } else if let Some(f) = n.as_f64() {
                            min = Some(min.map_or(f, |m: f64| m.min(f)));
                            has_float = true;
                        }
                    }
                }
                match min {
                    None => JsonValue::Null,
                    Some(val) if has_float => serde_json::Number::from_f64(val)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::Null),
                    Some(val) => JsonValue::Number((val as i64).into()),
                }
            }
            AggregateType::Max => {
                let mut max: Option<f64> = None;
                let mut has_float = false;
                for doc in &visible_docs {
                    if let Some(JsonValue::Number(n)) = doc.get(agg_def.field_index) {
                        if let Some(i) = n.as_i64() {
                            let v = i as f64;
                            max = Some(max.map_or(v, |m: f64| m.max(v)));
                        } else if let Some(f) = n.as_f64() {
                            max = Some(max.map_or(f, |m: f64| m.max(f)));
                            has_float = true;
                        }
                    }
                }
                match max {
                    None => JsonValue::Null,
                    Some(val) if has_float => serde_json::Number::from_f64(val)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::Null),
                    Some(val) => JsonValue::Number((val as i64).into()),
                }
            }
        }
    }

    /// Render a list of documents to a JSON array using the given render keys.
    fn render_docs_with_keys(
        docs: &[&Doc],
        render_keys: &[crate::document::RenderKey],
        type_name: Option<&str>,
    ) -> JsonValue {
        let mut array = Vec::with_capacity(docs.len());
        for doc in docs {
            let mut obj = serde_json::Map::new();
            for render_key in render_keys {
                if render_key.key == "_group" {
                    continue;
                }
                // Handle __typename
                if render_key.key == "__typename" {
                    if let Some(name) = type_name {
                        obj.insert(render_key.key.clone(), JsonValue::String(name.to_string()));
                        continue;
                    }
                }
                let value = doc
                    .get(render_key.index)
                    .cloned()
                    .unwrap_or(JsonValue::Null);
                obj.insert(render_key.key.clone(), value);
            }
            array.push(JsonValue::Object(obj));
        }
        JsonValue::Array(array)
    }
}

#[async_trait]
impl PlanNode for GroupByNode {
    async fn init(&mut self) -> Result<()> {
        self.groups.clear();
        self.position = 0;
        self.started = false;
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await?;
        self.started = true;

        // Buffer all documents and group them
        let mut group_map: HashMap<String, usize> = HashMap::new();

        while self.source.next().await? {
            let doc = self.source.value().deep_clone();
            let key = self.generate_key(&doc)?;

            if let Some(&idx) = group_map.get(&key) {
                self.groups[idx].1.add(doc);
            } else {
                let idx = self.groups.len();
                group_map.insert(key.clone(), idx);
                self.groups.push((key, DocumentGroup::new(doc)));
            }
        }

        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.started {
            self.start().await?;
        }

        if self.position >= self.groups.len() {
            return Ok(false);
        }

        // Return the representative document for the current group
        self.current_doc = self.groups[self.position].1.representative.deep_clone();

        // Populate _group field(s) — one per alias
        let group_docs = &self.groups[self.position].1.docs;
        for alias in &self.group_aliases {
            let group_array = self.build_group_array(
                group_docs,
                alias.index,
                alias.filter.as_ref(),
                alias.order.as_ref(),
                alias.limit.as_ref(),
                alias.doc_ids.as_deref(),
            );
            self.current_doc.set(alias.index, group_array);
        }

        self.position += 1;
        Ok(true)
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
        "groupNode"
    }

    fn explain_debug_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        // For GroupBy, Go inserts a pipeNode between selectNode and scanNode
        // The source chain is: groupNode -> selectNode -> scanNode
        // But Go expects: groupNode -> selectNode -> pipeNode -> scanNode
        if let Some(source) = self.source() {
            let child_explain = source.explain_debug();
            if let Some(child_obj) = child_explain.as_object() {
                // Check if child is selectNode
                if let Some(select_content) = child_obj.get("selectNode") {
                    // Insert pipeNode wrapper around selectNode's child (scanNode)
                    let mut modified_select = serde_json::Map::new();
                    if let Some(select_obj) = select_content.as_object() {
                        for (key, value) in select_obj {
                            if key == "scanNode" {
                                // Wrap scanNode in pipeNode
                                let pipe_node = serde_json::json!({
                                    "pipeNode": { "scanNode": value }
                                });
                                if let Some(pipe_obj) = pipe_node.as_object() {
                                    for (pk, pv) in pipe_obj {
                                        modified_select.insert(pk.clone(), pv.clone());
                                    }
                                }
                            } else {
                                modified_select.insert(key.clone(), value.clone());
                            }
                        }
                    }
                    obj.insert(
                        "selectNode".to_string(),
                        serde_json::Value::Object(modified_select),
                    );
                } else {
                    // Not selectNode, just merge as-is
                    for (key, value) in child_obj {
                        obj.insert(key.clone(), value.clone());
                    }
                }
            }
        }

        serde_json::Value::Object(obj)
    }

    fn current_group_docs(&self) -> Option<&[Doc]> {
        // Position is incremented after next(), so position-1 is the current group
        if self.position > 0 && self.position <= self.groups.len() {
            Some(&self.groups[self.position - 1].1.docs)
        } else {
            None
        }
    }

    fn is_grouped_source(&self) -> bool {
        true
    }
}
