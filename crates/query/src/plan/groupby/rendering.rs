use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::document::DocumentMapping;
use crate::mapper::{AggregateType, Filter, Limit, OrderBy, OrderDirection};
use crate::planner::Doc;

use super::node::GroupByNode;
use super::types::InnerAggregateDef;

impl GroupByNode {
    /// Build a JSON array of documents for the _group field.
    ///
    /// Each alias can have its own filter, order, limit, and docID filter.
    pub(super) fn build_group_array(
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
                .find(|rk| rk.key == "GROUP")
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
                        if render_key.key == "GROUP" || render_key.key == "__typename" {
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
                    mapping.render_keys.iter().find(|rk| rk.key == "GROUP")
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
                    obj.insert("GROUP".to_string(), inner_array);
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
                        if rk.key == "GROUP" || rk.key == "__typename" {
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
            .filter(|rk| rk.key != "GROUP")
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
                    .find(|rk| rk.key == "GROUP")
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

            obj.insert("GROUP".to_string(), inner_group_array);

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
                if render_key.key == "GROUP" {
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
