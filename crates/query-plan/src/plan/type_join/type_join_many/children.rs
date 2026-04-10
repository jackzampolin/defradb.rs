use serde_json::Value as JsonValue;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tracing::{debug, warn};

use crate::document::{documents_to_plan_docs, DocumentMapping};
use crate::error::Result;
use crate::mapper::{GroupBy, OrderDirection};
use crate::planner::{Doc, IndexScanParams, IndexScanType};
use document::NormalValue;

use super::super::RelationFilter;
use super::compare::{compare_json_values, resolve_nested_field};
use super::node::TypeJoinMany;

impl TypeJoinMany {
    /// Find all child documents that match the parent's _docID using the cache.
    /// Applies ordering, offset, and limit per-parent.
    /// Find all child docs for a parent, applying ordering but NOT limit/offset.
    /// Limit/offset are deferred to the runner's post-processing step so that
    /// relation aggregates (e.g., _count) can see ALL children.
    pub(super) fn find_child_docs(&self, parent_doc_id: &str) -> Vec<Doc> {
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
                    let field_idx = child_mapping
                        .try_find_index_from_render_key(field_name)
                        .or_else(|| child_mapping.first_index_of_name(field_name));

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
    pub(super) async fn build_child_cache(
        &mut self,
        parent_scope: Option<&HashSet<String>>,
    ) -> Result<()> {
        let build_start = Instant::now();
        self.child_cache.clear();
        self.child_scan_order.clear();

        if let (Some(parent_scope), Some(indexed_child_fetch)) =
            (parent_scope, self.indexed_child_fetch.clone())
        {
            if !parent_scope.is_empty() {
                return self
                    .build_child_cache_from_index(parent_scope, &indexed_child_fetch)
                    .await;
            }
        }

        self.child_plan.init().await?;
        self.child_plan.start().await?;

        while self.child_plan.next().await? {
            let child_doc = self.child_plan.value();
            let child_fk_value = child_doc.get(self.child_fk_index);

            // Record scan order for per-parent metric simulation (used when child_limit is set)
            let fk_str = child_fk_value
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            self.child_scan_order
                .push((fk_str, child_doc.stored_field_count as u64));

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
                if parent_scope
                    .map(|parent_doc_ids| parent_doc_ids.contains(fk))
                    .unwrap_or(true)
                {
                    self.child_cache
                        .entry(fk.to_string())
                        .or_default()
                        .push(child_doc.deep_clone());
                }
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
        self.go_child_metrics.index_fetches = self.child_exec_info.indexes_fetched;

        // Capture per-scan totals for Go-compatible metric simulation.
        // Go re-scans ALL children per parent, so we need these totals.
        self.total_children_in_cache = self.child_cache.values().map(|v| v.len() as u64).sum();
        self.total_fields_per_scan = self.child_exec_info.fields_fetched;

        self.child_plan.close().await?;

        // Debug: Log cache contents
        tracing::debug!(
            parent_side_index = self.parent_side.relation_field_index(),
            cache_keys = ?self.child_cache.keys().collect::<Vec<_>>(),
            total_children = self.child_cache.values().map(|v| v.len()).sum::<usize>(),
            parent_scope_size = parent_scope.map(|scope| scope.len()).unwrap_or(0),
            docs_fetched = self.child_exec_info.docs_fetched,
            fields_fetched = self.child_exec_info.fields_fetched,
            index_fetches = self.child_exec_info.indexes_fetched,
            elapsed = ?build_start.elapsed(),
            "TypeJoinMany::build_child_cache complete"
        );

        Ok(())
    }

    async fn build_child_cache_from_index(
        &mut self,
        parent_scope: &HashSet<String>,
        indexed_child_fetch: &super::node::IndexedChildFetch,
    ) -> Result<()> {
        let build_start = Instant::now();
        let mut parent_ids: Vec<String> = parent_scope.iter().cloned().collect();
        parent_ids.sort();

        let scan_type = if parent_ids.len() == 1 {
            IndexScanType::ExactMatch {
                values: vec![NormalValue::String(parent_ids[0].clone())],
            }
        } else {
            IndexScanType::InScan {
                values: parent_ids.into_iter().map(NormalValue::String).collect(),
                suffix_values: Vec::new(),
            }
        };

        let scan_result = indexed_child_fetch
            .fetcher
            .get_by_index_scan(
                &indexed_child_fetch.collection_name,
                &IndexScanParams {
                    index_name: indexed_child_fetch.index_name.clone(),
                    scan_type,
                    limit: None,
                    offset: 0,
                    value_filter: None,
                },
            )
            .await?;
        let raw_fetches = scan_result.raw_fetches();
        let fetched_docs = indexed_child_fetch
            .fetcher
            .get_by_ids(&indexed_child_fetch.collection_name, scan_result.doc_ids())
            .await?
            .into_docs();

        let child_docs = documents_to_plan_docs(&fetched_docs, self.child_plan.document_map())?;
        let total_fields = child_docs
            .iter()
            .map(|doc| doc.stored_field_count as u64)
            .sum::<u64>();

        for child_doc in child_docs {
            let child_fk_value = child_doc.get(self.child_fk_index);
            let fk_str = child_fk_value
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            self.child_scan_order
                .push((fk_str, child_doc.stored_field_count as u64));

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

            if let Some(fk) = child_fk_value.and_then(|v| v.as_str()) {
                if parent_scope.contains(fk) {
                    self.child_cache
                        .entry(fk.to_string())
                        .or_default()
                        .push(child_doc);
                }
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

        self.child_exec_info = crate::planner::ExecInfo {
            indexes_fetched: raw_fetches,
            docs_fetched: self
                .child_cache
                .values()
                .map(|docs| docs.len() as u64)
                .sum(),
            fields_fetched: total_fields,
            ..Default::default()
        };
        self.go_child_metrics.index_fetches = raw_fetches;
        self.total_children_in_cache = self.child_cache.values().map(|v| v.len() as u64).sum();
        self.total_fields_per_scan = total_fields;

        debug!(
            parent_side_index = self.parent_side.relation_field_index(),
            cache_keys = ?self.child_cache.keys().collect::<Vec<_>>(),
            total_children = self.child_cache.values().map(|v| v.len()).sum::<usize>(),
            fk_field = %indexed_child_fetch.fk_field_name,
            parent_scope_size = parent_scope.len(),
            docs_fetched = self.child_exec_info.docs_fetched,
            fields_fetched = self.child_exec_info.fields_fetched,
            index_fetches = self.child_exec_info.indexes_fetched,
            elapsed = ?build_start.elapsed(),
            "TypeJoinMany::build_child_cache_from_index complete"
        );

        Ok(())
    }

    /// Merge child documents into parent as an array.
    ///
    /// If `child_group_by` is set, groups children by the specified fields and
    /// outputs an array of group objects with `_group` arrays.
    /// Otherwise, outputs a simple array of child documents.
    pub(super) fn merge_children(&self, parent_doc: &mut Doc, children: Vec<Doc>) {
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
            .map(|doc| crate::document::render_doc_to_json(child_mapping, doc))
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
                    .map(|doc| crate::document::render_doc_to_json(group_mapping, doc))
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

            obj.insert("GROUP".to_string(), JsonValue::Array(group_array));
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
            if exclude_fields.contains(&rk.key) || rk.key == "GROUP" {
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
    pub(super) fn check_relation_filter(
        &self,
        children: &[Doc],
        rel_filter: &RelationFilter,
        use_filter_mapping: bool,
    ) -> Result<bool> {
        if children.is_empty() {
            return Ok(false);
        }

        // Use filter_child_plan's mapping when evaluating filter_child_cache children,
        // otherwise use child_plan's mapping.
        let child_mapping = if use_filter_mapping {
            self.filter_child_plan
                .as_ref()
                .map(|p| p.document_map())
                .unwrap_or(self.child_plan.document_map())
        } else {
            self.child_plan.document_map()
        };

        // Check if any child passes the filter
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

    /// Per-parent child scanning: re-run child plan for each parent.
    /// Matches Go's behavior where each parent triggers a fresh index scan.
    pub(super) async fn next_per_parent(&mut self) -> Result<bool> {
        loop {
            self.exec_info.iterations += 1;

            if !self.parent_plan.next().await? {
                return Ok(false);
            }

            let mut parent_doc = self.parent_plan.value().deep_clone();

            let parent_doc_id = match parent_doc.doc_id() {
                Some(id) => id.to_string(),
                None => {
                    if self.relation_filter.is_some() {
                        continue;
                    }
                    self.merge_children(&mut parent_doc, Vec::new());
                    self.current_doc = parent_doc;
                    return Ok(true);
                }
            };

            // Re-run child plan for this parent
            self.child_plan.init().await?;
            self.child_plan.start().await?;

            let mut all_children = Vec::new();
            let mut matching_count = 0u64;
            let mut total_scanned = 0u64;
            let mut limit_reached = false;

            while self.child_plan.next().await? {
                total_scanned += 1;
                let child_doc = self.child_plan.value();
                let child_fk_value = child_doc.get(self.child_fk_index);

                // Check FK match with parent
                let fk_matches = child_fk_value
                    .and_then(|v| v.as_str())
                    .map(|fk| fk == parent_doc_id)
                    .unwrap_or(false);

                if fk_matches {
                    matching_count += 1;
                    all_children.push(child_doc.deep_clone());

                    // Early termination is only safe when child ordering does not need
                    // exhaustive orphan/null merging before the per-parent limit.
                    if self.relation_filter.is_none()
                        && self.child_order_by.is_none()
                        && !(self.preserve_ordered_orphans && self.child_order_by.is_some())
                    {
                        if let Some(limit) = self.child_limit {
                            let effective_needed = self.child_offset + limit;
                            if matching_count >= effective_needed {
                                limit_reached = true;
                                break;
                            }
                        }
                    }
                }
            }

            // Capture child plan metrics for this parent scan
            let child_info = self.child_plan.exec_info();
            self.go_child_metrics.iterations += total_scanned + 1; // scanned entries + 1 false
            self.go_child_metrics.doc_fetches += child_info.docs_fetched;
            self.go_child_metrics.field_fetches += child_info.fields_fetched;
            self.go_child_metrics.index_fetches += child_info.indexes_fetched;

            // If limit was reached early, override index fetches with actual entries scanned.
            // Go stops scanning when limit reached, counting ALL index entries examined
            // (including those filtered out by residual filters like _like).
            // child_info.docs_fetched counts entries iterated in IndexScanNode.next(),
            // including those skipped by the residual filter, which matches Go's behavior.
            if limit_reached {
                self.go_child_metrics.index_fetches -= child_info.indexes_fetched;
                self.go_child_metrics.index_fetches += child_info.docs_fetched;
            }

            self.child_plan.close().await?;

            // Apply relation filter if present
            if let Some(ref rel_filter) = self.relation_filter {
                // Use filter_child_cache (from indexed filter plan) when available,
                // otherwise use per-parent scanned children.
                let use_filter = self.filter_child_plan.is_some();
                let filter_children = if use_filter {
                    self.filter_child_cache
                        .get(&parent_doc_id)
                        .map(|docs| docs.iter().map(|d| d.deep_clone()).collect())
                        .unwrap_or_default()
                } else {
                    all_children.clone()
                };
                if !self.check_relation_filter(&filter_children, rel_filter, use_filter)? {
                    continue;
                }
            }

            // Apply ordering if specified (needed when filter index != ordering index)
            if let Some(ref order_by) = self.child_order_by {
                let child_mapping = self.child_plan.document_map();
                all_children.sort_by(|a, b| {
                    for condition in &order_by.conditions {
                        let field_name = condition.fields.first().map(|s| s.as_str()).unwrap_or("");
                        let field_idx = child_mapping
                            .try_find_index_from_render_key(field_name)
                            .or_else(|| child_mapping.first_index_of_name(field_name));
                        if let Some(idx) = field_idx {
                            let val_a = a.get(idx);
                            let val_b = b.get(idx);
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
                            if cmp != std::cmp::Ordering::Equal {
                                return cmp;
                            }
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }

            // Defer limit/offset to QueryRunner::apply_relation_limits so relation
            // aggregates (_count/_sum/etc.) and parent ordering can observe the full
            // ordered child scope, matching the cached TypeJoinMany path.
            self.merge_children(&mut parent_doc, all_children);
            self.current_doc = parent_doc;
            return Ok(true);
        }
    }

    /// Get all children for a parent (unfiltered, for filter checking).
    /// This returns all children before limit/offset is applied.
    pub(super) fn get_all_children(&self, parent_doc_id: &str) -> Vec<Doc> {
        self.child_cache
            .get(parent_doc_id)
            .map(|docs| docs.iter().map(|d| d.deep_clone()).collect())
            .unwrap_or_default()
    }
}
