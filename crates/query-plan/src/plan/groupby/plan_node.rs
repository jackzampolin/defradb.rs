use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};

use crate::planner::{index_selection::CursorSeek, Doc, ExecInfo, PlanNode};
use query_types::document::DocumentMapping;
use query_types::error::Result;
use query_types::mapper::OrderDirection;

use super::node::GroupByNode;
use super::types::DocumentGroup;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for GroupByNode {
    async fn init(&mut self) -> Result<()> {
        self.groups.clear();
        self.position = 0;
        self.started = false;
        self.exec_info = ExecInfo::default();
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await?;
        self.started = true;

        // Buffer all documents from scan (storage order)
        let mut all_docs: Vec<Doc> = Vec::new();
        while self.source.next().await? {
            all_docs.push(self.source.value().deep_clone());
        }

        // Determine group key ordering.
        //
        // Go DefraDB's GroupNode uses an interleaved join of parent (scan order) and
        // child (sorted by _group order) documents. Both mergeParent and appendChild
        // can create new groups, so the group ordering depends on which source
        // encounters a new group key first. We replicate this interleaving here.
        //
        // The interleaving only applies when _group has a simple order (no inner
        // groupBy). When _group has a groupBy, Go's child source yields grouped
        // results (fewer items) with different interleaving semantics.
        let group_order = self.group_aliases.first().and_then(|a| a.order.clone());
        let has_simple_group_order = group_order
            .as_ref()
            .is_some_and(|o| !o.is_empty() && self.inner_group_by_fields.is_empty());
        let mut ordered_keys: Vec<String> = Vec::new();
        let mut key_set: HashSet<String> = HashSet::new();

        if !has_simple_group_order {
            let mut group_map: HashMap<String, usize> = HashMap::with_capacity(all_docs.len());
            self.groups.reserve(all_docs.len());

            for doc in all_docs {
                let key = self.generate_key(&doc)?;

                if let Some(&idx) = group_map.get(&key) {
                    self.groups[idx].1.docs.push(doc);
                    continue;
                }

                let idx = self.groups.len();
                let representative = doc.deep_clone();
                group_map.insert(key.clone(), idx);
                self.groups.push((
                    key,
                    DocumentGroup {
                        docs: vec![doc],
                        representative,
                    },
                ));
            }

            return Ok(());
        }

        if has_simple_group_order {
            let order = group_order.as_ref().unwrap();
            if !all_docs.is_empty() {
                // Sort indices by _group order (child order)
                let mut sorted_indices: Vec<usize> = (0..all_docs.len()).collect();
                sorted_indices.sort_by(|&ai, &bi| {
                    for cond in &order.conditions {
                        if let Some(field_name) = cond.fields.first() {
                            if let Some(idx) = self.document_mapping.first_index_of_name(field_name)
                            {
                                let val_a = all_docs[ai].get(idx);
                                let val_b = all_docs[bi].get(idx);
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

                // Interleave parent (scan order) and child (sorted order)
                for i in 0..all_docs.len() {
                    let parent_key = self.generate_key(&all_docs[i])?;
                    if key_set.insert(parent_key.clone()) {
                        ordered_keys.push(parent_key);
                    }

                    let child_key = self.generate_key(&all_docs[sorted_indices[i]])?;
                    if key_set.insert(child_key.clone()) {
                        ordered_keys.push(child_key);
                    }
                }
            }
        }

        // Pre-create groups in the determined order
        let mut group_map: HashMap<String, usize> = HashMap::new();
        for key in &ordered_keys {
            let idx = self.groups.len();
            group_map.insert(key.clone(), idx);
            self.groups.push((
                key.clone(),
                DocumentGroup {
                    docs: vec![],
                    representative: Doc::default(),
                },
            ));
        }

        // Populate groups with docs in scan order
        for doc in all_docs {
            let key = self.generate_key(&doc)?;
            if let Some(&idx) = group_map.get(&key) {
                let group = &mut self.groups[idx].1;
                if group.docs.is_empty() {
                    group.representative = doc.deep_clone();
                }
                group.docs.push(doc);
            }
        }

        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.started {
            self.start().await?;
        }

        // Track iterations (Go counts each call to next)
        self.exec_info.iterations += 1;

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

    fn explain_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        // groupByFields: array of field names being grouped by
        let group_by_fields: Vec<JsonValue> = self
            .group_by
            .fields
            .iter()
            .map(|f| JsonValue::String(f.clone()))
            .collect();
        obj.insert(
            "groupByFields".to_string(),
            JsonValue::Array(group_by_fields),
        );

        // childSelects: array of objects with child selection metadata
        if self.child_selects.is_empty() {
            // If no child_selects metadata, create default from collection_name
            if let Some(ref name) = self.collection_name {
                let child_select = serde_json::json!({
                    "collectionName": name,
                    "docID": serde_json::Value::Null,
                    "filter": serde_json::Value::Null,
                    "groupBy": serde_json::Value::Null,
                    "limit": serde_json::Value::Null,
                    "orderBy": serde_json::Value::Null
                });
                obj.insert(
                    "childSelects".to_string(),
                    JsonValue::Array(vec![child_select]),
                );
            } else {
                obj.insert("childSelects".to_string(), serde_json::Value::Null);
            }
        } else {
            let child_selects: Vec<JsonValue> = self
                .child_selects
                .iter()
                .map(|cs| {
                    let mut child_obj = serde_json::Map::new();
                    child_obj.insert(
                        "collectionName".to_string(),
                        JsonValue::String(cs.collection_name.clone()),
                    );
                    // docID
                    if let Some(ref ids) = cs.doc_ids {
                        let ids_arr: Vec<JsonValue> =
                            ids.iter().map(|id| JsonValue::String(id.clone())).collect();
                        child_obj.insert("docID".to_string(), JsonValue::Array(ids_arr));
                    } else {
                        child_obj.insert("docID".to_string(), serde_json::Value::Null);
                    }
                    // filter
                    if let Some(ref filter) = cs.filter {
                        child_obj
                            .insert("filter".to_string(), serde_json::json!(filter.conditions()));
                    } else {
                        child_obj.insert("filter".to_string(), serde_json::Value::Null);
                    }
                    // limit
                    if let Some(ref limit) = cs.limit {
                        child_obj.insert(
                            "limit".to_string(),
                            serde_json::json!({
                                "limit": limit.limit.unwrap_or(0),
                                "offset": limit.offset
                            }),
                        );
                    } else {
                        child_obj.insert("limit".to_string(), serde_json::Value::Null);
                    }
                    // orderBy
                    if let Some(ref order) = cs.order {
                        let orderings: Vec<JsonValue> = order
                            .conditions
                            .iter()
                            .map(|c| {
                                serde_json::json!({
                                    "fields": c.fields,
                                    "direction": match c.direction {
                                        OrderDirection::Asc => "ASC",
                                        OrderDirection::Desc => "DESC",
                                    }
                                })
                            })
                            .collect();
                        child_obj.insert("orderBy".to_string(), JsonValue::Array(orderings));
                    } else {
                        child_obj.insert("orderBy".to_string(), serde_json::Value::Null);
                    }
                    // groupBy
                    if let Some(ref gb) = cs.group_by {
                        let gb_arr: Vec<JsonValue> =
                            gb.iter().map(|f| JsonValue::String(f.clone())).collect();
                        child_obj.insert("groupBy".to_string(), JsonValue::Array(gb_arr));
                    } else {
                        child_obj.insert("groupBy".to_string(), serde_json::Value::Null);
                    }
                    JsonValue::Object(child_obj)
                })
                .collect();
            obj.insert("childSelects".to_string(), JsonValue::Array(child_selects));
        }

        // Recursively explain child node - merge their wrapped structure
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

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }

    fn explain_execute_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations),
        );
        obj.insert(
            "groups".to_string(),
            serde_json::json!(self.groups.len() as u64),
        );
        obj.insert(
            "childSelections".to_string(),
            serde_json::json!(self.child_selects.len() as u64),
        );
        obj.insert("hiddenBeforeOffset".to_string(), serde_json::json!(0u64));
        obj.insert("hiddenAfterLimit".to_string(), serde_json::json!(0u64));
        obj.insert("hiddenChildSelections".to_string(), serde_json::json!(0u64));

        // Recursively explain child node with execution info.
        // When _group child selections exist, Go's pipeNode architecture causes
        // the scanNode to be iterated one additional time by the childSource
        // exhausting the shared scan. Adjust the JSON output to match.
        let child_explain = self.source.explain_execute();
        let child_explain = if !self.child_selects.is_empty() {
            Self::increment_scan_iterations(child_explain)
        } else {
            child_explain
        };
        if let Some(child_obj) = child_explain.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(obj)
    }

    fn set_cursor_seek(&mut self, seek: CursorSeek) -> bool {
        self.source.set_cursor_seek(seek)
    }

    fn page_info(&self) -> Option<crate::plan::CursorPageInfo> {
        self.source.page_info()
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
