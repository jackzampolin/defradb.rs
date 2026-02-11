use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tracing::warn;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::OrderDirection;
use crate::planner::{Doc, ExecInfo, PlanNode};

use super::node::TypeJoinMany;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for TypeJoinMany {
    async fn init(&mut self) -> Result<()> {
        // Reset execution stats
        self.exec_info = ExecInfo::default();
        self.child_exec_info = ExecInfo::default();
        self.go_child_metrics.reset();
        self.total_children_in_cache = 0;
        self.total_fields_per_scan = 0;
        self.child_scan_order.clear();
        self.filter_child_cache.clear();

        // Build filter child cache if present (indexed relation filter evaluation)
        let filter_index_fetches = if let Some(ref mut filter_plan) = self.filter_child_plan {
            filter_plan.init().await?;
            filter_plan.start().await?;
            while filter_plan.next().await? {
                let doc = filter_plan.value();
                if let Some(fk_idx) = self.filter_child_fk_index {
                    if let Some(fk) = doc.get(fk_idx).and_then(|v| v.as_str()) {
                        self.filter_child_cache
                            .entry(fk.to_string())
                            .or_default()
                            .push(doc.deep_clone());
                    }
                }
            }
            let fetches = filter_plan.exec_info().indexes_fetched;
            filter_plan.close().await?;
            Some(fetches)
        } else {
            None
        };

        if self.per_parent_child_scan {
            // Per-parent mode: don't cache, we'll re-scan per parent in next()
            self.parent_plan.init().await?;
        } else {
            // Build child cache first (scans child_plan once)
            self.build_child_cache().await?;
            // Then init parent plan
            self.parent_plan.init().await?;
        }

        // Add filter child plan's index_fetches to the display child's
        if let Some(fetches) = filter_index_fetches {
            self.go_child_metrics.index_fetches += fetches;
        }

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

        if self.per_parent_child_scan {
            return self.next_per_parent().await;
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
                // Use filter_child_cache (from indexed filter plan) when available,
                // otherwise fall back to child_cache (display plan).
                let use_filter = self.filter_child_plan.is_some();
                let filter_children = if use_filter {
                    self.filter_child_cache
                        .get(&parent_doc_id)
                        .map(|docs| docs.iter().map(|d| d.deep_clone()).collect())
                        .unwrap_or_default()
                } else {
                    self.get_all_children(&parent_doc_id)
                };
                if !self.check_relation_filter(&filter_children, rel_filter, use_filter)? {
                    // No children pass the filter - skip this parent
                    continue;
                }
            }

            // Get children (with ordering, offset, limit applied)
            let children = self.find_child_docs(&parent_doc_id);

            // Simulate Go's per-parent child scan metrics.
            // In Go, fetchPrimaryDocsReferencingSecondaryDoc re-initializes the child
            // scan for each parent, reading ALL children from the collection. The scanNode
            // uses a filteredFetcher that skips non-matching docs inside FetchNext().
            if let Some(limit) = self.child_limit {
                // With a child limit, Go's collectDocs(limit) stops after finding
                // enough matches. The filteredFetcher reads docs from storage in CID
                // order, skipping non-matching docs internally. We simulate this by
                // walking the recorded scan order.
                let effective_limit = self.child_offset + limit;
                let mut matches_found = 0u64;
                let mut docs_read = 0u64;
                let mut fields_read = 0u64;
                let mut iterations = 0u64;

                for (fk, field_count) in &self.child_scan_order {
                    docs_read += 1;
                    fields_read += field_count;
                    if fk == &parent_doc_id {
                        matches_found += 1;
                        iterations += 1; // Each match ends a FetchNext call → Next() returns true
                        if matches_found >= effective_limit {
                            break; // collectDocs stops when limit reached
                        }
                    }
                }

                // If collection exhausted without hitting limit, add 1 for the final
                // false Next() call (FetchNext returns nil → Next returns false).
                if matches_found < effective_limit {
                    iterations += 1;
                }

                self.go_child_metrics.iterations += iterations;
                self.go_child_metrics.doc_fetches += docs_read;
                self.go_child_metrics.field_fetches += fields_read;
            } else {
                // Without a child limit, Go scans ALL children per parent.
                // Each FetchNext reads until finding a match (or end), so iterations
                // = matching children + 1 (for the final false Next()).
                let matching_count = self
                    .child_cache
                    .get(&parent_doc_id)
                    .map(|v| v.len() as u64)
                    .unwrap_or(0);
                self.go_child_metrics.iterations += matching_count + 1;
                self.go_child_metrics.doc_fetches += self.total_children_in_cache;
                self.go_child_metrics.field_fetches += self.total_fields_per_scan;
            }

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
        self.child_scan_order.clear();
        self.filter_child_cache.clear();
        // filter_child_plan was already closed in init()
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
            serde_json::json!(self.exec_info.iterations),
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
        obj.insert(
            "subTypeScanNode".to_string(),
            self.go_child_metrics.to_json(),
        );

        serde_json::Value::Object(obj)
    }
}
