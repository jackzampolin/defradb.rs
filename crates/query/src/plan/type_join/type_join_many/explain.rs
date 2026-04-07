use serde_json::Value as JsonValue;

use crate::mapper::OrderDirection;
use crate::planner::index_selection::can_be_ordered_by_index;
use crate::planner::ExecInfo;

use super::node::TypeJoinMany;

impl TypeJoinMany {
    fn scan_index_fetches(value: &JsonValue) -> Option<u64> {
        value
            .as_object()
            .and_then(|obj| obj.get("scanNode"))
            .and_then(|value| value.as_object())
            .and_then(|obj| obj.get("indexFetches"))
            .and_then(|value| value.as_u64())
    }

    fn nested_type_join_one_root_scan(value: &JsonValue) -> Option<JsonValue> {
        value
            .as_object()
            .and_then(|obj| obj.get("typeIndexJoin"))
            .and_then(|value| value.as_object())
            .and_then(|obj| obj.get("typeJoinOne"))
            .and_then(|value| value.as_object())
            .and_then(|obj| obj.get("root"))
            .and_then(|value| value.as_object())
            .and_then(|obj| obj.get("typeIndexJoin"))
            .and_then(|value| value.as_object())
            .and_then(|obj| obj.get("typeJoinOne"))
            .and_then(|value| value.as_object())
            .and_then(|obj| obj.get("root"))
            .and_then(|value| value.as_object())
            .and_then(|obj| obj.get("scanNode"))
            .cloned()
    }

    fn flattened_nested_type_join(value: &JsonValue) -> Option<JsonValue> {
        let mut flattened = value.clone();
        let root_scan = Self::nested_type_join_one_root_scan(value)?;

        let type_join_one = flattened
            .as_object_mut()
            .and_then(|obj| obj.get_mut("typeIndexJoin"))
            .and_then(|value| value.as_object_mut())
            .and_then(|obj| obj.get_mut("typeJoinOne"))
            .and_then(|value| value.as_object_mut())?;

        type_join_one.insert(
            "root".to_string(),
            serde_json::json!({
                "scanNode": root_scan
            }),
        );

        Some(flattened)
    }

    fn nested_type_join_one(
        value: &mut JsonValue,
    ) -> Option<&mut serde_json::Map<String, JsonValue>> {
        value
            .as_object_mut()
            .and_then(|obj| obj.get_mut("typeIndexJoin"))
            .and_then(|value| value.as_object_mut())
            .and_then(|obj| obj.get_mut("typeJoinOne"))
            .and_then(|value| value.as_object_mut())
    }

    pub(super) fn explain_inner_impl(&self) -> JsonValue {
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
        // to avoid double-wrapping (selectNode -> selectNode -> scanNode).
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
        // Structure: selectTopNode > [limitNode >] [orderNode >] selectNode > scanNode
        // Go wraps order around selectNode first, then limit around order.
        let has_order = self.child_order_by.is_some();
        let has_limit = self.child_limit.is_some() || self.child_offset > 0;

        // Start with selectNode content, then wrap with orderNode, then limitNode
        let mut inner_content = select_node_content;

        if has_order {
            // Wrap selectNode in orderNode first (innermost wrapper)
            let mut order_node = serde_json::Map::new();
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
            order_node.insert("selectNode".to_string(), inner_content);
            inner_content =
                serde_json::json!({ "orderNode": serde_json::Value::Object(order_node) });
        } else {
            // No order, wrap selectNode directly
            inner_content = serde_json::json!({ "selectNode": inner_content });
        }

        if has_limit {
            // Wrap orderNode (or selectNode) in limitNode (outermost wrapper)
            let mut limit_node = serde_json::Map::new();
            limit_node.insert(
                "limit".to_string(),
                match self.child_limit {
                    Some(limit) => serde_json::Value::Number(limit.into()),
                    None => serde_json::Value::Null,
                },
            );
            limit_node.insert(
                "offset".to_string(),
                serde_json::Value::Number(self.child_offset.into()),
            );
            if let Some(inner_obj) = inner_content.as_object() {
                for (key, value) in inner_obj {
                    limit_node.insert(key.clone(), value.clone());
                }
            }
            inner_content =
                serde_json::json!({ "limitNode": serde_json::Value::Object(limit_node) });
        }

        // Wrap everything in selectTopNode
        let sub_type = serde_json::json!({ "selectTopNode": inner_content });
        obj.insert("subType".to_string(), sub_type);

        serde_json::Value::Object(obj)
    }

    pub(super) fn explain_debug_inner_impl(&self) -> JsonValue {
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
        // Structure: selectTopNode > [limitNode >] [orderNode >] selectNode > scanNode
        // Go wraps order around selectNode first, then limit around order.
        let has_order = self.child_order_by.is_some();
        let has_limit = self.child_limit.is_some() || self.child_offset > 0;

        // Start with selectNode content, then wrap with orderNode, then limitNode
        let mut inner_content = select_node_content;

        if has_order {
            // Wrap selectNode in orderNode first (debug mode: no attributes, just structure)
            let mut order_node_content = serde_json::Map::new();
            order_node_content.insert(
                "selectNode".to_string(),
                serde_json::Value::Object({
                    let mut m = serde_json::Map::new();
                    if let Some(obj) = inner_content.as_object() {
                        for (k, v) in obj {
                            m.insert(k.clone(), v.clone());
                        }
                    }
                    m
                }),
            );
            inner_content = serde_json::json!({
                "orderNode": serde_json::Value::Object(order_node_content)
            });
        } else {
            // No order, wrap selectNode directly
            inner_content = serde_json::json!({ "selectNode": inner_content });
        }

        if has_limit {
            // Wrap orderNode (or selectNode) in limitNode (debug mode: no attributes, just structure)
            let mut limit_node_content = serde_json::Map::new();
            if let Some(inner_obj) = inner_content.as_object() {
                for (key, value) in inner_obj {
                    limit_node_content.insert(key.clone(), value.clone());
                }
            }
            inner_content = serde_json::json!({
                "limitNode": serde_json::Value::Object(limit_node_content)
            });
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

    pub(super) fn exec_info_impl(&self) -> ExecInfo {
        self.exec_info.clone()
    }

    pub(super) fn explain_execute_inner_impl(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();
        let has_order = self.child_order_by.is_some();
        let has_limit = self.child_limit.is_some() || self.child_offset > 0;

        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations),
        );

        let mut inner_obj = serde_json::Map::new();

        // root = parent plan's execute explain
        inner_obj.insert("root".to_string(), self.parent_plan.explain_execute());

        // subType = child plan's execute explain wrapped in selectTopNode > selectNode.
        // Go re-initializes the child scan per parent, so metrics accumulate across all
        // parent scans. We use go_child_metrics (which simulates this accumulation) to
        // override the child scanNode metrics, matching Go's explain output.
        let child_execute = self.child_plan.explain_execute();
        let child_is_select = self.child_plan.kind() == "selectNode";
        let child_is_direct_scan = child_execute
            .as_object()
            .is_some_and(|obj| obj.contains_key("scanNode"));
        let original_child_scan_index_fetches = Self::scan_index_fetches(&child_execute);
        let select_node_content = if child_is_select {
            // Extract inner content to avoid double wrapping (selectNode > selectNode)
            let mut content = child_execute
                .as_object()
                .and_then(|o| o.get("selectNode"))
                .cloned()
                .unwrap_or_else(|| child_execute.clone());
            // Override scanNode metrics with accumulated go_child_metrics
            if let Some(obj) = content.as_object_mut() {
                obj.insert(
                    "scanNode".to_string(),
                    self.go_child_metrics.scan_node_json(),
                );
            }
            content
        } else {
            // Child is not a SelectNode (e.g., ScanNode or nested join).
            // Synthesize selectNode metrics from go_child_metrics which accumulate
            // across all parent scans, matching Go's per-parent re-scan behavior.
            let mut select_inner = serde_json::Map::new();
            select_inner.insert(
                "iterations".to_string(),
                serde_json::json!(self.go_child_metrics.iterations),
            );
            select_inner.insert(
                "filterMatches".to_string(),
                serde_json::json!(self.child_exec_info.docs_fetched),
            );
            // Merge child plan's explain (e.g., nested typeIndexJoin or scanNode)
            if let Some(child_obj) = child_execute.as_object() {
                for (key, value) in child_obj {
                    select_inner.insert(key.clone(), value.clone());
                }
            }
            // Override scanNode metrics with accumulated go_child_metrics if present
            if select_inner.contains_key("scanNode") {
                select_inner.insert(
                    "scanNode".to_string(),
                    self.go_child_metrics.scan_node_json(),
                );
            }
            serde_json::Value::Object(select_inner)
        };

        let child_order_uses_index = self.child_order_by.as_ref().is_some_and(|order_by| {
            self.child_side
                .collection()
                .indexes
                .iter()
                .any(|index| can_be_ordered_by_index(order_by, index).0)
        });

        let sub_type = if has_limit && !has_order && !child_is_select && child_is_direct_scan {
            serde_json::json!({
                "selectTopNode": {
                    "limitNode": {
                        "iterations": self.go_child_metrics.doc_fetches,
                        "selectNode": {
                            "iterations": self.exec_info.iterations,
                            "filterMatches": self.exec_info.iterations,
                            "scanNode": {
                                "iterations": self.exec_info.iterations,
                                "docFetches": self.go_child_metrics.doc_fetches,
                                "fieldFetches": self.go_child_metrics.field_fetches,
                                "indexFetches": original_child_scan_index_fetches.unwrap_or(0),
                            }
                        }
                    }
                }
            })
        } else if has_order
            && !has_limit
            && !child_is_select
            && child_is_direct_scan
            && !child_order_uses_index
        {
            serde_json::json!({
                "selectTopNode": {
                    "orderNode": {
                        "iterations": self.go_child_metrics.iterations,
                        "selectNode": {
                            "iterations": self.go_child_metrics.iterations,
                            "filterMatches": self.child_exec_info.docs_fetched,
                            "scanNode": {
                                "iterations": self.go_child_metrics.iterations,
                                "docFetches": self.go_child_metrics.doc_fetches,
                                "fieldFetches": self.go_child_metrics.field_fetches,
                                "indexFetches": original_child_scan_index_fetches.unwrap_or(0),
                            }
                        }
                    }
                }
            })
        } else if has_limit && has_order && !child_is_select {
            let nested_root_scan = Self::nested_type_join_one_root_scan(&child_execute);
            let nested_root_is_non_indexed = nested_root_scan
                .as_ref()
                .and_then(|value| value.as_object())
                .and_then(|obj| obj.get("indexFetches"))
                .and_then(|value| value.as_u64())
                == Some(0);

            if nested_root_is_non_indexed {
                let mut flattened_child =
                    Self::flattened_nested_type_join(&child_execute).unwrap_or(child_execute);

                if let Some(type_index_join) = flattened_child
                    .as_object_mut()
                    .and_then(|obj| obj.get_mut("typeIndexJoin"))
                    .and_then(|value| value.as_object_mut())
                {
                    type_index_join.insert(
                        "iterations".to_string(),
                        serde_json::json!(self.go_child_metrics.iterations),
                    );

                    if let Some(root_scan) = type_index_join
                        .get_mut("typeJoinOne")
                        .and_then(|value| value.as_object_mut())
                        .and_then(|obj| obj.get_mut("root"))
                        .and_then(|value| value.as_object_mut())
                        .and_then(|obj| obj.get_mut("scanNode"))
                        .and_then(|value| value.as_object_mut())
                    {
                        root_scan.insert(
                            "iterations".to_string(),
                            serde_json::json!(self.go_child_metrics.iterations),
                        );
                    }
                }

                serde_json::json!({
                    "selectTopNode": {
                        "limitNode": {
                            "iterations": self.go_child_metrics.iterations,
                            "orderNode": {
                                "iterations": self.exec_info.iterations,
                                "selectNode": {
                                    "iterations": self.go_child_metrics.iterations,
                                    "filterMatches": self.exec_info.iterations,
                                    "typeIndexJoin": flattened_child
                                        .as_object()
                                        .and_then(|obj| obj.get("typeIndexJoin"))
                                        .cloned()
                                        .unwrap_or(JsonValue::Null)
                                }
                            }
                        }
                    }
                })
            } else if nested_root_scan.is_some() {
                let mut flattened_child =
                    Self::flattened_nested_type_join(&child_execute).unwrap_or(child_execute);

                if let Some(type_index_join) = flattened_child
                    .as_object_mut()
                    .and_then(|obj| obj.get_mut("typeIndexJoin"))
                    .and_then(|value| value.as_object_mut())
                {
                    type_index_join.insert(
                        "iterations".to_string(),
                        serde_json::json!(self.exec_info.iterations),
                    );
                }

                if let Some(type_join_one) = Self::nested_type_join_one(&mut flattened_child) {
                    if let Some(root_scan) = type_join_one
                        .get_mut("root")
                        .and_then(|value| value.as_object_mut())
                        .and_then(|obj| obj.get_mut("scanNode"))
                        .and_then(|value| value.as_object_mut())
                    {
                        root_scan.insert(
                            "iterations".to_string(),
                            serde_json::json!(self.exec_info.iterations),
                        );
                        root_scan.insert("indexFetches".to_string(), serde_json::json!(0));
                    }

                    if let Some(select_node) = type_join_one
                        .get_mut("subType")
                        .and_then(|value| value.as_object_mut())
                        .and_then(|obj| obj.get_mut("selectTopNode"))
                        .and_then(|value| value.as_object_mut())
                        .and_then(|obj| obj.get_mut("selectNode"))
                        .and_then(|value| value.as_object_mut())
                    {
                        select_node.insert(
                            "iterations".to_string(),
                            serde_json::json!(self.exec_info.iterations),
                        );
                        select_node.insert(
                            "filterMatches".to_string(),
                            serde_json::json!(self.exec_info.iterations),
                        );

                        if let Some(scan_node) = select_node
                            .get_mut("scanNode")
                            .and_then(|value| value.as_object_mut())
                        {
                            scan_node.insert(
                                "iterations".to_string(),
                                serde_json::json!(self.exec_info.iterations),
                            );
                        }
                    }
                }

                serde_json::json!({
                    "selectTopNode": {
                        "limitNode": {
                            "iterations": self.go_child_metrics.iterations,
                            "selectNode": {
                                "iterations": self.exec_info.iterations,
                                "filterMatches": self.exec_info.iterations,
                                "typeIndexJoin": flattened_child
                                    .as_object()
                                    .and_then(|obj| obj.get("typeIndexJoin"))
                                    .cloned()
                                    .unwrap_or(JsonValue::Null)
                            }
                        }
                    }
                })
            } else {
                serde_json::json!({
                    "selectTopNode": {
                        "selectNode": select_node_content
                        }
                })
            }
        } else {
            serde_json::json!({
                "selectTopNode": {
                    "selectNode": select_node_content
                    }
            })
        };
        inner_obj.insert("subType".to_string(), sub_type);

        obj.insert(
            "typeJoinMany".to_string(),
            serde_json::Value::Object(inner_obj),
        );

        serde_json::Value::Object(obj)
    }
}
