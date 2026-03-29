use serde_json::Value as JsonValue;

use crate::mapper::OrderDirection;
use crate::planner::ExecInfo;

use super::node::TypeJoinMany;

impl TypeJoinMany {
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

        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations),
        );

        let mut inner_obj = serde_json::Map::new();

        // root = parent plan's execute explain
        inner_obj.insert("root".to_string(), self.parent_plan.explain_execute());

        // subType = child plan's execute explain wrapped in selectTopNode > selectNode
        let child_execute = self.child_plan.explain_execute();
        let child_is_select = self.child_plan.kind() == "selectNode";

        let select_node_content = if child_is_select {
            // Extract inner content to avoid double wrapping (selectNode > selectNode)
            child_execute
                .as_object()
                .and_then(|o| o.get("selectNode"))
                .cloned()
                .unwrap_or(child_execute)
        } else {
            // Child is not a SelectNode (e.g., ScanNode or nested join).
            // Synthesize selectNode metrics from captured child exec info.
            let mut select_inner = serde_json::Map::new();
            select_inner.insert(
                "iterations".to_string(),
                serde_json::json!(self.child_exec_info.iterations),
            );
            select_inner.insert(
                "filterMatches".to_string(),
                serde_json::json!(self.child_exec_info.docs_fetched),
            );
            if let Some(child_obj) = child_execute.as_object() {
                for (key, value) in child_obj {
                    select_inner.insert(key.clone(), value.clone());
                }
            }
            serde_json::Value::Object(select_inner)
        };

        let sub_type = serde_json::json!({
            "selectTopNode": {
                "selectNode": select_node_content
                }
        });
        inner_obj.insert("subType".to_string(), sub_type);

        obj.insert(
            "typeJoinMany".to_string(),
            serde_json::Value::Object(inner_obj),
        );

        serde_json::Value::Object(obj)
    }
}
