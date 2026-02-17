use serde_json::Value as JsonValue;

use crate::mapper::{Requestable, Select};
use crate::query_parse::ExplainType;
use crate::txn::TransactionRegistry;

use super::super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Check if a select represents a top-level aggregate query (e.g., _avg, _count, _sum).
    ///
    /// Top-level aggregates are queries like `_count(Author)` where the aggregate function
    /// is the root query field name itself.
    ///
    /// This does NOT include queries like `Author { _count(books) }` - those are regular
    /// queries with aggregate sub-fields, not top-level aggregates.
    pub(crate) fn is_top_level_aggregate(select: &Select) -> bool {
        // Only true when the query field name itself is an aggregate function
        let field_name = select.field.name.as_str();
        ["COUNT", "SUM", "AVG", "MIN", "MAX"].contains(&field_name)
    }

    /// Aggregate node kind names that can wrap a selectNode in the plan explain.
    pub(crate) const AGGREGATE_NODE_KINDS: &'static [&'static str] =
        &["countNode", "sumNode", "averageNode", "minNode", "maxNode"];

    /// Aggregate-specific explain fields that should be stripped when unwrapping aggregate nodes.
    /// "sources" appears in default explain, "iterations" in execute explain.
    pub(crate) const AGGREGATE_EXPLAIN_FIELDS: [&'static str; 2] = ["sources", "iterations"];

    /// Strip aggregate wrapper nodes from explain output for top-level aggregate queries.
    ///
    /// The Rust planner wraps the plan in aggregate nodes (e.g., CountNode → SelectNode → ScanNode),
    /// but Go's explain format puts aggregates as siblings in topLevelNode, not as wrappers.
    /// This function peels off any top-level aggregate wrappers to expose the inner selectNode.
    ///
    /// Example: `{ "countNode": { "sources": [...], "selectNode": { "scanNode": {...} } } }`
    /// becomes: `{ "selectNode": { "scanNode": {...} } }`
    pub(crate) fn strip_aggregate_wrappers(mut explain: JsonValue) -> JsonValue {
        loop {
            // Check if this is an aggregate wrapper node
            let is_aggregate_wrapper = if let Some(obj) = explain.as_object() {
                // An aggregate wrapper has the aggregate node kind as the only top-level key
                obj.len() == 1
                    && obj
                        .keys()
                        .next()
                        .map(|k| Self::AGGREGATE_NODE_KINDS.contains(&k.as_str()))
                        .unwrap_or(false)
            } else {
                false
            };

            if is_aggregate_wrapper {
                // Unwrap: take the inner value from the aggregate node
                let obj = explain.as_object_mut().unwrap();
                let key = obj.keys().next().unwrap().clone();
                explain = obj.remove(&key).unwrap();

                // Remove aggregate-specific fields from the inner content
                if let Some(inner_obj) = explain.as_object_mut() {
                    for field in &Self::AGGREGATE_EXPLAIN_FIELDS {
                        inner_obj.remove(*field);
                    }
                }
            } else {
                break;
            }
        }
        explain
    }

    /// Build the explain output for a top-level aggregate query.
    ///
    /// Go's format: { "topLevelNode": [ {selectTopNode: ...}, {sumNode: {}}, {countNode: {}}, ... ] }
    pub(crate) fn build_top_level_aggregate_explain(
        &self,
        select: &Select,
        select_explain: JsonValue,
        explain_type: ExplainType,
    ) -> JsonValue {
        use crate::mapper::AggregateType;

        // Strip aggregate wrappers from the plan explain to get the inner selectNode content.
        // The Rust planner wraps aggregates around the plan, but Go puts them as siblings.
        let inner_explain = Self::strip_aggregate_wrappers(select_explain);

        let mut top_level_children: Vec<JsonValue> = Vec::new();

        // First element: the data source (selectTopNode)
        top_level_children.push(serde_json::json!({
            "selectTopNode": inner_explain
        }));

        // Add aggregate nodes based on what's in the fields
        for field in &select.fields {
            if let Requestable::Aggregate(agg) = field {
                let node_name = match agg.aggregate_type {
                    AggregateType::Sum => "sumNode",
                    AggregateType::Count => "countNode",
                    AggregateType::Average => "averageNode",
                    AggregateType::Min => "minNode",
                    AggregateType::Max => "maxNode",
                };

                // For execute explain, aggregate nodes show iterations instead of sources
                if explain_type == ExplainType::Execute {
                    if agg.aggregate_type == AggregateType::Average {
                        // Go decomposes average into sumNode + countNode + averageNode
                        // Each shows iterations: 1 in execute mode
                        top_level_children.push(serde_json::json!({
                            "sumNode": { "iterations": 1u64 }
                        }));
                        top_level_children.push(serde_json::json!({
                            "countNode": { "iterations": 1u64 }
                        }));
                        top_level_children.push(serde_json::json!({
                            "averageNode": { "iterations": 1u64 }
                        }));
                    } else {
                        top_level_children.push(serde_json::json!({
                            node_name: { "iterations": 1u64 }
                        }));
                    }
                    continue;
                }

                // Default/Debug explain: show sources metadata
                let target_filter = if !agg.targets.is_empty() {
                    agg.targets[0].filter.as_ref()
                } else {
                    None
                };

                let filter_value = if let Some(filter) = target_filter {
                    let conditions = filter.conditions();
                    if conditions.is_empty() {
                        JsonValue::Null
                    } else {
                        serde_json::json!(conditions)
                    }
                } else {
                    JsonValue::Null
                };

                // For aggregates that operate on a field (sum, min, max, avg), include childFieldName
                let child_field_name = if !agg.targets.is_empty() {
                    agg.targets[0].field_name.as_ref()
                } else {
                    None
                };

                // Go decomposes average into sumNode + countNode + averageNode
                if agg.aggregate_type == AggregateType::Average {
                    // Go adds {field: {_neq: null}} for both sum and count source filters,
                    // but only for regular fields (not aggregate refs like _avg).
                    let avg_filter = if let Some(field_name) = child_field_name {
                        if field_name.starts_with('_') {
                            // Aggregate field refs don't get neq filter
                            filter_value.clone()
                        } else if filter_value.is_null() {
                            serde_json::json!({field_name: {"_neq": serde_json::Value::Null}})
                        } else if let Some(obj) = filter_value.as_object() {
                            // Merge {field: {_neq: null}} into existing filter conditions
                            let mut merged = obj.clone();
                            merged
                                .entry(field_name.to_string())
                                .and_modify(|v| {
                                    if let JsonValue::Object(ref mut ops) = v {
                                        ops.insert("_neq".to_string(), serde_json::Value::Null);
                                    }
                                })
                                .or_insert(serde_json::json!({"_neq": serde_json::Value::Null}));
                            JsonValue::Object(merged)
                        } else {
                            serde_json::json!({field_name: {"_neq": serde_json::Value::Null}})
                        }
                    } else {
                        filter_value.clone()
                    };

                    // 1. sumNode with sources (includes childFieldName)
                    let sum_source = if let Some(field_name) = child_field_name {
                        serde_json::json!({
                            "fieldName": select.collection_name,
                            "childFieldName": field_name,
                            "filter": avg_filter
                        })
                    } else {
                        serde_json::json!({
                            "fieldName": select.collection_name,
                            "filter": avg_filter
                        })
                    };
                    top_level_children.push(serde_json::json!({
                        "sumNode": {
                            "sources": [sum_source]
                        }
                    }));

                    // 2. countNode with sources (no childFieldName, same filter as sum)
                    let count_source = serde_json::json!({
                        "fieldName": select.collection_name,
                        "filter": avg_filter
                    });
                    top_level_children.push(serde_json::json!({
                        "countNode": {
                            "sources": [count_source]
                        }
                    }));

                    // 3. averageNode (empty)
                    top_level_children.push(serde_json::json!({
                        "averageNode": {}
                    }));
                    continue;
                }

                let source = if let Some(field_name) = child_field_name {
                    serde_json::json!({
                        "fieldName": select.collection_name,
                        "childFieldName": field_name,
                        "filter": filter_value
                    })
                } else {
                    serde_json::json!({
                        "fieldName": select.collection_name,
                        "filter": filter_value
                    })
                };

                top_level_children.push(serde_json::json!({
                    node_name: {
                        "sources": [source]
                    }
                }));
            }
        }

        serde_json::json!({
            "topLevelNode": top_level_children
        })
    }
}
