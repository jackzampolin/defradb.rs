//! GroupBy, OrderBy, and Limit planning.
//!
//! Applies the final plan nodes after joins: grouping, aggregation, ordering, and limits.

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::{AggregateType, Filter, Requestable, Select};
use crate::plan::groupby::ChildSelectMeta;
use crate::plan::{
    AllDocsNode, GroupAlias, GroupByNode, InnerAggregateDef, LimitNode, OrderByNode, SelectNode,
};
use crate::planner::PlanNode;

impl super::Planner {
    /// Apply GroupBy/OrderBy/Limit nodes to the plan.
    ///
    /// Handles two paths:
    /// - With GROUP BY: GroupByNode → Aggregates → alias filter → OrderBy → Limit
    /// - Without GROUP BY: OrderBy → [AllDocsNode] → Aggregates → Limit
    pub(in crate::planner) fn apply_groupby_ordering_limit(
        &self,
        mut plan: Box<dyn PlanNode>,
        select: &Select,
        scan_mapping: &DocumentMapping,
        index_provides_ordering: bool,
        join_provides_ordering: bool,
    ) -> Result<Box<dyn PlanNode>> {
        let has_group_by = select.group_by.is_some();

        if has_group_by {
            // WITH GROUP BY: GroupByNode → Aggregates → OrderBy → Limit

            // 4b. Apply GroupBy
            // Use scan_mapping because the upstream plan produces docs with schema indices
            if let Some(ref group_by) = select.group_by {
                let mut group_node = GroupByNode::new(plan, group_by.clone(), scan_mapping.clone())
                    .with_collection_name(select.collection_name.clone());

                // Extract _group alias definitions and inner groupBy/aggregate info.
                // Each _group reference (including aliases like G1: _group(limit: 1))
                // gets its own GroupAlias with per-alias filter/limit/order/docIDs.
                let group_indices = scan_mapping
                    .indexes_of_name("GROUP")
                    .map(|s| s.to_vec())
                    .unwrap_or_default();
                let mut group_aliases = Vec::new();
                let mut alias_count = 0;
                let mut inner_extracted = false;

                for field in &select.fields {
                    if let Requestable::Select(nested) = field {
                        if nested.field.name == "GROUP" {
                            let alias_index = group_indices.get(alias_count).copied().unwrap_or(0);
                            alias_count += 1;

                            group_aliases.push(GroupAlias {
                                index: alias_index,
                                filter: nested.filter.clone(),
                                limit: nested.limit.clone(),
                                order: nested.order_by.clone(),
                                doc_ids: nested.doc_ids.clone(),
                            });

                            // Extract inner groupBy/aggregates from the first _group
                            // that has a groupBy clause (only once)
                            if !inner_extracted && nested.group_by.is_some() {
                                inner_extracted = true;

                                if let Some(ref inner_group_by) = nested.group_by {
                                    group_node = group_node
                                        .with_inner_group_by_fields(inner_group_by.fields.clone());
                                }

                                // Extract inner aggregate definitions
                                let mut inner_aggs = Vec::new();
                                for inner_field in &nested.fields {
                                    if let Requestable::Aggregate(inner_agg) = inner_field {
                                        let field_index = if !inner_agg.targets.is_empty() {
                                            if let Some(ref field_name) =
                                                inner_agg.targets[0].field_name
                                            {
                                                scan_mapping
                                                    .first_index_of_name(field_name)
                                                    .unwrap_or(0)
                                            } else {
                                                0
                                            }
                                        } else {
                                            0
                                        };
                                        inner_aggs.push(InnerAggregateDef {
                                            aggregate_type: inner_agg.aggregate_type,
                                            output_key: inner_agg.output_name().to_string(),
                                            field_index,
                                        });
                                    }
                                }
                                if !inner_aggs.is_empty() {
                                    group_node = group_node.with_inner_aggregates(inner_aggs);
                                }

                                // Extract inner _group filter/order (2nd nesting level)
                                // and 3rd-level groupBy/aggregates
                                for inner_field in &nested.fields {
                                    if let Requestable::Select(inner_nested) = inner_field {
                                        if inner_nested.field.name == "GROUP" {
                                            if let Some(ref inner_filter) = inner_nested.filter {
                                                group_node = group_node
                                                    .with_inner_group_filter(inner_filter.clone());
                                            }
                                            if let Some(ref inner_order) = inner_nested.order_by {
                                                group_node = group_node
                                                    .with_inner_group_order(inner_order.clone());
                                            }

                                            // 3rd level: extract groupBy fields
                                            if let Some(ref third_gb) = inner_nested.group_by {
                                                group_node = group_node
                                                    .with_third_level_group_by_fields(
                                                        third_gb.fields.clone(),
                                                    );
                                            }

                                            // 3rd level: extract aggregate definitions
                                            let mut third_aggs = Vec::new();
                                            for third_field in &inner_nested.fields {
                                                if let Requestable::Aggregate(third_agg) =
                                                    third_field
                                                {
                                                    let field_index =
                                                        if !third_agg.targets.is_empty() {
                                                            if let Some(ref field_name) =
                                                                third_agg.targets[0].field_name
                                                            {
                                                                scan_mapping
                                                                    .first_index_of_name(field_name)
                                                                    .unwrap_or(0)
                                                            } else {
                                                                0
                                                            }
                                                        } else {
                                                            0
                                                        };
                                                    third_aggs.push(InnerAggregateDef {
                                                        aggregate_type: third_agg.aggregate_type,
                                                        output_key: third_agg
                                                            .output_name()
                                                            .to_string(),
                                                        field_index,
                                                    });
                                                }
                                            }
                                            if !third_aggs.is_empty() {
                                                group_node = group_node
                                                    .with_third_level_aggregates(third_aggs);
                                            }

                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if !group_aliases.is_empty() {
                    group_node = group_node.with_group_aliases(group_aliases);
                }

                // Build child_selects metadata for explain output
                // Each _group nested select contributes a ChildSelectMeta
                let mut child_selects_meta: Vec<ChildSelectMeta> = Vec::new();
                for field in &select.fields {
                    if let Requestable::Select(nested) = field {
                        if nested.field.name == "GROUP" {
                            let mut meta = ChildSelectMeta {
                                collection_name: select.collection_name.clone(),
                                doc_ids: nested.doc_ids.clone(),
                                filter: nested.filter.clone(),
                                limit: nested.limit.clone(),
                                order: nested.order_by.clone(),
                                group_by: nested.group_by.as_ref().map(|gb| gb.fields.clone()),
                            };

                            // Merge outer groupBy fields into the _group's groupBy.
                            // Go convention: childSelects.groupBy = inner fields ++ outer fields.
                            if let Some(ref outer_gb) = select.group_by {
                                if let Some(ref mut inner_fields) = meta.group_by {
                                    for field in &outer_gb.fields {
                                        if !inner_fields.contains(field) {
                                            inner_fields.push(field.clone());
                                        }
                                    }
                                }
                            }

                            child_selects_meta.push(meta);
                        }
                    }
                }
                // Go adds {field: {_neq: null}} to childSelects filter for average aggregates.
                // Average excludes null values, so the filter is needed on the group's child select.
                // Collect field names from average aggregates targeting _group.
                // Only regular fields (not aggregate refs like _avg) get the neq filter.
                let mut avg_group_fields: Vec<String> = Vec::new();
                for field in &select.fields {
                    if let Requestable::Aggregate(agg) = field {
                        if agg.aggregate_type == AggregateType::Average {
                            for target in &agg.targets {
                                if target.host_name == "GROUP" {
                                    if let Some(ref field_name) = target.field_name {
                                        if !field_name.starts_with('_') {
                                            avg_group_fields.push(field_name.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if !avg_group_fields.is_empty() {
                    // Ensure at least one default child select exists
                    // (query may have _avg(_group: ...) without explicit _group { ... } select)
                    if child_selects_meta.is_empty() {
                        child_selects_meta.push(ChildSelectMeta {
                            collection_name: select.collection_name.clone(),
                            ..Default::default()
                        });
                    }
                    // Inject {field_name: {_neq: null}} for each avg target field
                    for field_name in &avg_group_fields {
                        for cs in &mut child_selects_meta {
                            let mut conditions = cs
                                .filter
                                .as_ref()
                                .map(|f| f.conditions().clone())
                                .unwrap_or_default();
                            conditions
                                .entry(field_name.clone())
                                .and_modify(|v| {
                                    if let serde_json::Value::Object(ref mut ops) = v {
                                        ops.insert("_neq".to_string(), serde_json::Value::Null);
                                    }
                                })
                                .or_insert(serde_json::json!({
                                    "_neq": serde_json::Value::Null
                                }));
                            cs.filter = Some(Filter::from_conditions(conditions));
                        }
                    }
                }
                if !child_selects_meta.is_empty() {
                    group_node = group_node.with_child_selects(child_selects_meta);
                }

                plan = Box::new(group_node);
            }

            // 5. Add aggregate nodes (after grouping)
            plan = self.add_aggregate_nodes(plan, select, scan_mapping)?;

            // 5b. Apply _alias filter AFTER aggregation
            // Alias filters on aggregate fields (e.g., filter: {_alias: {Total: {_gt: 100}}})
            // can only be evaluated after aggregate values have been computed.
            if let Some(ref filter) = select.filter {
                let (_non_alias, alias_filter) = filter.split_alias();
                if let Some(alias_f) = alias_filter {
                    plan =
                        Box::new(SelectNode::new(plan, scan_mapping.clone()).with_filter(alias_f));
                }
            }

            // 6. Apply order by (after grouping/aggregation)
            // Skip if index (own or via join) already provides the ordering
            if let Some(ref order_by) = select.order_by {
                if !index_provides_ordering && !join_provides_ordering {
                    plan = Box::new(OrderByNode::new(
                        plan,
                        order_by.clone(),
                        scan_mapping.clone(),
                    ));
                }
            }

            // 7. Apply limit/offset
            if let Some(ref limit) = select.limit {
                let effective_limit = match limit.limit {
                    Some(0) => None, // limit: 0 means no limit (Go compatibility)
                    other => other,
                };
                if effective_limit.is_some() || limit.offset > 0 {
                    plan = Box::new(LimitNode::new(plan, effective_limit, limit.offset));
                }
            }
        } else {
            // WITHOUT GROUP BY: OrderBy → [AllDocsNode] → Aggregates → Limit
            // Go applies limit AFTER aggregates so limit restricts the final output,
            // not the documents fed to aggregation.

            // 5. Apply order by (before aggregates)
            // Skip if index (own or via join) already provides the ordering
            if let Some(ref order_by) = select.order_by {
                if !index_provides_ordering && !join_provides_ordering {
                    plan = Box::new(OrderByNode::new(
                        plan,
                        order_by.clone(),
                        scan_mapping.clone(),
                    ));
                }
            }

            // 6. Count simple (non-per-row) aggregates to determine if we need AllDocsNode.
            // Relation-based and inline-array aggregates use child_aggregate_source and
            // operate per-row (each parent gets its own aggregate). They do NOT need
            // AllDocsNode. Only simple field aggregates (e.g., _sum(Age: {})) need it
            // because they accumulate across all documents.
            let simple_aggregate_count = select
                .fields
                .iter()
                .filter(|f| {
                    if let Requestable::Aggregate(agg) = f {
                        // Simple aggregate: all targets have empty host_name
                        agg.targets.iter().all(|t| t.host_name.is_empty())
                    } else {
                        false
                    }
                })
                .count();

            // If there are multiple simple aggregates, wrap in AllDocsNode so they all
            // can access the original documents via current_group_docs()
            if simple_aggregate_count > 1 {
                plan = Box::new(AllDocsNode::new(plan, scan_mapping.clone()));
            }

            // 7. Add aggregate nodes (for top-level aggregates without GROUP BY)
            plan = self.add_aggregate_nodes(plan, select, scan_mapping)?;

            // 8. Apply limit/offset (AFTER aggregates, matching Go behavior)
            if let Some(ref limit) = select.limit {
                let effective_limit = match limit.limit {
                    Some(0) => None, // limit: 0 means no limit (Go compatibility)
                    other => other,
                };
                if effective_limit.is_some() || limit.offset > 0 {
                    plan = Box::new(LimitNode::new(plan, effective_limit, limit.offset));
                }
            }
        }

        Ok(plan)
    }
}
