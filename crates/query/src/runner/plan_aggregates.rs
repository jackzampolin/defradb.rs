//! Aggregate node construction for query plans.

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{AggregateType, Requestable, Select};
use crate::plan::{
    AverageNode, CountNode, CountSourceMeta, MaxNode, MaxSourceMeta, MinNode, MinSourceMeta,
    SumNode, SumSourceMeta,
};
use crate::planner::PlanNode;

/// Add aggregate nodes to the plan based on the select's aggregate fields.
pub(crate) fn add_aggregate_nodes(
    mut plan: Box<dyn PlanNode>,
    select: &Select,
    mapping: &DocumentMapping,
) -> Result<Box<dyn PlanNode>> {
    for field in &select.fields {
        if let Requestable::Aggregate(agg) = field {
            // Get the index where the aggregate result should be stored.
            // Use the output name (alias if set, otherwise type name) to look up the
            // correct render_key index. This handles aliased aggregates correctly
            // (e.g., C1: _count(...) and C2: _count(...) get different indices).
            let agg_index = mapping
                .try_find_index_from_render_key(agg.output_name())
                .ok_or_else(|| {
                    QueryError::internal(format!(
                        "aggregate '{}' not found in document mapping render keys - this is a bug",
                        agg.output_name()
                    ))
                })?;

            // For aggregates that operate on a field, get the field index
            let field_index = if !agg.targets.is_empty() && agg.targets[0].field_name.is_some() {
                let target_field = agg.targets[0].field_name.as_ref().unwrap();
                mapping.first_index_of_name(target_field).ok_or_else(|| {
                    QueryError::execution(format!(
                        "aggregate target field '{}' not found in mapping",
                        target_field
                    ))
                })?
            } else {
                0 // Not used for count
            };

            // Extract filter and limit from aggregate target (if any)
            let target_filter = if !agg.targets.is_empty() {
                agg.targets[0].filter.clone()
            } else {
                None
            };
            let target_limit = if !agg.targets.is_empty() {
                agg.targets[0].limit.clone()
            } else {
                None
            };

            match agg.aggregate_type {
                AggregateType::Count => {
                    let mut node = CountNode::new(plan, mapping.clone(), agg_index);
                    if let Some(ref filter) = target_filter {
                        node = node.with_filter(filter.clone());
                    }
                    if let Some(limit) = target_limit {
                        node = node.with_limit(limit);
                    }
                    // Add sources for explain output
                    let sources: Vec<CountSourceMeta> = agg
                        .targets
                        .iter()
                        .map(|target| CountSourceMeta {
                            field_name: if !target.host_name.is_empty() {
                                target.host_name.clone()
                            } else {
                                select.collection_name.clone()
                            },
                            filter: target.filter.clone(),
                            is_inline_array: target.field_name.is_none(),
                        })
                        .collect();
                    node = node.with_sources(sources);
                    plan = Box::new(node);
                }
                AggregateType::Sum => {
                    let mut node = SumNode::new(plan, mapping.clone(), field_index, agg_index);
                    if let Some(ref filter) = target_filter {
                        node = node.with_filter(filter.clone());
                    }
                    if let Some(limit) = target_limit {
                        node = node.with_limit(limit);
                    }
                    let sources: Vec<SumSourceMeta> = agg
                        .targets
                        .iter()
                        .map(|target| SumSourceMeta {
                            field_name: if !target.host_name.is_empty() {
                                target.host_name.clone()
                            } else {
                                select.collection_name.clone()
                            },
                            child_field_name: target.field_name.clone(),
                            filter: target.filter.clone(),
                            is_inline_array: target.field_name.is_none(),
                        })
                        .collect();
                    node = node.with_sources(sources);
                    plan = Box::new(node);
                }
                AggregateType::Average => {
                    let mut node = AverageNode::new(plan, mapping.clone(), field_index, agg_index);
                    if let Some(filter) = target_filter {
                        node = node.with_filter(filter);
                    }
                    if let Some(limit) = target_limit {
                        node = node.with_limit(limit);
                    }
                    plan = Box::new(node);
                }
                AggregateType::Min => {
                    let mut node = MinNode::new(plan, mapping.clone(), field_index, agg_index);
                    if let Some(ref filter) = target_filter {
                        node = node.with_filter(filter.clone());
                    }
                    if let Some(limit) = target_limit {
                        node = node.with_limit(limit);
                    }
                    let sources: Vec<MinSourceMeta> = agg
                        .targets
                        .iter()
                        .map(|target| MinSourceMeta {
                            field_name: if !target.host_name.is_empty() {
                                target.host_name.clone()
                            } else {
                                select.collection_name.clone()
                            },
                            child_field_name: target.field_name.clone(),
                            filter: target.filter.clone(),
                            is_inline_array: target.field_name.is_none(),
                        })
                        .collect();
                    node = node.with_sources(sources);
                    plan = Box::new(node);
                }
                AggregateType::Max => {
                    let mut node = MaxNode::new(plan, mapping.clone(), field_index, agg_index);
                    if let Some(ref filter) = target_filter {
                        node = node.with_filter(filter.clone());
                    }
                    if let Some(limit) = target_limit {
                        node = node.with_limit(limit);
                    }
                    let sources: Vec<MaxSourceMeta> = agg
                        .targets
                        .iter()
                        .map(|target| MaxSourceMeta {
                            field_name: if !target.host_name.is_empty() {
                                target.host_name.clone()
                            } else {
                                select.collection_name.clone()
                            },
                            child_field_name: target.field_name.clone(),
                            filter: target.filter.clone(),
                            is_inline_array: target.field_name.is_none(),
                        })
                        .collect();
                    node = node.with_sources(sources);
                    plan = Box::new(node);
                }
            }
        }
    }
    Ok(plan)
}
