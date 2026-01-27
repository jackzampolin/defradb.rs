//! Plan building utilities for QueryRunner.

use schema::CollectionVersion;
use serde_json::{Map, Value as JsonValue};

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{AggregateType, Requestable, Select};
use crate::plan::{
    AllDocsNode, AverageNode, CountNode, GroupByNode, LimitNode, MaxNode, MinNode, OrderByNode,
    ScanNode, SelectNode, SumNode,
};
use crate::planner::{Doc, PlanNode};

/// Validate that the select doesn't use unsupported features.
pub(crate) fn validate_select(select: &Select, collection: &CollectionVersion) -> Result<()> {
    if select.cid.is_some() {
        return Err(QueryError::execution(
            "CID-based queries are not yet implemented; remove the 'cid' argument",
        ));
    }

    // Note: Nested selections (relations) are now supported via the Planner

    // Helper to check if a field exists in the collection schema
    // Special fields: _docID (document ID), _group (groupBy results)
    let field_exists = |name: &str| -> bool {
        name == "_docID" || name == "_group" || collection.fields.iter().any(|f| f.name == name)
    };

    // Validate that all requested simple fields exist in schema
    for requestable in &select.fields {
        if let Requestable::Field(field) = requestable {
            if !field_exists(&field.name) {
                return Err(QueryError::unknown_field(format!(
                    "Cannot query field \"{}\" on type \"{}\".",
                    field.name, select.collection_name
                )));
            }
        }
    }

    // Validate aggregate target fields exist in schema
    // Note: For relation-based aggregates (e.g., _sum(books: {field: score})),
    // the field belongs to the related collection, not the current one.
    // We skip validation here; it will be checked during execution.
    for requestable in &select.fields {
        if let Requestable::Aggregate(agg) = requestable {
            for target in &agg.targets {
                if let Some(ref field_name) = target.field_name {
                    // Only validate fields on the current collection (no host_name or empty)
                    // Relation-based aggregates have a non-empty host_name
                    let is_relation_aggregate = !target.host_name.is_empty()
                        && collection.fields.iter().any(|f| f.name == target.host_name);
                    if !is_relation_aggregate && !field_exists(field_name) {
                        return Err(QueryError::unknown_field(format!(
                            "aggregate target field '{}' not found in collection '{}'",
                            field_name, select.collection_name
                        )));
                    }
                }
            }
        }
    }

    // Validate GROUP BY fields exist in schema
    if let Some(ref group_by) = select.group_by {
        for field_name in &group_by.fields {
            if !field_exists(field_name) {
                return Err(QueryError::unknown_field(format!(
                    "GROUP BY field '{}' not found in collection '{}'",
                    field_name, select.collection_name
                )));
            }
        }
    }

    Ok(())
}

/// Build the document mapping for a select operation.
pub(crate) fn build_mapping(
    select: &Select,
    collection: &CollectionVersion,
) -> Result<DocumentMapping> {
    let mut mapping = DocumentMapping::new();

    // Add requested fields and aggregates
    for requestable in &select.fields {
        match requestable {
            Requestable::Field(field) => {
                let index = mapping.next_index();
                mapping.add(index, &field.name);
                mapping.add_render_key(index, field.output_name());
            }
            Requestable::Aggregate(agg) => {
                let index = mapping.next_index();
                let name = agg.aggregate_type.as_str();
                mapping.add(index, name);
                // Use alias if provided, otherwise use the aggregate name
                mapping.add_render_key(index, agg.output_name());
            }
            Requestable::Select(nested) => {
                // This code path should not be reached - nested selections should
                // be routed to execute_nested_select_with_planner. If we get here,
                // it indicates a bug in query routing.
                return Err(QueryError::internal(format!(
                    "Unexpected nested select '{}' in simple query path - \
                     this indicates a bug in query routing",
                    nested.field.name
                )));
            }
        }
    }

    // Add fields referenced by the filter (but not selected)
    // These are needed for filter evaluation but won't be rendered
    if let Some(ref filter) = select.filter {
        for field_name in filter.referenced_fields() {
            if mapping.first_index_of_name(&field_name).is_none() {
                let index = mapping.next_index();
                mapping.add(index, &field_name);
                // Don't add render_key - we don't want to output these fields
            }
        }
    }

    // Add GROUP BY fields (they need to be in mapping for grouping)
    if let Some(ref group_by) = select.group_by {
        for field_name in &group_by.fields {
            if mapping.first_index_of_name(field_name).is_none() {
                let index = mapping.next_index();
                mapping.add(index, field_name);
                // Don't add render_key - they may or may not be selected
            }
        }
    }

    // Add aggregate target fields (needed for aggregation but not rendered)
    for requestable in &select.fields {
        if let Requestable::Aggregate(agg) = requestable {
            for target in &agg.targets {
                if let Some(ref field_name) = target.field_name {
                    if mapping.first_index_of_name(field_name).is_none() {
                        let index = mapping.next_index();
                        mapping.add(index, field_name);
                        // Don't add render_key - we don't want to output these fields
                    }
                }
            }
        }
    }

    // Add ORDER BY fields if not already present (Go compatibility).
    // Go DefraDB allows ordering by fields not in the SELECT clause.
    if let Some(ref order_by) = select.order_by {
        for condition in &order_by.conditions {
            if let Some(field_name) = condition.fields.first() {
                if mapping.first_index_of_name(field_name).is_none() {
                    let index = mapping.next_index();
                    mapping.add(index, field_name);
                    // Don't add render_key - we don't want to output these fields
                }
            }
        }
    }

    // If no fields specified, add all from collection
    if mapping.next_index() == 0 {
        for (i, field) in collection.fields.iter().enumerate() {
            mapping.add(i, &field.name);
            mapping.add_render_key(i, &field.name);
        }
    }

    Ok(mapping)
}

/// Build a plan tree from a Select operation and documents.
pub(crate) fn build_plan(
    select: &Select,
    docs: Vec<Doc>,
    mapping: DocumentMapping,
    collection: &CollectionVersion,
) -> Result<Box<dyn PlanNode>> {
    // Create ScanNode with preloaded documents
    let scan = ScanNode::new(collection.clone(), mapping.clone())
        .with_docs(docs)
        .with_show_deleted(select.show_deleted);

    let mut plan: Box<dyn PlanNode> = Box::new(scan);

    // Add SelectNode for filtering
    if let Some(ref filter) = select.filter {
        let select_node = SelectNode::new(plan, mapping.clone()).with_filter(filter.clone());
        plan = Box::new(select_node);
    }

    // Check if we have GROUP BY
    let has_group_by = select.group_by.is_some();

    if has_group_by {
        // WITH GROUP BY: GroupByNode → Aggregates → OrderBy → Limit

        // Add GroupByNode
        if let Some(ref group_by) = select.group_by {
            plan = Box::new(GroupByNode::new(plan, group_by.clone(), mapping.clone()));
        }

        // Add aggregate nodes
        plan = add_aggregate_nodes(plan, select, &mapping)?;

        // Add OrderByNode for sorting (after grouping/aggregation)
        if let Some(ref order_by) = select.order_by {
            plan = Box::new(OrderByNode::new(plan, order_by.clone(), mapping.clone()));
        }

        // Add LimitNode
        // Note: limit=0 means "no limit" in Go DefraDB, so we convert Some(0) to None
        if let Some(ref limit) = select.limit {
            let effective_limit = match limit.limit {
                Some(0) => None,
                other => other,
            };
            if effective_limit.is_some() || limit.offset > 0 {
                plan = Box::new(LimitNode::new(plan, effective_limit, limit.offset));
            }
        }
    } else {
        // WITHOUT GROUP BY: OrderBy → Limit → [AllDocs if multiple aggs] → Aggregates

        // Add OrderByNode for sorting (after filtering, before limit)
        if let Some(ref order_by) = select.order_by {
            plan = Box::new(OrderByNode::new(plan, order_by.clone(), mapping.clone()));
        }

        // Add LimitNode
        // Note: limit=0 means "no limit" in Go DefraDB, so we convert Some(0) to None
        if let Some(ref limit) = select.limit {
            let effective_limit = match limit.limit {
                Some(0) => None,
                other => other,
            };
            if effective_limit.is_some() || limit.offset > 0 {
                plan = Box::new(LimitNode::new(plan, effective_limit, limit.offset));
            }
        }

        // Count aggregates to determine if we need AllDocsNode
        let aggregate_count = select
            .fields
            .iter()
            .filter(|f| matches!(f, Requestable::Aggregate(_)))
            .count();

        // If there are multiple aggregates, wrap in AllDocsNode so they all
        // can access the original documents via current_group_docs()
        if aggregate_count > 1 {
            plan = Box::new(AllDocsNode::new(plan, mapping.clone()));
        }

        // Add aggregate nodes
        // Without GROUP BY, aggregates return a single row for the entire result
        plan = add_aggregate_nodes(plan, select, &mapping)?;
    }

    Ok(plan)
}

/// Add aggregate nodes to the plan based on the select's aggregate fields.
fn add_aggregate_nodes(
    mut plan: Box<dyn PlanNode>,
    select: &Select,
    mapping: &DocumentMapping,
) -> Result<Box<dyn PlanNode>> {
    for field in &select.fields {
        if let Requestable::Aggregate(agg) = field {
            // Get the index where the aggregate result should be stored
            // Use the aggregate type name for lookup (that's how it's registered in mapping)
            let agg_type_name = agg.aggregate_type.as_str();
            let agg_index = mapping.first_index_of_name(agg_type_name).ok_or_else(|| {
                QueryError::internal(format!(
                    "aggregate '{}' not found in document mapping - this is a bug",
                    agg_type_name
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

            match agg.aggregate_type {
                AggregateType::Count => {
                    plan = Box::new(CountNode::new(plan, mapping.clone(), agg_index));
                }
                AggregateType::Sum => {
                    plan = Box::new(SumNode::new(plan, mapping.clone(), field_index, agg_index));
                }
                AggregateType::Average => {
                    plan = Box::new(AverageNode::new(
                        plan,
                        mapping.clone(),
                        field_index,
                        agg_index,
                    ));
                }
                AggregateType::Min => {
                    plan = Box::new(MinNode::new(plan, mapping.clone(), field_index, agg_index));
                }
                AggregateType::Max => {
                    plan = Box::new(MaxNode::new(plan, mapping.clone(), field_index, agg_index));
                }
            }
        }
    }
    Ok(plan)
}

/// Convert a plan Doc to JSON for output.
pub(crate) fn doc_to_json(doc: &Doc, mapping: &DocumentMapping) -> Result<JsonValue> {
    let mut obj = Map::new();

    for render_key in &mapping.render_keys {
        let value = doc
            .fields()
            .get(render_key.index)
            .cloned()
            .flatten()
            .unwrap_or(JsonValue::Null);
        obj.insert(render_key.key.clone(), value);
    }

    Ok(JsonValue::Object(obj))
}
