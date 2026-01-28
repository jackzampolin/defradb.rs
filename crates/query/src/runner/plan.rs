//! Plan building utilities for QueryRunner.

use schema::CollectionVersion;
use serde_json::{Map, Value as JsonValue};

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{AggregateType, Requestable, Select};
use crate::plan::{
    AllDocsNode, AverageNode, CountNode, GroupAlias, GroupByNode, LimitNode, MaxNode, MinNode,
    OrderByNode, ScanNode, SelectNode, SumNode,
};
use crate::planner::{Doc, PlanNode};

/// Validate that the select doesn't use unsupported features.
pub(crate) fn validate_select(select: &Select, collection: &CollectionVersion) -> Result<()> {
    // Note: CID-based queries are now handled by execute_cid_query() before this validation

    // Note: Nested selections (relations) are now supported via the Planner

    // Helper to check if a field exists in the collection schema
    // Special fields: _docID (document ID), _group (groupBy results), __typename (GraphQL introspection),
    // _version (CRDT version metadata)
    let field_exists = |name: &str| -> bool {
        name == "_docID"
            || name == "_group"
            || name == "__typename"
            || name == "_version"
            || collection.fields.iter().any(|f| f.name == name)
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
                    // Skip validation for:
                    // 1. Relation-based aggregates (non-empty host_name that's a relation field)
                    // 2. _group aggregates (host_name is "_group") - targets grouped results
                    // 3. Nested aggregates (field_name starts with "_") - targets other aggregate results
                    let is_relation_aggregate = !target.host_name.is_empty()
                        && collection.fields.iter().any(|f| f.name == target.host_name);
                    let is_group_aggregate = target.host_name == "_group";
                    let is_nested_aggregate = field_name.starts_with('_');

                    if !is_relation_aggregate
                        && !is_group_aggregate
                        && !is_nested_aggregate
                        && !field_exists(field_name)
                    {
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

        // Validate that non-special fields selected at group level are in the groupBy list
        let group_fields: Vec<&str> = group_by.fields.iter().map(|s| s.as_str()).collect();
        for requestable in &select.fields {
            match requestable {
                Requestable::Field(field) => {
                    let name = field.name.as_str();
                    // Skip special fields
                    if name == "_docID" || name == "_group" || name == "__typename" {
                        continue;
                    }
                    if group_fields.contains(&name) {
                        continue;
                    }
                    // Allow FK fields for relation groupBy fields (e.g. _authorID for author)
                    let is_fk_for_group = group_fields
                        .iter()
                        .any(|gb_field| name == format!("_{}ID", gb_field));
                    if is_fk_for_group {
                        continue;
                    }
                    return Err(QueryError::parse(
                        "cannot select a non-group-by field at group-level",
                    ));
                }
                Requestable::Select(nested) => {
                    if nested.field.name == "_group" {
                        // _group is always allowed in groupBy queries
                        continue;
                    }
                }
                Requestable::Aggregate(_) => {
                    // Aggregates are allowed at group level
                }
            }
        }
    }

    // Validate _group references only appear within groupBy context
    let has_group_by = select.group_by.is_some();
    for requestable in &select.fields {
        // Check for _count(_group: {}) or similar aggregates referencing _group
        if let Requestable::Aggregate(agg) = requestable {
            for target in &agg.targets {
                if target.host_name == "_group" && !has_group_by {
                    return Err(QueryError::parse(
                        "_group may only be referenced when within a groupBy request",
                    ));
                }
            }
        }

        // Check for _group references inside nested _group selections
        if let Requestable::Select(nested) = requestable {
            if nested.field.name == "_group" {
                for inner in &nested.fields {
                    if let Requestable::Aggregate(inner_agg) = inner {
                        for target in &inner_agg.targets {
                            if target.host_name == "_group" && nested.group_by.is_none() {
                                return Err(QueryError::parse(
                                    "_group may only be referenced when within a groupBy request",
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    // Validate bare aggregates have a property to aggregate
    for requestable in &select.fields {
        if let Requestable::Aggregate(agg) = requestable {
            if agg.targets.is_empty() {
                return Err(QueryError::parse(
                    "aggregate must be provided with a property to aggregate",
                ));
            }
        }
    }

    // Validate top-level filter field names exist in schema
    if let Some(ref filter) = select.filter {
        for key in filter.conditions().keys() {
            // Skip logical operators and special filter directives
            if key == "_and" || key == "_or" || key == "_not" || key == "_alias" {
                continue;
            }
            if !field_exists(key) {
                let filter_repr = format_graphql_conditions(filter.conditions());
                return Err(QueryError::parse(format!(
                    "Argument \"filter\" has invalid value {}.\nIn field \"{}\": Unknown field.",
                    filter_repr, key
                )));
            }
        }
    }

    Ok(())
}

/// Format filter conditions in Go graphql-go style (unquoted keys).
fn format_graphql_conditions(conditions: &std::collections::HashMap<String, JsonValue>) -> String {
    let entries: Vec<String> = conditions
        .iter()
        .map(|(k, v)| format!("{}: {}", k, format_graphql_value(v)))
        .collect();
    format!("{{{}}}", entries.join(", "))
}

/// Format a JSON value in Go graphql-go style.
fn format_graphql_value(val: &JsonValue) -> String {
    match val {
        JsonValue::Object(obj) => {
            let entries: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("{}: {}", k, format_graphql_value(v)))
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
        JsonValue::String(s) => format!("\"{}\"", s),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null => "null".to_string(),
        JsonValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_graphql_value).collect();
            format!("[{}]", items.join(", "))
        }
    }
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
                // Handle __typename for GraphQL introspection
                if field.name == "__typename" {
                    mapping.set_type_name(&select.collection_name);
                    let index = mapping.first_index_of_name("__typename").unwrap();
                    mapping.add_render_key(index, field.output_name());
                    continue;
                }
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
                // Also add fields referenced by the aggregate target's filter
                // This is needed for top-level aggregates like _count(Users: {filter: {Age: {_gt: 26}}})
                if let Some(ref filter) = target.filter {
                    for field_name in filter.referenced_fields() {
                        if mapping.first_index_of_name(&field_name).is_none() {
                            let index = mapping.next_index();
                            mapping.add(index, &field_name);
                            // Don't add render_key - we don't want to output these fields
                        }
                    }
                }
            }
        }
    }

    // Add ORDER BY fields if not already present (Go compatibility).
    // Go DefraDB allows ordering by fields not in the SELECT clause.
    // But for _alias directive ORDER BY, we should NOT add alias names as fields -
    // they must already exist in render_keys from selected aliased fields.
    if let Some(ref order_by) = select.order_by {
        for condition in &order_by.conditions {
            if let Some(field_name) = condition.fields.first() {
                // Skip if already in mapping (by name or as render_key/alias)
                if mapping.first_index_of_name(field_name).is_some()
                    || mapping.try_find_index_from_render_key(field_name).is_some()
                {
                    continue;
                }
                // Only add if it's a valid schema field (not an alias name)
                // This prevents adding non-existent alias names like "UserAge"
                // when the query uses _alias directive with a non-existent alias
                if collection.fields.iter().any(|f| f.name == *field_name) {
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

    // Add SelectNode (Go always wraps in selectNode, even without a filter)
    let select_node = if let Some(ref filter) = select.filter {
        SelectNode::new(plan, mapping.clone()).with_filter(filter.clone())
    } else {
        SelectNode::new(plan, mapping.clone())
    };
    plan = Box::new(select_node);

    // Check if we have GROUP BY
    let has_group_by = select.group_by.is_some();

    if has_group_by {
        // WITH GROUP BY: GroupByNode → Aggregates → OrderBy → Limit

        // Add GroupByNode
        if let Some(ref group_by) = select.group_by {
            let mut group_node = GroupByNode::new(plan, group_by.clone(), mapping.clone());
            // Extract _group alias definitions with per-alias filter/limit/order
            let group_indices = mapping
                .indexes_of_name("_group")
                .map(|s| s.to_vec())
                .unwrap_or_default();
            let mut group_aliases = Vec::new();
            let mut alias_count = 0;
            for field in &select.fields {
                if let Requestable::Select(nested) = field {
                    if nested.field.name == "_group" {
                        let alias_index = group_indices.get(alias_count).copied().unwrap_or(0);
                        alias_count += 1;
                        group_aliases.push(GroupAlias {
                            index: alias_index,
                            filter: nested.filter.clone(),
                            limit: nested.limit.clone(),
                            order: nested.order_by.clone(),
                            doc_ids: nested.doc_ids.clone(),
                        });
                    }
                }
            }
            if !group_aliases.is_empty() {
                group_node = group_node.with_group_aliases(group_aliases);
            }
            plan = Box::new(group_node);
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
                    if let Some(filter) = target_filter {
                        node = node.with_filter(filter);
                    }
                    if let Some(limit) = target_limit {
                        node = node.with_limit(limit);
                    }
                    plan = Box::new(node);
                }
                AggregateType::Sum => {
                    let mut node = SumNode::new(plan, mapping.clone(), field_index, agg_index);
                    if let Some(filter) = target_filter {
                        node = node.with_filter(filter);
                    }
                    if let Some(limit) = target_limit {
                        node = node.with_limit(limit);
                    }
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
                    if let Some(filter) = target_filter {
                        node = node.with_filter(filter);
                    }
                    if let Some(limit) = target_limit {
                        node = node.with_limit(limit);
                    }
                    plan = Box::new(node);
                }
                AggregateType::Max => {
                    let mut node = MaxNode::new(plan, mapping.clone(), field_index, agg_index);
                    if let Some(filter) = target_filter {
                        node = node.with_filter(filter);
                    }
                    if let Some(limit) = target_limit {
                        node = node.with_limit(limit);
                    }
                    plan = Box::new(node);
                }
            }
        }
    }
    Ok(plan)
}

/// Convert a plan Doc to JSON for output.
///
/// Special handling for `__typename`: if the mapping has type_info set and the
/// render_key index matches the __typename field index, the stored type name is used.
pub(crate) fn doc_to_json(doc: &Doc, mapping: &DocumentMapping) -> Result<JsonValue> {
    let mut obj = Map::new();

    // Get the __typename index and name if set
    let typename_info = mapping
        .first_index_of_name("__typename")
        .and_then(|idx| mapping.type_name().map(|name| (idx, name.to_string())));

    // Get the _deleted index if present
    let deleted_index = mapping.first_index_of_name("_deleted");

    for render_key in &mapping.render_keys {
        // Check for _deleted special handling
        let value = if Some(render_key.index) == deleted_index && render_key.key == "_deleted" {
            JsonValue::Bool(doc.is_deleted())
        } else if let Some((typename_idx, ref typename)) = typename_info {
            if render_key.index == typename_idx {
                // Return the stored type name for __typename
                JsonValue::String(typename.clone())
            } else {
                doc.fields()
                    .get(render_key.index)
                    .cloned()
                    .flatten()
                    .unwrap_or(JsonValue::Null)
            }
        } else {
            doc.fields()
                .get(render_key.index)
                .cloned()
                .flatten()
                .unwrap_or(JsonValue::Null)
        };
        obj.insert(render_key.key.clone(), value);
    }

    Ok(JsonValue::Object(obj))
}
