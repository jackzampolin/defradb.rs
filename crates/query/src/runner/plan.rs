//! Plan building utilities for QueryRunner.

use std::sync::Arc;

use acp::{DocumentACP, Identity};
use schema::CollectionVersion;
use serde_json::{Map, Value as JsonValue};

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::fetcher::DocFetcher;
use crate::limits::QueryLimits;
use crate::mapper::{AggregateType, Filter, Requestable, Select};
use crate::plan::{
    AllDocsNode, ChildSelectMeta, GroupAlias, GroupByNode, LimitNode, OrderByNode,
    PermissionFilterNode, ScanNode, SelectNode,
};
use crate::planner::{Doc, PlanNode};

pub(crate) use super::plan_aggregates::add_aggregate_nodes;
pub(crate) use super::plan_validation::validate_select;

/// ACP filter configuration for inserting PermissionFilterNode into plan trees.
pub(crate) struct AcpFilter {
    pub acp: Arc<dyn DocumentACP>,
    pub identity: Identity,
    pub policy_id: String,
    pub resource_name: String,
}

/// Build the document mapping for a select operation.
pub(crate) fn build_mapping(
    select: &Select,
    collection: &CollectionVersion,
) -> Result<DocumentMapping> {
    let mut mapping = DocumentMapping::new();

    // ALWAYS reserve index 0 for _docID (required for Doc::doc_id() to work).
    // Only add a render key if _docID was explicitly requested in the query.
    mapping.add(0, "_docID");
    for requestable in &select.fields {
        if let Requestable::Field(field) = requestable {
            if field.name == "_docID" {
                mapping.add_render_key(0, field.output_name());
                break;
            }
        }
    }

    // Add requested fields and aggregates (starting from index 1)
    for requestable in &select.fields {
        match requestable {
            Requestable::Field(field) => {
                // Skip _docID (already handled at index 0)
                if field.name == "_docID" {
                    continue;
                }
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
            Requestable::Similarity(sim) => {
                let index = mapping.next_index();
                mapping.add(index, "SIMILARITY");
                mapping.add_render_key(index, sim.output_name());
            }
            Requestable::FullTextSearch(fts) => {
                let index = mapping.next_index();
                mapping.add(index, "BM25");
                mapping.add_render_key(index, fts.output_name());
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
                // And the target's groupBy fields, needed to build group keys
                // for COUNT(Users: {groupBy: [Age]}). Relation targets group
                // against their own collection, resolved in the join builder.
                if target.host_name.is_empty() || target.host_name == collection.name {
                    if let Some(ref group_by) = target.group_by {
                        for field_name in group_by.resolved_fields(collection)? {
                            if mapping.first_index_of_name(&field_name).is_none() {
                                let index = mapping.next_index();
                                mapping.add(index, field_name);
                                // Don't add render_key - we don't want to output these fields
                            }
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

    // If no fields specified (only _docID reserved at index 0, no render keys),
    // add all collection fields. This handles the "SELECT *" case.
    // Note: if _docID was explicitly requested, render_keys won't be empty.
    if mapping.next_index() == 1 && mapping.render_keys.is_empty() {
        for field in &collection.fields {
            let index = mapping.next_index();
            mapping.add(index, &field.name);
            mapping.add_render_key(index, &field.name);
        }
    }

    Ok(mapping)
}

/// Where a plan's `ScanNode` gets its documents.
pub(crate) enum ScanSource {
    /// Pre-materialized documents: docID lookups, index scans, and explain paths.
    Docs(Vec<Doc>),
    /// A fetcher the scan streams from, so a bounded query stops the fetch.
    Fetcher(Arc<dyn DocFetcher>),
}

/// Build a plan tree from a Select operation and a document source.
///
/// If `acp_filter` is provided, a PermissionFilterNode is inserted after SelectNode
/// but before any OrderBy/Limit/Aggregate nodes. This ensures aggregates operate
/// on ACP-filtered documents rather than the full set.
pub(crate) fn build_plan(
    select: &Select,
    source: ScanSource,
    mapping: DocumentMapping,
    collection: &CollectionVersion,
    acp_filter: Option<AcpFilter>,
    query_limits: QueryLimits,
) -> Result<Box<dyn PlanNode>> {
    // Materialized ID lookups have already resolved aliases to canonical IDs.
    // Rechecking the input aliases would discard those documents.
    let scan_doc_ids = match &source {
        ScanSource::Docs(docs) if select.doc_ids.is_some() && !docs.is_empty() => Some(
            docs.iter()
                .filter_map(|doc| doc.doc_id().map(str::to_owned))
                .collect(),
        ),
        _ => select.doc_ids.clone(),
    };

    // Create ScanNode with a document source, filter, and docIDs
    let scan = ScanNode::new(collection.clone(), mapping.clone());
    let mut scan = match source {
        ScanSource::Docs(docs) => scan.with_docs(docs),
        ScanSource::Fetcher(fetcher) => scan.with_fetcher(fetcher),
    }
    .with_show_deleted(select.show_deleted);

    // Pass filter and docIDs to ScanNode for explain output
    // First check select.filter, then fall back to aggregate target filter
    let mut filter_for_scan = select.filter.clone().or_else(|| {
        // For top-level aggregates, the filter might be on the aggregate target
        select.fields.iter().find_map(|f| {
            if let Requestable::Aggregate(agg) = f {
                if !agg.targets.is_empty() {
                    return agg.targets[0].filter.clone();
                }
            }
            None
        })
    });

    // For top-level average with a field, Go adds {field: {_neq: null}} to exclude nulls.
    // Merge this into the existing filter on the same field (not via _and wrapper).
    for field in &select.fields {
        if let Requestable::Aggregate(agg) = field {
            if agg.aggregate_type == AggregateType::Average {
                if let Some(field_name) = agg.targets.first().and_then(|t| t.field_name.as_ref()) {
                    filter_for_scan = Some(match filter_for_scan {
                        Some(existing) => {
                            // Merge {field: {_neq: null}} into existing conditions
                            let mut merged = existing.conditions().clone();
                            merged
                                .entry(field_name.clone())
                                .and_modify(|v| {
                                    if let serde_json::Value::Object(ref mut ops) = v {
                                        ops.insert("_neq".to_string(), serde_json::Value::Null);
                                    }
                                })
                                .or_insert(serde_json::json!({"_neq": serde_json::Value::Null}));
                            Filter::from_conditions_with_max_depth(merged, existing.max_depth())
                        }
                        None => {
                            let mut conditions = serde_json::Map::new();
                            conditions.insert(
                                field_name.clone(),
                                serde_json::json!({"_neq": serde_json::Value::Null}),
                            );
                            Filter::from_conditions_with_max_depth(
                                conditions,
                                query_limits.max_filter_depth,
                            )
                        }
                    });
                }
            }
        }
    }
    if let Some(ref filter) = filter_for_scan {
        scan = scan.with_filter(filter.clone());
    }
    if let Some(doc_ids) = scan_doc_ids {
        scan = scan.with_doc_ids(doc_ids);
    }

    let mut plan: Box<dyn PlanNode> = Box::new(scan);

    // Add SelectNode (Go always wraps in selectNode, even without a filter)
    // Use the same filter we used for scanNode
    let mut select_node = if let Some(ref filter) = filter_for_scan {
        SelectNode::new(plan, mapping.clone()).with_filter(filter.clone())
    } else {
        SelectNode::new(plan, mapping.clone())
    };
    // Pass doc_ids to SelectNode for explain output
    if let Some(ref doc_ids) = select.doc_ids {
        select_node = select_node.with_doc_ids(doc_ids.clone());
    }
    plan = Box::new(select_node);

    // Insert ACP permission filter after Select, before OrderBy/Limit/Aggregates.
    // This ensures aggregates (count, average, etc.) operate on filtered documents.
    if let Some(acf) = acp_filter {
        plan = Box::new(PermissionFilterNode::new(
            plan,
            acf.acp,
            acf.identity,
            acf.policy_id,
            acf.resource_name,
        ));
    }

    // Check if we have GROUP BY
    let has_group_by = select.group_by.is_some();

    if has_group_by {
        // WITH GROUP BY: GroupByNode -> Aggregates -> OrderBy -> Limit

        // Add GroupByNode
        if let Some(ref group_by) = select.group_by {
            let mut group_node = GroupByNode::new(plan, group_by.clone(), mapping.clone())
                .with_collection_name(select.collection_name.clone());
            // Extract _group alias definitions with per-alias filter/limit/order
            let group_indices = mapping
                .indexes_of_name("GROUP")
                .map(|s| s.to_vec())
                .unwrap_or_default();
            let mut group_aliases = Vec::new();
            let mut alias_count = 0;
            let mut child_selects_meta: Vec<ChildSelectMeta> = Vec::new();
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
            if !group_aliases.is_empty() {
                group_node = group_node.with_group_aliases(group_aliases);
            }
            // Go adds {field: {_neq: null}} to childSelects for average aggregates.
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
                if child_selects_meta.is_empty() {
                    child_selects_meta.push(ChildSelectMeta {
                        collection_name: select.collection_name.clone(),
                        ..Default::default()
                    });
                }
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
                        let max_depth = cs
                            .filter
                            .as_ref()
                            .map(Filter::max_depth)
                            .unwrap_or(query_limits.max_filter_depth);
                        cs.filter = Some(Filter::from_conditions_with_max_depth(
                            conditions, max_depth,
                        ));
                    }
                }
            }
            if !child_selects_meta.is_empty() {
                group_node = group_node.with_child_selects(child_selects_meta);
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
        // WITHOUT GROUP BY: OrderBy -> [AllDocs if multiple aggs] -> Aggregates -> Limit
        // Go applies limit AFTER aggregates so limit restricts the final output,
        // not the documents fed to aggregation.

        // Add OrderByNode for sorting (after filtering, before aggregates)
        if let Some(ref order_by) = select.order_by {
            plan = Box::new(OrderByNode::new(plan, order_by.clone(), mapping.clone()));
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

        // Add LimitNode (AFTER aggregates, matching Go behavior)
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
