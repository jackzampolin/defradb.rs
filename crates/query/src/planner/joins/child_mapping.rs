//! Child scan mapping enrichment for selection joins.

use schema::CollectionVersion;

use query_types::document::DocumentMapping;
use query_types::mapper::{Filter, Requestable, Select};

/// Enrich child_scan_mapping with fields needed by aggregates that target this relation.
///
/// For example, `_count(published: {filter: {rating: {_gt: 4.8}}})` needs the `rating`
/// field to be included even if it's not in the selection set.
pub(super) fn enrich_child_scan_mapping_from_aggregates(
    child_scan_mapping: &mut DocumentMapping,
    select: &Select,
    relation_field_name: &str,
    target_collection: &CollectionVersion,
) {
    // Check if aggregates reference this relation and need additional fields for filters.
    // For example, _count(published: {filter: {rating: {_gt: 4.8}}}) needs the 'rating'
    // field to be included even if it's not in the selection set.
    for requestable in &select.fields {
        if let Requestable::Aggregate(agg) = requestable {
            for target in &agg.targets {
                if target.host_name == *relation_field_name {
                    // This aggregate references this relation - add filter fields
                    if let Some(ref filter) = target.filter {
                        for filter_field in filter.referenced_fields() {
                            // Skip special fields
                            if filter_field.starts_with('_') {
                                continue;
                            }
                            // Find the field index in the target collection
                            if let Some(idx) = target_collection
                                .fields
                                .iter()
                                .position(|f| f.name == filter_field)
                            {
                                // Add the field to child_scan_mapping if not present
                                if child_scan_mapping
                                    .first_index_of_name(&filter_field)
                                    .is_none()
                                {
                                    child_scan_mapping.add(idx, &filter_field);
                                }
                                // Add render_key so the field appears in the output
                                if !child_scan_mapping
                                    .render_keys
                                    .iter()
                                    .any(|rk| rk.key == filter_field)
                                {
                                    child_scan_mapping.add_render_key(idx, &filter_field);
                                }
                            }
                        }
                    }
                    // Also add the aggregate target field if specified
                    if let Some(ref field_name) = target.field_name {
                        if let Some(idx) = target_collection
                            .fields
                            .iter()
                            .position(|f| f.name == *field_name)
                        {
                            if child_scan_mapping.first_index_of_name(field_name).is_none() {
                                child_scan_mapping.add(idx, field_name);
                            }
                            if !child_scan_mapping
                                .render_keys
                                .iter()
                                .any(|rk| rk.key == *field_name)
                            {
                                child_scan_mapping.add_render_key(idx, field_name);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Enrich child_scan_mapping with fields from a parent filter on this relation.
///
/// For example, `Author(filter: {published: {rating: {_gt: 3}}})` needs the `rating`
/// field to be included even if it's not in the selection set.
pub(super) fn enrich_child_scan_mapping_from_parent_filter(
    child_scan_mapping: &mut DocumentMapping,
    parent_filter: Option<&Filter>,
    relation_field_name: &str,
    target_collection: &CollectionVersion,
) {
    // Check if parent's filter has a relation filter for this relation.
    // If so, add those fields to child_scan_mapping so the filter can be evaluated.
    // For example, Author(filter: {published: {rating: {_gt: 3}}}) needs the 'rating'
    // field to be included even if it's not in the selection set.
    if let Some(pf) = parent_filter {
        if let Some(nested_filter) = pf.extract_relation_filter(relation_field_name) {
            for filter_field in nested_filter.referenced_fields() {
                // Skip special fields
                if filter_field.starts_with('_') {
                    continue;
                }
                // Find the field index in the target collection
                if let Some(idx) = target_collection
                    .fields
                    .iter()
                    .position(|f| f.name == filter_field)
                {
                    // Add the field to child_scan_mapping if not present
                    if child_scan_mapping
                        .first_index_of_name(&filter_field)
                        .is_none()
                    {
                        child_scan_mapping.add(idx, &filter_field);
                    }
                    // Add render_key so the field appears in the output
                    if !child_scan_mapping
                        .render_keys
                        .iter()
                        .any(|rk| rk.key == filter_field)
                    {
                        child_scan_mapping.add_render_key(idx, &filter_field);
                    }
                }
            }
        }
    }
}

/// Enrich child_scan_mapping with fields from parent ORDER BY on this relation.
///
/// Fields are added as render_keys so they're available in the merged JSON for ordering.
/// They won't appear in the final output unless they're also in the selection set.
pub(super) fn enrich_child_scan_mapping_from_order_by(
    child_scan_mapping: &mut DocumentMapping,
    select: &Select,
    relation_field_name: &str,
) {
    // Check if parent's order_by references fields in this relation.
    // If so, add those fields to child_scan_mapping.render_keys so they're
    // available in the merged JSON for ordering. The fields won't appear in
    // the final output unless they're also in the selection set.
    if let Some(ref order_by) = select.order_by {
        for condition in &order_by.conditions {
            // Check if this order condition starts with this relation field
            if condition.fields.len() > 1 && condition.fields[0] == *relation_field_name {
                // Get the nested field name (e.g., "verified" from ["author", "verified"])
                let nested_field = &condition.fields[1];
                // Find the schema index for this field
                if let Some(idx) = child_scan_mapping.first_index_of_name(nested_field) {
                    // Add render_key if not already present
                    if !child_scan_mapping
                        .render_keys
                        .iter()
                        .any(|rk| rk.key == *nested_field)
                    {
                        child_scan_mapping.add_render_key(idx, nested_field);
                    }
                }
            }
        }
    }
}

/// Multi-level filter paths starting with this relation, with the leading segment stripped.
pub(super) fn multi_level_paths_for_relation(
    select: &Select,
    relation_field_name: &str,
) -> Vec<Vec<String>> {
    // Check if parent's filter has multi-level paths starting with this relation.
    // If so, we need to add nested relation fields to the child mapping and build
    // sub-joins for them so the filter can be evaluated on the merged document.
    select
        .filter
        .as_ref()
        .map(|f| {
            f.get_multi_level_relation_paths()
                .into_iter()
                .filter(|path| {
                    path.first()
                        .is_some_and(|first| first == relation_field_name)
                })
                .map(|path| path[1..].to_vec()) // Get remaining path after this relation
                .filter(|remaining| !remaining.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// Add render_keys for nested relation fields needed by multi-level filters.
pub(super) fn enrich_child_scan_mapping_from_multi_level_paths(
    child_scan_mapping: &mut DocumentMapping,
    multi_level_paths_for_relation: &[Vec<String>],
) {
    // Add render_keys for nested relation fields needed by multi-level filters.
    // The relation filter in check_relation_filter evaluates via
    // filter.matches(child.fields(), child_mapping), and the field value
    // at publisher_index is the rendered JSON from TypeJoinOne. The filter
    // needs the mapping to have the field indexed but the render_key is
    // needed for the relation filter to find the publisher JSON object.
    for remaining_path in multi_level_paths_for_relation {
        if let Some(first_nested) = remaining_path.first() {
            if let Some(idx) = child_scan_mapping.first_index_of_name(first_nested) {
                if !child_scan_mapping
                    .render_keys
                    .iter()
                    .any(|rk| rk.key == *first_nested)
                {
                    child_scan_mapping.add_render_key(idx, first_nested);
                }
            }
        }
    }
}

/// Ensure BM25 target fields (and local order fields used with them) are temporarily
/// rendered so nested relation-local BM25 rescoring can run after joins.
pub(super) fn enrich_child_scan_mapping_for_deferred_scoped_fulltext(
    child_scan_mapping: &mut DocumentMapping,
    nested_select: &Select,
    has_deferred_scoped_fulltext: bool,
) {
    // Nested relation-local BM25 rescoring runs after joins over the rendered child JSON.
    // Ensure the BM25 target fields, and any local order fields used with them, are
    // temporarily rendered so the runner can rescore and reorder the relation scope.
    if !has_deferred_scoped_fulltext {
        return;
    }

    for requestable in &nested_select.fields {
        let Requestable::FullTextSearch(fts) = requestable else {
            continue;
        };
        if !fts.target_fields.iter().all(|field| !field.contains('.')) {
            continue;
        }

        for target_field in &fts.target_fields {
            if let Some(idx) = child_scan_mapping.first_index_of_name(target_field) {
                if !child_scan_mapping
                    .render_keys
                    .iter()
                    .any(|rk| rk.key == *target_field)
                {
                    child_scan_mapping.add_render_key(idx, target_field);
                }
            }
        }
    }

    if let Some(order_by) = &nested_select.order_by {
        for condition in &order_by.conditions {
            if condition.fields.len() != 1 {
                continue;
            }

            let field_name = &condition.fields[0];
            if let Some(idx) = child_scan_mapping.first_index_of_name(field_name) {
                if !child_scan_mapping
                    .render_keys
                    .iter()
                    .any(|rk| rk.key == *field_name)
                {
                    child_scan_mapping.add_render_key(idx, field_name);
                }
            }
        }
    }
}

/// Whether nested relation-local BM25 rescoring should run after joins.
pub(super) fn has_deferred_scoped_fulltext(nested_select: &Select) -> bool {
    nested_select.group_by.is_none()
        && nested_select
            .filter
            .as_ref()
            .map(|filter| !filter.has_alias_filter())
            .unwrap_or(true)
        && nested_select.fields.iter().any(|requestable| {
            matches!(
                requestable,
                Requestable::FullTextSearch(fts)
                    if fts.target_fields.iter().all(|field| !field.contains('.'))
            )
        })
}
