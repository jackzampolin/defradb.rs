//! Filter preparation for query planning.
//!
//! Splits and transforms the query filter into scalar, relation, and plan-level
//! components for use in different plan tree positions.

use schema::CollectionVersion;
use serde_json::{Map, Value as JsonValue};

use query_types::mapper::{Filter, Select};

/// Result of filter preparation: pre-processed filter components for plan construction.
pub(in crate::planner) struct FilterParts {
    pub(in crate::planner) scalar_filter: Option<Filter>,
    pub(in crate::planner) relation_filter: Option<Filter>,
    pub(in crate::planner) filter_for_plan: Option<Filter>,
    pub(in crate::planner) is_complex_filter: bool,
}

impl super::Planner {
    /// Prepare filter components for plan construction.
    ///
    /// Strips aggregate alias conditions, merges doc_ids, detects complexity,
    /// splits into scalar/relation parts, and transforms relation _docID patterns
    /// to use FK fields.
    pub(in crate::planner) fn prepare_filter(
        &self,
        select: &Select,
        collection: &CollectionVersion,
        computed_field_names: &[&str],
    ) -> FilterParts {
        // Strip _alias conditions that reference computed fields (aggregates/similarity) from the filter.
        // These must be evaluated after the computed fields are set, not during plan execution.
        let filter_for_plan = select.filter.as_ref().map(|f| {
            let (stripped, _) = f.strip_aggregate_alias_conditions(computed_field_names);
            stripped
        });

        // Convert doc_ids to a _docID filter and merge with the explicit filter.
        // The docID parameter (e.g., User(docID: "...")) must be applied as a real
        // filter condition, not just used for explain output.
        let filter_for_plan = if let Some(ref doc_ids) = select.doc_ids {
            let doc_ids_filter = if doc_ids.len() == 1 {
                let mut conditions = Map::new();
                conditions.insert("_docID".to_string(), serde_json::json!({"_eq": doc_ids[0]}));
                Filter::from_conditions(conditions)
            } else {
                let mut conditions = Map::new();
                conditions.insert("_docID".to_string(), serde_json::json!({"_in": doc_ids}));
                Filter::from_conditions(conditions)
            };
            match filter_for_plan {
                Some(existing) => {
                    // Flat-merge _docID into existing conditions instead of wrapping in _and.
                    // Using .and() would create {_and: [conditions, {_docID: ...}]} which defeats
                    // split_by_relation() — the entire _and block gets classified as "relation"
                    // if any inner condition references a relation field.
                    let mut merged = existing.conditions().clone();
                    for (k, v) in doc_ids_filter.conditions() {
                        merged.insert(k.clone(), v.clone());
                    }
                    Some(Filter::from_conditions(merged))
                }
                None => Some(doc_ids_filter),
            }
        } else {
            filter_for_plan
        };

        // Check if filter is complex (has relation conditions inside logical operators)
        // or has multi-level relation paths (e.g., {author: {published: {rating: ...}}})
        let is_complex_filter = filter_for_plan
            .as_ref()
            .map(|f| {
                f.is_complex()
                    || !f.get_multi_level_relation_paths().is_empty()
                    || f.has_alias_filter()
            })
            .unwrap_or(false);

        // Split filter into scalar and relation parts (only useful for non-complex filters)
        // Note: JSON field nested access looks like relation filters structurally, but should
        // be treated as scalar filters. We recombine them below based on schema info.
        let (scalar_filter_raw, relation_filter) = filter_for_plan
            .as_ref()
            .map(|f| f.split_by_relation())
            .unwrap_or((None, None));

        // Move JSON field conditions from relation_filter back to scalar_filter.
        // The split_by_relation function can't distinguish JSON nested access from relation
        // traversal without schema info. Here we have the collection, so we can fix it.
        //
        // Also transform {relationField: {_docID: {...}}} to {_relationFieldID: {...}}.
        // This allows relation _docID filters to work as scalar filters without requiring a join.
        // Example: {author: {_docID: {_eq: "bae-..."}}} → {_authorID: {_eq: "bae-..."}}
        let scalar_filter = {
            let mut combined_conditions: Map<String, JsonValue> = scalar_filter_raw
                .as_ref()
                .map(|f| f.conditions().clone())
                .unwrap_or_default();

            if let Some(ref rel_filter) = relation_filter {
                for (field_name, condition) in rel_filter.conditions() {
                    // Check if this field is a relation
                    if let Some(field) = collection.field_by_name(field_name) {
                        if field.kind.is_relation() {
                            // Check if this is a {_docID: {...}} pattern AND the FK field exists locally.
                            // For "primary" relations (FK on this side), we can transform to use the FK.
                            // For "secondary" relations (FK on other side), we can't transform - need join.
                            let fk_field_name =
                                schema::CollectionVersion::relation_id_field_name(field_name);
                            let has_local_fk = collection.field_by_name(&fk_field_name).is_some();

                            if has_local_fk {
                                if let Some(obj) = condition.as_object() {
                                    if obj.len() == 1 {
                                        if let Some(docid_condition) = obj.get("_docID") {
                                            // Transform {relationField: {_docID: {...}}} to {_relationFieldID: {...}}
                                            combined_conditions
                                                .insert(fk_field_name, docid_condition.clone());
                                            continue;
                                        }
                                    }
                                }
                            }
                            // Not a _docID-only pattern or no local FK, keep as relation filter
                            continue;
                        }
                    }
                    // Skip logical operators (_and, _or, _not) that contain relation filters.
                    // These were put in relation_filter by split_by_relation() because they
                    // contain relation conditions. They must be evaluated AFTER joins, not here.
                    if field_name == "_and" || field_name == "_or" || field_name == "_not" {
                        continue;
                    }
                    // Not a relation field - treat as scalar (could be JSON, etc.)
                    combined_conditions.insert(field_name.clone(), condition.clone());
                }
            }

            if combined_conditions.is_empty() {
                None
            } else {
                Some(Filter::from_conditions(combined_conditions))
            }
        };

        // For grouped queries, strip _alias conditions from the pre-aggregation filter.
        // Alias filters on aggregate fields must be applied AFTER aggregation.
        let scalar_filter = if select.group_by.is_some() {
            scalar_filter.and_then(|f| f.split_alias().0)
        } else {
            scalar_filter
        };

        FilterParts {
            scalar_filter,
            relation_filter,
            filter_for_plan,
            is_complex_filter,
        }
    }
}
