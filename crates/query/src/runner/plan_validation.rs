//! Select validation for query plans.

use schema::CollectionVersion;

use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};

use super::plan_formatting::format_graphql_conditions;

/// Validate that the select doesn't use unsupported features.
pub(crate) fn validate_select(select: &Select, collection: &CollectionVersion) -> Result<()> {
    // Note: CID-based queries are now handled by execute_cid_query() before this validation

    // Note: Nested selections (relations) are now supported via the Planner

    // Helper to check if a field exists in the collection schema
    // Special fields: _docID (document ID), _group (groupBy results), __typename (GraphQL introspection),
    // _version (CRDT version metadata), _deleted (document deletion status)
    let field_exists = |name: &str| -> bool {
        name == "_docID"
            || name == "_deleted"
            || name == "GROUP"
            || name == "__typename"
            || name == "_version"
            || collection.fields.iter().any(|f| f.name == name)
    };

    // Validate that all requested simple fields exist in schema.
    // Skip _<relation>ID fields here; they are validated separately below
    // with a more specific error message for array relations.
    for requestable in &select.fields {
        if let Requestable::Field(field) = requestable {
            if !field_exists(&field.name) {
                let is_relation_id = field.name.starts_with('_')
                    && field.name.ends_with("ID")
                    && field.name.len() > 3;
                if !is_relation_id {
                    return Err(QueryError::unknown_field(format!(
                        "Cannot query field \"{}\" on type \"{}\".",
                        field.name, select.collection_name
                    )));
                }
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
                    // 2. _group aggregates (host_name is "GROUP") - targets grouped results
                    // 3. Nested aggregates (field_name starts with "_") - targets other aggregate results
                    let is_relation_aggregate = !target.host_name.is_empty()
                        && collection.fields.iter().any(|f| f.name == target.host_name);
                    let is_group_aggregate = target.host_name == "GROUP";
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

    // Reject _<relation>ID fields that reference array (one-to-many) relations
    // or that don't correspond to any known relation.
    // For example, if Author has `published: [Book]`, then `_publishedID` does not exist
    // because the FK is on the Book side (_authorID), not the Author side.
    // Go catches this at the GraphQL schema validation level; we catch it here.
    for requestable in &select.fields {
        if let Requestable::Field(field) = requestable {
            if field.name.starts_with('_') && field.name.ends_with("ID") && field.name.len() > 3 {
                if field_exists(&field.name) {
                    continue;
                }
                let is_group_by_field = select
                    .group_by
                    .as_ref()
                    .map(|group_by| group_by.fields.contains(&field.name))
                    .unwrap_or(false);
                let relation_name = &field.name[1..field.name.len() - 2];
                if let Some(rel_field) = collection.field_by_name(relation_name) {
                    if rel_field.kind.is_relation() && rel_field.kind.is_array() {
                        if is_group_by_field {
                            return Err(QueryError::parse(format!(
                                "Argument \"groupBy\" has invalid value [{}].\nIn element #1: Expected type \"{}Field\", found {}.",
                                field.name, select.collection_name, field.name
                            )));
                        }
                        return Err(QueryError::unknown_field(format!(
                            "Cannot query field \"{}\" on type \"{}\". ",
                            field.name, select.collection_name
                        )));
                    }
                } else {
                    return Err(QueryError::unknown_field(format!(
                        "Cannot query field \"{}\" on type \"{}\".",
                        field.name, select.collection_name
                    )));
                }
            }
        }
    }

    // Validate GROUP BY fields exist in schema and are groupable
    if let Some(ref group_by) = select.group_by {
        for field_name in &group_by.fields {
            if !field_exists(field_name) {
                return Err(QueryError::parse(format!(
                    "Argument \"groupBy\" has invalid value [{}].\nIn element #1: Expected type \"{}Field\", found {}.",
                    field_name, select.collection_name, field_name
                )));
            }
            // Reject array relation fields (one-to-many) - can't group by a list value
            if let Some(field) = collection.field_by_name(field_name) {
                if field.kind.is_object() && field.kind.is_array() {
                    return Err(QueryError::parse(format!(
                        "invalid field value to groupBy. Field: {}",
                        field_name
                    )));
                }
            }
        }

        // Validate that non-special fields selected at group level are in the groupBy list
        let group_fields: Vec<&str> = group_by.fields.iter().map(|s| s.as_str()).collect();
        for requestable in &select.fields {
            match requestable {
                Requestable::Field(field) => {
                    let name = field.name.as_str();
                    // Skip special fields
                    if name == "_docID" || name == "GROUP" || name == "__typename" {
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
                    if nested.field.name == "GROUP" {
                        // _group is always allowed in groupBy queries
                        continue;
                    }
                }
                Requestable::Aggregate(_) => {
                    // Aggregates are allowed at group level
                }
                Requestable::Similarity(_) => {
                    // Similarity is allowed at group level
                }
                Requestable::FullTextSearch(_) => {
                    // Full-text search is allowed at group level
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
                if target.host_name == "GROUP" && !has_group_by {
                    return Err(QueryError::parse(
                        "_group may only be referenced when within a groupBy request",
                    ));
                }
            }
        }

        // Check for _group references inside nested _group selections
        if let Requestable::Select(nested) = requestable {
            if nested.field.name == "GROUP" {
                for inner in &nested.fields {
                    if let Requestable::Aggregate(inner_agg) = inner {
                        for target in &inner_agg.targets {
                            if target.host_name == "GROUP" && nested.group_by.is_none() {
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
