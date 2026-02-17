//! Scan mapping construction for query planning.
//!
//! Builds the `ScanSetup` struct that determines which fields appear in scan output
//! and whether joins are needed.

use schema::CollectionVersion;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{OrderCondition, OrderDirection, Requestable, Select};

use super::Planner;

/// Format an order condition as a Go-style value string for error messages.
///
/// For `fields: ["articles", "pages"], direction: Asc`, produces `{articles: {pages: ASC}}`.
fn format_order_value(condition: &OrderCondition) -> String {
    let dir = match condition.direction {
        OrderDirection::Asc => "ASC",
        OrderDirection::Desc => "DESC",
    };
    let mut result = dir.to_string();
    for field in condition.fields.iter().rev() {
        result = format!("{{{}: {}}}", field, result);
    }
    result
}

/// Result of building scan setup: contains mappings and flags needed for plan construction.
pub(in crate::planner) struct ScanSetup {
    pub(in crate::planner) scan_mapping: DocumentMapping,
    pub(in crate::planner) needs_joins: bool,
    pub(in crate::planner) filter_relation_fields: Vec<String>,
    pub(in crate::planner) filter_has_relations: bool,
    pub(in crate::planner) ordering_only_fields: Vec<(String, String)>,
}

impl Planner {
    /// Build scan setup: document mappings, join flags, and ordering-only fields.
    ///
    /// This corresponds to the section of plan_with_index_info that determines which
    /// fields appear in the scan output and whether the query requires join nodes.
    pub(in crate::planner) fn build_scan_setup(
        &self,
        select: &Select,
        collection: &CollectionVersion,
    ) -> Result<ScanSetup> {
        // Build the document mapping for this query (controls which fields appear in output)
        let render_mapping = self.build_mapping(select, collection)?;

        // Check if this query has nested selections that require joins
        let has_nested = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Select(_)));

        // Check if filter references relation fields (needs joins even if not selected)
        let filter_relation_fields: Vec<String> = select
            .filter
            .as_ref()
            .map(|f| f.relation_field_names())
            .unwrap_or_default();
        let filter_has_relations = !filter_relation_fields.is_empty();

        // Check if order references relation fields.
        // Go rejects ordering by relation fields at the GraphQL validation level
        // (the relation field doesn't exist in the OrderArg input type).
        let order_relation_fields: Vec<String> = select
            .order_by
            .as_ref()
            .map(|o| o.relation_field_names())
            .unwrap_or_default();
        let order_has_relations = !order_relation_fields.is_empty();

        // One-to-one relation ordering (e.g., User(order: {device: {model: ASC}})) is handled
        // by the ordered inverted join in apply_joins().
        // One-to-many (array) relation ordering is rejected — ambiguous which child to sort by.
        if order_has_relations {
            if let Some(ref order_by) = select.order_by {
                for condition in &order_by.conditions {
                    if condition.fields.len() > 1 {
                        let relation_name = &condition.fields[0];
                        if let Some(field) = collection.field_by_name(relation_name) {
                            if field.kind.is_array() {
                                let order_value_str = format_order_value(condition);
                                return Err(QueryError::parse(format!(
                                    "Argument \"order\" has invalid value {}.\n\
                                     In field \"{}\": Unknown field.",
                                    order_value_str, relation_name
                                )));
                            }
                        }
                    }
                }
            }
        }

        // Compute ordering-only fields: nested relation fields used in ORDER BY but not in selection.
        // These will be stripped from the final output.
        let ordering_only_fields: Vec<(String, String)> = select
            .order_by
            .as_ref()
            .map(|order_by| {
                let mut result = Vec::new();
                for condition in &order_by.conditions {
                    // Look for nested relation orders like ["author", "verified"]
                    if condition.fields.len() > 1 {
                        let relation_field_name = &condition.fields[0];
                        let nested_field_name = &condition.fields[1];

                        // Check if there's a nested selection for this relation
                        let nested_selection_fields: Vec<&String> = select
                            .fields
                            .iter()
                            .filter_map(|f| {
                                if let Requestable::Select(nested) = f {
                                    if &nested.field.name == relation_field_name {
                                        // Get selected field names from nested selection
                                        return Some(
                                            nested
                                                .fields
                                                .iter()
                                                .filter_map(|nf| {
                                                    if let Requestable::Field(field) = nf {
                                                        Some(&field.name)
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect::<Vec<_>>(),
                                        );
                                    }
                                }
                                None
                            })
                            .flatten()
                            .collect();

                        // If nested_field is not in the selected fields, it's ordering-only
                        if !nested_selection_fields.contains(&nested_field_name) {
                            result.push((relation_field_name.clone(), nested_field_name.clone()));
                        }
                    }
                }
                result
            })
            .unwrap_or_default();

        // Check if GROUP BY references relation fields (needs full schema mapping for joins)
        let group_by_has_relations = select
            .group_by
            .as_ref()
            .map(|gb| {
                gb.fields.iter().any(|field_name| {
                    collection
                        .field_by_name(field_name)
                        .map(|f| f.kind.is_relation())
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);

        // Check if aggregates reference relation fields (needs full schema mapping for joins)
        let aggregates_have_relations = select.fields.iter().any(|f| {
            if let Requestable::Aggregate(agg) = f {
                agg.targets.iter().any(|t| {
                    if t.host_name.is_empty() || t.host_name == "GROUP" {
                        return false;
                    }
                    collection
                        .field_by_name(&t.host_name)
                        .map(|f| f.kind.is_relation())
                        .unwrap_or(false)
                })
            } else {
                false
            }
        });

        // Check if any secondary relation ID fields are selected (e.g., `_authorID`).
        // These require a TypeJoinOne to reverse-lookup the FK, which needs full schema mapping.
        let has_secondary_id_field = select.fields.iter().any(|f| {
            if let Requestable::Field(field) = f {
                let name = &field.name;
                if name.starts_with('_') && name.ends_with("ID") && name.len() > 3 {
                    let rel_name = &name[1..name.len() - 2];
                    if let Some(rel_field) = collection.field_by_name(rel_name) {
                        return rel_field.kind.is_relation() && !rel_field.is_primary;
                    }
                }
            }
            false
        });

        // Build scan mapping: for queries with nested selections, relation filters, relation ordering,
        // relation aggregates, relation groupBy fields, or secondary relation ID fields, use full
        // schema mapping so that FK fields are available for TypeJoin lookups and schema indices
        // don't collide with sequential render indices.
        let needs_joins = has_nested
            || filter_has_relations
            || order_has_relations
            || aggregates_have_relations
            || group_by_has_relations
            || has_secondary_id_field;
        let mut scan_mapping = if needs_joins {
            self.build_scan_mapping_for_join(collection, &render_mapping)
        } else {
            render_mapping.clone()
        };

        // Add _group fields to scan_mapping if present in render_mapping.
        // _group is a virtual field (not in schema) that needs to be explicitly copied.
        // Multiple _group entries may exist when aliases are used (e.g., G1: _group(...), G2: _group(...)).
        if let Some(group_indices) = render_mapping.indexes_of_name("GROUP") {
            let group_indices = group_indices.to_vec();
            for render_index in group_indices {
                let scan_index = scan_mapping.next_index();
                scan_mapping.add(scan_index, "GROUP");
                // Copy the render_key for this specific _group entry
                for rk in &render_mapping.render_keys {
                    if rk.index == render_index {
                        scan_mapping.add_render_key(scan_index, &rk.key);
                        break;
                    }
                }
                // Copy child mapping if present (for _group { field1, field2 } syntax)
                if let Some(child) = render_mapping.child_at(render_index) {
                    scan_mapping.set_child_at(scan_index, child.clone());
                }
            }
        }

        // Add ORDER BY fields to scan mapping if not already present (Go compatibility).
        // Go DefraDB allows ordering by fields not in the SELECT clause.
        if let Some(ref order_by) = select.order_by {
            for condition in &order_by.conditions {
                if let Some(field_name) = condition.fields.first() {
                    // Skip if already in mapping
                    if scan_mapping.first_index_of_name(field_name).is_some() {
                        continue;
                    }
                    // Verify the field exists in the collection schema
                    if collection.field_by_name(field_name).is_some() {
                        // Use next_index to avoid collisions with existing mapping positions.
                        // Using schema_idx would overwrite fields if a schema position
                        // is already occupied (e.g., _ownerID at schema pos 1 overwriting model).
                        let next_idx = scan_mapping.next_index();
                        scan_mapping.add(next_idx, field_name);
                    }
                }
            }
        }

        // Add aggregate fields to scan_mapping if present in render_mapping.
        // Aggregates are virtual fields (not in schema) that need explicit copying.
        // Each aliased aggregate gets its own index/render_key, even if they share
        // the same type (e.g., sum1: _sum(...) and sum2: _sum(...) need separate slots).
        for field in &select.fields {
            if let Requestable::Aggregate(agg) = field {
                let agg_type_name = agg.aggregate_type.as_str();
                let output_name = agg.output_name();
                // Add a new index if this specific output name isn't already registered
                if scan_mapping
                    .try_find_index_from_render_key(output_name)
                    .is_none()
                {
                    let scan_index = scan_mapping.next_index();
                    scan_mapping.add(scan_index, agg_type_name);
                    scan_mapping.add_render_key(scan_index, output_name);
                }

                // Always add aggregate target fields if present (even if aggregate type exists)
                for target in &agg.targets {
                    if let Some(ref field_name) = target.field_name {
                        if scan_mapping.first_index_of_name(field_name).is_none() {
                            // Verify field exists in collection schema
                            if collection.field_by_name(field_name).is_some() {
                                // Use next available index, not schema index,
                                // to avoid conflicts with other allocated indices
                                let new_index = scan_mapping.next_index();
                                scan_mapping.add(new_index, field_name);
                            }
                        }
                    }

                    // For inline array aggregates (e.g., _count(favouriteIntegers: {})),
                    // the host_name refers to an inline array field, not a relation.
                    // We need to render the field data so compute_relation_aggregates()
                    // can operate on it after plan execution.
                    if !target.host_name.is_empty() && target.host_name != "GROUP" {
                        let host_name = &target.host_name;
                        if let Some(field_desc) = collection.field_by_name(host_name) {
                            if !field_desc.kind.is_relation() {
                                // It's an inline array field — ensure it's in scan_mapping
                                // with a render_key so data appears in output for
                                // compute_relation_aggregates().
                                let idx = if let Some(existing) =
                                    scan_mapping.first_index_of_name(host_name)
                                {
                                    existing
                                } else {
                                    let new_idx = scan_mapping.next_index();
                                    scan_mapping.add(new_idx, host_name);
                                    new_idx
                                };
                                if !scan_mapping
                                    .render_keys
                                    .iter()
                                    .any(|rk| rk.key == *host_name)
                                {
                                    scan_mapping.add_render_key(idx, host_name);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Add similarity fields to scan_mapping.
        // Similarity results are virtual computed fields stored at specific indices.
        for field in &select.fields {
            if let Requestable::Similarity(sim) = field {
                // Add the _similarity output slot
                let output_name = sim.output_name();
                if scan_mapping
                    .try_find_index_from_render_key(output_name)
                    .is_none()
                {
                    let scan_index = scan_mapping.next_index();
                    scan_mapping.add(scan_index, "SIMILARITY");
                    scan_mapping.add_render_key(scan_index, output_name);
                }

                // Ensure the target field (document's vector) is in scan_mapping
                if scan_mapping
                    .first_index_of_name(&sim.target_field)
                    .is_none()
                    && collection.field_by_name(&sim.target_field).is_some()
                {
                    let idx = scan_mapping.next_index();
                    scan_mapping.add(idx, &sim.target_field);
                }
            }
        }

        Ok(ScanSetup {
            scan_mapping,
            needs_joins,
            filter_relation_fields,
            filter_has_relations,
            ordering_only_fields,
        })
    }
}
