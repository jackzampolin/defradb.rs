//! Per-relation selection join pipeline.
//!
//! Orchestrates mapping setup, child plan construction, sub-joins, and
//! TypeJoinMany/TypeJoinOne assembly for a single nested select.

use std::collections::HashMap;

use schema::CollectionVersion;

use crate::planner::PlanNode;
use query_types::document::DocumentMapping;
use query_types::error::{QueryError, Result};
use query_types::mapper::{Filter, Select};

use super::super::builder::Planner;
use super::child_mapping;

impl Planner {
    /// Apply a single selection relation join (one nested select) onto the plan.
    ///
    /// Updates `mapping`, `aggregate_internal_keys`, and optionally
    /// `join_provides_ordering`. Returns the updated plan.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_selection_relation_join(
        &self,
        mut plan: Box<dyn PlanNode>,
        nested_select: &Select,
        group_index: Option<usize>,
        is_synthetic_order_relation: bool,
        select: &Select,
        parent_collection: &CollectionVersion,
        mapping: &mut DocumentMapping,
        aggregate_internal_keys: &mut HashMap<String, (String, String)>,
        join_provides_ordering: &mut bool,
        depth: usize,
        ancestor_exhaustive: bool,
        parent_filter: Option<&Filter>,
        scope_path: &[String],
    ) -> Result<Box<dyn PlanNode>> {
        let relation_field_name = &nested_select.field.name;
        let output_name = nested_select.field.output_name();

        // Ensure the relation field is in the parent mapping.
        // This is especially important for relation fields inside _group
        // which aren't direct children of the select.
        if mapping.first_index_of_name(relation_field_name).is_none() {
            let index = mapping.next_index();
            mapping.add(index, relation_field_name);
            // Don't add render_key - for _group fields, rendering is handled by GroupByNode
        }

        // Find the relation field in the parent collection
        let relation_field = parent_collection
            .field_by_name(relation_field_name)
            .ok_or_else(|| QueryError::unknown_field(relation_field_name))?;

        // Verify it's a relation field
        if !relation_field.kind.is_relation() {
            return Err(QueryError::execution(format!(
                "field '{}' on collection '{}' is not a relation (type: {}). \
                         Only relation fields can have nested selections.",
                relation_field_name,
                parent_collection.name,
                relation_field.kind.graphql_type_name()
            )));
        }

        let target_collection = self.resolve_relation_target_collection(
            parent_collection,
            relation_field,
            relation_field_name,
        )?;

        // Build child mapping for rendering (only selected fields)
        let child_render_mapping = self.build_mapping(nested_select, &target_collection)?;

        // Build scan mapping that includes ALL fields at schema indices.
        // This is required because JoinSide derives FK field indices from the schema,
        // so the doc fields must be at their schema positions for FK lookups to work.
        let mut child_scan_mapping =
            self.build_scan_mapping_for_join(&target_collection, &child_render_mapping);

        child_mapping::enrich_child_scan_mapping_from_aggregates(
            &mut child_scan_mapping,
            select,
            relation_field_name,
            &target_collection,
        );
        child_mapping::enrich_child_scan_mapping_from_parent_filter(
            &mut child_scan_mapping,
            parent_filter,
            relation_field_name,
            &target_collection,
        );
        child_mapping::enrich_child_scan_mapping_from_order_by(
            &mut child_scan_mapping,
            select,
            relation_field_name,
        );

        let multi_level_paths_for_relation =
            child_mapping::multi_level_paths_for_relation(select, relation_field_name);
        child_mapping::enrich_child_scan_mapping_from_multi_level_paths(
            &mut child_scan_mapping,
            &multi_level_paths_for_relation,
        );

        // Get the relation field index in the parent mapping.
        // First try by render_key (for aliased fields), then fall back to name lookup.
        // The fallback handles relation fields inside _group which are added without render_keys.
        let relation_field_index = mapping
            .try_find_index_from_render_key(output_name)
            .or_else(|| mapping.first_index_of_name(relation_field_name))
            .ok_or_else(|| {
                QueryError::internal(format!(
                    "relation field '{}' (output name '{}') not in mapping",
                    relation_field_name, output_name
                ))
            })?;

        // If this relation field is inside _group, update the _group child mapping
        // to use the correct index for rendering. TypeJoinMany stores the relation
        // data at relation_field_index, so the child mapping must use the same index.
        if let Some(grp_idx) = group_index {
            if let Some(group_child_mapping) = mapping.child_at_mut(grp_idx) {
                // Update the child mapping: replace the dynamic index with relation_field_index
                // First, find and remove any existing entry for this field
                let old_index = group_child_mapping.first_index_of_name(relation_field_name);
                if let Some(old_idx) = old_index {
                    // Remove old render_key with the wrong index
                    group_child_mapping
                        .render_keys
                        .retain(|rk| rk.index != old_idx);
                }
                // Add with the correct index
                group_child_mapping.add(relation_field_index, relation_field_name);
                group_child_mapping.add_render_key(relation_field_index, output_name);
            }
        }

        // Set up child scan mapping in parent (for TypeJoin to render children).
        // We use child_scan_mapping (not child_render_mapping) because child docs
        // have fields at schema indices, and render_keys need to match those indices.
        mapping.set_child_at(relation_field_index, child_scan_mapping.clone());

        let mut child = self.build_relation_child_plan(
            nested_select,
            select,
            parent_collection,
            relation_field,
            relation_field_name,
            output_name,
            relation_field_index,
            child_scan_mapping,
            child_render_mapping,
            target_collection,
            multi_level_paths_for_relation,
            parent_filter,
        )?;

        let child_plan_provides_ordering = self.apply_relation_sub_joins(
            &mut child,
            nested_select,
            select,
            mapping,
            aggregate_internal_keys,
            depth,
            ancestor_exhaustive,
            scope_path,
        )?;

        if child.relation_field.kind.is_array() {
            plan = self.assemble_type_join_many(
                plan,
                &mut child,
                nested_select,
                select,
                parent_collection,
                mapping,
                child_plan_provides_ordering,
                ancestor_exhaustive,
            )?;
        } else {
            let (new_plan, ordering_update) = self.assemble_type_join_one(
                plan,
                &mut child,
                nested_select,
                select,
                parent_collection,
                mapping,
                parent_filter,
                is_synthetic_order_relation,
                depth,
                ancestor_exhaustive,
            )?;
            plan = new_plan;
            if let Some(v) = ordering_update {
                *join_provides_ordering = v;
            }
        }

        Ok(plan)
    }
}
