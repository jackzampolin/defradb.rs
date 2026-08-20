//! Sub-join application for nested selection joins.
//!
//! Applies recursive joins, order-by relation sub-joins, aggregate sub-joins,
//! multi-level filter path sub-joins, nested filter relation sub-joins,
//! BM25 nodes, SelectNode filter wrapping, and ACP.

use std::collections::HashMap;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::{Requestable, Select};
use crate::plan::SelectNode;

use super::super::builder::Planner;
use super::child_plan::RelationChildPlan;

impl Planner {
    /// Apply recursive joins and all sub-joins needed for a selection relation, then
    /// wrap with SelectNode filter and ACP.
    ///
    /// Updates `child` in place (plan and scan mapping). Consumes `combined_filter`.
    /// Updates `aggregate_internal_keys` with nested aggregate keys from recursion.
    /// Also updates the parent `mapping` child slot with the final child mapping.
    ///
    /// Returns whether the child plan provides ordering for the parent.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_relation_sub_joins(
        &self,
        child: &mut RelationChildPlan,
        nested_select: &Select,
        select: &Select,
        mapping: &mut DocumentMapping,
        aggregate_internal_keys: &mut HashMap<String, (String, String)>,
        depth: usize,
        ancestor_exhaustive: bool,
        scope_path: &[String],
    ) -> Result<bool> {
        let target_collection = child.target_collection.clone();
        let relation_field_name = child.relation_field_name.clone();
        let output_name = child.output_name.clone();
        let relation_field_index = child.relation_field_index;
        let multi_level_paths_for_relation = child.multi_level_paths_for_relation.clone();
        let combined_filter = child.combined_filter.take();
        let mut child_plan = child
            .child_plan
            .take()
            .expect("child_plan present before sub-joins");
        let mut child_scan_mapping = child.child_scan_mapping.clone();

        // Recursively apply joins for any nested selections within this nested select.
        // This handles multi-level nesting like Users -> Posts -> Comments.
        // Note: We pass None for parent_filter since relation filters only apply at the top level.
        //
        // IMPORTANT: We do NOT reassign child_scan_mapping from the recursive result.
        // The recursive call may modify the mapping's nested child mappings (for deeper relations),
        // but the render_keys at THIS level were already correctly set when child_scan_mapping
        // was built. Reassigning would lose those render_keys, causing empty selection items
        // when both an aggregate and selection target the same relation.
        // Leaf relation selects have no recursive join work; skipping the
        // full planner call keeps nested synthetic order dependencies from
        // burning another large synchronous planner stack frame.
        let needs_recursive_joins = nested_select
            .fields
            .iter()
            .any(|field| matches!(field, Requestable::Select(_) | Requestable::Aggregate(_)))
            || nested_select
                .order_by
                .as_ref()
                .is_some_and(|order| order.has_relation_order());
        let child_plan_provides_ordering = if needs_recursive_joins {
            let nested_joins_result = self.apply_joins(
                child_plan,
                nested_select,
                &target_collection,
                child_scan_mapping.clone(),
                depth + 1,
                ancestor_exhaustive || select.exhaustive,
                None, // Nested relation filters handled differently
                &{
                    let mut child_scope_path = scope_path.to_vec();
                    child_scope_path.push(output_name.clone());
                    child_scope_path
                },
            )?;
            child_plan = nested_joins_result.0;
            // Merge nested aggregate internal keys into our collection
            aggregate_internal_keys.extend(nested_joins_result.2);
            nested_joins_result.3
        } else {
            false
        };

        // Apply sub-joins for order_by references to relation fields within this nested select.
        // For example, if the nested select is `book(order: {publisher: {yearOpened: ASC}})`,
        // the child plan for Book needs a TypeJoinOne for Book→Publisher so the publisher
        // data is available for sorting.
        if let Some(ref order_by) = nested_select.order_by {
            // Collect relation fields already joined from the nested selection
            let already_joined: Vec<&str> = nested_select
                .fields
                .iter()
                .filter_map(|f| {
                    if let Requestable::Select(s) = f {
                        Some(s.field.name.as_str())
                    } else {
                        None
                    }
                })
                .collect();

            for condition in &order_by.conditions {
                if condition.fields.len() >= 2 {
                    let order_relation_name = &condition.fields[0];
                    if already_joined.contains(&order_relation_name.as_str()) {
                        continue; // Already joined from selection
                    }
                    if let Some(rel_idx) =
                        child_scan_mapping.first_index_of_name(order_relation_name)
                    {
                        if child_scan_mapping.child_at(rel_idx).is_some() {
                            continue;
                        }
                    }
                    // Check if this is a relation field on the target collection
                    if let Some(order_rel_field) =
                        target_collection.field_by_name(order_relation_name)
                    {
                        if order_rel_field.kind.is_relation() {
                            let (new_child_plan, new_child_mapping) = self
                                .apply_filter_relation_join(
                                    child_plan,
                                    &target_collection,
                                    order_rel_field,
                                    order_relation_name,
                                    child_scan_mapping.clone(),
                                )?;
                            child_plan = new_child_plan;
                            child_scan_mapping = new_child_mapping;
                        }
                    }
                }
            }
        }

        // Also apply sub-joins for order_by in aggregate targets.
        // For example, `_sum(book: {field: rating, order: {publisher: {yearOpened: ASC}}})`.
        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                for target in &agg.targets {
                    if target.host_name != *relation_field_name {
                        continue;
                    }
                    if let Some(ref order) = target.order {
                        let already_joined: Vec<&str> = nested_select
                            .fields
                            .iter()
                            .filter_map(|f| {
                                if let Requestable::Select(s) = f {
                                    Some(s.field.name.as_str())
                                } else {
                                    None
                                }
                            })
                            .collect();

                        for condition in &order.conditions {
                            if condition.fields.len() >= 2 {
                                let order_relation_name = &condition.fields[0];
                                if already_joined.contains(&order_relation_name.as_str()) {
                                    continue;
                                }
                                // Check if already joined from nested_select.order_by above
                                if child_scan_mapping
                                    .first_index_of_name(order_relation_name)
                                    .is_some()
                                {
                                    continue;
                                }
                                if let Some(order_rel_field) =
                                    target_collection.field_by_name(order_relation_name)
                                {
                                    if order_rel_field.kind.is_relation() {
                                        let (new_child_plan, new_child_mapping) = self
                                            .apply_filter_relation_join(
                                                child_plan,
                                                &target_collection,
                                                order_rel_field,
                                                order_relation_name,
                                                child_scan_mapping.clone(),
                                            )?;
                                        child_plan = new_child_plan;
                                        child_scan_mapping = new_child_mapping;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Apply sub-joins for filter relation fields in aggregate targets.
        // For example, _sum(book: {field: rating, filter: {publisher: {yearOpened: {_eq: 2013}}}})
        // needs a TypeJoinOne for publisher inside the book plan so publisher data appears
        // in rendered JSON for post-processing filter evaluation in compute_relation_aggregates.
        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                for target in &agg.targets {
                    if target.host_name != *relation_field_name {
                        continue;
                    }
                    if let Some(ref filter) = target.filter {
                        for filter_field in filter.referenced_fields() {
                            if filter_field.starts_with('_') {
                                continue;
                            }
                            if let Some(filter_rel_field) =
                                target_collection.field_by_name(&filter_field)
                            {
                                if filter_rel_field.kind.is_relation() {
                                    // Skip if already joined
                                    if let Some(rel_idx) =
                                        child_scan_mapping.first_index_of_name(&filter_field)
                                    {
                                        if child_scan_mapping.child_at(rel_idx).is_some() {
                                            continue;
                                        }
                                    }
                                    let (new_child_plan, new_child_mapping) = self
                                        .apply_filter_relation_join(
                                            child_plan,
                                            &target_collection,
                                            filter_rel_field,
                                            &filter_field,
                                            child_scan_mapping.clone(),
                                        )?;
                                    child_plan = new_child_plan;
                                    child_scan_mapping = new_child_mapping;
                                    // Aggregate filters evaluate on rendered JSON via
                                    // compute_relation_aggregates → matches_json_object,
                                    // so publisher must appear in the rendered output.
                                    if let Some(rel_idx) =
                                        child_scan_mapping.first_index_of_name(&filter_field)
                                    {
                                        if !child_scan_mapping
                                            .render_keys
                                            .iter()
                                            .any(|rk| rk.key == filter_field)
                                        {
                                            child_scan_mapping
                                                .add_render_key(rel_idx, &filter_field);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Apply sub-joins for multi-level filter paths within this relation.
        // For example, if we're joining Book → Author and the filter has path
        // ["author", "published"], we need to add a sub-join for "published" here.
        // Skip relations already joined from the nested select's fields to avoid
        // duplicate sub-joins that would overwrite the selection's mapping.
        let nested_joined_relations: Vec<&str> = nested_select
            .fields
            .iter()
            .filter_map(|f| {
                if let Requestable::Select(s) = f {
                    Some(s.field.name.as_str())
                } else {
                    None
                }
            })
            .collect();
        for remaining_path in &multi_level_paths_for_relation {
            if let Some(first_nested) = remaining_path.first() {
                if nested_joined_relations.contains(&first_nested.as_str()) {
                    // Already joined from the nested selection (e.g., publisher in
                    // book { publisher { yearOpened } }). Skip to avoid overwriting
                    // the selection's child mapping with a full-field mapping.
                    continue;
                }
            }
            let (new_child_plan, new_child_mapping) = self.apply_multi_level_sub_joins(
                child_plan,
                remaining_path,
                &target_collection,
                child_scan_mapping.clone(),
            )?;
            child_plan = new_child_plan;
            child_scan_mapping = new_child_mapping;
        }

        // Add TypeJoinOne sub-joins for relation fields in the nested select's own filter.
        // For example, book(filter: {publisher: {yearOpened: {_geq: 2020}}}) needs a
        // TypeJoinOne for publisher so the filter can evaluate on joined data.
        if let Some(ref explicit_filter) = nested_select.filter {
            for filter_field in explicit_filter.referenced_fields() {
                if filter_field.starts_with('_') {
                    continue;
                }
                if let Some(filter_rel_field) = target_collection.field_by_name(&filter_field) {
                    if filter_rel_field.kind.is_relation() {
                        // Skip if already joined from selection or other sub-joins
                        if let Some(rel_idx) = child_scan_mapping.first_index_of_name(&filter_field)
                        {
                            if child_scan_mapping.child_at(rel_idx).is_some() {
                                continue;
                            }
                        }
                        let (new_plan, new_mapping) = self.apply_filter_relation_join(
                            child_plan,
                            &target_collection,
                            filter_rel_field,
                            &filter_field,
                            child_scan_mapping.clone(),
                        )?;
                        child_plan = new_plan;
                        child_scan_mapping = new_mapping;
                        // No render_key needed: SelectNode evaluates filters on raw
                        // Doc fields via DocumentMapping, not rendered JSON. Adding
                        // a render_key would leak the relation into the output.
                    }
                }
            }
        }

        let mut child_scope_path = scope_path.to_vec();
        child_scope_path.push(output_name);
        child_plan = self.add_bm25_nodes(
            child_plan,
            nested_select,
            &child_scan_mapping,
            &child_scope_path,
        )?;

        // Now wrap with SelectNode if there's a filter (deferred from earlier).
        // At this point, relation sub-joins are in place so the filter can evaluate
        // conditions on joined relation data (e.g., publisher.yearOpened).
        if let Some(ref filter) = combined_filter {
            child_plan = Box::new(
                SelectNode::new(child_plan, child_scan_mapping.clone()).with_filter(filter.clone()),
            );
        }

        // Insert ACP permission filter for the child collection (if ACP-protected).
        child_plan = self.maybe_wrap_with_acp_filter(child_plan, &target_collection);

        // Update parent mapping with the final child mapping (after sub-joins)
        // This ensures the nested relation mappings are included
        mapping.set_child_at(relation_field_index, child_scan_mapping.clone());

        child.child_plan = Some(child_plan);
        child.child_scan_mapping = child_scan_mapping;

        Ok(child_plan_provides_ordering)
    }
}
