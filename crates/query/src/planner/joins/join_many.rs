//! TypeJoinMany assembly for one-to-many selection joins.

use schema::CollectionVersion;
use tracing::{debug, warn};

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::{Requestable, Select};
use crate::plan::{JoinSide, RelationFilter, TypeJoinMany};
use crate::planner::PlanNode;

use super::super::builder::Planner;
use super::child_plan::RelationChildPlan;
use super::shared::can_use_direct_indexed_child_cache;

impl Planner {
    /// Assemble a TypeJoinMany node for a one-to-many selection join.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn assemble_type_join_many(
        &self,
        plan: Box<dyn PlanNode>,
        child: &mut RelationChildPlan,
        nested_select: &Select,
        select: &Select,
        parent_collection: &CollectionVersion,
        mapping: &DocumentMapping,
        child_plan_provides_ordering: bool,
        ancestor_exhaustive: bool,
    ) -> Result<Box<dyn PlanNode>> {
        let child_plan = child
            .child_plan
            .take()
            .expect("child_plan present before TypeJoinMany");
        let filter_child_plan = child.filter_child_plan.take();
        let target_collection = &child.target_collection;
        let relation_field = &child.relation_field;
        let relation_field_name = child.relation_field_name.as_str();
        let relation_field_index = child.relation_field_index;
        let nested_limit = child.nested_limit;
        let nested_offset = child.nested_offset;
        let nested_order_by = child.nested_order_by.clone();
        let child_uses_index = child.child_uses_index;
        let has_deferred_scoped_fulltext = child.has_deferred_scoped_fulltext;
        let multi_level_paths_for_relation = &child.multi_level_paths_for_relation;
        let child_scan_mapping = &child.child_scan_mapping;
        let parent_relation_filter_for_child = &child.parent_relation_filter_for_child;

        // Find the other side of the relation
        let target_relation_field = if let Some(rel_name) = &relation_field.relation_name {
            target_collection.field_by_relation(
                rel_name,
                &parent_collection.name,
                relation_field_name,
            )
        } else {
            None
        };

        // Debug: Log relation field resolution
        debug!(
            parent_collection = %parent_collection.name,
            target_collection = %target_collection.name,
            relation_field_name = %relation_field_name,
            relation_name = ?relation_field.relation_name,
            parent_is_primary = relation_field.is_primary,
            target_relation_field_found = target_relation_field.is_some(),
            target_field_name = ?target_relation_field.map(|f| &f.name),
            target_is_primary = ?target_relation_field.map(|f| f.is_primary),
            "Resolving relation for join"
        );

        // Get child relation field index (if it exists).
        // For bidirectional relations, this is the index of the back-reference field
        // (e.g., `author` field on posts when joining from users.posts).
        // For unidirectional relations (no back-reference), we default to index 0.
        // This is safe because TypeJoin nodes use the relation_id_field_index()
        // (derived from the FK field) for actual join matching, not this index.
        let child_relation_index = target_relation_field
            .and_then(|f| {
                target_collection
                    .fields
                    .iter()
                    .position(|tf| tf.name == f.name)
            })
            .unwrap_or_else(|| {
                warn!(
                    parent_collection = %parent_collection.name,
                    target_collection = %target_collection.name,
                    relation_field = %relation_field_name,
                    "No back-reference field found for relation, using default index 0. \
                     This may indicate a unidirectional relation or schema misconfiguration."
                );
                0
            });

        // Create join sides
        let parent_side = JoinSide::new(
            parent_collection.clone(),
            relation_field.clone(),
            relation_field_index,
        )?;

        let child_side = JoinSide::new(
            target_collection.as_ref().clone(),
            target_relation_field
                .cloned()
                .unwrap_or_else(|| relation_field.clone()),
            child_relation_index,
        )?;

        // Build RelationFilter from the already-extracted parent relation filter
        let relation_filter = parent_relation_filter_for_child
            .as_ref()
            .map(|nested_filter| RelationFilter {
                relation_field: relation_field_name.to_string(),
                conditions: nested_filter.clone(),
            });

        // Create the appropriate join node
        // Note: We pass child_render_mapping as the output mapping (for TypeJoin to render children)
        // but the child_plan uses child_scan_mapping internally (for FK lookups)
        // One-to-many: TypeJoinMany
        let mut join_many =
            TypeJoinMany::new(plan, child_plan, parent_side, child_side, mapping.clone())?;

        // Apply relation filter (filters parents by children)
        if let Some(rel_filter) = relation_filter {
            join_many = join_many.with_relation_filter(rel_filter);
        }

        if has_deferred_scoped_fulltext {
            join_many = join_many.with_parent_scoped_child_cache();
        }

        // Check if child has an index on its FK field for this relation.
        // When FK is indexed, a global child scan can efficiently map children
        // to parents, so per-parent scanning is not needed for ordering.
        let child_fk_index_info = target_relation_field.and_then(|trf| {
            let rel_name = trf.relation_name.as_deref()?;
            // Find the FK ID field (same relation, kind Scalar(DocID))
            let fk_field = target_collection.fields.iter().find(|f| {
                f.relation_name.as_deref() == Some(rel_name)
                    && matches!(f.kind, schema::FieldKind::Scalar(schema::ScalarKind::DocID))
            })?;
            // Check if any index covers this FK field
            let index = target_collection.indexes.iter().find(|idx| {
                !idx.auto_generated && idx.fields.first().is_some_and(|f| f.name == fk_field.name)
            })?;
            Some((fk_field.name.clone(), index.name.clone()))
        });
        let has_child_fk_index = child_fk_index_info.is_some();

        // Determine per-parent mode before moving filter_child_plan.
        // Per-parent scanning re-inits the child plan for each parent:
        // - With limit: needed for early termination per parent
        // - With child sub-filter but no parent filter_child_plan: child filter
        //   runs per parent (when filter_child_plan exists, it handles globally)
        // - With ordering + no FK index + no parent filter: Go scans the
        //   ordering index per parent without FK index for efficient matching
        let use_per_parent = nested_limit.is_some()
            || (child_uses_index
                && ((nested_select.filter.is_some() && filter_child_plan.is_none())
                    || (filter_child_plan.is_none()
                        && nested_order_by.is_some()
                        && !has_child_fk_index)));
        let has_filter_child_plan = filter_child_plan.is_some();

        // Apply filter child plan for indexed relation filter evaluation
        if let Some(fcp) = filter_child_plan {
            join_many = join_many.with_filter_child_plan(fcp);
        }

        // Apply per-parent limit/offset/ordering
        if let Some(limit) = nested_limit {
            join_many = join_many.with_limit(limit);
        }
        if nested_offset > 0 {
            join_many = join_many.with_offset(nested_offset);
        }
        if let Some(order_by) = nested_order_by.clone() {
            join_many = join_many.with_order_by(order_by);
        }
        if (select.exhaustive || ancestor_exhaustive)
            && nested_order_by
                .as_ref()
                .is_some_and(|order| order.has_relation_order())
        {
            join_many = join_many.with_preserve_ordered_orphans();
        }
        if nested_order_by
            .as_ref()
            .is_some_and(|order| order.has_relation_order())
            && child_plan_provides_ordering
        {
            join_many = join_many.with_child_plan_provides_ordering();
        }
        if use_per_parent {
            join_many = join_many.with_per_parent_child_scan();
        } else if let (Some(fetcher), Some((fk_field_name, index_name))) =
            (self.fetcher.clone(), child_fk_index_info.clone())
        {
            if can_use_direct_indexed_child_cache(nested_select)
                && !has_filter_child_plan
                && multi_level_paths_for_relation.is_empty()
                && target_collection.policy.is_none()
                && !select.show_deleted
            {
                join_many = join_many.with_indexed_child_fetch(
                    fetcher,
                    target_collection.name.clone(),
                    fk_field_name,
                    index_name,
                );
            }
        }

        // Apply nested groupBy if present
        if let Some(ref group_by) = nested_select.group_by {
            join_many = join_many.with_group_by(group_by.clone());

            // Find the _group nested select and build its mapping
            // Use indices from child_scan_mapping so the mapping matches
            // the child document's field array indices.
            for field in &nested_select.fields {
                if let Requestable::Select(group_select) = field {
                    if group_select.field.name == "GROUP" {
                        // Build mapping for _group contents using child_scan_mapping indices
                        let mut group_mapping = DocumentMapping::new();
                        for group_field in &group_select.fields {
                            if let Requestable::Field(f) = group_field {
                                // Use the index from child_scan_mapping
                                if let Some(idx) = child_scan_mapping.first_index_of_name(&f.name) {
                                    let output_name = f.output_name().to_string();
                                    group_mapping.add(idx, &output_name);
                                    group_mapping.add_render_key(idx, output_name);
                                }
                            }
                        }
                        join_many = join_many.with_group_mapping(group_mapping);
                        break;
                    }
                }
            }
        }

        Ok(Box::new(join_many))
    }
}
