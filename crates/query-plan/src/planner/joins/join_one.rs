//! TypeJoinOne assembly for one-to-one selection joins.
//!
//! Does not touch the production `plan/type_join/type_join_one.rs` node
//! implementation — only planner-side construction of that node.

use schema::CollectionVersion;
use tracing::{debug, warn};

use crate::plan::{JoinSide, OrphanNode, RelationFilter, ScanNode, TypeJoinOne};
use crate::planner::PlanNode;
use query_types::document::DocumentMapping;
use query_types::error::Result;
use query_types::mapper::{Filter, OrderDirection, Select};

use super::super::builder::Planner;
use super::child_plan::RelationChildPlan;
use super::shared::can_use_direct_indexed_child_cache;

impl Planner {
    /// Assemble a TypeJoinOne node for a one-to-one selection join.
    ///
    /// Returns the updated plan and an optional `join_provides_ordering` update.
    /// `None` means the outer flag should be left unchanged (non-inverted paths).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn assemble_type_join_one(
        &self,
        plan: Box<dyn PlanNode>,
        child: &mut RelationChildPlan,
        nested_select: &Select,
        select: &Select,
        parent_collection: &CollectionVersion,
        mapping: &DocumentMapping,
        parent_filter: Option<&Filter>,
        is_synthetic_order_relation: bool,
        depth: usize,
        ancestor_exhaustive: bool,
    ) -> Result<(Box<dyn PlanNode>, Option<bool>)> {
        let child_plan = child
            .child_plan
            .take()
            .expect("child_plan present before TypeJoinOne");
        let target_collection = &child.target_collection;
        let relation_field = &child.relation_field;
        let relation_field_name = child.relation_field_name.as_str();
        let relation_field_index = child.relation_field_index;
        let child_uses_index = child.child_uses_index;
        let multi_level_paths_for_relation = &child.multi_level_paths_for_relation;
        let child_scan_mapping = &child.child_scan_mapping;
        let parent_order_for_child = &child.parent_order_for_child;
        let parent_relation_filter_for_child = &child.parent_relation_filter_for_child;

        let mut join_provides_ordering: Option<bool> = None;

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

        // One-to-one: TypeJoinOne
        //
        // Check if we should invert the join so the indexed child scan
        // drives iteration. Go uses this both for relation ordering and
        // for one-to-one relation filters on indexed child fields.
        let should_invert_for_child_index = (parent_order_for_child.is_some()
            || relation_filter.is_some())
            && child_uses_index
            && !((select.exhaustive || ancestor_exhaustive)
                && depth > 0
                && is_synthetic_order_relation); // Exhaustive nested order dependencies must preserve the full parent set so orphan merging can happen in the parent relation scope.

        tracing::debug!(
            parent_order_for_child_is_some = parent_order_for_child.is_some(),
            relation_filter_is_some = relation_filter.is_some(),
            child_uses_index = child_uses_index,
            should_invert_for_child_index = should_invert_for_child_index,
            relation_field_name = %relation_field.name,
            "TypeJoinOne: checking child-index inversion"
        );

        let plan = if should_invert_for_child_index {
            // Inverted join: child index scan drives iteration.
            // Determine how to look up the parent for each child:
            //
            // Case 1 (primary-first): Child has FK (e.g., Device._ownerID → User)
            //   - Read FK from child doc → direct docID lookup on parent
            //
            // Case 2 (secondary-first): Parent has FK (e.g., User._deviceID → Device)
            //   - Scan parent's FK index for child._docID
            let parent_residual_filter = parent_filter.and_then(|f| f.split_by_relation().0);
            let child_has_fk = target_relation_field.map(|f| f.is_primary).unwrap_or(false);

            if child_has_fk {
                // Case 1: Child has FK → use InvertedIndex with docID-based parent lookup.
                // The child's FK field (e.g., _ownerID) contains the parent's _docID.
                let child_fk_field_name = target_relation_field
                    .map(|f| schema::CollectionVersion::relation_id_field_name(&f.name))
                    .unwrap_or_default();
                let child_fk_idx = child_scan_mapping
                    .first_index_of_name(&child_fk_field_name)
                    .unwrap_or(0);

                let parent_scan_mapping = plan.document_map().clone();
                let parent_col = parent_collection.clone();
                let fetcher = self.fetcher.clone();

                // Save copies for orphan node before values move into join
                let orphan_col = parent_col.clone();
                let orphan_mapping = parent_scan_mapping.clone();
                let orphan_fetcher = fetcher.clone();

                let mut join =
                    TypeJoinOne::new(plan, child_plan, parent_side, child_side, mapping.clone())
                        .with_ordered_inverted_primary(
                            child_fk_idx,
                            parent_col,
                            parent_scan_mapping,
                            fetcher,
                        );
                if let Some(rel_filter) = relation_filter.clone() {
                    join = join.with_relation_filter(rel_filter);
                }
                if let Some(filter) = parent_residual_filter.clone() {
                    join = join.with_parent_residual_filter(filter);
                }
                if select.exhaustive {
                    let shared_ids: crate::plan::SharedYieldedIds = std::sync::Arc::new(
                        async_lock::RwLock::new(std::collections::HashSet::new()),
                    );
                    let child_fk_field_name = target_relation_field
                        .as_ref()
                        .map(|f| schema::CollectionVersion::relation_id_field_name(&f.name))
                        .unwrap_or_default();
                    let child_fk_index_name = target_collection
                        .indexes
                        .iter()
                        .find(|idx| {
                            idx.fields
                                .first()
                                .is_some_and(|f| f.name == child_fk_field_name)
                        })
                        .map(|idx| idx.name.clone())
                        .unwrap_or_else(|| {
                            format!(
                                "{}__{}_ASC",
                                target_collection.name,
                                child_fk_field_name.trim_start_matches('_')
                            )
                        });
                    let orphan_scan = ScanNode::new(orphan_col, orphan_mapping)
                        .with_fetcher(orphan_fetcher.unwrap());
                    let orphan = OrphanNode::secondary_side(
                        Box::new(orphan_scan),
                        shared_ids.clone(),
                        self.fetcher.clone().unwrap(),
                        target_collection.name.clone(),
                        child_fk_index_name,
                        mapping.clone(),
                    );
                    let direction = parent_order_for_child
                        .as_ref()
                        .and_then(|o| o.conditions.first())
                        .map(|c| c.direction)
                        .unwrap_or(OrderDirection::Asc);
                    let join = join.with_orphan_config(orphan, direction, shared_ids, child_has_fk);
                    join_provides_ordering = Some(parent_order_for_child.is_some());
                    Box::new(join)
                } else {
                    join_provides_ordering = Some(parent_order_for_child.is_some());
                    Box::new(join)
                }
            } else {
                // Case 2: Parent has FK → use InvertedIndex with FK index scan on parent.
                // Same mechanism as filter-based InvertedIndex.
                let parent_fk_field_name =
                    schema::CollectionVersion::relation_id_field_name(&relation_field.name);
                let parent_fk_index = parent_collection.indexes.iter().find(|idx| {
                    idx.fields
                        .first()
                        .is_some_and(|f| f.name == parent_fk_field_name)
                });

                if let Some(fk_index) = parent_fk_index {
                    let fk_index_name = fk_index.name.clone();
                    let parent_scan_mapping = plan.document_map().clone();
                    let parent_col = parent_collection.clone();
                    let fk_field_index = parent_scan_mapping
                        .first_index_of_name(&parent_fk_field_name)
                        .unwrap_or(0);
                    let fetcher = self.fetcher.clone();
                    let sort_dir = parent_order_for_child
                        .as_ref()
                        .and_then(|o| o.conditions.first())
                        .map(|c| c.direction)
                        .unwrap_or_default();

                    // Save copies for orphan node before values move into join
                    let orphan_col = parent_col.clone();
                    let orphan_mapping = parent_scan_mapping.clone();
                    let orphan_fetcher = fetcher.clone();

                    let mut join = TypeJoinOne::new(
                        plan,
                        child_plan,
                        parent_side,
                        child_side,
                        mapping.clone(),
                    )
                    .with_inverted_index(
                        fk_index_name,
                        fk_field_index,
                        parent_col,
                        parent_scan_mapping,
                        fetcher,
                        sort_dir,
                    );
                    if let Some(rel_filter) = relation_filter.clone() {
                        join = join.with_relation_filter(rel_filter);
                    }
                    if let Some(filter) = parent_residual_filter.clone() {
                        join = join.with_parent_residual_filter(filter);
                    }
                    if select.exhaustive {
                        let null_filter = Filter::from_conditions(serde_json::Map::from_iter([(
                            parent_fk_field_name.clone(),
                            serde_json::json!({"_eq": null}),
                        )]));
                        let orphan_scan = ScanNode::new(orphan_col, orphan_mapping)
                            .with_filter(null_filter)
                            .with_fetcher(orphan_fetcher.unwrap());
                        let orphan =
                            OrphanNode::primary_side(Box::new(orphan_scan), mapping.clone());
                        let direction = parent_order_for_child
                            .as_ref()
                            .and_then(|o| o.conditions.first())
                            .map(|c| c.direction)
                            .unwrap_or(OrderDirection::Asc);
                        let shared_ids: crate::plan::SharedYieldedIds = std::sync::Arc::new(
                            async_lock::RwLock::new(std::collections::HashSet::new()),
                        );
                        let join =
                            join.with_orphan_config(orphan, direction, shared_ids, child_has_fk);
                        join_provides_ordering = Some(parent_order_for_child.is_some());
                        Box::new(join)
                    } else {
                        join_provides_ordering = Some(parent_order_for_child.is_some());
                        Box::new(join)
                    }
                } else {
                    // No FK index on parent → fall back to normal join + OrderByNode
                    let mut join = TypeJoinOne::new(
                        plan,
                        child_plan,
                        parent_side,
                        child_side,
                        mapping.clone(),
                    );
                    if let Some(rel_filter) = relation_filter {
                        join = join.with_relation_filter(rel_filter);
                    }
                    Box::new(join)
                }
            }
        } else {
            let mut join =
                TypeJoinOne::new(plan, child_plan, parent_side, child_side, mapping.clone());
            if let (Some(fetcher), Some(target_relation_field)) =
                (self.fetcher.clone(), target_relation_field)
            {
                if target_relation_field.is_primary
                    && can_use_direct_indexed_child_cache(nested_select)
                    && multi_level_paths_for_relation.is_empty()
                    && target_collection.policy.is_none()
                    && !select.show_deleted
                {
                    let child_fk_field_name = schema::CollectionVersion::relation_id_field_name(
                        &target_relation_field.name,
                    );
                    if let Some(child_fk_index_name) = target_collection
                        .indexes
                        .iter()
                        .find(|idx| {
                            idx.fields
                                .first()
                                .is_some_and(|f| f.name == child_fk_field_name)
                        })
                        .map(|idx| idx.name.clone())
                    {
                        join = join.with_indexed_inverted_child_fetch(
                            fetcher,
                            target_collection.name.clone(),
                            child_fk_index_name,
                        );
                    }
                }
            }
            if let Some(rel_filter) = relation_filter {
                join = join.with_relation_filter(rel_filter);
            }
            Box::new(join)
        };

        Ok((plan, join_provides_ordering))
    }
}
