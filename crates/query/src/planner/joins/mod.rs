//! Join planning utilities.
//!
//! Contains methods for applying relation joins to query plans:
//! - `apply_joins()` - Main join application for nested selects
//! - `apply_filter_only_joins()` - Joins for filter-only relations
//! - `apply_secondary_id_joins()` - Joins for secondary relation ID fields
//! - `apply_aggregate_joins()` - Joins for aggregate relation targets
//! - `build_scan_mapping_for_join()` - Scan mapping with schema indices
//! - `apply_filter_relation_join()` - Join for filter-only relations
//! - `apply_multi_level_sub_joins()` - Sub-joins for multi-level filter paths
//! - `apply_multi_level_filter_joins()` - Join chains for deep filter paths

mod aggregate_joins;
mod child_mapping;
mod child_plan;
mod filter_only;
mod filter_relation;
mod join_many;
mod join_one;
mod mapping;
mod multi_level;
mod secondary_id;
mod selection;
mod selection_join;
mod shared;
mod sub_joins;

pub(super) use shared::{JoinResult, SelectionJoinInfo};

use std::collections::HashMap;

use schema::CollectionVersion;

use super::builder::{Planner, MAX_NESTING_DEPTH};
use crate::planner::PlanNode;
use query_types::document::DocumentMapping;
use query_types::error::QueryError;
use query_types::mapper::Select;

impl Planner {
    /// Apply join nodes for nested selects (relation fields)
    ///
    /// The `depth` parameter tracks recursion depth to prevent stack overflow
    /// from deeply nested or circular query structures.
    ///
    /// If `parent_filter` is provided, relation filters are extracted and passed
    /// to the TypeJoin nodes to filter parents based on their children.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_joins(
        &self,
        mut plan: Box<dyn PlanNode>,
        select: &Select,
        parent_collection: &CollectionVersion,
        mut mapping: DocumentMapping,
        depth: usize,
        ancestor_exhaustive: bool,
        parent_filter: Option<&query_types::mapper::Filter>,
        scope_path: &[String],
    ) -> JoinResult {
        // Internal keys for aggregate relation data when there's a collision with a relation selection.
        let mut aggregate_internal_keys: HashMap<String, (String, String)> = HashMap::new();
        let mut join_provides_ordering = false;

        // Check recursion depth to prevent stack overflow
        if depth > MAX_NESTING_DEPTH {
            return Err(QueryError::execution(format!(
                "Query nesting depth {} exceeds maximum allowed depth of {}. \
                 Consider simplifying the query or using separate queries for deeply nested data.",
                depth, MAX_NESTING_DEPTH
            )));
        }

        let mut selects_to_process = selection::collect_selects_to_process(select, &mapping);

        let already_selected: std::collections::HashSet<&str> = selects_to_process
            .iter()
            .map(|(s, _)| s.field.name.as_str())
            .collect();
        let synthetic_order_selects = selection::collect_synthetic_order_selects(
            select,
            parent_collection,
            &already_selected,
        );
        let synthetic_order_relations: std::collections::HashSet<&str> = synthetic_order_selects
            .iter()
            .map(|s| s.field.name.as_str())
            .collect();
        for syn_select in &synthetic_order_selects {
            selects_to_process.push((syn_select, None));
        }

        let selection_join_info =
            selection::build_selection_join_info(selects_to_process.iter().map(|(s, _)| *s));

        for (nested_select, group_index) in selects_to_process {
            let is_synthetic_order_relation =
                synthetic_order_relations.contains(nested_select.field.name.as_str());

            plan = self.apply_selection_relation_join(
                plan,
                nested_select,
                group_index,
                is_synthetic_order_relation,
                select,
                parent_collection,
                &mut mapping,
                &mut aggregate_internal_keys,
                &mut join_provides_ordering,
                depth,
                ancestor_exhaustive,
                parent_filter,
                scope_path,
            )?;
        }

        // Apply the three extracted join phases
        plan = self.apply_filter_only_joins(
            plan,
            &mut mapping,
            select,
            parent_collection,
            parent_filter,
        )?;
        plan = self.apply_secondary_id_joins(plan, &mut mapping, select, parent_collection)?;
        plan = self.apply_aggregate_joins(
            plan,
            &mut mapping,
            &mut aggregate_internal_keys,
            select,
            parent_collection,
            &selection_join_info,
        )?;

        Ok((
            plan,
            mapping,
            aggregate_internal_keys,
            join_provides_ordering,
        ))
    }
}
