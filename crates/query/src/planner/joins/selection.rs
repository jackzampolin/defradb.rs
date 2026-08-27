//! Collect nested selects and synthetic ORDER BY joins for planning.

use std::collections::HashMap;

use schema::CollectionVersion;

use crate::document::DocumentMapping;
use crate::mapper::{Field, Requestable, Select};

use super::shared::SelectionJoinInfo;

/// Collect nested Select items to process for joins, including those inside `_group`.
///
/// Returns tuples of `(select, _group_index if from inside _group)`.
pub(super) fn collect_selects_to_process<'a>(
    select: &'a Select,
    mapping: &DocumentMapping,
) -> Vec<(&'a Select, Option<usize>)> {
    // Collect all Select items to process, including those inside _group.
    // Track the _group index for relation fields inside _group so we can update
    // the _group child mapping with the correct relation field index later.
    // Tuple: (select, _group_index if from inside _group)
    let mut selects_to_process: Vec<(&Select, Option<usize>)> = Vec::new();
    for requestable in &select.fields {
        if let Requestable::Select(nested_select) = requestable {
            if nested_select.field.name == "GROUP" {
                // Get the _group index in the parent mapping
                let group_index = mapping.first_index_of_name("GROUP");
                // _group is a virtual field - process its inner relation fields
                for inner_requestable in &nested_select.fields {
                    if let Requestable::Select(inner_select) = inner_requestable {
                        // Skip special fields and nested GROUP selects
                        if !inner_select.field.name.starts_with('_')
                            && inner_select.field.name != "GROUP"
                        {
                            selects_to_process.push((inner_select, group_index));
                        }
                    }
                }
            } else {
                selects_to_process.push((nested_select, None));
            }
        }
    }
    selects_to_process
}

/// Build synthetic selects for ORDER BY relation fields not already in the selection.
///
/// Go's resolveOrderDependencies creates joins for relations referenced in ORDER BY
/// even when they're not explicitly selected in the query.
pub(super) fn collect_synthetic_order_selects(
    select: &Select,
    parent_collection: &CollectionVersion,
    already_selected: &[&str],
) -> Vec<Select> {
    // Add synthetic selects for ORDER BY relation fields not already in the selection.
    // Go's resolveOrderDependencies creates joins for relations referenced in ORDER BY
    // even when they're not explicitly selected in the query.
    let mut synthetic_order_selects: Vec<Select> = Vec::new();
    if let Some(ref order_by) = select.order_by {
        for condition in &order_by.conditions {
            // Path like ["device", "model"] — first element is the relation field
            if condition.fields.len() >= 2 {
                let relation_name = &condition.fields[0];
                if !already_selected.contains(&relation_name.as_str()) {
                    // Check this is actually a relation field
                    if let Some(field) = parent_collection.field_by_name(relation_name) {
                        if field.kind.is_relation() {
                            let mut syn_select = Select::new("");
                            syn_select.field = Field::new(relation_name);
                            synthetic_order_selects.push(syn_select);
                        }
                    }
                }
            }
        }
    }
    synthetic_order_selects
}

/// Collect info about selection joins so aggregates can share when compatible.
///
/// Go shares joins when: same relation, same filter, no limit on selection.
pub(super) fn build_selection_join_info<'a, I>(selects: I) -> HashMap<String, SelectionJoinInfo>
where
    I: IntoIterator<Item = &'a Select>,
{
    // Collect info about selection joins so aggregates can share when compatible.
    // Go shares joins when: same relation, same filter, no limit on selection.
    selects
        .into_iter()
        .map(|s| {
            let filter_json = s
                .filter
                .as_ref()
                .map(|f| serde_json::to_string(f.conditions()).unwrap_or_default());
            let has_limit = s.limit.as_ref().is_some_and(|l| l.limit.is_some());
            (
                s.field.name.clone(),
                SelectionJoinInfo {
                    filter_json,
                    has_limit,
                },
            )
        })
        .collect()
}
