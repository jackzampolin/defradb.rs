//! Shared types and helpers for join planning.

use std::collections::HashMap;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::Select;
use crate::planner::PlanNode;

/// Result of applying joins: the updated plan node, document mapping, aggregate internal keys,
/// and whether a child index scan provides the parent's ORDER BY.
pub(in crate::planner) type JoinResult = Result<(
    Box<dyn PlanNode>,
    DocumentMapping,
    HashMap<String, (String, String)>,
    bool, // join_provides_ordering
)>;

/// Info about a selection join, used for aggregate join sharing decisions.
pub(in crate::planner) struct SelectionJoinInfo {
    pub(in crate::planner) filter_json: Option<String>,
    pub(in crate::planner) has_limit: bool,
}

/// Check if a nested select is simple enough to use the direct indexed child cache.
pub(in crate::planner) fn can_use_direct_indexed_child_cache(nested_select: &Select) -> bool {
    use crate::mapper::Requestable;

    nested_select.filter.is_none()
        && nested_select.group_by.is_none()
        && nested_select
            .order_by
            .as_ref()
            .map(|order_by| !order_by.has_relation_order())
            .unwrap_or(true)
        && nested_select.fields.iter().all(|field| match field {
            Requestable::Field(_) => true,
            Requestable::FullTextSearch(fts) => {
                fts.target_fields.iter().all(|field| !field.contains('.'))
            }
            _ => false,
        })
}
