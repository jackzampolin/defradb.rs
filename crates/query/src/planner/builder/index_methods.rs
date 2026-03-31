//! Index selection methods for query planning.
//!
//! Determines whether a query can use an index for filtering or ordering.

use schema::CollectionVersion;

use crate::mapper::{Filter, OrderBy, Select};
use crate::planner::index_selection::{
    can_be_ordered_by_index, filter_to_index_scan, or_filter_to_index_scan, select_best_index,
    IndexScanParams, IndexScanType,
};

impl super::Planner {
    /// Try to select an index for the given query.
    ///
    /// Returns `Some((IndexScanParams, index_provides_ordering))` if an index
    /// can be used, `None` otherwise. Tries filter-based selection first,
    /// then falls back to ordering-based selection (matching Go behavior).
    pub(in crate::planner) fn try_select_index(
        &self,
        select: &Select,
        collection: &CollectionVersion,
    ) -> Option<(IndexScanParams, bool)> {
        if collection.indexes.is_empty() {
            return None;
        }

        // Extract limit/offset from select for passing to index scan
        let limit = select.limit.as_ref().and_then(|l| l.limit);
        let offset = select.limit.as_ref().map(|l| l.offset).unwrap_or(0);

        // Try filter-based index selection.
        // Relation fields in the filter (e.g., `owner: {name: {_eq: "X"}}`) are
        // handled by TypeJoin nodes, but scalar/FK field conditions in the same
        // filter can still use indexes. `extract_field_conditions` only extracts
        // leaf-level conditions with operator values, so relation sub-objects
        // naturally map to their field names (not index field names) and won't
        // match any index. This means `select_best_index` safely ignores them.
        if let Some(filter) = select.filter.as_ref() {
            if let Some(best_index) = select_best_index(filter, &collection.indexes) {
                if let Some(params) = filter_to_index_scan(
                    filter,
                    best_index,
                    select.order_by.as_ref(),
                    &collection.fields,
                    limit,
                    offset,
                ) {
                    // Check if this index also provides ordering
                    let provides_ordering = select
                        .order_by
                        .as_ref()
                        .map(|o| can_be_ordered_by_index(o, best_index).0)
                        .unwrap_or(false);
                    return Some((params, provides_ordering));
                }
            }
        }

        // Try OR filter index selection (e.g., {_or: [{age: {_eq: 55}}, {age: {_eq: 19}}]})
        if let Some(filter) = select.filter.as_ref() {
            if let Some(params) =
                or_filter_to_index_scan(filter, &collection.indexes, &collection.fields)
            {
                return Some((params, false));
            }
        }

        // Fallback: try ordering-based index selection (no filter needed)
        if let Some(ref order_by) = select.order_by {
            for index in &collection.indexes {
                let (can_order, needs_reverse) = can_be_ordered_by_index(order_by, index);
                if can_order {
                    let params = IndexScanParams {
                        index_name: index.name.clone(),
                        scan_type: IndexScanType::PrefixScan {
                            prefix_values: vec![],
                            reverse: needs_reverse,
                        },
                        // Pass limit/offset for early termination (index provides ordering)
                        limit,
                        offset,
                        value_filter: None,
                    };
                    return Some((params, true));
                }
            }
        }

        None
    }

    /// Try to select an index for a child collection scan.
    ///
    /// Returns `Some((IndexScanParams, per_parent_scan))` if an index can service
    /// the filter or ordering, `None` otherwise. `per_parent_scan` is true when
    /// the index is used (enabling per-parent re-scanning for correct Go metrics).
    pub(in crate::planner) fn try_select_child_index(
        &self,
        filter: Option<&Filter>,
        order_by: Option<&OrderBy>,
        collection: &CollectionVersion,
    ) -> Option<(IndexScanParams, bool)> {
        if collection.indexes.is_empty() {
            return None;
        }
        // When a fetcher is available, require it to support index queries.
        // When no fetcher is present (explain-only mode), allow index selection
        // so the plan structure matches Go's explain output.
        if let Some(ref fetcher) = self.fetcher {
            if !fetcher.supports_index_queries() {
                return None;
            }
        }
        // Try filter-based index first
        if let Some(filter) = filter {
            if let Some(best_index) = select_best_index(filter, &collection.indexes) {
                if let Some(params) =
                    filter_to_index_scan(filter, best_index, None, &collection.fields, None, 0)
                {
                    return Some((params, true));
                }
            }
        }
        // Fallback: try ordering-based index selection (scan all in index order)
        if let Some(order_by) = order_by {
            for index in &collection.indexes {
                let (can_order, needs_reverse) = can_be_ordered_by_index(order_by, index);
                if can_order {
                    return Some((
                        IndexScanParams {
                            index_name: index.name.clone(),
                            scan_type: IndexScanType::PrefixScan {
                                prefix_values: vec![],
                                reverse: needs_reverse,
                            },
                            limit: None,
                            offset: 0,
                            value_filter: None,
                        },
                        true,
                    ));
                }
            }
        }
        None
    }
}
