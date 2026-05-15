//! Cursor pagination planner expansion.
//!
//! Mirrors Go's `expandCursorPlan` and `validateCursorIndex` from
//! defradb's `internal/planner/planner.go` (PR #4617, branch pr-4617).

use crate::plan::{CursorDirection, CursorNode};
use crate::planner::PlanNode;
use cursor::Cursor;
use query_types::error::{QueryError, Result};
use query_types::mapper::{OrderCondition, OrderDirection, Select};
use schema::{CollectionVersion, IndexDescription};

/// Wrap a plan tree with `CursorNode`, configure any scan in the tree
/// with `cursor_seek` (deferred to Task 11), and validate that ordering
/// is supported by an index when the ordering is non-empty and not
/// docID-only.
// Task 11 will call this from groupby.rs; allow dead_code until then.
#[allow(dead_code)]
pub(crate) fn expand_cursor_plan(
    select: &Select,
    collection: &CollectionVersion,
    plan: Box<dyn PlanNode>,
) -> Result<Box<dyn PlanNode>> {
    let params = select
        .cursor_params
        .as_ref()
        .ok_or_else(|| QueryError::internal("expand_cursor_plan called on non-cursor select"))?;

    let order_fields: Vec<OrderCondition> = select
        .order_by
        .as_ref()
        .map(|o| o.conditions.clone())
        .unwrap_or_default();

    // Validate index coverage; may error with cursor_no_supporting_index.
    let (reversed, _matched_index) = validate_cursor_index(collection, &order_fields)?;

    // Decode tokens (empty/whitespace tokens are treated as no token).
    let after = decode_optional(&params.after)?;
    let before = decode_optional(&params.before)?;

    // Determine direction and page size.
    let (direction, page_size) = if params.is_backward() {
        (CursorDirection::Backward, params.last.unwrap_or(0))
    } else {
        (CursorDirection::Forward, params.first.unwrap_or(0))
    };

    // Configure scan for cursor seek — stubbed; Task 11 implements the tree walk.
    let (plan, index_seek_active) = configure_scan_for_cursor(
        plan,
        &after,
        &before,
        direction,
        reversed,
        &order_fields,
    )?;

    // Wrap with CursorNode.
    Ok(Box::new(CursorNode::new(
        plan,
        direction,
        page_size,
        after,
        before,
        select.cursor_page_info,
        order_fields,
        index_seek_active,
    )))
}

#[allow(dead_code)]
fn decode_optional(token: &Option<String>) -> Result<Option<Cursor>> {
    match token {
        Some(s) if !s.is_empty() => Cursor::decode(s)
            .map(Some)
            .map_err(|_| QueryError::cursor_invalid()),
        _ => Ok(None),
    }
}

/// Mirrors Go's `validateCursorIndex` at `planner.go:313-337` on branch pr-4617.
/// Returns `(reversed, matched_index)`. When `order_fields` is empty or only
/// orders by `_docID`, returns `(false, None)` — **no index required**.
// Task 11 will call this from groupby.rs; allow dead_code until then.
#[allow(dead_code)]
pub(crate) fn validate_cursor_index(
    collection: &CollectionVersion,
    order_fields: &[OrderCondition],
) -> Result<(bool, Option<IndexDescription>)> {
    if order_fields.is_empty() {
        return Ok((false, None));
    }
    if is_doc_id_order(order_fields) {
        return Ok((false, None));
    }

    // Find an index that supports the requested ordering.
    let Some((idx, reversed)) = find_matching_index(&collection.indexes, order_fields) else {
        return Err(QueryError::cursor_no_supporting_index());
    };

    // Composite prefix rule: a non-unique index must cover ALL order fields.
    // Go's `isUnsupportedCursorCompositePrefix`: `!index.Unique && len(ordering) < len(index.Fields)`.
    if !idx.unique && order_fields.len() < idx.fields.len() {
        return Err(QueryError::cursor_no_supporting_index());
    }

    Ok((reversed, Some(idx)))
}

#[allow(dead_code)]
fn is_doc_id_order(order_fields: &[OrderCondition]) -> bool {
    matches!(
        order_fields
            .first()
            .and_then(|c| c.fields.first())
            .map(String::as_str),
        Some("_docID")
    )
}

/// Return `(index, reversed)` for the first index that supports the requested
/// ordering. `reversed` means iteration must run in reverse to produce the
/// requested order.
#[allow(dead_code)]
fn find_matching_index(
    indexes: &[IndexDescription],
    order_fields: &[OrderCondition],
) -> Option<(IndexDescription, bool)> {
    indexes
        .iter()
        .find_map(|idx| index_covers_ordering(idx, order_fields).map(|r| (idx.clone(), r)))
}

/// Check whether `idx` can produce `order_fields`. Each order condition must
/// match the index's field at the same position. Direction handling:
/// - if all conditions match the index direction exactly, reversed=false
/// - if all conditions are the opposite, reversed=true
/// - mixed → no match (None)
#[allow(dead_code)]
fn index_covers_ordering(idx: &IndexDescription, order_fields: &[OrderCondition]) -> Option<bool> {
    if idx.fields.len() < order_fields.len() {
        return None;
    }
    let mut required_reversed: Option<bool> = None;
    for (i, cond) in order_fields.iter().enumerate() {
        let idx_field = &idx.fields[i];
        let cond_field = cond.fields.first().map(String::as_str)?;
        if cond_field != idx_field.name {
            return None;
        }
        let cond_desc = matches!(cond.direction, OrderDirection::Desc);
        let needs_reverse = cond_desc != idx_field.descending;
        match required_reversed {
            None => required_reversed = Some(needs_reverse),
            Some(prev) if prev == needs_reverse => {}
            _ => return None,
        }
    }
    Some(required_reversed.unwrap_or(false))
}

/// Stub: Task 11 will implement the plan-tree walk that sets `cursor_seek`
/// on the IndexScanNode. For now, always return `(plan, false)` — slow path.
#[allow(dead_code)]
fn configure_scan_for_cursor(
    plan: Box<dyn PlanNode>,
    _after: &Option<Cursor>,
    _before: &Option<Cursor>,
    _direction: CursorDirection,
    _reversed: bool,
    _order_fields: &[OrderCondition],
) -> Result<(Box<dyn PlanNode>, bool)> {
    Ok((plan, false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{IndexDescription, IndexedFieldDescription};

    fn make_index(name: &str, fields: Vec<(&str, bool)>, unique: bool) -> IndexDescription {
        IndexDescription {
            name: name.to_string(),
            id: 0,
            unique,
            auto_generated: false,
            fields: fields
                .into_iter()
                .map(|(name, desc)| IndexedFieldDescription {
                    name: name.to_string(),
                    descending: desc,
                })
                .collect(),
        }
    }

    fn make_collection_with_indexes(indexes: Vec<IndexDescription>) -> CollectionVersion {
        let mut coll = CollectionVersion::new("test", "v1", "coll_test_001", vec![]);
        coll.indexes = indexes;
        coll
    }

    fn order_asc(field: &str) -> Vec<OrderCondition> {
        vec![OrderCondition::new(field, OrderDirection::Asc)]
    }

    fn order_desc(field: &str) -> Vec<OrderCondition> {
        vec![OrderCondition::new(field, OrderDirection::Desc)]
    }

    #[test]
    fn empty_order_returns_no_index_needed() {
        let coll = make_collection_with_indexes(vec![]);
        let (reversed, matched) = validate_cursor_index(&coll, &[]).unwrap();
        assert!(!reversed);
        assert!(matched.is_none());
    }

    #[test]
    fn doc_id_order_returns_no_index_needed() {
        let coll = make_collection_with_indexes(vec![]);
        let order = order_asc("_docID");
        let (reversed, matched) = validate_cursor_index(&coll, &order).unwrap();
        assert!(!reversed);
        assert!(matched.is_none());
    }

    #[test]
    fn matching_unique_index_returns_ok() {
        let coll = make_collection_with_indexes(vec![make_index(
            "idx_age",
            vec![("age", false)],
            true,
        )]);
        let order = order_asc("age");
        let (reversed, matched) = validate_cursor_index(&coll, &order).unwrap();
        assert!(!reversed);
        assert!(matched.is_some());
    }

    #[test]
    fn matching_asc_index_with_desc_order_returns_reversed() {
        let coll = make_collection_with_indexes(vec![make_index(
            "idx_age",
            vec![("age", false)],
            true,
        )]);
        let order = order_desc("age");
        let (reversed, matched) = validate_cursor_index(&coll, &order).unwrap();
        assert!(reversed);
        assert!(matched.is_some());
    }

    #[test]
    fn no_matching_index_returns_error() {
        let coll = make_collection_with_indexes(vec![make_index(
            "idx_age",
            vec![("age", false)],
            false,
        )]);
        let order = order_asc("name");
        let err = validate_cursor_index(&coll, &order).unwrap_err();
        assert_eq!(err.to_string(), "no supporting index for cursor order field");
    }

    #[test]
    fn non_unique_composite_prefix_returns_error() {
        // Index on (age, name) non-unique. Ordering only by age = prefix mismatch.
        let coll = make_collection_with_indexes(vec![make_index(
            "idx_age_name",
            vec![("age", false), ("name", false)],
            false,
        )]);
        let order = order_asc("age");
        let err = validate_cursor_index(&coll, &order).unwrap_err();
        assert_eq!(err.to_string(), "no supporting index for cursor order field");
    }

    #[test]
    fn unique_composite_prefix_succeeds() {
        // Unique index on (age, name); ordering only by age is allowed because
        // unique indexes don't need full coverage (Go's rule).
        let coll = make_collection_with_indexes(vec![make_index(
            "idx_age_name",
            vec![("age", false), ("name", false)],
            true,
        )]);
        let order = order_asc("age");
        let (_reversed, matched) = validate_cursor_index(&coll, &order).unwrap();
        assert!(matched.is_some());
    }
}
