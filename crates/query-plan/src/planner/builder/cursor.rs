//! Cursor pagination planner expansion.
//!
//! Mirrors Go's `expandCursorPlan` and `validateCursorIndex` from
//! defradb's `internal/planner/planner.go` (PR #4617, branch pr-4617).

use crate::plan::{CursorDirection, CursorNode};
use crate::planner::index_selection::CursorSeek;
use crate::planner::PlanNode;
use cursor::Cursor;
use query_types::error::{QueryError, Result};
use query_types::mapper::{OrderCondition, OrderDirection, Select};
use schema::{CollectionVersion, IndexDescription};
use storage::field_value::encode_field_value;
use storage::keys::IndexDataStoreKey;

use crate::planner::index_selection::{json_to_normal_value, normalize_for_index_field};

/// Wrap a plan tree with `CursorNode`, configure any scan in the tree
/// with `cursor_seek`, and validate that ordering is supported by an
/// index when the ordering is non-empty and not docID-only.
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
    let (reversed, matched_index) = validate_cursor_index(collection, &order_fields)?;

    // Decode tokens (empty/whitespace tokens are treated as no token).
    let after = decode_optional(&params.after)?;
    let before = decode_optional(&params.before)?;

    // Determine direction and page limit. A missing `first`/`last` (`None`)
    // means "no limit" — the CursorNode returns all remaining rows, matching
    // Go's `cursorNode` which gates its limit on `first/last.HasValue()`.
    let (direction, limit) = if params.is_backward() {
        (CursorDirection::Backward, params.last)
    } else {
        (CursorDirection::Forward, params.first)
    };

    // Configure scan for cursor seek — walks the plan tree to set cursor_seek
    // on any IndexScanNode found inside.
    let (plan, index_seek_active) = configure_scan_for_cursor(
        plan,
        &after,
        &before,
        direction,
        reversed,
        &order_fields,
        collection,
        matched_index.as_ref(),
    )?;

    // Wrap with CursorNode.
    Ok(Box::new(CursorNode::new(
        plan,
        direction,
        limit,
        after,
        before,
        select.cursor_page_info.clone(),
        order_fields,
        index_seek_active,
    )))
}

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

/// Walk the plan tree to configure cursor seek on the underlying `IndexScanNode`.
///
/// Returns `(plan, applied)` where `applied` is `true` when a seek was
/// successfully set on an `IndexScanNode` inside the plan tree.
///
/// Falls back to `(plan, false)` — the slow path — when:
/// - No cursor token is present.
/// - The cursor token has no keys.
/// - No index is matched (doc-ID order or empty order).
/// - The plan tree has no `IndexScanNode` (full scan).
#[allow(clippy::too_many_arguments)]
fn configure_scan_for_cursor(
    mut plan: Box<dyn PlanNode>,
    after: &Option<Cursor>,
    before: &Option<Cursor>,
    direction: CursorDirection,
    reversed: bool,
    order_fields: &[OrderCondition],
    collection: &CollectionVersion,
    matched_index: Option<&IndexDescription>,
) -> Result<(Box<dyn PlanNode>, bool)> {
    let active_cursor = match direction {
        CursorDirection::Forward => after.as_ref(),
        CursorDirection::Backward => before.as_ref(),
    };

    // No cursor token → slow path.
    let Some(cursor_token) = active_cursor else {
        return Ok((plan, false));
    };
    if cursor_token.keys.is_empty() {
        return Ok((plan, false));
    }

    // No matched index → no index seek possible.
    let Some(idx) = matched_index else {
        return Ok((plan, false));
    };

    // Unique composite-prefix case: the seek key encodes only `order_fields`
    // (see `build_cursor_seek_key`), so when `order_fields` covers fewer fields
    // than the unique index, the seek key is ambiguous within that prefix tier
    // and forward-exclusive can drop the whole tier. Fall back to slow path.
    // The check uses `order_fields.len()` rather than `cursor.keys.len()`
    // because the latter is untrusted client input — a stale or malicious
    // token could carry extra unrelated keys and bypass this guard.
    if idx.unique && order_fields.len() < idx.fields.len() {
        return Ok((plan, false));
    }

    let seek_key = build_cursor_seek_key(cursor_token, order_fields, collection, idx)?;
    let seek = CursorSeek {
        seek_key,
        // Both `after` (forward) and `before` (backward) identify the boundary
        // row that should NOT appear in the page — both are exclusive.
        inclusive: false,
        // `reversed` from validate_cursor_index means "must scan in reverse to
        // satisfy ORDER BY". For a forward cursor we keep that direction; for a
        // backward cursor we flip it so we scan against the ORDER BY direction
        // (i.e., iterate from the boundary toward the beginning of the order).
        reversed: match direction {
            CursorDirection::Forward => reversed,
            CursorDirection::Backward => !reversed,
        },
        expected_index_name: idx.name.clone(),
    };

    let applied = plan.set_cursor_seek(seek);
    Ok((plan, applied))
}

/// Build the storage-encoded seek key for a cursor token.
///
/// The key format matches the one used by `RangeIterator` bounds:
///   `IndexDataStoreKey::index_prefix(collection_short_id, index_id)`
///   + `encode_field_value(...)` for each ordered field.
///
/// Field values are extracted from `cursor.keys` in the order given by
/// `order_fields` and encoded using the same direction (ascending/descending)
/// as the matched index fields.
fn build_cursor_seek_key(
    cursor: &Cursor,
    order_fields: &[OrderCondition],
    collection: &CollectionVersion,
    idx: &IndexDescription,
) -> Result<Vec<u8>> {
    let collection_short_id = collection.resolved_root_id();
    let index_id = idx.id;

    let mut key = IndexDataStoreKey::index_prefix(collection_short_id, index_id);

    for (i, cond) in order_fields.iter().enumerate() {
        let Some(field_name) = cond.fields.first() else {
            continue;
        };
        let Some(json_val) = cursor.keys.get(field_name) else {
            return Err(QueryError::cursor_invalid());
        };
        let raw = json_to_normal_value(json_val).ok_or_else(QueryError::cursor_invalid)?;
        // Normalize to the schema field's encoding type so the seek key bytes
        // match what the index actually stores (e.g., Float64 JSON → Float32).
        let normal = normalize_for_index_field(raw, field_name, &collection.fields);

        // Use the index's descending flag for this field position, if available.
        let descending = idx.fields.get(i).map(|f| f.descending).unwrap_or(false);

        key = encode_field_value(key, &normal, descending)
            .map_err(|e| QueryError::execution(format!("failed to encode cursor seek key: {e}")))?;
    }

    // On-disk key shape:
    // - Non-unique:          [prefix][values][doc_id]   (doc_id always in key)
    // - Unique non-null:     [prefix][values]           (doc_id in value)
    // - Unique with any nil: [prefix][values][doc_id]   (per unique.rs:228 — has_nil_field)
    //
    // Without the doc_id suffix for non-unique indexes, the seek key matches the
    // prefix of EVERY row sharing those field values. An exclusive forward seek at
    // that prefix would reject all of them — not just the boundary doc — causing
    // the query to skip every row with the same indexed values (the duplicate-keys bug).
    //
    // For unique indexes with non-null values, the doc_id is stored in the VALUE so
    // the key alone identifies one row. But when any cursor key is null, the on-disk
    // format appends doc_id to the key (same as non-unique), so we must include it.
    // Only consider the values that correspond to actual index fields.
    // Checking all cursor.keys.values() would be vulnerable to extra unrelated
    // keys in a malicious or stale token triggering (or suppressing) the doc_id
    // suffix incorrectly.
    let has_null_value = idx
        .fields
        .iter()
        .filter_map(|f| cursor.keys.get(&f.name))
        .any(|v| v.is_null());
    if !idx.unique || has_null_value {
        key.extend_from_slice(cursor.doc_id.as_bytes());
    }

    Ok(key)
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
        let coll =
            make_collection_with_indexes(vec![make_index("idx_age", vec![("age", false)], true)]);
        let order = order_asc("age");
        let (reversed, matched) = validate_cursor_index(&coll, &order).unwrap();
        assert!(!reversed);
        assert!(matched.is_some());
    }

    #[test]
    fn matching_asc_index_with_desc_order_returns_reversed() {
        let coll =
            make_collection_with_indexes(vec![make_index("idx_age", vec![("age", false)], true)]);
        let order = order_desc("age");
        let (reversed, matched) = validate_cursor_index(&coll, &order).unwrap();
        assert!(reversed);
        assert!(matched.is_some());
    }

    #[test]
    fn no_matching_index_returns_error() {
        let coll =
            make_collection_with_indexes(vec![make_index("idx_age", vec![("age", false)], false)]);
        let order = order_asc("name");
        let err = validate_cursor_index(&coll, &order).unwrap_err();
        assert_eq!(
            err.to_string(),
            "no supporting index for cursor order field"
        );
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
        assert_eq!(
            err.to_string(),
            "no supporting index for cursor order field"
        );
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

    // P2.2: build_cursor_seek_key must normalize JSON values against field schema type.
    // A JSON number for a Float32 field arrives as Float64 from serde_json; without
    // normalization the encoded bytes won't match the index (which stores Float32).
    #[test]
    fn seek_key_normalizes_float64_to_float32_for_float32_field() {
        use document::NormalValue;
        use schema::{FieldDescription, FieldKind};
        use storage::field_value::encode_field_value;
        use storage::keys::IndexDataStoreKey;

        let idx = make_index("idx_score", vec![("score", false)], true);

        // Build a collection whose "score" field is Float32.
        let float32_field = FieldDescription::new("1", "score", FieldKind::float32());
        let mut coll = CollectionVersion::new("test", "v1", "coll_test_001", vec![float32_field]);
        coll.indexes = vec![idx.clone()];

        // Cursor token with "score" as a JSON float (arrives as Float64).
        let mut keys = std::collections::BTreeMap::new();
        keys.insert("score".to_string(), serde_json::json!(1.5));
        let cursor = Cursor {
            keys,
            doc_id: "bae-test".to_string(),
            direction: String::new(),
        };
        let order = order_asc("score");

        let key = build_cursor_seek_key(&cursor, &order, &coll, &idx).unwrap();

        // Verify the key is non-empty and matches what a Float32-encoded value produces.
        let prefix = IndexDataStoreKey::index_prefix(coll.resolved_root_id(), idx.id);
        let expected = encode_field_value(prefix, &NormalValue::Float32(1.5_f32), false).unwrap();
        // For a unique index, no doc_id suffix is appended.
        assert_eq!(
            key, expected,
            "seek key must use Float32 encoding to match index bytes"
        );
    }
}
