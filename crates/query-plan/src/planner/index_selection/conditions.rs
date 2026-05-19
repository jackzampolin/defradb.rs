//! Condition analysis and scoring for index selection.

use document::NormalValue;
use schema::{FieldKind, IndexDescription};
use serde_json::Map;
use serde_json::Value as JsonValue;

use query_types::mapper::{Filter, FilterOp, OrderBy, OrderDirection};

use super::types::{ConditionValue, FieldCondition};

/// Determines if a filter condition should force a fallback to full scan instead of using the index.
/// Matches Go's `shouldFallbackToFullScan` in indexer_iterators.go.
pub(super) fn should_fallback_to_full_scan(
    cond: &FieldCondition,
    is_json_field: bool,
    field_kind: Option<&FieldKind>,
) -> bool {
    let is_null = matches!(
        &cond.value,
        ConditionValue::Single(document::NormalValue::Null)
    );
    let has_nested_path = cond
        .json_path
        .as_ref()
        .map(|p| !p.is_empty())
        .unwrap_or(false);

    if is_null {
        // _gte: null matches everything → fallback
        if cond.op == FilterOp::Gte {
            return true;
        }
        // _lte: null on nested JSON path → can't find missing fields
        if cond.op == FilterOp::Lte && has_nested_path {
            return true;
        }
        // _ne: null on root-level JSON → can't find empty objects/arrays
        if cond.op == FilterOp::Ne && is_json_field && !has_nested_path {
            return true;
        }
    }

    // JSON indexes only store leaf values (scalars), not objects or arrays.
    // If the filter value is a complex type, fall back to full scan.
    if is_json_field {
        // JSON ordering operators only work with numeric values. Non-numeric
        // values must take the normal scan path so filter evaluation returns
        // the correct type error.
        if matches!(
            cond.op,
            FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte
        ) && !is_numeric_filter_value(&cond.value)
            && !(cond.op == FilterOp::Lte && is_null && !has_nested_path)
        {
            return true;
        }

        // Root-level JSON LIKE filters need all documents. JSON indexes only
        // store leaf values, so objects and arrays without leaf index entries
        // would otherwise be missed.
        if matches!(
            cond.op,
            FilterOp::Like | FilterOp::Nlike | FilterOp::Ilike | FilterOp::Nilike
        ) && !has_nested_path
        {
            return true;
        }

        // _in/_nin with empty values (all objects were filtered out) → fallback
        if matches!(cond.op, FilterOp::In | FilterOp::Nin) {
            if let ConditionValue::Multiple(vs) = &cond.value {
                if vs.is_empty() {
                    return true;
                }
            }
        }
    }

    if field_kind.map(|kind| kind.is_array()).unwrap_or(false)
        && matches!(cond.op, FilterOp::Eq | FilterOp::Ne)
        && matches!(cond.value, ConditionValue::Multiple(_))
    {
        return true;
    }

    false
}

fn is_numeric_filter_value(value: &ConditionValue) -> bool {
    matches!(
        value,
        ConditionValue::Single(NormalValue::Int(_))
            | ConditionValue::Single(NormalValue::Float32(_))
            | ConditionValue::Single(NormalValue::Float64(_))
    )
}

/// Extract field conditions from a filter.
pub fn extract_field_conditions(filter: &Filter) -> Vec<FieldCondition> {
    let mut conditions = Vec::new();
    extract_conditions_recursive(filter.conditions(), &mut conditions);
    conditions
}

fn extract_conditions_recursive(
    obj: &Map<String, JsonValue>,
    conditions: &mut Vec<FieldCondition>,
) {
    for (key, value) in obj {
        // Skip logical operators at the top level
        if FilterOp::parse(key)
            .map(|op| op.is_logical())
            .unwrap_or(false)
        {
            // For AND, recurse into each sub-condition
            if key == "_and" {
                if let Some(arr) = value.as_array() {
                    for item in arr {
                        if let Some(obj) = item.as_object() {
                            extract_conditions_recursive(obj, conditions);
                        }
                    }
                }
            }
            continue;
        }

        // Field condition
        if let Some(ops) = value.as_object() {
            conditions.extend(FieldCondition::parse(key, ops));
        }
    }
}

/// Determine if a filter can use an index for optimization.
///
/// Returns true if the filter contains conditions on the first field(s)
/// of the index that can be efficiently evaluated using the index.
///
/// # Array Field Support
///
/// For array fields with multi-value indexing:
/// - `_any` with `_eq`, `_gt`, `_gte`, `_lt`, `_lte`, `_in` - CAN use index
/// - `_all` with `_eq` - CAN use index (but may need post-filtering)
/// - `_none` - CANNOT use index efficiently (requires full scan)
pub fn can_use_index(filter: &Filter, index: &IndexDescription) -> bool {
    if filter.is_empty() || index.fields.is_empty() {
        return false;
    }

    let conditions = extract_field_conditions(filter);
    if conditions.is_empty() {
        return false;
    }

    // Check if any condition matches the first field of the index
    let first_field = &index.fields[0].name;

    // For composite indexes, we can use the index if:
    // 1. The first field has a compatible condition (not _none)
    // 2. Other fields with _none are handled via residual filter
    //
    // Example: {name: {_eq: "X"}, numbers: {_none: {_eq: 3}}}
    // - first field "name" has _eq (compatible) → use index
    // - second field "numbers" has _none → handled by residual filter
    let first_field_conditions: Vec<_> = conditions
        .iter()
        .filter(|c| &c.field_name == first_field)
        .collect();

    // Check if any first-field condition can use the index
    first_field_conditions.iter().any(|cond| {
        // Check if the operator is index-compatible.
        // Go DefraDB uses indexes for _ne/_like too (full scan + matcher),
        // not just narrowing operators. This matches Go's behavior.
        let base_op_compatible = matches!(
            cond.op,
            FilterOp::Eq
                | FilterOp::Gt
                | FilterOp::Gte
                | FilterOp::Lt
                | FilterOp::Lte
                | FilterOp::In
                | FilterOp::Nin
                | FilterOp::Ne
                | FilterOp::Like
                | FilterOp::Nlike
                | FilterOp::Ilike
                | FilterOp::Nilike
        );

        // For array operators on the FIRST field, check if the combination is index-friendly
        // _none on first field cannot use index; _none on OTHER fields is fine (residual filter)
        match cond.array_op {
            Some(FilterOp::Any) => {
                // _any with comparison ops can use index
                base_op_compatible
            }
            Some(FilterOp::All) => {
                // _all can use index with any narrowing operator.
                // Index provides candidates, residual filter verifies ALL match.
                base_op_compatible
            }
            Some(FilterOp::None) => {
                // _none on FIRST field cannot efficiently use index (requires full scan)
                false
            }
            Some(_) => false,
            None => base_op_compatible,
        }
    })
}

/// Determine if an index can be used to satisfy the query ordering.
///
/// Returns a tuple of (can_use_index, needs_reverse):
/// - `can_use_index`: true if the index can satisfy the ordering
/// - `needs_reverse`: true if the index scan should be reversed
///
/// The index can satisfy ordering if:
/// - All ORDER BY fields are covered by the first N fields of the index (in order)
/// - Either all field directions match, OR all field directions are opposite
///
/// This matches Go DefraDB's `CanBeOrderedByIndex` behavior.
pub fn can_be_ordered_by_index(order_by: &OrderBy, index: &IndexDescription) -> (bool, bool) {
    if order_by.is_empty() || order_by.conditions.len() > index.fields.len() {
        return (false, false);
    }

    let mut mismatch_count = 0;

    for (i, condition) in order_by.conditions.iter().enumerate() {
        // Only consider simple field ordering (not nested relation paths)
        if condition.fields.len() != 1 {
            return (false, false);
        }

        let order_field = &condition.fields[0];
        let index_field = &index.fields[i];

        // Field names must match in order
        if order_field != &index_field.name {
            return (false, false);
        }

        // Check direction match
        let order_is_desc = condition.direction == OrderDirection::Desc;
        if index_field.descending != order_is_desc {
            mismatch_count += 1;
        }
    }

    // Can use index if:
    // - All directions match (mismatch_count == 0) → no reversal needed
    // - All directions are opposite (mismatch_count == len) → reverse needed
    let all_match = mismatch_count == 0;
    let all_mismatch = mismatch_count == order_by.conditions.len();

    if all_match {
        (true, false)
    } else if all_mismatch {
        (true, true)
    } else {
        (false, false)
    }
}

/// Select the best index for a filter from available indexes.
///
/// Returns the index that can most efficiently evaluate the filter.
pub fn select_best_index<'a>(
    filter: &Filter,
    indexes: &'a [IndexDescription],
) -> Option<&'a IndexDescription> {
    let mut best_index: Option<&IndexDescription> = None;
    let mut best_score = 0;

    for index in indexes {
        if let Some(score) = score_index_for_filter(filter, index) {
            if score > best_score {
                best_score = score;
                best_index = Some(index);
            }
        }
    }

    best_index
}

/// Score an index for a filter (higher is better).
///
/// Returns None if the index cannot be used.
fn score_index_for_filter(filter: &Filter, index: &IndexDescription) -> Option<u32> {
    if !can_use_index(filter, index) {
        return None;
    }

    let conditions = extract_field_conditions(filter);
    let mut score = 0;

    // Check how many index fields are covered by filter conditions
    for (i, field) in index.fields.iter().enumerate() {
        let field_covered = conditions.iter().any(|c| c.field_name == field.name);
        if field_covered {
            // Earlier fields in index are more valuable
            score += 10 - i as u32;

            // Exact match is most valuable (including through _any/_all)
            if conditions.iter().any(|c| {
                c.field_name == field.name
                    && c.op == FilterOp::Eq
                    && c.array_op != Some(FilterOp::None)
            }) {
                score += 5;
            }

            // _in is multi-exact-match (better than range, worse than single eq)
            if conditions
                .iter()
                .any(|c| c.field_name == field.name && c.op == FilterOp::In)
            {
                score += 4;
            }

            // Range operators narrow the scan (better than full-scan like _like/_ne)
            if conditions.iter().any(|c| {
                c.field_name == field.name
                    && matches!(
                        c.op,
                        FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte
                    )
            }) {
                score += 3;
            }
        } else {
            // Stop if we hit a gap in field coverage
            break;
        }
    }

    // Bonus for unique indexes (single lookup vs scan)
    if index.unique {
        score += 3;
    }

    if score > 0 {
        Some(score)
    } else {
        None
    }
}
