//! Index selection and filter-to-index translation
//!
//! Provides utilities for determining when filters can use indexes
//! and translating filter conditions to index scan parameters.

use std::collections::HashMap;

#[cfg(test)]
use document::JsonPathPart;
use document::{JsonLeafValue, JsonPath, JsonScalarValue, NormalValue};
use schema::{FieldKind, IndexDescription, ScalarKind};
use serde_json::Value as JsonValue;
use storage::index::Bound;

use crate::mapper::{Filter, FilterOp, OrderBy, OrderDirection};

/// Parameters for executing an index scan.
#[derive(Debug, Clone)]
pub struct IndexScanParams {
    /// The index to use
    pub index_name: String,
    /// Scan type and parameters
    pub scan_type: IndexScanType,
    /// Optional limit for early termination (for ORDER BY + LIMIT optimization)
    pub limit: Option<u64>,
    /// Offset to skip before collecting results (for ORDER BY + LIMIT + OFFSET optimization)
    pub offset: u64,
}

/// Type of index scan to perform.
#[derive(Debug, Clone)]
pub enum IndexScanType {
    /// Exact match on all indexed fields
    ExactMatch { values: Vec<NormalValue> },
    /// Prefix match on first N fields
    PrefixScan {
        prefix_values: Vec<NormalValue>,
        reverse: bool,
    },
    /// Range scan with optional bounds
    RangeScan {
        prefix_values: Vec<NormalValue>,
        lower: Bound,
        upper: Bound,
        reverse: bool,
    },
    /// Multiple exact match values (IN operator).
    /// For composite indexes, `suffix_values` holds Eq values for subsequent fields,
    /// enabling exact-match lookups instead of prefix scans.
    InScan {
        values: Vec<NormalValue>,
        suffix_values: Vec<NormalValue>,
    },
}

/// A parsed filter condition on a single field.
#[derive(Debug, Clone)]
pub struct FieldCondition {
    /// The field name
    pub field_name: String,
    /// The operator
    pub op: FilterOp,
    /// The value(s) to match
    pub value: ConditionValue,
    /// Array operator wrapper (if this is an array field condition)
    /// e.g., for `numbers: {_any: {_eq: 30}}`, this would be Some(Any)
    pub array_op: Option<FilterOp>,
    /// JSON path for JSON field conditions
    /// e.g., for `custom: {height: {_gt: 170}}`, this would be Some(["height"])
    pub json_path: Option<JsonPath>,
}

/// Value in a filter condition.
#[derive(Debug, Clone)]
pub enum ConditionValue {
    /// Single value
    Single(NormalValue),
    /// Multiple values (for _in, _nin)
    Multiple(Vec<NormalValue>),
    /// Pattern string (for _like, _nlike)
    Pattern(String),
}

impl FieldCondition {
    /// Parse a field condition from JSON value.
    pub fn parse(field_name: &str, ops: &serde_json::Map<String, JsonValue>) -> Vec<Self> {
        Self::parse_with_path(field_name, ops, None, None)
    }

    /// Parse a field condition with optional JSON path and array operator.
    fn parse_with_path(
        field_name: &str,
        ops: &serde_json::Map<String, JsonValue>,
        json_path: Option<JsonPath>,
        array_op: Option<FilterOp>,
    ) -> Vec<Self> {
        let mut conditions = Vec::new();

        for (op_str, value) in ops {
            // First check if this is a recognized operator
            if let Some(op) = FilterOp::parse(op_str) {
                // Handle array element operators (_any, _all, _none)
                // These wrap inner conditions: {_any: {_eq: 30}}
                if op.is_array_element_op() {
                    if let Some(inner_ops) = value.as_object() {
                        // For JSON fields with _any, add Index to path
                        let new_path = json_path.as_ref().map(|p| p.append_index());
                        let inner_conditions =
                            Self::parse_with_path(field_name, inner_ops, new_path, Some(op));
                        conditions.extend(inner_conditions);
                    }
                    continue;
                }

                let condition_value = match op {
                    FilterOp::In | FilterOp::Nin => {
                        if let Some(arr) = value.as_array() {
                            ConditionValue::Multiple(
                                arr.iter().filter_map(json_to_normal_value).collect(),
                            )
                        } else {
                            continue;
                        }
                    }
                    FilterOp::Like | FilterOp::Nlike | FilterOp::Ilike | FilterOp::Nilike => {
                        if let Some(s) = value.as_str() {
                            ConditionValue::Pattern(s.to_string())
                        } else {
                            continue;
                        }
                    }
                    FilterOp::ContainedIn => {
                        // _contained_in expects an array value
                        if let Some(arr) = value.as_array() {
                            ConditionValue::Multiple(
                                arr.iter().filter_map(json_to_normal_value).collect(),
                            )
                        } else {
                            continue;
                        }
                    }
                    FilterOp::HasKey => {
                        // _has_key expects a string key
                        if let Some(s) = value.as_str() {
                            ConditionValue::Pattern(s.to_string())
                        } else {
                            continue;
                        }
                    }
                    // _contains and other operators expect single values
                    _ => {
                        if let Some(nv) = json_to_normal_value(value) {
                            ConditionValue::Single(nv)
                        } else {
                            continue;
                        }
                    }
                };

                conditions.push(FieldCondition {
                    field_name: field_name.to_string(),
                    op,
                    value: condition_value,
                    array_op,
                    json_path: json_path.clone(),
                });
            } else {
                // Not an operator - this is a JSON path property
                // e.g., for {custom: {height: {_gt: 170}}}, "height" is a JSON path part
                if let Some(inner_ops) = value.as_object() {
                    let new_path = match &json_path {
                        Some(p) => p.append_property(op_str),
                        None => JsonPath::default().append_property(op_str),
                    };
                    let inner_conditions =
                        Self::parse_with_path(field_name, inner_ops, Some(new_path), array_op);
                    conditions.extend(inner_conditions);
                }
            }
        }

        conditions
    }
}

/// Convert JSON value to NormalValue.
fn json_to_normal_value(value: &JsonValue) -> Option<NormalValue> {
    match value {
        JsonValue::Null => Some(NormalValue::Null),
        JsonValue::Bool(b) => Some(NormalValue::Bool(*b)),
        JsonValue::Number(n) => n
            .as_i64()
            .map(NormalValue::Int)
            .or_else(|| n.as_f64().map(NormalValue::Float64)),
        JsonValue::String(s) => {
            // Try to parse as DateTime first (RFC3339/ISO8601)
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                return Some(NormalValue::Time(dt));
            }
            Some(NormalValue::String(s.clone()))
        }
        _ => None,
    }
}

/// Convert NormalValue to JsonScalarValue for use in JsonLeafValue.
fn normal_value_to_json_scalar(value: &NormalValue) -> Option<JsonScalarValue> {
    match value {
        NormalValue::Null => Some(JsonScalarValue::Null),
        NormalValue::Bool(b) => Some(JsonScalarValue::Bool(*b)),
        NormalValue::Int(i) => Some(JsonScalarValue::Number(*i as f64)),
        NormalValue::Float64(f) => Some(JsonScalarValue::Number(*f)),
        NormalValue::Float32(f) => Some(JsonScalarValue::Number(*f as f64)),
        NormalValue::String(s) => Some(JsonScalarValue::String(s.clone())),
        _ => None,
    }
}

/// Wrap a NormalValue in JsonLeafValue if a JSON path is present.
fn wrap_value_for_json_path(value: NormalValue, json_path: Option<&JsonPath>) -> NormalValue {
    match json_path {
        Some(path) if !path.0.is_empty() => {
            if let Some(scalar) = normal_value_to_json_scalar(&value) {
                NormalValue::JsonLeaf(JsonLeafValue {
                    path: path.clone(),
                    value: scalar,
                })
            } else {
                value
            }
        }
        _ => value,
    }
}

/// Wrap multiple values for JSON path (for _in operator).
fn wrap_values_for_json_path(
    values: Vec<NormalValue>,
    json_path: Option<&JsonPath>,
) -> Vec<NormalValue> {
    match json_path {
        Some(path) if !path.0.is_empty() => values
            .into_iter()
            .filter_map(|v| {
                normal_value_to_json_scalar(&v).map(|scalar| {
                    NormalValue::JsonLeaf(JsonLeafValue {
                        path: path.clone(),
                        value: scalar,
                    })
                })
            })
            .collect(),
        _ => values,
    }
}

/// Normalize a NormalValue to match the schema field's encoding type.
/// This ensures filter values use the same encoding as stored index values.
/// For example, a Float32 field stores values with `encode_float32_ascending`,
/// so lookup values must also be Float32 (not Float64 or Int).
fn normalize_value_for_field(value: NormalValue, field_kind: &FieldKind) -> NormalValue {
    match (&value, field_kind) {
        // Float64 → Float32 when schema says Float32
        (NormalValue::Float64(f), FieldKind::Scalar(ScalarKind::Float32)) => {
            NormalValue::Float32(*f as f32)
        }
        // Int → Float32 when schema says Float32
        (NormalValue::Int(i), FieldKind::Scalar(ScalarKind::Float32)) => {
            NormalValue::Float32(*i as f32)
        }
        // Int → Float64 when schema says Float64
        (NormalValue::Int(i), FieldKind::Scalar(ScalarKind::Float64)) => {
            NormalValue::Float64(*i as f64)
        }
        _ => value,
    }
}

/// Normalize a NormalValue for a named index field using collection field metadata.
fn normalize_for_index_field(
    value: NormalValue,
    field_name: &str,
    collection_fields: &[schema::FieldDescription],
) -> NormalValue {
    if let Some(field) = collection_fields.iter().find(|f| f.name == field_name) {
        normalize_value_for_field(value, &field.kind)
    } else {
        value
    }
}

/// Extract field conditions from a filter.
pub fn extract_field_conditions(filter: &Filter) -> Vec<FieldCondition> {
    let mut conditions = Vec::new();
    extract_conditions_recursive(filter.conditions(), &mut conditions);
    conditions
}

fn extract_conditions_recursive(
    obj: &HashMap<String, JsonValue>,
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
                            let nested: HashMap<String, JsonValue> =
                                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                            extract_conditions_recursive(&nested, conditions);
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

/// Convert a filter to index scan parameters.
///
/// Returns None if the filter cannot use the index efficiently.
///
/// # Ordering Support
///
/// If `order_by` is provided, the scan direction will be set based on whether
/// the index can satisfy the ordering:
/// - If ordering matches index direction: forward scan
/// - If ordering is opposite of index direction: reverse scan
///
/// # Limit/Offset Support
///
/// If `limit` and `offset` are provided and the index provides ordering,
/// these are passed through to enable early termination during index scan.
/// This optimization allows ORDER BY + LIMIT queries to stop scanning early.
///
/// # Array Field Support
///
/// For array fields with `_any`, `_all` operators, the scan type is determined
/// by the inner operator. For example:
/// - `{numbers: {_any: {_eq: 30}}}` → ExactMatch{values: [30]}
/// - `{tags: {_any: {_in: ["red", "blue"]}}}` → InScan{values: ["red", "blue"]}
pub fn filter_to_index_scan(
    filter: &Filter,
    index: &IndexDescription,
    order_by: Option<&OrderBy>,
    collection_fields: &[schema::FieldDescription],
    limit: Option<u64>,
    offset: u64,
) -> Option<IndexScanParams> {
    if !can_use_index(filter, index) {
        return None;
    }

    let conditions = extract_field_conditions(filter);
    let first_field = &index.fields[0].name;

    // Check if the first index field is JSON-typed
    let first_field_is_json = collection_fields
        .iter()
        .any(|f| &f.name == first_field && matches!(f.kind, FieldKind::Scalar(ScalarKind::Json)));

    // Find conditions on the first index field
    // For JSON fields, ensure top-level conditions get an empty json_path
    let first_field_conditions: Vec<_> = conditions
        .iter()
        .map(|c| {
            if &c.field_name == first_field && first_field_is_json && c.json_path.is_none() {
                // Top-level JSON filter (e.g., custom: {_gt: 20}) needs an empty path
                let mut c = c.clone();
                c.json_path = Some(JsonPath::new());
                c
            } else {
                c.clone()
            }
        })
        .filter(|c| &c.field_name == first_field)
        .collect();

    if first_field_conditions.is_empty() {
        return None;
    }

    // Analyze conditions to determine scan type
    // For array operators, we look at the inner operator
    // Track JSON path for wrapping values when needed
    let mut has_eq = false;
    let mut eq_value = None;
    let mut eq_json_path: Option<JsonPath> = None;
    let mut has_in = false;
    let mut in_values = None;
    let mut in_json_path: Option<JsonPath> = None;
    let mut has_scan_all = false;
    let mut lower_bound = Bound::Unbounded;
    let mut upper_bound = Bound::Unbounded;
    let mut range_json_path: Option<JsonPath> = None;

    for cond in &first_field_conditions {
        // Skip _none operators (they don't use index)
        if cond.array_op == Some(FilterOp::None) {
            continue;
        }

        match cond.op {
            FilterOp::Eq => {
                if let ConditionValue::Single(v) = &cond.value {
                    has_eq = true;
                    eq_value = Some(v.clone());
                    eq_json_path = cond.json_path.clone();
                }
            }
            FilterOp::In => {
                if let ConditionValue::Multiple(vs) = &cond.value {
                    has_in = true;
                    in_values = Some(vs.clone());
                    in_json_path = cond.json_path.clone();
                }
            }
            FilterOp::Gt => {
                if let ConditionValue::Single(v) = &cond.value {
                    lower_bound = Bound::Exclusive(v.clone());
                    range_json_path = cond.json_path.clone();
                }
            }
            FilterOp::Gte => {
                if let ConditionValue::Single(v) = &cond.value {
                    lower_bound = Bound::Inclusive(v.clone());
                    range_json_path = cond.json_path.clone();
                }
            }
            FilterOp::Lt => {
                if let ConditionValue::Single(v) = &cond.value {
                    upper_bound = Bound::Exclusive(v.clone());
                    range_json_path = cond.json_path.clone();
                }
            }
            FilterOp::Lte => {
                if let ConditionValue::Single(v) = &cond.value {
                    upper_bound = Bound::Inclusive(v.clone());
                    range_json_path = cond.json_path.clone();
                }
            }
            // _ne/_nin/_like/_nlike use full index scan with post-filtering (matches Go behavior)
            // For JSON fields, we still need to track the path to constrain the scan
            FilterOp::Ne
            | FilterOp::Nin
            | FilterOp::Like
            | FilterOp::Nlike
            | FilterOp::Ilike
            | FilterOp::Nilike => {
                has_scan_all = true;
                // Track JSON path for scan_all so we can constrain to the path
                if cond.json_path.is_some() {
                    range_json_path = cond.json_path.clone();
                }
            }
            _ => {}
        }
    }

    // Determine if we need to reverse the scan based on ordering
    let reverse = order_by
        .map(|o| can_be_ordered_by_index(o, index))
        .map(|(can_order, needs_reverse)| can_order && needs_reverse)
        .unwrap_or(false);

    // For descending indexes, range bounds must be swapped because the encoding
    // reverses the byte order: higher values have lower encoded bytes.
    // e.g., _gt:30 on DESC index → upper_bound (not lower) in byte order.
    let first_field_descending = index.fields.first().map(|f| f.descending).unwrap_or(false);
    if first_field_descending {
        std::mem::swap(&mut lower_bound, &mut upper_bound);
    }

    // For composite indexes, check if subsequent fields also have eq conditions.
    // If all fields are matched exactly, use ExactMatch; otherwise use PrefixScan.
    //
    // Note: We exclude _none and _all array operators here because they should be
    // handled as residual filters, not used to narrow the index scan.
    // - _none: cannot narrow scan (requires checking ALL elements don't match)
    // - _all: index provides candidates but residual filter verifies ALL match
    let is_composite = index.fields.len() > 1;
    let mut subsequent_eq_values: Vec<NormalValue> = Vec::new();
    if is_composite && has_eq {
        for field_desc in index.fields.iter().skip(1) {
            let field_cond = conditions.iter().find(|c| {
                c.field_name == field_desc.name
                    && c.op == FilterOp::Eq
                    && c.array_op != Some(FilterOp::None)
                    && c.array_op != Some(FilterOp::All)
            });
            if let Some(cond) = field_cond {
                if let ConditionValue::Single(v) = &cond.value {
                    subsequent_eq_values
                        .push(wrap_value_for_json_path(v.clone(), cond.json_path.as_ref()));
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }
    let all_fields_matched =
        is_composite && has_eq && subsequent_eq_values.len() == index.fields.len() - 1;

    // Normalize filter values to match schema field encoding types.
    // e.g., a Float32 field stores with encode_float32, so lookup values must be Float32.
    if let Some(ref mut v) = eq_value {
        *v = normalize_for_index_field(v.clone(), first_field, collection_fields);
    }
    if let Some(ref mut vs) = in_values {
        *vs = vs
            .iter()
            .map(|v| normalize_for_index_field(v.clone(), first_field, collection_fields))
            .collect();
    }
    lower_bound = match lower_bound {
        Bound::Inclusive(v) => {
            Bound::Inclusive(normalize_for_index_field(v, first_field, collection_fields))
        }
        Bound::Exclusive(v) => {
            Bound::Exclusive(normalize_for_index_field(v, first_field, collection_fields))
        }
        Bound::Unbounded => Bound::Unbounded,
    };
    upper_bound = match upper_bound {
        Bound::Inclusive(v) => {
            Bound::Inclusive(normalize_for_index_field(v, first_field, collection_fields))
        }
        Bound::Exclusive(v) => {
            Bound::Exclusive(normalize_for_index_field(v, first_field, collection_fields))
        }
        Bound::Unbounded => Bound::Unbounded,
    };
    for (i, v) in subsequent_eq_values.iter_mut().enumerate() {
        if let Some(field_desc) = index.fields.get(i + 1) {
            *v = normalize_for_index_field(v.clone(), &field_desc.name, collection_fields);
        }
    }

    // Determine scan type (narrowing scans take priority over full scans)
    // Wrap values in JsonLeafValue when JSON path is present
    //
    // For composite indexes where we only match the first field, we use PrefixScan
    // instead of ExactMatch because ExactMatchIterator expects the doc_id right
    // after the encoded values, but composite index keys have additional fields.
    let scan_type = if has_eq {
        let wrapped_value = wrap_value_for_json_path(eq_value.unwrap(), eq_json_path.as_ref());
        if all_fields_matched {
            // All fields of composite index have eq conditions - use ExactMatch
            let mut values = vec![wrapped_value];
            values.extend(subsequent_eq_values);
            IndexScanType::ExactMatch { values }
        } else if is_composite && !subsequent_eq_values.is_empty() {
            // Multiple consecutive fields matched but not all - use PrefixScan with all eq values
            let mut values = vec![wrapped_value];
            values.extend(subsequent_eq_values);
            IndexScanType::PrefixScan {
                prefix_values: values,
                reverse,
            }
        } else if is_composite {
            // Only first field matched - use PrefixScan with just first value
            IndexScanType::PrefixScan {
                prefix_values: vec![wrapped_value],
                reverse,
            }
        } else {
            IndexScanType::ExactMatch {
                values: vec![wrapped_value],
            }
        }
    } else if has_in {
        let wrapped_values = wrap_values_for_json_path(in_values.unwrap(), in_json_path.as_ref());
        // For composite indexes, check if subsequent fields have Eq conditions.
        // If so, attach them as suffix_values to enable exact-match lookups
        // instead of prefix scans (e.g., _in on first field + _eq on second).
        let mut in_suffix_values: Vec<NormalValue> = Vec::new();
        if is_composite {
            for field_desc in index.fields.iter().skip(1) {
                let field_cond = conditions.iter().find(|c| {
                    c.field_name == field_desc.name
                        && c.op == FilterOp::Eq
                        && c.array_op != Some(FilterOp::None)
                        && c.array_op != Some(FilterOp::All)
                });
                if let Some(cond) = field_cond {
                    if let ConditionValue::Single(v) = &cond.value {
                        let normalized = normalize_for_index_field(
                            v.clone(),
                            &field_desc.name,
                            collection_fields,
                        );
                        in_suffix_values.push(wrap_value_for_json_path(
                            normalized,
                            cond.json_path.as_ref(),
                        ));
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        IndexScanType::InScan {
            values: wrapped_values,
            suffix_values: in_suffix_values,
        }
    } else if !lower_bound.is_unbounded() || !upper_bound.is_unbounded() {
        // Wrap range bounds for JSON paths
        // When we have a JSON path and one bound is unbounded, we need to constrain
        // the scan to entries with the same path using PathMin/PathMax sentinels
        let lower = match lower_bound {
            Bound::Inclusive(v) => {
                Bound::Inclusive(wrap_value_for_json_path(v, range_json_path.as_ref()))
            }
            Bound::Exclusive(v) => {
                Bound::Exclusive(wrap_value_for_json_path(v, range_json_path.as_ref()))
            }
            Bound::Unbounded => {
                // For JSON paths, use PathMin to constrain lower bound
                if let Some(path) = &range_json_path {
                    Bound::Inclusive(NormalValue::JsonLeaf(JsonLeafValue::new(
                        path.clone(),
                        JsonScalarValue::PathMin,
                    )))
                } else {
                    Bound::Unbounded
                }
            }
        };
        let upper = match upper_bound {
            Bound::Inclusive(v) => {
                Bound::Inclusive(wrap_value_for_json_path(v, range_json_path.as_ref()))
            }
            Bound::Exclusive(v) => {
                Bound::Exclusive(wrap_value_for_json_path(v, range_json_path.as_ref()))
            }
            Bound::Unbounded => {
                // For JSON paths, use PathMax to constrain upper bound
                if let Some(path) = &range_json_path {
                    Bound::Exclusive(NormalValue::JsonLeaf(JsonLeafValue::new(
                        path.clone(),
                        JsonScalarValue::PathMax,
                    )))
                } else {
                    Bound::Unbounded
                }
            }
        };
        IndexScanType::RangeScan {
            prefix_values: vec![],
            lower,
            upper,
            reverse,
        }
    } else if has_scan_all {
        // Full index scan with residual filter (for _ne, _like, etc.)
        // For JSON fields, constrain the scan to the specific path
        if let Some(path) = &range_json_path {
            IndexScanType::RangeScan {
                prefix_values: vec![],
                lower: Bound::Inclusive(NormalValue::JsonLeaf(JsonLeafValue::new(
                    path.clone(),
                    JsonScalarValue::PathMin,
                ))),
                upper: Bound::Exclusive(NormalValue::JsonLeaf(JsonLeafValue::new(
                    path.clone(),
                    JsonScalarValue::PathMax,
                ))),
                reverse,
            }
        } else {
            IndexScanType::PrefixScan {
                prefix_values: vec![],
                reverse,
            }
        }
    } else {
        return None;
    };

    // Only pass limit/offset if the index provides ordering
    // (otherwise the limit needs to be applied after sorting)
    let index_provides_ordering = order_by
        .map(|o| can_be_ordered_by_index(o, index).0)
        .unwrap_or(false);

    Some(IndexScanParams {
        index_name: index.name.clone(),
        scan_type,
        limit: if index_provides_ordering { limit } else { None },
        offset: if index_provides_ordering { offset } else { 0 },
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use schema::IndexedFieldDescription;
    use serde_json::json;

    fn make_filter(conditions: HashMap<String, JsonValue>) -> Filter {
        Filter::from_conditions(conditions)
    }

    fn single_field_index(field: &str) -> IndexDescription {
        IndexDescription {
            id: 1,
            name: format!("{}_idx", field),
            unique: false,
            fields: vec![IndexedFieldDescription {
                name: field.to_string(),
                descending: false,
            }],
        }
    }

    fn composite_index(fields: &[&str]) -> IndexDescription {
        IndexDescription {
            id: 2,
            name: "composite_idx".to_string(),
            unique: false,
            fields: fields
                .iter()
                .map(|f| IndexedFieldDescription {
                    name: f.to_string(),
                    descending: false,
                })
                .collect(),
        }
    }

    fn unique_index(field: &str) -> IndexDescription {
        IndexDescription {
            id: 3,
            name: format!("{}_unique_idx", field),
            unique: true,
            fields: vec![IndexedFieldDescription {
                name: field.to_string(),
                descending: false,
            }],
        }
    }

    #[test]
    fn test_can_use_index_eq() {
        let filter = make_filter(HashMap::from([(
            "name".to_string(),
            json!({"_eq": "alice"}),
        )]));
        let index = single_field_index("name");

        assert!(can_use_index(&filter, &index));
    }

    #[test]
    fn test_can_use_index_wrong_field() {
        let filter = make_filter(HashMap::from([("age".to_string(), json!({"_eq": 30}))]));
        let index = single_field_index("name");

        assert!(!can_use_index(&filter, &index));
    }

    #[test]
    fn test_can_use_index_range() {
        let filter = make_filter(HashMap::from([(
            "age".to_string(),
            json!({"_gt": 18, "_lt": 65}),
        )]));
        let index = single_field_index("age");

        assert!(can_use_index(&filter, &index));
    }

    #[test]
    fn test_can_use_index_in() {
        let filter = make_filter(HashMap::from([(
            "status".to_string(),
            json!({"_in": ["active", "pending"]}),
        )]));
        let index = single_field_index("status");

        assert!(can_use_index(&filter, &index));
    }

    #[test]
    fn test_can_use_index_ne() {
        // _ne uses full index scan (matching Go behavior)
        let filter = make_filter(HashMap::from([(
            "name".to_string(),
            json!({"_ne": "alice"}),
        )]));
        let index = single_field_index("name");

        assert!(can_use_index(&filter, &index));
    }

    #[test]
    fn test_can_use_index_like() {
        // _like uses full index scan (matching Go behavior)
        let filter = make_filter(HashMap::from([(
            "name".to_string(),
            json!({"_like": "%alice%"}),
        )]));
        let index = single_field_index("name");

        assert!(can_use_index(&filter, &index));
    }

    #[test]
    fn test_filter_to_scan_exact_match() {
        let filter = make_filter(HashMap::from([(
            "name".to_string(),
            json!({"_eq": "alice"}),
        )]));
        let index = single_field_index("name");

        let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();
        assert_eq!(params.index_name, "name_idx");

        match params.scan_type {
            IndexScanType::ExactMatch { values } => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0], NormalValue::String("alice".to_string()));
            }
            _ => panic!("expected ExactMatch scan type"),
        }
    }

    #[test]
    fn test_filter_to_scan_in() {
        let filter = make_filter(HashMap::from([(
            "status".to_string(),
            json!({"_in": ["active", "pending"]}),
        )]));
        let index = single_field_index("status");

        let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();

        match params.scan_type {
            IndexScanType::InScan { values, .. } => {
                assert_eq!(values.len(), 2);
            }
            _ => panic!("expected InScan scan type"),
        }
    }

    #[test]
    fn test_filter_to_scan_range() {
        let filter = make_filter(HashMap::from([(
            "age".to_string(),
            json!({"_gte": 18, "_lt": 65}),
        )]));
        let index = single_field_index("age");

        let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();

        match params.scan_type {
            IndexScanType::RangeScan {
                lower,
                upper,
                prefix_values,
                ..
            } => {
                assert!(prefix_values.is_empty());
                match lower {
                    Bound::Inclusive(v) => assert_eq!(v, NormalValue::Int(18)),
                    _ => panic!("expected inclusive lower bound"),
                }
                match upper {
                    Bound::Exclusive(v) => assert_eq!(v, NormalValue::Int(65)),
                    _ => panic!("expected exclusive upper bound"),
                }
            }
            _ => panic!("expected RangeScan scan type"),
        }
    }

    #[test]
    fn test_select_best_index_prefers_eq() {
        let filter = make_filter(HashMap::from([
            ("name".to_string(), json!({"_eq": "alice"})),
            ("age".to_string(), json!({"_gt": 18})),
        ]));

        let indexes = vec![single_field_index("name"), single_field_index("age")];

        let best = select_best_index(&filter, &indexes).unwrap();
        assert_eq!(best.fields[0].name, "name"); // eq is preferred
    }

    #[test]
    fn test_select_best_index_prefers_unique() {
        let filter = make_filter(HashMap::from([(
            "email".to_string(),
            json!({"_eq": "a@b.com"}),
        )]));

        let indexes = vec![single_field_index("email"), unique_index("email")];

        let best = select_best_index(&filter, &indexes).unwrap();
        assert!(best.unique);
    }

    #[test]
    fn test_select_best_index_composite() {
        let filter = make_filter(HashMap::from([
            ("category".to_string(), json!({"_eq": "electronics"})),
            ("brand".to_string(), json!({"_eq": "sony"})),
        ]));

        let indexes = vec![
            single_field_index("category"),
            composite_index(&["category", "brand"]),
        ];

        let best = select_best_index(&filter, &indexes).unwrap();
        assert_eq!(best.fields.len(), 2); // composite is preferred
    }

    #[test]
    fn test_extract_field_conditions_simple() {
        let filter = make_filter(HashMap::from([
            ("name".to_string(), json!({"_eq": "alice"})),
            ("age".to_string(), json!({"_gt": 18})),
        ]));

        let conditions = extract_field_conditions(&filter);
        assert_eq!(conditions.len(), 2);

        let name_cond = conditions.iter().find(|c| c.field_name == "name").unwrap();
        assert_eq!(name_cond.op, FilterOp::Eq);

        let age_cond = conditions.iter().find(|c| c.field_name == "age").unwrap();
        assert_eq!(age_cond.op, FilterOp::Gt);
    }

    #[test]
    fn test_extract_field_conditions_and() {
        let filter = make_filter(HashMap::from([(
            "_and".to_string(),
            json!([
                {"name": {"_eq": "alice"}},
                {"age": {"_gt": 18}}
            ]),
        )]));

        let conditions = extract_field_conditions(&filter);
        assert_eq!(conditions.len(), 2);
    }

    #[test]
    fn test_json_to_normal_value() {
        assert_eq!(json_to_normal_value(&json!(null)), Some(NormalValue::Null));
        assert_eq!(
            json_to_normal_value(&json!(true)),
            Some(NormalValue::Bool(true))
        );
        assert_eq!(json_to_normal_value(&json!(42)), Some(NormalValue::Int(42)));
        assert_eq!(
            json_to_normal_value(&json!(3.14)),
            Some(NormalValue::Float64(3.14))
        );
        assert_eq!(
            json_to_normal_value(&json!("hello")),
            Some(NormalValue::String("hello".to_string()))
        );
        assert_eq!(json_to_normal_value(&json!([1, 2, 3])), None); // arrays not supported
    }

    #[test]
    fn test_empty_filter_cannot_use_index() {
        let filter = Filter::new();
        let index = single_field_index("name");

        assert!(!can_use_index(&filter, &index));
    }

    #[test]
    fn test_condition_value_variants() {
        let ops = serde_json::from_str::<serde_json::Map<String, JsonValue>>(
            r#"{"_eq": "alice", "_in": ["a", "b"], "_like": "test%"}"#,
        )
        .unwrap();

        let conditions = FieldCondition::parse("name", &ops);
        assert_eq!(conditions.len(), 3);

        let eq_cond = conditions.iter().find(|c| c.op == FilterOp::Eq).unwrap();
        assert!(matches!(eq_cond.value, ConditionValue::Single(_)));

        let in_cond = conditions.iter().find(|c| c.op == FilterOp::In).unwrap();
        assert!(matches!(in_cond.value, ConditionValue::Multiple(_)));

        let like_cond = conditions.iter().find(|c| c.op == FilterOp::Like).unwrap();
        assert!(matches!(like_cond.value, ConditionValue::Pattern(_)));
    }

    #[test]
    fn test_can_use_index_array_any() {
        // Filter: {numbers: {_any: {_eq: 30}}}
        let filter = make_filter(HashMap::from([(
            "numbers".to_string(),
            json!({"_any": {"_eq": 30}}),
        )]));
        let index = single_field_index("numbers");

        assert!(can_use_index(&filter, &index));
    }

    #[test]
    fn test_can_use_index_array_all() {
        // Filter: {numbers: {_all: {_eq: 30}}}
        let filter = make_filter(HashMap::from([(
            "numbers".to_string(),
            json!({"_all": {"_eq": 30}}),
        )]));
        let index = single_field_index("numbers");

        assert!(can_use_index(&filter, &index));
    }

    #[test]
    fn test_cannot_use_index_array_none() {
        // Filter: {numbers: {_none: {_eq: 30}}} - _none cannot use index
        let filter = make_filter(HashMap::from([(
            "numbers".to_string(),
            json!({"_none": {"_eq": 30}}),
        )]));
        let index = single_field_index("numbers");

        assert!(!can_use_index(&filter, &index));
    }

    #[test]
    fn test_can_use_index_array_all_with_range_op() {
        // Filter: {numbers: {_all: {_geq: 33}}}
        // _all with range operators (not just _eq/_in) should use index
        // Index provides candidates, residual filter verifies ALL match
        let filter = make_filter(HashMap::from([(
            "numbers".to_string(),
            json!({"_all": {"_gte": 33}}),
        )]));
        let index = single_field_index("numbers");

        assert!(can_use_index(&filter, &index));
    }

    #[test]
    fn test_can_use_composite_index_with_none_on_second_field() {
        // Filter: {name: {_eq: "Shahzad"}, numbers: {_none: {_eq: 3}}}
        // Composite index [name, numbers] should be usable because first field has _eq
        // _none on second field is handled by residual filter
        let filter = make_filter(HashMap::from([
            ("name".to_string(), json!({"_eq": "Shahzad"})),
            ("numbers".to_string(), json!({"_none": {"_eq": 3}})),
        ]));
        let index = composite_index(&["name", "numbers"]);

        assert!(can_use_index(&filter, &index));
    }

    #[test]
    fn test_cannot_use_composite_index_with_none_on_first_field() {
        // Filter: {numbers: {_none: {_eq: 3}}, name: {_eq: "Shahzad"}}
        // Composite index [numbers, name] cannot be used because first field has _none
        let filter = make_filter(HashMap::from([
            ("numbers".to_string(), json!({"_none": {"_eq": 3}})),
            ("name".to_string(), json!({"_eq": "Shahzad"})),
        ]));
        let index = composite_index(&["numbers", "name"]);

        assert!(!can_use_index(&filter, &index));
    }

    #[test]
    fn test_filter_to_scan_array_any() {
        let filter = make_filter(HashMap::from([(
            "numbers".to_string(),
            json!({"_any": {"_eq": 30}}),
        )]));
        let index = single_field_index("numbers");

        let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();
        assert_eq!(params.index_name, "numbers_idx");

        match params.scan_type {
            IndexScanType::ExactMatch { values } => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0], NormalValue::Int(30));
            }
            _ => panic!("expected ExactMatch scan type for _any with _eq"),
        }
    }

    #[test]
    fn test_extract_array_conditions() {
        // Parse: {_any: {_eq: 30}}
        let ops =
            serde_json::from_str::<serde_json::Map<String, JsonValue>>(r#"{"_any": {"_eq": 30}}"#)
                .unwrap();

        let conditions = FieldCondition::parse("numbers", &ops);
        assert_eq!(conditions.len(), 1);

        let cond = &conditions[0];
        assert_eq!(cond.field_name, "numbers");
        assert_eq!(cond.op, FilterOp::Eq);
        assert_eq!(cond.array_op, Some(FilterOp::Any));
        match &cond.value {
            ConditionValue::Single(v) => assert_eq!(*v, NormalValue::Int(30)),
            _ => panic!("expected single value"),
        }
    }

    #[test]
    fn test_extract_json_path_simple() {
        // Parse: {height: {_gt: 170}} - JSON field filter
        let ops = serde_json::from_str::<serde_json::Map<String, JsonValue>>(
            r#"{"height": {"_gt": 170}}"#,
        )
        .unwrap();

        let conditions = FieldCondition::parse("custom", &ops);
        assert_eq!(conditions.len(), 1);

        let cond = &conditions[0];
        assert_eq!(cond.field_name, "custom");
        assert_eq!(cond.op, FilterOp::Gt);
        assert!(cond.json_path.is_some());

        let path = cond.json_path.as_ref().unwrap();
        assert_eq!(path.0.len(), 1);
        assert_eq!(path.0[0], JsonPathPart::Property("height".to_string()));
    }

    #[test]
    fn test_extract_json_path_nested() {
        // Parse: {profile: {address: {city: {_eq: "NYC"}}}}
        let ops = serde_json::from_str::<serde_json::Map<String, JsonValue>>(
            r#"{"profile": {"address": {"city": {"_eq": "NYC"}}}}"#,
        )
        .unwrap();

        let conditions = FieldCondition::parse("custom", &ops);
        assert_eq!(conditions.len(), 1);

        let cond = &conditions[0];
        assert_eq!(cond.field_name, "custom");
        assert_eq!(cond.op, FilterOp::Eq);
        assert!(cond.json_path.is_some());

        let path = cond.json_path.as_ref().unwrap();
        assert_eq!(path.0.len(), 3);
        assert_eq!(path.0[0], JsonPathPart::Property("profile".to_string()));
        assert_eq!(path.0[1], JsonPathPart::Property("address".to_string()));
        assert_eq!(path.0[2], JsonPathPart::Property("city".to_string()));
    }

    #[test]
    fn test_can_use_index_json_path() {
        // Filter: {custom: {height: {_gt: 170}}}
        let filter = make_filter(HashMap::from([(
            "custom".to_string(),
            json!({"height": {"_gt": 170}}),
        )]));
        let index = single_field_index("custom");

        assert!(can_use_index(&filter, &index));
    }

    #[test]
    fn test_filter_to_scan_json_path_eq() {
        // Filter: {custom: {height: {_eq: 168}}}
        let filter = make_filter(HashMap::from([(
            "custom".to_string(),
            json!({"height": {"_eq": 168}}),
        )]));
        let index = single_field_index("custom");

        let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();
        assert_eq!(params.index_name, "custom_idx");

        match params.scan_type {
            IndexScanType::ExactMatch { values } => {
                assert_eq!(values.len(), 1);
                // The value should be wrapped in JsonLeaf with the path
                match &values[0] {
                    NormalValue::JsonLeaf(leaf) => {
                        assert_eq!(leaf.path.0.len(), 1);
                        assert_eq!(leaf.path.0[0], JsonPathPart::Property("height".to_string()));
                        assert_eq!(leaf.value, JsonScalarValue::Number(168.0));
                    }
                    _ => panic!("expected JsonLeaf value, got {:?}", values[0]),
                }
            }
            _ => panic!("expected ExactMatch scan type"),
        }
    }

    #[test]
    fn test_filter_to_scan_json_path_range() {
        // Filter: {custom: {height: {_gt: 170}}}
        let filter = make_filter(HashMap::from([(
            "custom".to_string(),
            json!({"height": {"_gt": 170}}),
        )]));
        let index = single_field_index("custom");

        let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();
        assert_eq!(params.index_name, "custom_idx");

        match params.scan_type {
            IndexScanType::RangeScan { lower, upper, .. } => {
                // Lower bound should be wrapped in JsonLeaf
                match lower {
                    Bound::Exclusive(v) => match v {
                        NormalValue::JsonLeaf(leaf) => {
                            assert_eq!(
                                leaf.path.0[0],
                                JsonPathPart::Property("height".to_string())
                            );
                            assert_eq!(leaf.value, JsonScalarValue::Number(170.0));
                        }
                        _ => panic!("expected JsonLeaf value, got {:?}", v),
                    },
                    _ => panic!("expected Exclusive lower bound"),
                }
                // Upper bound should be constrained to PathMax for the JSON path
                match upper {
                    Bound::Exclusive(v) => match v {
                        NormalValue::JsonLeaf(leaf) => {
                            assert_eq!(
                                leaf.path.0[0],
                                JsonPathPart::Property("height".to_string())
                            );
                            assert_eq!(leaf.value, JsonScalarValue::PathMax);
                        }
                        _ => panic!("expected JsonLeaf value for upper bound, got {:?}", v),
                    },
                    _ => panic!("expected Exclusive upper bound with PathMax"),
                }
            }
            _ => panic!("expected RangeScan scan type"),
        }
    }

    #[test]
    fn test_filter_to_scan_json_path_in() {
        // Filter: {custom: {status: {_in: ["active", "pending"]}}}
        let filter = make_filter(HashMap::from([(
            "custom".to_string(),
            json!({"status": {"_in": ["active", "pending"]}}),
        )]));
        let index = single_field_index("custom");

        let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();
        assert_eq!(params.index_name, "custom_idx");

        match params.scan_type {
            IndexScanType::InScan { values, .. } => {
                assert_eq!(values.len(), 2);
                // All values should be wrapped in JsonLeaf with the path
                for value in &values {
                    match value {
                        NormalValue::JsonLeaf(leaf) => {
                            assert_eq!(leaf.path.0.len(), 1);
                            assert_eq!(
                                leaf.path.0[0],
                                JsonPathPart::Property("status".to_string())
                            );
                        }
                        _ => panic!("expected JsonLeaf value"),
                    }
                }
            }
            _ => panic!("expected InScan scan type"),
        }
    }
}
