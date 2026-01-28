//! Index selection and filter-to-index translation
//!
//! Provides utilities for determining when filters can use indexes
//! and translating filter conditions to index scan parameters.

use std::collections::HashMap;

use document::NormalValue;
use schema::IndexDescription;
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
    /// Multiple exact match values (IN operator)
    InScan { values: Vec<NormalValue> },
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
        let mut conditions = Vec::new();

        for (op_str, value) in ops {
            if let Some(op) = FilterOp::parse(op_str) {
                // Handle array element operators (_any, _all, _none)
                // These wrap inner conditions: {_any: {_eq: 30}}
                if op.is_array_element_op() {
                    if let Some(inner_ops) = value.as_object() {
                        // Parse the inner conditions with the array operator wrapper
                        let inner_conditions = Self::parse_inner(field_name, inner_ops, Some(op));
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
                    array_op: None,
                });
            }
        }

        conditions
    }

    /// Parse inner conditions with an array operator wrapper.
    fn parse_inner(
        field_name: &str,
        ops: &serde_json::Map<String, JsonValue>,
        array_op: Option<FilterOp>,
    ) -> Vec<Self> {
        let mut conditions = Vec::new();

        for (op_str, value) in ops {
            if let Some(op) = FilterOp::parse(op_str) {
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
                });
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
        JsonValue::String(s) => Some(NormalValue::String(s.clone())),
        _ => None,
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

    conditions.iter().any(|cond| {
        if &cond.field_name != first_field {
            return false;
        }

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
                | FilterOp::Ne
                | FilterOp::Like
                | FilterOp::Nlike
                | FilterOp::Ilike
                | FilterOp::Nilike
        );

        // For array operators, check if the combination is index-friendly
        match cond.array_op {
            Some(FilterOp::Any) => {
                // _any with comparison ops can use index
                base_op_compatible
            }
            Some(FilterOp::All) => {
                // _all with _eq can use index (with post-filtering)
                matches!(cond.op, FilterOp::Eq | FilterOp::In)
            }
            Some(FilterOp::None) => {
                // _none cannot efficiently use index (requires full scan)
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
) -> Option<IndexScanParams> {
    if !can_use_index(filter, index) {
        return None;
    }

    let conditions = extract_field_conditions(filter);
    let first_field = &index.fields[0].name;

    // Find conditions on the first index field
    let first_field_conditions: Vec<_> = conditions
        .iter()
        .filter(|c| &c.field_name == first_field)
        .collect();

    if first_field_conditions.is_empty() {
        return None;
    }

    // Analyze conditions to determine scan type
    // For array operators, we look at the inner operator
    let mut has_eq = false;
    let mut eq_value = None;
    let mut has_in = false;
    let mut in_values = None;
    let mut has_scan_all = false;
    let mut lower_bound = Bound::Unbounded;
    let mut upper_bound = Bound::Unbounded;

    for cond in first_field_conditions {
        // Skip _none operators (they don't use index)
        if cond.array_op == Some(FilterOp::None) {
            continue;
        }

        match cond.op {
            FilterOp::Eq => {
                if let ConditionValue::Single(v) = &cond.value {
                    has_eq = true;
                    eq_value = Some(v.clone());
                }
            }
            FilterOp::In => {
                if let ConditionValue::Multiple(vs) = &cond.value {
                    has_in = true;
                    in_values = Some(vs.clone());
                }
            }
            FilterOp::Gt => {
                if let ConditionValue::Single(v) = &cond.value {
                    lower_bound = Bound::Exclusive(v.clone());
                }
            }
            FilterOp::Gte => {
                if let ConditionValue::Single(v) = &cond.value {
                    lower_bound = Bound::Inclusive(v.clone());
                }
            }
            FilterOp::Lt => {
                if let ConditionValue::Single(v) = &cond.value {
                    upper_bound = Bound::Exclusive(v.clone());
                }
            }
            FilterOp::Lte => {
                if let ConditionValue::Single(v) = &cond.value {
                    upper_bound = Bound::Inclusive(v.clone());
                }
            }
            // _ne/_like use full index scan with post-filtering (matches Go behavior)
            FilterOp::Ne | FilterOp::Like | FilterOp::Nlike | FilterOp::Ilike
            | FilterOp::Nilike => {
                has_scan_all = true;
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

    // Determine scan type (narrowing scans take priority over full scans)
    let scan_type = if has_eq {
        IndexScanType::ExactMatch {
            values: vec![eq_value.unwrap()],
        }
    } else if has_in {
        IndexScanType::InScan {
            values: in_values.unwrap(),
        }
    } else if !lower_bound.is_unbounded() || !upper_bound.is_unbounded() {
        IndexScanType::RangeScan {
            prefix_values: vec![],
            lower: lower_bound,
            upper: upper_bound,
            reverse,
        }
    } else if has_scan_all {
        // Full index scan with residual filter (for _ne, _like, etc.)
        IndexScanType::PrefixScan {
            prefix_values: vec![],
            reverse,
        }
    } else {
        return None;
    };

    Some(IndexScanParams {
        index_name: index.name.clone(),
        scan_type,
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

        let params = filter_to_index_scan(&filter, &index, None).unwrap();
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

        let params = filter_to_index_scan(&filter, &index, None).unwrap();

        match params.scan_type {
            IndexScanType::InScan { values } => {
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

        let params = filter_to_index_scan(&filter, &index, None).unwrap();

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
    fn test_filter_to_scan_array_any() {
        let filter = make_filter(HashMap::from([(
            "numbers".to_string(),
            json!({"_any": {"_eq": 30}}),
        )]));
        let index = single_field_index("numbers");

        let params = filter_to_index_scan(&filter, &index, None).unwrap();
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
}
