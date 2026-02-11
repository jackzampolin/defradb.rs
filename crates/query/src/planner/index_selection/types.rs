//! Type definitions for index selection.

use document::{JsonPath, JsonScalarValue, NormalValue};
use serde_json::Value as JsonValue;
use storage::index::Bound;

use crate::mapper::FilterOp;

use super::values::json_to_normal_value;

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
    /// Optional value filter applied to each index entry during scan.
    /// Used for _like/_nlike on JSON fields where the index does a range scan
    /// and non-string entries must be filtered at the scan level (matching Go's indexLikeMatcher).
    pub value_filter: Option<ScanValueFilter>,
}

/// Value-level filter applied to individual index entries during scan iteration.
/// Matches Go's `indexLikeMatcher` behavior: non-string values return false.
#[derive(Debug, Clone)]
pub enum ScanValueFilter {
    Like(String),
    Nlike(String),
    Ilike(String),
    Nilike(String),
}

impl ScanValueFilter {
    /// Check if an index entry's first value matches this filter.
    /// Non-string values always return false (matching Go's indexLikeMatcher).
    pub fn matches_value(&self, value: &NormalValue) -> bool {
        // Extract the string from the value (supports both String and JsonLeaf with string)
        let s = match value {
            NormalValue::String(s) => s.as_str(),
            NormalValue::JsonLeaf(leaf) => match &leaf.value {
                JsonScalarValue::String(s) => s.as_str(),
                _ => return false, // Non-string JSON leaf: exclude
            },
            _ => return false, // Non-string value: exclude
        };

        let (pattern, is_like, case_insensitive) = match self {
            ScanValueFilter::Like(p) => (p.as_str(), true, false),
            ScanValueFilter::Nlike(p) => (p.as_str(), false, false),
            ScanValueFilter::Ilike(p) => (p.as_str(), true, true),
            ScanValueFilter::Nilike(p) => (p.as_str(), false, true),
        };

        let (s_cmp, p_cmp): (std::borrow::Cow<str>, std::borrow::Cow<str>) = if case_insensitive {
            (s.to_lowercase().into(), pattern.to_lowercase().into())
        } else {
            (s.into(), pattern.into())
        };

        use crate::mapper::like_pattern_match;
        let matches = like_pattern_match(&s_cmp, &p_cmp);
        if is_like {
            matches
        } else {
            !matches
        }
    }
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
    /// Multiple scans combined (for _or filters).
    /// Each branch is executed separately and results are deduplicated.
    OrScan { branches: Vec<IndexScanType> },
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
