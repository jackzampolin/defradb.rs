//! Filter operator evaluation - comparison, equality, and pattern matching

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

use crate::error::{QueryError, Result};
use crate::mapper::filter::FilterOp;

/// Check if two JSON values are equal.
/// Handles type coercion for numbers and datetime strings.
pub fn values_equal(a: &JsonValue, b: &JsonValue) -> bool {
    match (a, b) {
        (JsonValue::Null, JsonValue::Null) => true,
        (JsonValue::Bool(a), JsonValue::Bool(b)) => a == b,
        (JsonValue::Number(a), JsonValue::Number(b)) => {
            // Handle int/float comparison
            if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
                a == b
            } else if let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) {
                (a - b).abs() < f64::EPSILON
            } else {
                false
            }
        }
        (JsonValue::String(a), JsonValue::String(b)) => {
            // Try direct string comparison first
            if a == b {
                return true;
            }
            // Try parsing as datetime values - stored values are in UTC format,
            // filter values may have timezone offsets. Both represent the same time
            // if they parse to the same UTC timestamp.
            datetimes_equal(a, b)
        }
        (JsonValue::Array(a), JsonValue::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| values_equal(a, b))
        }
        (JsonValue::Object(a), JsonValue::Object(b)) => {
            // Objects are equal if they have the same keys with equal values
            if a.len() != b.len() {
                return false;
            }
            a.iter()
                .all(|(key, val_a)| b.get(key).is_some_and(|val_b| values_equal(val_a, val_b)))
        }
        _ => false,
    }
}

/// Try to compare two strings as datetime values.
/// Returns true if both can be parsed as RFC 3339 datetime strings and represent the same time.
pub fn datetimes_equal(a: &str, b: &str) -> bool {
    let a_dt: Option<DateTime<Utc>> = a.parse().ok();
    let b_dt: Option<DateTime<Utc>> = b.parse().ok();
    match (a_dt, b_dt) {
        (Some(a), Some(b)) => a == b,
        _ => false, // If either isn't a valid datetime, they're not equal
    }
}

/// Compare two values for ordering.
/// Implements Go DefraDB semantics:
/// - null vs null → Equal (null == null)
/// - null vs non-null → None (null document values are excluded from comparisons)
/// - non-null vs null → Greater (filter value is null, any non-null value is greater)
pub fn compare(a: &JsonValue, b: &JsonValue) -> Result<Option<std::cmp::Ordering>> {
    match (a, b) {
        // Go DefraDB treats null as smallest value
        // null vs null → Equal
        (JsonValue::Null, JsonValue::Null) => Ok(Some(std::cmp::Ordering::Equal)),
        // value vs null → Greater (any non-null value is greater than null)
        (_, JsonValue::Null) => Ok(Some(std::cmp::Ordering::Greater)),
        // null vs non-null → incomparable (Go DefraDB excludes null documents from comparisons)
        (JsonValue::Null, _) => Ok(None),

        // Number comparisons: support int/float coercion (Go's numbers.TryUpcast behavior)
        (JsonValue::Number(a), JsonValue::Number(b)) => {
            let a_val = a.as_f64().ok_or_else(|| {
                QueryError::invalid_filter(format!("number {} cannot be compared", a))
            })?;
            let b_val = b.as_f64().ok_or_else(|| {
                QueryError::invalid_filter(format!("number {} cannot be compared", b))
            })?;
            Ok(a_val.partial_cmp(&b_val)) // Returns None for NaN, which becomes false
        }

        // String comparisons - try datetime parsing first, then fall back to lexicographic
        (JsonValue::String(a), JsonValue::String(b)) => {
            // Try parsing as datetime values for proper temporal comparison
            let a_dt: Option<DateTime<Utc>> = a.parse().ok();
            let b_dt: Option<DateTime<Utc>> = b.parse().ok();
            match (a_dt, b_dt) {
                (Some(a_time), Some(b_time)) => Ok(Some(a_time.cmp(&b_time))),
                _ => Ok(Some(a.cmp(b))), // Fall back to lexicographic comparison
            }
        }

        // Type mismatch handling:
        // - If filter value is a number but stored value isn't, return None (no match)
        //   This is the "AllTypes" case where we have mixed-type JSON fields
        // - If filter value is string, bool, object, or array, return an error
        //   Go DefraDB only allows numeric comparisons with _gt/_ge/_lt/_le
        (_, JsonValue::Number(_)) => {
            // Filter value is number, but stored value type doesn't match
            // Return no match instead of error
            Ok(None)
        }
        // String filter values are NOT valid for comparison operators
        (_, JsonValue::String(_)) => Err(QueryError::UnexpectedType {
            property: "condition".to_string(),
            actual: go_type_name(b),
        }),
        // Error case: filter value (b) is an invalid type for comparison
        // Use Go-compatible error format: "unexpected type. Property: condition, Actual: <type>"
        _ => Err(QueryError::UnexpectedType {
            property: "condition".to_string(),
            actual: go_type_name(b),
        }),
    }
}

/// Get Go-compatible type name for a JSON value
pub fn go_type_name(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "nil".to_string(),
        JsonValue::Bool(_) => "bool".to_string(),
        JsonValue::Number(_) => "float64".to_string(),
        JsonValue::String(_) => "string".to_string(),
        JsonValue::Array(_) => "[]interface {}".to_string(),
        JsonValue::Object(_) => "map[string]interface {}".to_string(),
    }
}

/// Evaluate a single filter operator against actual and expected values.
pub fn eval_op(actual: &JsonValue, op: FilterOp, expected: &JsonValue) -> Result<bool> {
    match op {
        FilterOp::Eq => Ok(values_equal(actual, expected)),
        FilterOp::Ne => Ok(!values_equal(actual, expected)),
        // Comparison operators: None (from null or NaN) returns false (Go DefraDB behavior)
        FilterOp::Gt => compare(actual, expected).map(|opt| opt.is_some_and(|ord| ord.is_gt())),
        FilterOp::Gte => compare(actual, expected).map(|opt| opt.is_some_and(|ord| ord.is_ge())),
        FilterOp::Lt => compare(actual, expected).map(|opt| opt.is_some_and(|ord| ord.is_lt())),
        FilterOp::Lte => compare(actual, expected).map(|opt| opt.is_some_and(|ord| ord.is_le())),
        FilterOp::In => {
            let arr = expected
                .as_array()
                .ok_or_else(|| QueryError::invalid_filter("_in requires array"))?;
            Ok(arr.iter().any(|v| values_equal(actual, v)))
        }
        FilterOp::Nin => {
            let arr = expected
                .as_array()
                .ok_or_else(|| QueryError::invalid_filter("_nin requires array"))?;
            Ok(!arr.iter().any(|v| values_equal(actual, v)))
        }
        FilterOp::Like => like_match(actual, expected, false, false),
        FilterOp::Nlike => like_match(actual, expected, true, false),
        FilterOp::Ilike => like_match(actual, expected, false, true),
        FilterOp::Nilike => like_match(actual, expected, true, true),
        FilterOp::Contains => {
            // Array field contains the expected value
            // Null fields never match (standard database behavior)
            if actual.is_null() {
                return Ok(false);
            }
            let arr = actual
                .as_array()
                .ok_or_else(|| QueryError::invalid_filter("_contains requires array field"))?;
            Ok(arr.iter().any(|v| values_equal(v, expected)))
        }
        FilterOp::ContainedIn => {
            // All elements of actual array are in expected array (actual is subset of expected)
            // Null fields never match (standard database behavior)
            if actual.is_null() {
                return Ok(false);
            }
            let actual_arr = actual
                .as_array()
                .ok_or_else(|| QueryError::invalid_filter("_contained_in requires array field"))?;
            let expected_arr = expected
                .as_array()
                .ok_or_else(|| QueryError::invalid_filter("_contained_in requires array value"))?;
            Ok(actual_arr
                .iter()
                .all(|v| expected_arr.iter().any(|e| values_equal(v, e))))
        }
        FilterOp::HasKey => {
            // Object/map has the specified key
            // Null fields never match (standard database behavior)
            if actual.is_null() {
                return Ok(false);
            }
            let key = expected
                .as_str()
                .ok_or_else(|| QueryError::invalid_filter("_has_key requires string key"))?;
            let obj = actual
                .as_object()
                .ok_or_else(|| QueryError::invalid_filter("_has_key requires object field"))?;
            Ok(obj.contains_key(key))
        }
        FilterOp::Any => {
            // Return true if ANY element matches the nested condition
            // Null or non-array field → no match (matches Go DefraDB behavior for JSON fields)
            let arr = match actual.as_array() {
                Some(a) => a,
                None => return Ok(false), // Non-array doesn't have any matching elements
            };
            // Empty array → no match (no elements to match)
            if arr.is_empty() {
                return Ok(false);
            }
            // Expected is a nested condition object like {_gt: 70}
            let nested_filter = expected
                .as_object()
                .ok_or_else(|| QueryError::invalid_filter("_any requires object condition"))?;
            for elem in arr {
                if eval_conditions_on_value(elem, nested_filter)? {
                    return Ok(true); // Found match
                }
            }
            Ok(false)
        }
        FilterOp::All => {
            // Return true if ALL elements match the nested condition
            // Non-array field → false (no elements, but not vacuous truth - matches Go behavior)
            let arr = match actual.as_array() {
                Some(a) => a,
                None => return Ok(false), // Non-array fails _all
            };
            // Empty array → vacuous truth (all zero elements match)
            if arr.is_empty() {
                return Ok(true);
            }
            let nested_filter = expected
                .as_object()
                .ok_or_else(|| QueryError::invalid_filter("_all requires object condition"))?;
            for elem in arr {
                if !eval_conditions_on_value(elem, nested_filter)? {
                    return Ok(false); // Found non-match
                }
            }
            Ok(true)
        }
        FilterOp::None => {
            // Return true if NO elements match the nested condition
            // Non-array field → false (not an array, so _none doesn't apply - Go behavior)
            let arr = match actual.as_array() {
                Some(a) => a,
                None => return Ok(false), // Non-array excluded from _none results
            };
            // Empty array → true (no elements match)
            if arr.is_empty() {
                return Ok(true);
            }
            let nested_filter = expected
                .as_object()
                .ok_or_else(|| QueryError::invalid_filter("_none requires object condition"))?;
            for elem in arr {
                if eval_conditions_on_value(elem, nested_filter)? {
                    return Ok(false); // Found match → fail
                }
            }
            Ok(true)
        }
        FilterOp::And | FilterOp::Or | FilterOp::Not => Err(QueryError::internal(
            "logical ops should be handled at top level",
        )),
    }
}

/// Evaluate a filter condition against a single JSON value (used by array element operators).
///
/// For example, when evaluating `{_gt: 70}` against `85`, this method checks if 85 > 70.
pub fn eval_conditions_on_value(
    value: &JsonValue,
    conditions: &serde_json::Map<String, JsonValue>,
) -> Result<bool> {
    for (op_str, expected) in conditions {
        let op = FilterOp::parse(op_str).ok_or_else(|| {
            QueryError::invalid_filter(format!("unknown operator in array condition: {}", op_str))
        })?;
        if !eval_op(value, op, expected)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// SQL LIKE pattern matching
fn like_match(
    actual: &JsonValue,
    pattern: &JsonValue,
    negate: bool,
    case_insensitive: bool,
) -> Result<bool> {
    // Null fields never match (standard database behavior, matches Go DefraDB)
    if actual.is_null() {
        return Ok(negate);
    }

    let op_name = if case_insensitive { "_ilike" } else { "_like" };

    // Non-string fields don't match _like (return false, not error)
    // This matches Go DefraDB behavior for JSON fields with mixed types
    let actual_str = match actual.as_str() {
        Some(s) => s,
        None => return Ok(negate), // Non-string doesn't match, so _like=false, _nlike=true
    };
    let pattern_str = pattern.as_str().ok_or_else(|| {
        QueryError::invalid_filter(format!("{} requires string pattern", op_name))
    })?;

    // Apply case transformation if case-insensitive
    let (actual_cmp, pattern_cmp): (std::borrow::Cow<str>, std::borrow::Cow<str>) =
        if case_insensitive {
            (
                actual_str.to_lowercase().into(),
                pattern_str.to_lowercase().into(),
            )
        } else {
            (actual_str.into(), pattern_str.into())
        };

    // SQL LIKE pattern matching following Go DefraDB behavior:
    // - '%' matches zero or more characters
    // - '_' is treated as literal (matches Go behavior)
    // - Supports arbitrary combinations of '%' wildcards
    let matches = like_pattern_match(&actual_cmp, &pattern_cmp);

    Ok(if negate { !matches } else { matches })
}

/// SQL LIKE pattern matching with `%` as wildcard for zero or more characters.
/// `_` is treated as a literal character (matches Go DefraDB behavior).
pub fn like_pattern_match(text: &str, pattern: &str) -> bool {
    let text_bytes = text.as_bytes();
    let pattern_bytes = pattern.as_bytes();
    let p_len = pattern_bytes.len();

    // dp[j] = true means text[0..i] matches pattern[0..j]
    let mut dp = vec![false; p_len + 1];
    dp[0] = true;

    // Initialize: leading '%' can match empty string
    for j in 0..p_len {
        if pattern_bytes[j] == b'%' {
            dp[j + 1] = dp[j];
        } else {
            break;
        }
    }

    for &text_byte in text_bytes {
        let mut prev = dp[0];
        dp[0] = false;
        for j in 0..p_len {
            let temp = dp[j + 1];
            if pattern_bytes[j] == b'%' {
                // '%' matches zero or more chars: either skip '%' (dp[j]) or extend match (dp[j+1])
                dp[j + 1] = dp[j] || dp[j + 1];
            } else if text_byte == pattern_bytes[j] {
                dp[j + 1] = prev;
            } else {
                dp[j + 1] = false;
            }
            prev = temp;
        }
    }

    dp[p_len]
}
