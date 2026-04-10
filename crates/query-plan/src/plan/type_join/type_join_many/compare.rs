use serde_json::Value as JsonValue;
use std::cmp::Ordering;

/// Resolve a nested field path within a JSON value.
/// For example, given a JSON object `{"name": "Math"}` and path `["name"]`,
/// returns `Some(JsonValue::String("Math"))`.
pub fn resolve_nested_field(value: Option<&JsonValue>, path: &[String]) -> Option<JsonValue> {
    let mut current = value?.clone();
    for key in path {
        match current {
            JsonValue::Object(ref obj) => {
                current = obj.get(key.as_str())?.clone();
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Compare two JSON values for ordering.
/// Follows SQL-like ordering: NULL < bool < number < string < array < object
pub fn compare_json_values(a: Option<&JsonValue>, b: Option<&JsonValue>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(JsonValue::Null), Some(JsonValue::Null)) => Ordering::Equal,
        (Some(JsonValue::Null), Some(_)) => Ordering::Less,
        (Some(_), Some(JsonValue::Null)) => Ordering::Greater,
        (Some(JsonValue::Bool(a)), Some(JsonValue::Bool(b))) => a.cmp(b),
        (Some(JsonValue::Number(a)), Some(JsonValue::Number(b))) => {
            // Compare as f64 for numeric ordering
            let fa = a.as_f64().unwrap_or(0.0);
            let fb = b.as_f64().unwrap_or(0.0);
            fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
        }
        (Some(JsonValue::String(a)), Some(JsonValue::String(b))) => a.cmp(b),
        // Different types: order by type precedence
        (Some(a), Some(b)) => type_precedence(a).cmp(&type_precedence(b)),
    }
}

/// Get type precedence for ordering (lower = comes first)
fn type_precedence(v: &JsonValue) -> u8 {
    match v {
        JsonValue::Null => 0,
        JsonValue::Bool(_) => 1,
        JsonValue::Number(_) => 2,
        JsonValue::String(_) => 3,
        JsonValue::Array(_) => 4,
        JsonValue::Object(_) => 5,
    }
}
