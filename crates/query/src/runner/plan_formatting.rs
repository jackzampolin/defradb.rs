//! GraphQL-style formatting helpers for filter conditions.

use serde_json::Value as JsonValue;

/// Format filter conditions in Go graphql-go style (unquoted keys).
pub(crate) fn format_graphql_conditions(conditions: &serde_json::Map<String, JsonValue>) -> String {
    let entries: Vec<String> = conditions
        .iter()
        .map(|(k, v)| format!("{}: {}", k, format_graphql_value(v)))
        .collect();
    format!("{{{}}}", entries.join(", "))
}

/// Format a JSON value in Go graphql-go style.
pub(crate) fn format_graphql_value(val: &JsonValue) -> String {
    match val {
        JsonValue::Object(obj) => {
            let entries: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("{}: {}", k, format_graphql_value(v)))
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
        JsonValue::String(s) => format!("\"{}\"", s),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null => "null".to_string(),
        JsonValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_graphql_value).collect();
            format!("[{}]", items.join(", "))
        }
    }
}
