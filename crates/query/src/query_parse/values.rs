//! GraphQL value conversion utilities

use std::collections::HashMap;

use graphql_parser::query::Value;
use serde_json::Value as JsonValue;

use crate::error::{QueryError, Result};

/// Convert GraphQL Value to JSON Value without variable resolution.
///
/// Used for converting default values where variable references are not allowed.
pub(crate) fn graphql_value_to_json_no_vars(value: &Value<'_, String>) -> Result<JsonValue> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Int(n) => n
            .as_i64()
            .map(|i| JsonValue::Number(i.into()))
            .ok_or_else(|| QueryError::parse("integer out of range")),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .ok_or_else(|| QueryError::parse("invalid float value")),
        Value::String(s) => Ok(JsonValue::String(s.clone())),
        Value::Boolean(b) => Ok(JsonValue::Bool(*b)),
        Value::Enum(e) => Ok(JsonValue::String(e.clone())),
        Value::List(items) => {
            let arr: Result<Vec<JsonValue>> =
                items.iter().map(graphql_value_to_json_no_vars).collect();
            Ok(JsonValue::Array(arr?))
        }
        Value::Object(obj) => {
            let mut map = serde_json::Map::new();
            for (k, v) in obj {
                map.insert(k.clone(), graphql_value_to_json_no_vars(v)?);
            }
            Ok(JsonValue::Object(map))
        }
        Value::Variable(name) => Err(QueryError::parse(format!(
            "variable '{}' cannot be used in default value",
            name
        ))),
    }
}

/// Convert GraphQL Value to JSON Value, resolving variables if present.
pub(crate) fn graphql_value_to_json(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<JsonValue> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Int(n) => n
            .as_i64()
            .map(|i| JsonValue::Number(i.into()))
            .ok_or_else(|| QueryError::parse("integer out of range")),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .ok_or_else(|| QueryError::parse("invalid float value")),
        Value::String(s) => Ok(JsonValue::String(s.clone())),
        Value::Boolean(b) => Ok(JsonValue::Bool(*b)),
        Value::Enum(e) => Ok(JsonValue::String(e.clone())),
        Value::List(items) => {
            let arr: Result<Vec<JsonValue>> = items
                .iter()
                .map(|v| graphql_value_to_json(v, variables))
                .collect();
            Ok(JsonValue::Array(arr?))
        }
        Value::Object(obj) => {
            let mut map = serde_json::Map::new();
            for (k, v) in obj {
                map.insert(k.clone(), graphql_value_to_json(v, variables)?);
            }
            Ok(JsonValue::Object(map))
        }
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    name
                ))
            })?;
            vars.get(name).cloned().ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })
        }
    }
}

/// Parse an integer value from GraphQL Value, resolving variables if present.
pub(crate) fn parse_int_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<i64> {
    match value {
        Value::Int(n) => n
            .as_i64()
            .ok_or_else(|| QueryError::parse("integer out of range")),
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    name
                ))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            json_val.as_i64().ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" must be of type Int", name))
            })
        }
        _ => Err(QueryError::parse("expected integer value")),
    }
}

/// Parse an optional integer value (returns None for null).
/// This matches Go DefraDB's behavior where null is treated as "not provided".
pub(crate) fn parse_optional_int_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Option<i64>> {
    match value {
        Value::Null => Ok(None),
        Value::Int(n) => n
            .as_i64()
            .map(Some)
            .ok_or_else(|| QueryError::parse("integer out of range")),
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    name
                ))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            if json_val.is_null() {
                Ok(None)
            } else {
                json_val.as_i64().map(Some).ok_or_else(|| {
                    QueryError::parse(format!("Variable \"${}\" must be of type Int", name))
                })
            }
        }
        _ => Err(QueryError::parse("expected integer value")),
    }
}

/// Resolve a string value from GraphQL Value, supporting variable substitution.
pub(crate) fn resolve_string_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
    field_name: &str,
) -> Result<String> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    name
                ))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            json_val.as_str().map(String::from).ok_or_else(|| {
                QueryError::parse(format!(
                    "Variable \"${}\" must be of type String for field '{}'",
                    name, field_name
                ))
            })
        }
        _ => Err(QueryError::parse(format!(
            "expected string value for field '{}'",
            field_name
        ))),
    }
}

/// Resolve a boolean value from GraphQL Value, supporting variable substitution.
pub(crate) fn resolve_bool_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
    field_name: &str,
) -> Result<bool> {
    match value {
        Value::Boolean(b) => Ok(*b),
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    name
                ))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            json_val.as_bool().ok_or_else(|| {
                QueryError::parse(format!(
                    "Variable \"${}\" must be of type Boolean for field '{}'",
                    name, field_name
                ))
            })
        }
        _ => Err(QueryError::parse(format!(
            "expected boolean value for field '{}'",
            field_name
        ))),
    }
}
