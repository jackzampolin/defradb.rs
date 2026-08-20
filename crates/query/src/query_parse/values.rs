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
                QueryError::parse(format!("Variable \"${}\" was not provided.", name))
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
                QueryError::parse(format!("Variable \"${}\" was not provided.", name))
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
                QueryError::parse(format!("Variable \"${}\" was not provided.", name))
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
#[allow(dead_code)]
pub(crate) fn resolve_string_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
    arg_name: &str,
) -> Result<String> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided.", name))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            json_val.as_str().map(|s| s.to_string()).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" must be of type String", name))
            })
        }
        _ => Err(QueryError::parse(format!(
            "{} argument must be a string",
            arg_name
        ))),
    }
}

/// Parse docIDs argument into vector of strings.
pub(crate) fn parse_doc_ids_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Vec<String>> {
    match value {
        Value::List(items) => {
            let ids: Result<Vec<String>> = items
                .iter()
                .map(|v| match v {
                    Value::String(s) => Ok(s.clone()),
                    Value::Variable(name) => {
                        let vars = variables.ok_or_else(|| {
                            QueryError::parse(format!("Variable \"${}\" was not provided.", name))
                        })?;
                        let json_val = vars.get(name).ok_or_else(|| {
                            QueryError::parse(format!("Variable \"${}\" was not provided", name))
                        })?;
                        json_val.as_str().map(|s| s.to_string()).ok_or_else(|| {
                            QueryError::parse(format!(
                                "Variable \"${}\" must be of type String",
                                name
                            ))
                        })
                    }
                    _ => Err(QueryError::parse("docIDs items must be strings")),
                })
                .collect();
            ids
        }
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided.", name))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            if let Some(s) = json_val.as_str() {
                Ok(vec![s.to_string()])
            } else if let Some(arr) = json_val.as_array() {
                arr.iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or_else(|| QueryError::parse("docIDs items must be strings"))
                    })
                    .collect()
            } else {
                Err(QueryError::parse(format!(
                    "Variable \"${}\" must be of type String or [String]",
                    name
                )))
            }
        }
        _ => Err(QueryError::parse("docIDs must be a string or list")),
    }
}

/// Parse cid argument into vector of strings.
///
/// Accepts `[String!]` (array) or a single `String` (wrapped into a vec).
pub(crate) fn parse_cid_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Vec<String>> {
    match value {
        Value::List(items) => {
            let cids: Result<Vec<String>> = items
                .iter()
                .map(|v| match v {
                    Value::String(s) => Ok(s.clone()),
                    Value::Variable(name) => {
                        let vars = variables.ok_or_else(|| {
                            QueryError::parse(format!("Variable \"${}\" was not provided.", name))
                        })?;
                        let json_val = vars.get(name).ok_or_else(|| {
                            QueryError::parse(format!("Variable \"${}\" was not provided", name))
                        })?;
                        json_val.as_str().map(|s| s.to_string()).ok_or_else(|| {
                            QueryError::parse(format!(
                                "Variable \"${}\" must be of type String",
                                name
                            ))
                        })
                    }
                    _ => Err(QueryError::parse("cid items must be strings")),
                })
                .collect();
            cids
        }
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided.", name))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            if let Some(s) = json_val.as_str() {
                Ok(vec![s.to_string()])
            } else if let Some(arr) = json_val.as_array() {
                arr.iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or_else(|| QueryError::parse("cid items must be strings"))
                    })
                    .collect()
            } else {
                Err(QueryError::parse(format!(
                    "Variable \"${}\" must be of type String or [String]",
                    name
                )))
            }
        }
        _ => Err(QueryError::parse("cid must be a string or list")),
    }
}

/// Resolve a boolean value from GraphQL Value, supporting variable substitution.
pub(crate) fn resolve_bool_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
    arg_name: &str,
) -> Result<bool> {
    match value {
        Value::Boolean(b) => Ok(*b),
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided.", name))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            json_val.as_bool().ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" must be of type Boolean", name))
            })
        }
        _ => Err(QueryError::parse(format!(
            "{} argument must be a boolean",
            arg_name
        ))),
    }
}
