//! Order parsing helpers
//!
//! Standalone functions for parsing GraphQL order arguments:
//! - `parse_order_value()` - Parse order argument into OrderBy
//! - `parse_order_from_json()` - Parse order from resolved JSON variable
//! - `parse_order_condition()` - Parse a single order condition

use graphql_parser::query::Value;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::error::{QueryError, Result};
use crate::mapper::{OrderBy, OrderCondition, OrderDirection};

/// Parse order argument into OrderBy.
/// Supports both single object `{field: ASC}` and array `[{field: ASC}, {other: DESC}]` formats.
pub(super) fn parse_order_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<OrderBy> {
    let mut order_by = OrderBy::new();

    match value {
        Value::Object(obj) => {
            // Go DefraDB requires each order argument to only define one field
            if obj.len() > 1 {
                return Err(QueryError::parse(
                    "each order argument can only define one field",
                ));
            }
            for (field_name, direction_val) in obj {
                if let Some(condition) =
                    parse_order_condition(field_name.clone(), direction_val, variables)?
                {
                    order_by = order_by.with_condition(condition);
                }
            }
        }
        Value::List(items) => {
            // Array of order objects: [{rating: ASC}, {publisher: {yearOpened: DESC}}]
            for item in items {
                if let Value::Object(obj) = item {
                    if obj.len() > 1 {
                        return Err(QueryError::parse(
                            "each order argument can only define one field",
                        ));
                    }
                    for (field_name, direction_val) in obj {
                        if let Some(condition) =
                            parse_order_condition(field_name.clone(), direction_val, variables)?
                        {
                            order_by = order_by.with_condition(condition);
                        }
                    }
                } else {
                    return Err(QueryError::parse(
                        "each order item in array must be an object",
                    ));
                }
            }
        }
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided.", name))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            return parse_order_from_json(json_val);
        }
        _ => return Err(QueryError::parse("order must be an object or array")),
    }

    Ok(order_by)
}

/// Parse order from a resolved JSON variable value.
pub(super) fn parse_order_from_json(json: &JsonValue) -> Result<OrderBy> {
    let mut order_by = OrderBy::new();
    match json {
        JsonValue::Object(obj) => {
            for (field_name, dir_val) in obj {
                if let Some(dir_str) = dir_val.as_str() {
                    if let Some(direction) = OrderDirection::parse(dir_str) {
                        order_by =
                            order_by.with_condition(OrderCondition::new(field_name, direction));
                    }
                }
            }
        }
        JsonValue::Array(items) => {
            for item in items {
                if let JsonValue::Object(obj) = item {
                    for (field_name, dir_val) in obj {
                        if let Some(dir_str) = dir_val.as_str() {
                            if let Some(direction) = OrderDirection::parse(dir_str) {
                                order_by = order_by
                                    .with_condition(OrderCondition::new(field_name, direction));
                            }
                        }
                    }
                }
            }
        }
        _ => {
            return Err(QueryError::parse(
                "order variable must be an object or array",
            ))
        }
    }
    Ok(order_by)
}

/// Parse a single order condition, handling nested relation ordering.
/// Supports both simple `{field: ASC}` and nested `{relation: {field: DESC}}`.
/// Returns None for null values (order field is ignored).
pub(super) fn parse_order_condition(
    field_name: String,
    direction_val: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Option<OrderCondition>> {
    match direction_val {
        // Null order direction means skip this field (Go compatibility)
        Value::Null => Ok(None),
        Value::Enum(s) | Value::String(s) => {
            let direction = OrderDirection::parse(s).ok_or_else(|| {
                QueryError::parse(format!(
                    "invalid order direction: {{{}: {}}}",
                    field_name, s
                ))
            })?;
            Ok(Some(OrderCondition::new(field_name, direction)))
        }
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided.", name))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            let s = json_val.as_str().ok_or_else(|| {
                QueryError::parse(format!(
                    "Variable \"${}\" must be of type Ordering (ASC or DESC)",
                    name
                ))
            })?;
            let direction = OrderDirection::parse(s).ok_or_else(|| {
                QueryError::parse(format!(
                    "invalid order direction: {{{}: {}}}",
                    field_name, s
                ))
            })?;
            Ok(Some(OrderCondition::new(field_name, direction)))
        }
        Value::Object(nested_obj) => {
            // Nested ordering: {relation: {field: ASC}} or {_alias: {aliasName: ASC}}
            // Empty nested order is a no-op (Go compatibility)
            if nested_obj.is_empty() {
                return Ok(None);
            }
            // Recursively parse the nested object
            if nested_obj.len() != 1 {
                return Err(QueryError::parse(
                    "nested order must have exactly one field",
                ));
            }
            let (nested_field, nested_direction) = nested_obj.iter().next().unwrap();
            let nested_condition =
                parse_order_condition(nested_field.clone(), nested_direction, variables)?;
            // If nested condition is None (null value), propagate the None
            match nested_condition {
                Some(mut cond) => {
                    // Handle _alias directive: don't prepend "_alias", just use the nested field name.
                    // This allows ordering by aliased fields like: order: {_alias: {MyAge: ASC}}
                    // where MyAge is an alias for the Age field.
                    if field_name != "_alias" {
                        // For regular nested ordering (relations), prepend the parent field to the path
                        cond.fields.insert(0, field_name);
                    }
                    Ok(Some(cond))
                }
                None => Ok(None),
            }
        }
        _ => Err(QueryError::parse("invalid order input")),
    }
}
