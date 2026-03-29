//! Filter parsing helpers
//!
//! Standalone functions for parsing GraphQL filter arguments:
//! - `parse_filter_value()` - Parse a filter argument into a Filter
//! - `parse_filter_object()` - Parse a filter object into conditions map

use graphql_parser::query::Value;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;

use crate::error::{QueryError, Result};
use crate::mapper::Filter;

use super::values::graphql_value_to_json;

/// Parse a filter argument value into a Filter.
pub(super) fn parse_filter_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Filter> {
    match value {
        Value::Object(obj) => {
            let conditions = parse_filter_object(obj, variables)?;
            Ok(Filter::from_conditions(conditions))
        }
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "Variable \"${}\" was not provided.",
                    name
                ))
            })?;
            let json_val = vars.get(name.as_str()).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            if let JsonValue::Object(obj) = json_val {
                Ok(Filter::from_conditions(obj.clone()))
            } else {
                Err(QueryError::parse("filter must be an object"))
            }
        }
        _ => Err(QueryError::parse("filter must be an object")),
    }
}

/// Parse a filter object into conditions map.
pub(super) fn parse_filter_object(
    obj: &std::collections::BTreeMap<String, Value<'_, String>>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Map<String, JsonValue>> {
    let mut conditions = Map::new();

    for (key, val) in obj {
        let json_val = graphql_value_to_json(val, variables)?;
        conditions.insert(key.clone(), json_val);
    }

    Ok(conditions)
}
