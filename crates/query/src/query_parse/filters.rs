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

use super::values::{graphql_value_to_json, graphql_value_to_json_no_vars};

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
                QueryError::parse(format!("Variable \"${}\" was not provided.", name))
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

/// Parse Go's string filter form into filter conditions.
///
/// Go types a filter as `any` and accepts GraphQL source for it, which its own
/// client and the JS client both send (`internal/db/document_update.go:168-176`
/// via `NewFilterFromString`). An empty string is `ErrEmptyFilter` there, so it
/// is an error here too rather than a match-all.
///
/// Parsed rather than pasted: the source is wrapped in a throwaway operation
/// and run through the real parser, so what comes back is a JSON object that
/// goes on to be validated like any other filter. Splicing the caller's text
/// into the mutation would reopen exactly the hole key validation closes.
pub fn parse_filter_string(source: &str) -> Result<JsonValue> {
    if source.trim().is_empty() {
        return Err(QueryError::parse("filter cannot be empty"));
    }

    let document = format!("query {{ probe(filter: {source}) {{ _docID }} }}");
    let parsed = graphql_parser::parse_query::<String>(&document)
        .map_err(|e| QueryError::parse(format!("invalid filter: {e}")))?;

    let filter = parsed
        .definitions
        .iter()
        .find_map(|definition| match definition {
            graphql_parser::query::Definition::Operation(
                graphql_parser::query::OperationDefinition::Query(query),
            ) => query
                .selection_set
                .items
                .iter()
                .find_map(|item| match item {
                    graphql_parser::query::Selection::Field(field) => field
                        .arguments
                        .iter()
                        .find(|(name, _)| name == "filter")
                        .map(|(_, value)| value),
                    _ => None,
                }),
            _ => None,
        })
        .ok_or_else(|| QueryError::parse("invalid filter"))?;

    match graphql_value_to_json_no_vars(filter)? {
        JsonValue::Object(conditions) => Ok(JsonValue::Object(conditions)),
        _ => Err(QueryError::parse("filter must be an object")),
    }
}
