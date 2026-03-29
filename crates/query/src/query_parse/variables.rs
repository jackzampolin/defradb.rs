//! Variable handling for GraphQL queries

use std::collections::HashMap;

use graphql_parser::query::VariableDefinition;
use serde_json::Value as JsonValue;

use graphql_parser::query::Type;

use crate::error::{QueryError, Result};

use super::values::graphql_value_to_json_no_vars;

/// Merge provided variables with default values.
///
/// Provided variables take precedence over defaults.
pub(crate) fn merge_variables(
    provided: Option<&HashMap<String, JsonValue>>,
    defaults: &HashMap<String, JsonValue>,
) -> HashMap<String, JsonValue> {
    let mut merged = defaults.clone();
    if let Some(vars) = provided {
        for (k, v) in vars {
            merged.insert(k.clone(), v.clone());
        }
    }
    merged
}

/// Extract default values from variable definitions.
///
/// Returns a HashMap of variable name -> default value for all variables
/// that have a default value defined.
pub(crate) fn extract_variable_defaults(
    var_defs: &[VariableDefinition<'_, String>],
) -> Result<HashMap<String, JsonValue>> {
    let mut defaults = HashMap::new();
    for var_def in var_defs {
        if let Some(default_value) = &var_def.default_value {
            // Convert the default value without variable resolution (defaults can't reference other variables)
            let json_val = graphql_value_to_json_no_vars(default_value)?;
            defaults.insert(var_def.name.clone(), json_val);
        }
    }
    Ok(defaults)
}

/// Validate that all non-null (required) variables have values.
///
/// Checks each variable definition and if its type ends with `!` (NonNullType),
/// verifies that a value was provided in the effective variables map.
pub(crate) fn validate_required_variables(
    var_defs: &[VariableDefinition<'_, String>],
    effective_variables: &HashMap<String, JsonValue>,
) -> Result<()> {
    for var_def in var_defs {
        if let Type::NonNullType(inner) = &var_def.var_type {
            if !effective_variables.contains_key(&var_def.name) {
                let type_str = format_type(inner);
                return Err(QueryError::parse(format!(
                    "Variable \"${}\" of required type \"{}!\" was not provided.",
                    var_def.name, type_str
                )));
            }
        }
    }
    Ok(())
}

fn format_type(t: &Type<'_, String>) -> String {
    match t {
        Type::NamedType(name) => name.clone(),
        Type::ListType(inner) => format!("[{}]", format_type(inner)),
        Type::NonNullType(inner) => format!("{}!", format_type(inner)),
    }
}
