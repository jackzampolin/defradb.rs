//! Variable handling for GraphQL queries

use std::collections::HashMap;

use graphql_parser::query::{Type, VariableDefinition};
use serde_json::Value as JsonValue;

use query_types::error::{QueryError, Result};

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

/// Format a GraphQL type as a string (e.g. `Int!`, `[String]`, `[Int!]!`).
fn format_type(ty: &Type<'_, String>) -> String {
    match ty {
        Type::NamedType(name) => name.clone(),
        Type::NonNullType(inner) => format!("{}!", format_type(inner)),
        Type::ListType(inner) => format!("[{}]", format_type(inner)),
    }
}

/// Validate that all required (non-null) variables have been provided.
///
/// Returns an error for the first required variable that is missing from
/// the effective variables map.
pub(crate) fn validate_required_variables(
    var_defs: &[VariableDefinition<'_, String>],
    effective_variables: &HashMap<String, JsonValue>,
) -> Result<()> {
    for var_def in var_defs {
        if matches!(&var_def.var_type, Type::NonNullType(_))
            && !effective_variables.contains_key(&var_def.name)
        {
            let type_str = format_type(&var_def.var_type);
            return Err(QueryError::parse(format!(
                "Variable \"${}\" of required type \"{}\" was not provided.",
                var_def.name, type_str
            )));
        }
    }
    Ok(())
}
