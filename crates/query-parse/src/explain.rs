//! Explain directive parsing

use graphql_parser::query::{Directive, Value};

use query_model::error::{QueryError, Result};

use super::ExplainType;

/// Check if a directive list contains @explain and parse its type.
/// Returns Ok(Some(ExplainType)) if @explain is present, Ok(None) if not,
/// or Err if @explain has an invalid type argument.
pub(crate) fn parse_explain_directive(
    directives: &[Directive<'_, String>],
) -> Result<Option<ExplainType>> {
    for directive in directives {
        if directive.name == "explain" {
            // Check for type argument: @explain(type: simple|execute|debug)
            for (name, value) in &directive.arguments {
                if name == "type" {
                    let type_str = match value {
                        Value::Enum(s) => s.as_str(),
                        Value::String(s) => s.as_str(),
                        _ => {
                            return Err(QueryError::parse(
                                "Argument \"type\" has invalid value.\nExpected type \"ExplainType\"."
                                    .to_string(),
                            ));
                        }
                    };
                    if let Some(explain_type) = ExplainType::parse_str(type_str) {
                        return Ok(Some(explain_type));
                    }
                    return Err(QueryError::parse(format!(
                        "Argument \"type\" has invalid value {}.\nExpected type \"ExplainType\", found {}.",
                        type_str, type_str
                    )));
                }
            }
            // No type argument - default to Simple
            return Ok(Some(ExplainType::Simple));
        }
    }
    Ok(None)
}

/// Check if a field's directive list contains @explain (which is invalid on fields).
/// Returns an error if @explain is found on a field selection.
pub(crate) fn check_field_explain_directive(directives: &[Directive<'_, String>]) -> Result<()> {
    for directive in directives {
        if directive.name == "explain" {
            return Err(QueryError::parse(
                "Directive \"explain\" may not be used on FIELD.".to_string(),
            ));
        }
    }
    Ok(())
}

/// Check if a directive list contains @exhaustive.
pub(crate) fn parse_exhaustive_directive(directives: &[Directive<'_, String>]) -> bool {
    directives.iter().any(|d| d.name == "exhaustive")
}
