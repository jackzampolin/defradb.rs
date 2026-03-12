//! Field and directive parsing methods
//!
//! Contains SdlParser methods for parsing field-level and type-level directives:
//! - `parse_field_directives()`, `check_directive_arguments()`
//! - `get_bool_with_warning()`, `get_string_with_warning()`, `get_int_with_warning()`
//! - `parse_type_directives()`, `parse_crdt_directive()`
//! - `parse_index_directive()`, `parse_includes_argument()`, `parse_default_directive()`

use graphql_parser::schema::{Directive, Field};
use schema::CType;

use crate::error::{QueryError, Result};

use super::directives::{
    default_type_error, get_directive_arg, get_directive_string, get_directive_string_list,
    known_directive_arguments, IndexConfig, IndexDirection, ParsedDirectives,
    KNOWN_FIELD_DIRECTIVES, KNOWN_TYPE_DIRECTIVES,
};
use super::helpers::{
    graphql_schema_value_to_json, normalize_datetime_string, parse_graphql_type,
    parse_policy_directive,
};
use super::parser::{CompositeIndex, ParsedField, ParsedTypeDirectives, SdlParser};
use super::warnings::{DirectiveLocation, ParseWarning};

impl<'a> SdlParser<'a> {
    pub(super) fn parse_field(&mut self, field: &Field<'_, String>) -> Result<ParsedField> {
        let field_type = parse_graphql_type(&field.field_type);
        let directives = self.parse_field_directives(&field.directives, &field.name)?;

        Ok(ParsedField {
            name: field.name.clone(),
            field_type,
            directives,
        })
    }

    pub(super) fn parse_field_directives(
        &mut self,
        directives: &[Directive<'_, String>],
        field_name: &str,
    ) -> Result<ParsedDirectives> {
        let mut result = ParsedDirectives::default();
        let type_name = self.current_type.clone().unwrap_or_default();

        for directive in directives {
            let name = directive.name.as_str();

            // Check arguments for all known directives upfront
            if KNOWN_FIELD_DIRECTIVES.contains(&name) {
                self.check_directive_arguments(directive, &type_name, Some(field_name));
            }

            match name {
                "primary" => result.is_primary = true,
                "crdt" => result.crdt_type = Some(self.parse_crdt_directive(directive)?),
                "index" => result.index = Some(self.parse_index_directive(directive, field_name)?),
                "relation" => result.relation_name = get_directive_string(directive, "name"),
                "default" => {
                    let arg_name = directive.arguments.first().map(|(n, _)| n.clone());
                    result.default_value =
                        Some(self.parse_default_directive(directive, field_name)?);
                    result.default_arg_name = arg_name;
                }
                "constraints" => {
                    if let Some(size) =
                        self.get_int_with_warning(directive, "size", &type_name, Some(field_name))
                    {
                        if size < 0 {
                            return Err(QueryError::parse(format!(
                                "@constraints size must be non-negative, got {}",
                                size
                            )));
                        }
                        result.size_constraint = Some(size as usize);
                    }
                }
                "encryptedIndex" => {
                    // Searchable encryption index
                    result.encrypted_index = true;
                }
                "fulltext" => {
                    let language = self.get_string_with_warning(
                        directive,
                        "language",
                        &type_name,
                        Some(field_name),
                    );
                    let k1 =
                        self.get_float_with_warning(directive, "k1", &type_name, Some(field_name));
                    let b =
                        self.get_float_with_warning(directive, "b", &type_name, Some(field_name));
                    result.fulltext = Some(super::directives::FullTextConfig { language, k1, b });
                }
                "embedding" => {
                    let provider = get_directive_string(directive, "provider").unwrap_or_default();
                    let model = get_directive_string(directive, "model").unwrap_or_default();
                    let url = get_directive_string(directive, "url").unwrap_or_default();
                    let fields = get_directive_string_list(directive, "fields");
                    let template = get_directive_string(directive, "template").unwrap_or_default();

                    result.embedding = Some(super::directives::EmbeddingConfig {
                        provider,
                        model,
                        url,
                        fields,
                        template,
                    });
                }
                "policy" => {
                    // Known but not yet implemented - emit warning so users know
                    self.warnings.push(ParseWarning::UnimplementedDirective {
                        directive_name: directive.name.clone(),
                        type_name: type_name.clone(),
                        field_name: Some(field_name.to_string()),
                    });
                }
                _ => {
                    // Unknown directive - emit warning for forward compatibility
                    self.warnings.push(ParseWarning::UnknownDirective {
                        directive_name: directive.name.clone(),
                        location: DirectiveLocation::Field,
                        type_name: type_name.clone(),
                        field_name: Some(field_name.to_string()),
                    });
                }
            }
        }

        Ok(result)
    }

    /// Check directive arguments and warn about unknown ones
    pub(super) fn check_directive_arguments(
        &mut self,
        directive: &Directive<'_, String>,
        type_name: &str,
        field_name: Option<&str>,
    ) {
        let known_args = known_directive_arguments(&directive.name);
        for (arg_name, _) in &directive.arguments {
            if !known_args.contains(&arg_name.as_str()) {
                self.warnings.push(ParseWarning::UnknownDirectiveArgument {
                    directive_name: directive.name.clone(),
                    argument_name: arg_name.clone(),
                    type_name: type_name.to_string(),
                    field_name: field_name.map(|s| s.to_string()),
                });
            }
        }
    }

    /// Get a boolean argument, emitting a warning if the type is wrong.
    ///
    /// Returns `None` if the argument is missing or has the wrong type.
    /// Callers typically use `.unwrap_or(default)` to apply a default value,
    /// meaning invalid types result in the default being used silently
    /// (with a warning emitted to `self.warnings`).
    pub(super) fn get_bool_with_warning(
        &mut self,
        directive: &Directive<'_, String>,
        arg_name: &str,
        type_name: &str,
        field_name: Option<&str>,
    ) -> Option<bool> {
        if let Some(value) = get_directive_arg(directive, arg_name) {
            match value {
                graphql_parser::schema::Value::Boolean(b) => Some(*b),
                _ => {
                    self.warnings.push(ParseWarning::InvalidArgumentType {
                        directive_name: directive.name.clone(),
                        argument_name: arg_name.to_string(),
                        expected_type: "boolean".to_string(),
                        type_name: type_name.to_string(),
                        field_name: field_name.map(|s| s.to_string()),
                    });
                    None
                }
            }
        } else {
            None
        }
    }

    /// Get a string argument, emitting a warning if the type is wrong.
    ///
    /// Returns `None` if the argument is missing or has the wrong type.
    /// Accepts both String and Enum GraphQL values as valid strings.
    pub(super) fn get_string_with_warning(
        &mut self,
        directive: &Directive<'_, String>,
        arg_name: &str,
        type_name: &str,
        field_name: Option<&str>,
    ) -> Option<String> {
        if let Some(value) = get_directive_arg(directive, arg_name) {
            match value {
                graphql_parser::schema::Value::String(s)
                | graphql_parser::schema::Value::Enum(s) => Some(s.clone()),
                _ => {
                    self.warnings.push(ParseWarning::InvalidArgumentType {
                        directive_name: directive.name.clone(),
                        argument_name: arg_name.to_string(),
                        expected_type: "string".to_string(),
                        type_name: type_name.to_string(),
                        field_name: field_name.map(|s| s.to_string()),
                    });
                    None
                }
            }
        } else {
            None
        }
    }

    /// Get an integer argument, emitting a warning if the type is wrong.
    ///
    /// Returns `None` if the argument is missing, has the wrong type, or
    /// is out of i64 range. A warning is emitted for type mismatches.
    pub(super) fn get_int_with_warning(
        &mut self,
        directive: &Directive<'_, String>,
        arg_name: &str,
        type_name: &str,
        field_name: Option<&str>,
    ) -> Option<i64> {
        if let Some(value) = get_directive_arg(directive, arg_name) {
            match value {
                graphql_parser::schema::Value::Int(n) => n.as_i64().or_else(|| {
                    self.warnings.push(ParseWarning::InvalidArgumentType {
                        directive_name: directive.name.clone(),
                        argument_name: arg_name.to_string(),
                        expected_type: "integer (within i64 range)".to_string(),
                        type_name: type_name.to_string(),
                        field_name: field_name.map(|s| s.to_string()),
                    });
                    None
                }),
                _ => {
                    self.warnings.push(ParseWarning::InvalidArgumentType {
                        directive_name: directive.name.clone(),
                        argument_name: arg_name.to_string(),
                        expected_type: "integer".to_string(),
                        type_name: type_name.to_string(),
                        field_name: field_name.map(|s| s.to_string()),
                    });
                    None
                }
            }
        } else {
            None
        }
    }

    /// Get a float argument, emitting a warning if the type is wrong.
    pub(super) fn get_float_with_warning(
        &mut self,
        directive: &Directive<'_, String>,
        arg_name: &str,
        type_name: &str,
        field_name: Option<&str>,
    ) -> Option<f64> {
        if let Some(value) = get_directive_arg(directive, arg_name) {
            match value {
                graphql_parser::schema::Value::Float(f) => Some(*f),
                graphql_parser::schema::Value::Int(n) => n.as_i64().map(|i| i as f64),
                _ => {
                    self.warnings.push(ParseWarning::InvalidArgumentType {
                        directive_name: directive.name.clone(),
                        argument_name: arg_name.to_string(),
                        expected_type: "float".to_string(),
                        type_name: type_name.to_string(),
                        field_name: field_name.map(|s| s.to_string()),
                    });
                    None
                }
            }
        } else {
            None
        }
    }

    pub(super) fn parse_type_directives(
        &mut self,
        directives: &[Directive<'_, String>],
    ) -> Result<ParsedTypeDirectives> {
        let mut result = ParsedTypeDirectives::default();
        let type_name = self.current_type.clone().unwrap_or_default();

        for directive in directives {
            let name = directive.name.as_str();

            // Check arguments for all known directives upfront
            if KNOWN_TYPE_DIRECTIVES.contains(&name) {
                self.check_directive_arguments(directive, &type_name, None);
            }

            match name {
                "index" => {
                    // Parse top-level direction argument (default for all fields)
                    let default_descending = self
                        .get_string_with_warning(directive, "direction", &type_name, None)
                        .as_deref()
                        .map(|d| matches!(d, "DESC" | "desc" | "Descending"))
                        .unwrap_or(false);

                    // Try "fields" argument first (simple format: ["name", "age"])
                    let simple_fields = get_directive_string_list(directive, "fields");
                    let fields: Vec<(String, bool)> = if !simple_fields.is_empty() {
                        // Apply default direction to all fields from simple format
                        simple_fields
                            .into_iter()
                            .map(|f| (f, default_descending))
                            .collect()
                    } else {
                        // Try "includes" argument (Go format: [{field: "name", direction: DESC}, ...])
                        self.parse_includes_argument(directive, default_descending)
                    };

                    let idx_name = get_directive_string(directive, "name");
                    let unique = self
                        .get_bool_with_warning(directive, "unique", &type_name, None)
                        .unwrap_or(false);

                    if !fields.is_empty() {
                        result.indexes.push(CompositeIndex {
                            fields,
                            name: idx_name,
                            unique,
                        });
                    }
                }
                "materialized" => {
                    result.is_materialized = self
                        .get_bool_with_warning(directive, "if", &type_name, None)
                        .unwrap_or(true);
                }
                "downsample" => {
                    let interval = self
                        .get_int_with_warning(directive, "interval", &type_name, None)
                        .ok_or_else(|| {
                            QueryError::parse(
                                "@downsample directive requires a positive integer 'interval' argument",
                            )
                        })?;
                    if interval <= 0 {
                        return Err(QueryError::parse(format!(
                            "@downsample interval must be positive, got {}",
                            interval
                        )));
                    }
                    result.is_materialized = true;
                    result.downsample_interval = Some(interval as u64);
                }
                "branchable" => {
                    result.is_branchable = self
                        .get_bool_with_warning(directive, "if", &type_name, None)
                        .unwrap_or(true);
                }
                "policy" => {
                    result.policy = Some(parse_policy_directive(directive)?);
                }
                _ => {
                    // Unknown directive - emit warning for forward compatibility
                    self.warnings.push(ParseWarning::UnknownDirective {
                        directive_name: directive.name.clone(),
                        location: DirectiveLocation::Type,
                        type_name: type_name.clone(),
                        field_name: None,
                    });
                }
            }
        }

        Ok(result)
    }

    pub(super) fn parse_crdt_directive(&self, directive: &Directive<'_, String>) -> Result<CType> {
        let type_name = get_directive_string(directive, "type")
            .ok_or_else(|| QueryError::parse("@crdt directive requires 'type' argument"))?;

        match type_name.to_lowercase().as_str() {
            "lwwregister" | "lww" | "lww_register" => Ok(CType::LwwRegister),
            "pncounter" | "counter" | "pn_counter" => Ok(CType::PnCounter),
            "pcounter" | "p_counter" => Ok(CType::PCounter),
            other => Err(QueryError::parse(format!(
                "Argument \"type\" has invalid value \"{}\"",
                other
            ))),
        }
    }

    pub(super) fn parse_index_directive(
        &mut self,
        directive: &Directive<'_, String>,
        field_name: &str,
    ) -> Result<IndexConfig> {
        let type_name = self.current_type.clone().unwrap_or_default();
        let name = get_directive_string(directive, "name");
        let unique = self
            .get_bool_with_warning(directive, "unique", &type_name, Some(field_name))
            .unwrap_or(false);
        let direction = match self
            .get_string_with_warning(directive, "direction", &type_name, Some(field_name))
            .as_deref()
        {
            Some("DESC") | Some("desc") | Some("Descending") => IndexDirection::Desc,
            _ => IndexDirection::Asc,
        };

        // Parse includes for composite indexes
        // The default direction for included fields is inherited from the top-level direction
        let default_descending = matches!(direction, IndexDirection::Desc);
        let includes = self.parse_includes_argument(directive, default_descending);

        Ok(IndexConfig {
            name,
            unique,
            direction,
            includes,
        })
    }

    /// Parse the `includes` argument for composite indexes.
    ///
    /// Go format: `@index(includes: [{field: "name"}, {field: "age", direction: DESC}])`
    /// Returns (field_name, descending) tuples extracted from the objects.
    ///
    /// The `default_descending` parameter is used when a field in the includes array
    /// doesn't specify its own direction. This comes from the top-level `direction`
    /// argument on the @index directive.
    pub(super) fn parse_includes_argument(
        &self,
        directive: &Directive<'_, String>,
        default_descending: bool,
    ) -> Vec<(String, bool)> {
        let Some(value) = get_directive_arg(directive, "includes") else {
            return Vec::new();
        };

        let graphql_parser::schema::Value::List(items) = value else {
            return Vec::new();
        };

        items
            .iter()
            .filter_map(|item| {
                // Each item should be an object like {field: "name", direction: DESC}
                let graphql_parser::schema::Value::Object(obj) = item else {
                    return None;
                };

                // Extract field name
                let field_name = obj.get("field").and_then(|v| match v {
                    graphql_parser::schema::Value::String(s)
                    | graphql_parser::schema::Value::Enum(s) => Some(s.clone()),
                    _ => None,
                })?;

                // Extract direction (defaults to the top-level direction or ASC)
                let descending = obj
                    .get("direction")
                    .map(|v| match v {
                        graphql_parser::schema::Value::String(s)
                        | graphql_parser::schema::Value::Enum(s) => {
                            matches!(s.as_str(), "DESC" | "desc" | "Descending")
                        }
                        _ => default_descending,
                    })
                    .unwrap_or(default_descending);

                Some((field_name, descending))
            })
            .collect()
    }

    pub(super) fn parse_default_directive(
        &self,
        directive: &Directive<'_, String>,
        field_name: &str,
    ) -> Result<serde_json::Value> {
        // Go supports: string, bool, int, float, float32, float64, dateTime, json, blob
        let Some((name, value)) = directive.arguments.first() else {
            return Err(QueryError::parse(
                "@default directive requires a value argument",
            ));
        };

        // Multiple arguments not allowed
        if directive.arguments.len() > 1 {
            return Err(QueryError::parse(format!(
                "default value must specify one argument. Field: {}",
                field_name
            )));
        }

        match name.as_str() {
            "string" | "value" => match value {
                graphql_parser::schema::Value::String(s) => Ok(serde_json::Value::String(s.clone())),
                other => Err(default_type_error(name, "string", other)),
            },
            "bool" => match value {
                graphql_parser::schema::Value::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
                other => Err(default_type_error("bool", "boolean", other)),
            },
            "int" => match value {
                graphql_parser::schema::Value::Int(n) => {
                    let int_val = n.as_i64().ok_or_else(|| {
                        QueryError::parse("@default int value is out of i64 range")
                    })?;
                    Ok(serde_json::Value::Number(serde_json::Number::from(int_val)))
                }
                other => Err(default_type_error("int", "integer", other)),
            },
            "float" | "float64" => match value {
                graphql_parser::schema::Value::Float(f) => serde_json::Number::from_f64(*f)
                    .map(serde_json::Value::Number)
                    .ok_or_else(|| {
                        QueryError::parse(
                            "@default float value is not a valid JSON number (NaN or Infinity)",
                        )
                    }),
                // Accept Int values for Float (common in schemas where 10 means 10.0)
                graphql_parser::schema::Value::Int(n) => {
                    let int_val = n.as_i64().ok_or_else(|| {
                        QueryError::parse("@default float value is out of i64 range")
                    })?;
                    Ok(serde_json::Value::Number(serde_json::Number::from(int_val)))
                }
                other => Err(default_type_error("float", "float", other)),
            },
            "float32" => match value {
                graphql_parser::schema::Value::Float(f) => {
                    let f32_val = *f as f32;
                    if f32_val.is_infinite() && !f.is_infinite() {
                        return Err(QueryError::parse(
                            "@default float32 value is out of f32 range",
                        ));
                    }
                    serde_json::Number::from_f64(f32_val as f64)
                        .map(serde_json::Value::Number)
                        .ok_or_else(|| {
                            QueryError::parse(
                                "@default float32 value is not a valid JSON number (NaN or Infinity)",
                            )
                        })
                }
                // Accept Int values for Float32 (common in schemas where 10 means 10.0)
                graphql_parser::schema::Value::Int(n) => {
                    let int_val = n.as_i64().ok_or_else(|| {
                        QueryError::parse("@default float32 value is out of i64 range")
                    })?;
                    Ok(serde_json::Value::Number(serde_json::Number::from(int_val)))
                }
                other => Err(default_type_error("float32", "float", other)),
            },
            "dateTime" => match value {
                graphql_parser::schema::Value::String(s) => {
                    // Normalize to RFC3339 format to match Go's time.Time serialization.
                    // Go parses the datetime string into time.Time then formats it back,
                    // which drops trailing zeros in the fractional seconds.
                    // Parse as DateTime and re-format, or pass through if it's a special value.
                    let normalized = normalize_datetime_string(s);
                    Ok(serde_json::Value::String(normalized))
                }
                // Accept Enum for special values like UTC_NOW
                graphql_parser::schema::Value::Enum(s) => Ok(serde_json::Value::String(s.clone())),
                other => Err(default_type_error("dateTime", "string", other)),
            },
            "json" => {
                // JSON @default accepts various value types
                match value {
                    // String containing JSON - validate but store as string literal
                    // Go stores JSON defaults as strings and returns them as strings
                    graphql_parser::schema::Value::String(s) => {
                        // Validate the JSON is parseable
                        let _: serde_json::Value = serde_json::from_str(s).map_err(|e| {
                            QueryError::parse(format!("@default json contains invalid JSON: {}", e))
                        })?;
                        // Store as string literal to match Go behavior
                        Ok(serde_json::Value::String(s.clone()))
                    }
                    // Primitives and structured types are converted directly
                    graphql_parser::schema::Value::Int(n) => {
                        let int_val = n.as_i64().unwrap_or(0);
                        Ok(serde_json::Value::Number(serde_json::Number::from(int_val)))
                    }
                    graphql_parser::schema::Value::Float(f) => serde_json::Number::from_f64(*f)
                        .map(serde_json::Value::Number)
                        .ok_or_else(|| QueryError::parse("@default json float is invalid (NaN or Infinity)")),
                    graphql_parser::schema::Value::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
                    graphql_parser::schema::Value::Null => {
                        Err(QueryError::parse("default value is invalid for type JSON"))
                    }
                    graphql_parser::schema::Value::Enum(s) => Ok(serde_json::Value::String(s.clone())),
                    // JSON array/object defaults are stored as serialized strings to match Go behavior
                    graphql_parser::schema::Value::List(arr) => {
                        let items: Vec<serde_json::Value> = arr
                            .iter()
                            .map(|v| graphql_schema_value_to_json(v))
                            .collect();
                        let json_array = serde_json::Value::Array(items);
                        Ok(serde_json::Value::String(serde_json::to_string(&json_array).unwrap_or_default()))
                    }
                    graphql_parser::schema::Value::Object(obj) => {
                        let items: serde_json::Map<String, serde_json::Value> = obj
                            .iter()
                            .map(|(k, v)| (k.clone(), graphql_schema_value_to_json(v)))
                            .collect();
                        let json_obj = serde_json::Value::Object(items);
                        Ok(serde_json::Value::String(serde_json::to_string(&json_obj).unwrap_or_default()))
                    }
                    graphql_parser::schema::Value::Variable(v) => Ok(serde_json::Value::String(format!("${}", v))),
                }
            }
            "blob" => match value {
                graphql_parser::schema::Value::String(s) => Ok(serde_json::Value::String(s.clone())),
                other => Err(default_type_error("blob", "string", other)),
            },
            unknown => Err(QueryError::parse(format!(
                "unknown @default argument '{}'. Valid arguments are: string, value, bool, int, float, float32, float64, dateTime, json, blob",
                unknown
            ))),
        }
    }
}
