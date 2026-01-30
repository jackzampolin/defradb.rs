//! SDL parser implementation
//!
//! Parses GraphQL Schema Definition Language (SDL) into DefraDB CollectionVersion schemas.
//! Designed for compatibility with Go DefraDB's SDL parsing behavior.

use cid::Cid;

use crate::error::{QueryError, Result};
use graphql_parser::schema::{
    Definition, Directive, Document, Field, InterfaceType, ObjectType, Type, TypeDefinition,
};
use schema::{
    CType, CollectionVersion, FieldDescription, FieldKind, IndexDescription,
    IndexedFieldDescription, ScalarKind,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use super::directives::{
    default_type_error, get_directive_arg, get_directive_string, get_directive_string_list,
    known_directive_arguments, IndexConfig, IndexDirection, ParsedDirectives,
    KNOWN_FIELD_DIRECTIVES, KNOWN_TYPE_DIRECTIVES,
};
use super::warnings::{DirectiveLocation, ParseOutput, ParseWarning};
use regex::Regex;

/// Placeholder field name used to make empty types parseable.
/// graphql_parser requires at least one field per type, but Go DefraDB allows empty types.
const EMPTY_TYPE_PLACEHOLDER: &str = "__defradb_empty_type_placeholder__";

/// Preprocess SDL to handle empty type/interface definitions.
/// graphql_parser doesn't allow empty types, so we insert a placeholder field.
fn preprocess_empty_types(sdl: &str) -> String {
    // Match patterns like `type Name @directive(...)* {}` or `interface Name {}`
    // This regex finds `{` followed by optional whitespace then `}` in type/interface definitions
    let re =
        Regex::new(r"(\b(?:type|interface)\s+\w+(?:\s*@\w+(?:\([^)]*\))?)*\s*)\{\s*\}").unwrap();

    re.replace_all(sdl, |caps: &regex::Captures| {
        format!("{}{{ {}: String }}", &caps[1], EMPTY_TYPE_PLACEHOLDER)
    })
    .to_string()
}

/// Generate an index name matching Go's `{Col}_{firstField}_ASC` pattern.
///
/// If the base name already exists, appends `_2`, `_3`, etc. to avoid collisions.
/// This matches Go's `generateIndexName()` in collection_index.go.
fn generate_index_name(collection_name: &str, first_field: &str, existing_names: &[String]) -> String {
    let base = format!("{}_{}_ASC", collection_name, first_field);
    if !existing_names.contains(&base) {
        return base;
    }
    let mut suffix = 2u32;
    loop {
        let candidate = format!("{}_{}", base, suffix);
        if !existing_names.contains(&candidate) {
            return candidate;
        }
        suffix += 1;
    }
}

/// Convert a GraphQL schema Value to a serde_json Value
fn graphql_schema_value_to_json(
    value: &graphql_parser::schema::Value<'_, String>,
) -> serde_json::Value {
    match value {
        graphql_parser::schema::Value::String(s) => serde_json::Value::String(s.clone()),
        graphql_parser::schema::Value::Int(n) => {
            let int_val = n.as_i64().unwrap_or(0);
            serde_json::Value::Number(serde_json::Number::from(int_val))
        }
        graphql_parser::schema::Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        graphql_parser::schema::Value::Boolean(b) => serde_json::Value::Bool(*b),
        graphql_parser::schema::Value::Null => serde_json::Value::Null,
        graphql_parser::schema::Value::Enum(s) => serde_json::Value::String(s.clone()),
        graphql_parser::schema::Value::List(arr) => {
            let items: Vec<serde_json::Value> =
                arr.iter().map(graphql_schema_value_to_json).collect();
            serde_json::Value::Array(items)
        }
        graphql_parser::schema::Value::Object(obj) => {
            let items: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), graphql_schema_value_to_json(v)))
                .collect();
            serde_json::Value::Object(items)
        }
        graphql_parser::schema::Value::Variable(v) => serde_json::Value::String(format!("${}", v)),
    }
}

/// Parse @policy directive arguments with Go-compatible error messages.
fn parse_policy_directive(directive: &Directive<'_, String>) -> Result<PolicyConfig> {
    let id_raw = get_directive_arg(directive, "id");
    let resource_raw = get_directive_arg(directive, "resource");

    // Check for non-string argument types first (Go's graphql-go reports these)
    if let Some(v) = id_raw {
        if !matches!(v, graphql_parser::schema::Value::String(_)) {
            return Err(QueryError::parse(format!(
                "Argument \"id\" has invalid value {}",
                format_graphql_value(v)
            )));
        }
    }
    if let Some(v) = resource_raw {
        if !matches!(v, graphql_parser::schema::Value::String(_)) {
            return Err(QueryError::parse(format!(
                "Argument \"resource\" has invalid value {}",
                format_graphql_value(v)
            )));
        }
    }

    // Extract string values (None if argument not present)
    let id = id_raw.and_then(|v| match v {
        graphql_parser::schema::Value::String(s) => Some(s.clone()),
        _ => None,
    });
    let resource = resource_raw.and_then(|v| match v {
        graphql_parser::schema::Value::String(s) => Some(s.clone()),
        _ => None,
    });

    let id_empty = id.as_ref().map_or(true, |s| s.is_empty());
    let resource_empty = resource.as_ref().map_or(true, |s| s.is_empty());

    if id_empty && resource_empty {
        return Err(QueryError::parse(
            "missing policy arguments, must have both id and resource",
        ));
    }
    if id_empty {
        return Err(QueryError::parse("policyID must not be empty"));
    }
    if resource_empty {
        return Err(QueryError::parse("resource name must not be empty"));
    }

    Ok(PolicyConfig {
        id: id.unwrap(),
        resource: resource.unwrap(),
    })
}

/// Format a GraphQL value for error messages (matches Go's graphql-go formatting).
fn format_graphql_value(value: &graphql_parser::schema::Value<'_, String>) -> String {
    match value {
        graphql_parser::schema::Value::Int(n) => {
            n.as_i64().map_or("0".to_string(), |v| v.to_string())
        }
        graphql_parser::schema::Value::Float(f) => f.to_string(),
        graphql_parser::schema::Value::Boolean(b) => b.to_string(),
        graphql_parser::schema::Value::String(s) => format!("\"{}\"", s),
        graphql_parser::schema::Value::Null => "null".to_string(),
        graphql_parser::schema::Value::Enum(s) => s.clone(),
        graphql_parser::schema::Value::List(_) => "[list]".to_string(),
        graphql_parser::schema::Value::Object(_) => "{object}".to_string(),
        graphql_parser::schema::Value::Variable(v) => format!("${}", v),
    }
}

/// SDL parser for DefraDB schemas
pub struct SdlParser<'a> {
    sdl: &'a str,
    /// Parsed type definitions by name
    type_defs: HashMap<String, ParsedTypeDef>,
    /// Type names in SDL definition order (Go returns collections in this order)
    definition_order: Vec<String>,
    /// Warnings collected during parsing
    warnings: Vec<ParseWarning>,
    /// Current type being parsed (for warning context)
    current_type: Option<String>,
    /// External type names (e.g. existing collection types) that can be referenced
    /// in field types but are not defined in the SDL being parsed.
    known_external_types: std::collections::HashSet<String>,
}

#[derive(Debug)]
struct ParsedTypeDef {
    name: String,
    fields: Vec<ParsedField>,
    directives: ParsedTypeDirectives,
    /// Whether this type was defined with the `interface` keyword (not directly queryable)
    is_interface: bool,
}

/// Type-level directives
#[derive(Debug)]
struct ParsedTypeDirectives {
    indexes: Vec<CompositeIndex>,
    /// Default true for collections (Go compatibility)
    is_materialized: bool,
    is_branchable: bool,
    policy: Option<PolicyConfig>,
}

impl Default for ParsedTypeDirectives {
    fn default() -> Self {
        Self {
            indexes: Vec::new(),
            // Go defaults IsMaterialized to true for regular collections
            is_materialized: true,
            is_branchable: false,
            policy: None,
        }
    }
}

/// Policy configuration from @policy directive
#[derive(Debug, Clone)]
struct PolicyConfig {
    id: String,
    resource: String,
}

#[derive(Debug)]
struct CompositeIndex {
    fields: Vec<(String, bool)>, // (field_name, descending)
    name: Option<String>,
    unique: bool,
}

#[derive(Debug)]
struct ParsedField {
    name: String,
    field_type: ParsedType,
    directives: ParsedDirectives,
}

#[derive(Debug)]
struct ParsedType {
    base_type: String,
    is_list: bool,
    is_non_null: bool,
    element_non_null: bool,
}

impl<'a> SdlParser<'a> {
    pub fn new(sdl: &'a str) -> Self {
        Self {
            sdl,
            type_defs: HashMap::new(),
            definition_order: Vec::new(),
            warnings: Vec::new(),
            current_type: None,
            known_external_types: std::collections::HashSet::new(),
        }
    }

    /// Set external type names that can be referenced but aren't defined in the SDL.
    pub fn with_known_types(mut self, types: std::collections::HashSet<String>) -> Self {
        self.known_external_types = types;
        self
    }

    /// Parse the SDL and return collection versions
    pub fn parse(&mut self) -> Result<Vec<CollectionVersion>> {
        let output = self.parse_with_warnings()?;
        Ok(output.collections)
    }

    /// Parse the SDL and return collection versions with warnings
    pub fn parse_with_warnings(&mut self) -> Result<ParseOutput> {
        // Handle empty or whitespace-only input
        if self.sdl.trim().is_empty() {
            return Ok(ParseOutput {
                collections: Vec::new(),
                warnings: Vec::new(),
            });
        }

        // Preprocess SDL to handle empty type definitions (Go compatibility)
        let preprocessed = preprocess_empty_types(self.sdl);

        let doc: Document<'_, String> = graphql_parser::parse_schema(&preprocessed)
            .map_err(|e| QueryError::parse(e.to_string()))?;

        // First pass: collect all type definitions (both `type` and `interface` keywords)
        for def in &doc.definitions {
            match def {
                Definition::TypeDefinition(TypeDefinition::Object(obj)) => {
                    self.parse_object_type(obj)?;
                }
                Definition::TypeDefinition(TypeDefinition::Interface(iface)) => {
                    self.parse_interface_type(iface)?;
                }
                _ => {}
            }
        }

        // Second pass: resolve relations and build CollectionVersions
        let collections = self.build_collections()?;

        Ok(ParseOutput {
            collections,
            warnings: std::mem::take(&mut self.warnings),
        })
    }

    fn parse_object_type(&mut self, obj: &ObjectType<'_, String>) -> Result<()> {
        let name = obj.name.clone();

        // Check for duplicate type names (Go compatibility: error on duplicates in same SDL)
        if self.type_defs.contains_key(&name) {
            return Err(QueryError::parse(format!(
                "collection already exists. Name: {}",
                name
            )));
        }

        self.current_type = Some(name.clone());
        let mut fields = Vec::new();

        for field in &obj.fields {
            // Skip placeholder fields used for empty type preprocessing
            if field.name == EMPTY_TYPE_PLACEHOLDER {
                continue;
            }
            let parsed_field = self.parse_field(field)?;
            fields.push(parsed_field);
        }

        let type_directives = self.parse_type_directives(&obj.directives)?;

        self.definition_order.push(name.clone());
        self.type_defs.insert(
            name.clone(),
            ParsedTypeDef {
                name,
                fields,
                directives: type_directives,
                is_interface: false,
            },
        );

        self.current_type = None;
        Ok(())
    }

    /// Parse an interface type definition.
    /// Go's SDL parser treats `interface` the same as `type` for view embedded schemas.
    fn parse_interface_type(&mut self, iface: &InterfaceType<'_, String>) -> Result<()> {
        let name = iface.name.clone();

        if self.type_defs.contains_key(&name) {
            return Err(QueryError::parse(format!(
                "collection already exists. Name: {}",
                name
            )));
        }

        self.current_type = Some(name.clone());
        let mut fields = Vec::new();

        for field in &iface.fields {
            if field.name == EMPTY_TYPE_PLACEHOLDER {
                continue;
            }
            let parsed_field = self.parse_field(field)?;
            fields.push(parsed_field);
        }

        let type_directives = self.parse_type_directives(&iface.directives)?;

        self.definition_order.push(name.clone());
        self.type_defs.insert(
            name.clone(),
            ParsedTypeDef {
                name,
                fields,
                directives: type_directives,
                is_interface: true,
            },
        );

        self.current_type = None;
        Ok(())
    }

    fn parse_field(&mut self, field: &Field<'_, String>) -> Result<ParsedField> {
        let field_type = parse_graphql_type(&field.field_type);
        let directives = self.parse_field_directives(&field.directives, &field.name)?;

        Ok(ParsedField {
            name: field.name.clone(),
            field_type,
            directives,
        })
    }

    fn parse_field_directives(
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
                "default" => result.default_value = Some(self.parse_default_directive(directive)?),
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
                "embedding" | "encryptedIndex" | "policy" => {
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
    fn check_directive_arguments(
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
    fn get_bool_with_warning(
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
    fn get_string_with_warning(
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
    fn get_int_with_warning(
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

    fn parse_type_directives(
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
                    // Try "fields" argument first (simple format: ["name", "age"])
                    let simple_fields = get_directive_string_list(directive, "fields");
                    let fields: Vec<(String, bool)> = if !simple_fields.is_empty() {
                        simple_fields.into_iter().map(|f| (f, false)).collect()
                    } else {
                        // Try "includes" argument (Go format: [{field: "name", direction: DESC}, ...])
                        self.parse_includes_argument(directive)
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

    fn parse_crdt_directive(&self, directive: &Directive<'_, String>) -> Result<CType> {
        let type_name = get_directive_string(directive, "type")
            .ok_or_else(|| QueryError::parse("@crdt directive requires 'type' argument"))?;

        match type_name.to_lowercase().as_str() {
            "lwwregister" | "lww" | "lww_register" => Ok(CType::LwwRegister),
            "pncounter" | "counter" | "pn_counter" => Ok(CType::PnCounter),
            "pcounter" | "p_counter" => Ok(CType::PCounter),
            other => Err(QueryError::parse(format!("unknown CRDT type: {}", other))),
        }
    }

    fn parse_index_directive(
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

        Ok(IndexConfig {
            name,
            unique,
            direction,
        })
    }

    /// Parse the `includes` argument for composite indexes.
    ///
    /// Go format: `@index(includes: [{field: "name"}, {field: "age", direction: DESC}])`
    /// Returns (field_name, descending) tuples extracted from the objects.
    fn parse_includes_argument(&self, directive: &Directive<'_, String>) -> Vec<(String, bool)> {
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

                // Extract direction (defaults to ASC)
                let descending = obj
                    .get("direction")
                    .map(|v| match v {
                        graphql_parser::schema::Value::String(s)
                        | graphql_parser::schema::Value::Enum(s) => {
                            matches!(s.as_str(), "DESC" | "desc" | "Descending")
                        }
                        _ => false,
                    })
                    .unwrap_or(false);

                Some((field_name, descending))
            })
            .collect()
    }

    fn parse_default_directive(
        &self,
        directive: &Directive<'_, String>,
    ) -> Result<serde_json::Value> {
        // Go supports: string, bool, int, float, float32, float64, dateTime, json, blob
        let Some((name, value)) = directive.arguments.first() else {
            return Err(QueryError::parse(
                "@default directive requires a value argument",
            ));
        };

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
                graphql_parser::schema::Value::String(s) => Ok(serde_json::Value::String(s.clone())),
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

    /// Build a map of which relation fields have the @primary directive.
    /// Key: (source_type, target_type) -> bool (has_primary)
    ///
    /// This is used to determine which side of a 1:1 relation is the "primary" side.
    /// In Go, if one side has @primary and the other doesn't, the side WITHOUT @primary
    /// is considered secondary and gets empty FieldID (not included in CID calculation).
    fn collect_primary_directives(
        &self,
        type_names: &std::collections::HashSet<String>,
    ) -> std::collections::HashMap<(String, String), bool> {
        let mut result = std::collections::HashMap::new();

        for (type_name, type_def) in &self.type_defs {
            for field in &type_def.fields {
                let target = &field.field_type.base_type;

                // Only consider relations to other types in the schema
                if type_names.contains(target) {
                    // Key: (source_type, target_type) -> has_primary directive
                    // Use OR logic: if ANY field in this (source, target) pair has @primary, it's true.
                    // This handles self-referencing types where multiple fields share the same key.
                    let entry = result
                        .entry((type_name.clone(), target.clone()))
                        .or_insert(false);
                    if field.directives.is_primary {
                        *entry = true;
                    }
                }
            }
        }

        result
    }

    fn build_collections(&self) -> Result<Vec<CollectionVersion>> {
        // Build collection names set for relation detection, including external types
        let mut type_names: std::collections::HashSet<_> = self.type_defs.keys().cloned().collect();
        type_names.extend(self.known_external_types.iter().cloned());

        // Collect @primary directive information for determining actual primaryness
        let primary_directives = self.collect_primary_directives(&type_names);

        // Detect circular relation sets - types that form TRUE cycles
        // A cycle only occurs if BOTH sides of a mutual reference are PRIMARY
        // (i.e., neither has @primary making the other secondary)
        let collection_set = self.detect_collection_set(&type_names, &primary_directives);

        // Process types in alphabetical order (Go behavior)
        let mut sorted_type_names: Vec<_> = self.type_defs.keys().cloned().collect();
        sorted_type_names.sort();

        // TOPOLOGICAL ORDER APPROACH (matches Go behavior):
        // Process types in topological order based on CID dependencies.
        //
        // A type A depends on type B if A has a PRIMARY relation field to B
        // (meaning B's CollectionID must be known to calculate A's CID).
        //
        // Types are sorted by:
        // 1. Dependency order (types with fewer dependencies first)
        // 2. Alphabetical order as tiebreaker
        //
        // This ensures that when we process a type, all types it depends on
        // have already been processed and their CollectionIDs are known.

        // Build dependency graph: which types does each type's CID depend on?
        let mut dependencies: std::collections::HashMap<String, std::collections::HashSet<String>> =
            std::collections::HashMap::new();

        for type_name in &sorted_type_names {
            let type_def = self.type_defs.get(type_name).unwrap();
            let mut deps = std::collections::HashSet::new();

            for field in &type_def.fields {
                let target = &field.field_type.base_type;
                if !type_names.contains(target) || target == type_name {
                    continue; // Not a relation to another type in schema, or self-ref
                }
                if self.known_external_types.contains(target) {
                    continue; // External type already exists, no CID dependency
                }

                // Check if this field is PRIMARY (included in CID calculation)
                let is_array = field.field_type.is_list;
                if is_array {
                    continue; // Arrays are secondary, not in CID
                }

                let has_primary = field.directives.is_primary;
                let counterpart_has_primary = primary_directives
                    .get(&(target.clone(), type_name.clone()))
                    .copied()
                    .unwrap_or(false);

                let is_field_primary = has_primary || !counterpart_has_primary;
                if is_field_primary {
                    // This field is PRIMARY, so this type's CID depends on target's CID
                    deps.insert(target.clone());
                }
            }

            dependencies.insert(type_name.clone(), deps);
        }

        // Topological sort using Kahn's algorithm
        // In-degree = number of types this type depends on (not how many depend on it).
        // Types with in-degree 0 have no unresolved dependencies and can be processed.
        let mut in_degree: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (type_name, deps) in &dependencies {
            in_degree.insert(type_name.clone(), deps.len());
        }

        // Queue starts with types that have no dependencies
        let mut queue: Vec<&String> = sorted_type_names
            .iter()
            .filter(|name| {
                dependencies
                    .get(*name)
                    .map(|d| d.is_empty())
                    .unwrap_or(true)
            })
            .collect();
        // Sort queue alphabetically for determinism
        queue.sort();

        let mut processing_order = Vec::new();
        while !queue.is_empty() {
            // Sort queue alphabetically for deterministic ordering
            queue.sort();
            let current = queue.remove(0);
            processing_order.push(current.clone());

            // For each type that depends on current, decrease its in-degree
            for (type_name, deps) in &dependencies {
                if deps.contains(current) {
                    let degree = in_degree.get_mut(type_name).unwrap();
                    *degree = degree.saturating_sub(1);
                    if *degree == 0 && !processing_order.contains(type_name) {
                        queue.push(type_name);
                    }
                }
            }
        }

        // If there's a cycle, fall back to alphabetical order for remaining types
        for type_name in &sorted_type_names {
            if !processing_order.contains(type_name) {
                processing_order.push(type_name.clone());
            }
        }

        // TWO-PASS APPROACH:
        // Pass 1: Calculate CollectionIDs in topological order
        // This ensures CID dependencies are resolved correctly.
        // Also simulates Go's headstore to replicate prefix collision behavior.
        let mut all_collection_ids: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut headstore: HashMap<String, (Cid, u64)> = HashMap::new();

        for type_name in &processing_order {
            let type_def = self.type_defs.get(type_name).unwrap();
            let collection = self.build_collection(
                type_def,
                &type_names,
                &collection_set,
                &all_collection_ids, // Pass already-calculated CollectionIDs
                &primary_directives,
                &headstore,
            )?;
            // Store this type's CollectionID for later types to reference
            all_collection_ids.insert(type_name.clone(), collection.collection_id.clone());

            // Update simulated headstore: store this collection's CID with height=1
            // (Go stores collection definition CIDs at prefix /g/<CollectionName>)
            if let Ok(cid) = collection.collection_id.parse::<Cid>() {
                // Determine height: check if any prefix collision occurred
                let prefix = format!("/g/{}", type_name);
                let max_height: u64 = headstore
                    .iter()
                    .filter(|(k, _)| format!("/g/{}", k).starts_with(&prefix))
                    .map(|(_, (_, h))| *h)
                    .max()
                    .unwrap_or(0);
                let height = max_height + 1;
                headstore.insert(type_name.clone(), (cid, height));
            }
        }
        // Pass 2: Rebuild all collections with ALL CollectionIDs known
        // This ensures all relation fields have proper CollectionKind (not NamedKind)
        // for query resolution, even for secondary fields pointing to later types.
        // Return in SDL definition order to match Go's parser behavior (Go's sequential
        // prefix counter assigns IDs in the order types appear in the SDL).
        //
        // IMPORTANT: Use the collection_id/version_id from Pass 1, not the regenerated
        // ones from Pass 2. Pass 1 computes CIDs in topological order with headstore
        // simulation matching Go's behavior. Pass 2 may produce different field CIDs
        // (because Named → Relation resolution changes the field kind), which would
        // change the collection CID incorrectly.
        let mut collections = Vec::new();

        for type_name in &self.definition_order {
            let type_def = self.type_defs.get(type_name).unwrap();
            let mut collection = self.build_collection(
                type_def,
                &type_names,
                &collection_set,
                &all_collection_ids, // Now has ALL CollectionIDs
                &primary_directives,
                &headstore,
            )?;

            // Override with Pass 1's CID (computed in topological order with headstore)
            if let Some(pass1_id) = all_collection_ids.get(type_name) {
                collection.collection_id = pass1_id.clone();
                collection.version_id = pass1_id.clone();
            }

            // Interface types are embedded-only (not root-queryable)
            if type_def.is_interface {
                collection.is_embedded_only = true;
            }

            collections.push(collection);
        }

        Ok(collections)
    }

    /// Detect types that form circular relation sets.
    /// Returns a map from type name to its sorted index within its connected cycle group.
    /// Types with circular relations use SelfKind with relative indices for CID generation.
    ///
    /// IMPORTANT: A cycle only exists if BOTH sides of a mutual reference are PRIMARY.
    /// If one side has @primary and the other doesn't, the side WITHOUT @primary is SECONDARY
    /// and gets empty FieldID (not included in CID calculation), breaking the cycle.
    ///
    /// IMPORTANT: Only types that are ACTUALLY part of a cycle are included.
    /// Different cycle groups (e.g., Employee self-ref) are treated as separate collection sets.
    fn detect_collection_set(
        &self,
        type_names: &std::collections::HashSet<String>,
        primary_directives: &std::collections::HashMap<(String, String), bool>,
    ) -> std::collections::HashMap<String, i32> {
        // Helper to check if a relation field from source->target is actually primary
        // (will be included in CID calculation)
        let is_field_primary = |source: &str, target: &str, is_array: bool| -> bool {
            if is_array {
                // Arrays are always secondary
                return false;
            }

            // Check if this field has @primary directive
            let has_primary = primary_directives
                .get(&(source.to_string(), target.to_string()))
                .copied()
                .unwrap_or(false);

            if has_primary {
                return true;
            }

            // Check if the counterpart (target->source) has @primary
            // If it does, this side is SECONDARY
            let counterpart_has_primary = primary_directives
                .get(&(target.to_string(), source.to_string()))
                .copied()
                .unwrap_or(false);

            if counterpart_has_primary {
                // Counterpart has @primary, so this side is secondary
                return false;
            }

            // Neither side has @primary - single-object relation defaults to primary
            true
        };

        // Build relation graph: which types reference which other types via PRIMARY relations only
        let mut references: std::collections::HashMap<String, std::collections::HashSet<String>> =
            std::collections::HashMap::new();

        for (type_name, type_def) in &self.type_defs {
            let mut refs = std::collections::HashSet::new();
            for field in &type_def.fields {
                let target = &field.field_type.base_type;
                // Only consider relations to other types that are:
                // 1. In the current schema (type_names)
                // 2. Actually PRIMARY (will be included in CID)
                if type_names.contains(target)
                    && is_field_primary(type_name, target, field.field_type.is_list)
                {
                    refs.insert(target.clone());
                }
            }
            references.insert(type_name.clone(), refs);
        }

        // Build bidirectional cycle edges: only keep edges that form cycles
        // A->B is a cycle edge if A->B and B->A (mutual reference) or A->A (self-ref)
        // where BOTH sides are PRIMARY
        let mut cycle_edges: std::collections::HashMap<String, std::collections::HashSet<String>> =
            std::collections::HashMap::new();

        for (type_a, refs_a) in &references {
            let mut edges = std::collections::HashSet::new();

            // Self-reference (single-object only - already filtered above)
            if refs_a.contains(type_a) {
                edges.insert(type_a.clone());
            }

            // Mutual references where BOTH directions are primary
            for type_b in refs_a {
                if type_a != type_b {
                    if let Some(refs_b) = references.get(type_b) {
                        // Both A->B and B->A are in PRIMARY references
                        if refs_b.contains(type_a) {
                            edges.insert(type_b.clone());
                        }
                    }
                }
            }

            if !edges.is_empty() {
                cycle_edges.insert(type_a.clone(), edges);
            }
        }

        if cycle_edges.is_empty() {
            return std::collections::HashMap::new();
        }

        // Find connected components of cycle graph using union-find style approach
        let mut component: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        fn find_root(
            node: &str,
            component: &mut std::collections::HashMap<String, String>,
        ) -> String {
            if let Some(parent) = component.get(node).cloned() {
                if parent != node {
                    let root = find_root(&parent, component);
                    component.insert(node.to_string(), root.clone());
                    return root;
                }
            }
            node.to_string()
        }

        fn union(a: &str, b: &str, component: &mut std::collections::HashMap<String, String>) {
            let root_a = find_root(a, component);
            let root_b = find_root(b, component);
            if root_a != root_b {
                // Use lexicographically smaller as root for determinism
                if root_a < root_b {
                    component.insert(root_b, root_a);
                } else {
                    component.insert(root_a, root_b);
                }
            }
        }

        // Initialize each node as its own component
        for type_name in cycle_edges.keys() {
            component.insert(type_name.clone(), type_name.clone());
        }

        // Union nodes that are connected via cycle edges
        for (type_a, edges) in &cycle_edges {
            for type_b in edges {
                union(type_a, type_b, &mut component);
            }
        }

        // Group types by their component root
        let mut components: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for type_name in cycle_edges.keys() {
            let root = find_root(type_name, &mut component);
            components.entry(root).or_default().push(type_name.clone());
        }

        // Build result: each type gets index within its component
        let mut result = std::collections::HashMap::new();
        for (_root, mut members) in components {
            members.sort(); // Sort alphabetically within component
            for (idx, name) in members.into_iter().enumerate() {
                result.insert(name, idx as i32);
            }
        }

        result
    }

    fn build_collection(
        &self,
        type_def: &ParsedTypeDef,
        type_names: &std::collections::HashSet<String>,
        collection_set: &std::collections::HashMap<String, i32>,
        known_collection_ids: &std::collections::HashMap<String, String>,
        primary_directives: &std::collections::HashMap<(String, String), bool>,
        headstore: &HashMap<String, (Cid, u64)>,
    ) -> Result<CollectionVersion> {
        // collection_id will be generated after fields are created (like Go)
        let mut fields = Vec::new();
        let mut indexes = Vec::new();
        let mut existing_index_names: Vec<String> = Vec::new();
        let mut field_id_counter = 1u32;

        // Add implicit _docID field
        // NOTE: Go uses CType::None (0) for _docID, not LwwRegister (1)
        let doc_id_kind = FieldKind::doc_id();
        let doc_id_field_id = generate_field_id("_docID", &doc_id_kind, CType::None);
        fields.push(
            FieldDescription::new(&doc_id_field_id, "_docID", doc_id_kind)
                .with_crdt_type(CType::None),
        );
        field_id_counter += 1;

        // Process user-defined fields
        for parsed_field in &type_def.fields {
            let kind = self.resolve_field_kind(
                &parsed_field.field_type,
                type_names,
                &type_def.name,
                collection_set,
                known_collection_ids,
            )?;

            // Determine if this relation creates an implicit _id field (FK):
            // - Single-object relations (not arrays) get an implicit {field}_id field
            // - But only on the PRIMARY side (the side with @primary OR the default primary)
            let creates_fk_field = kind.is_relation() && !kind.is_array();

            // Check if this is a self-reference relation (field type == current type)
            let is_self_ref_relation =
                kind.is_relation() && parsed_field.field_type.base_type == type_def.name;

            // Determine the actual primary status for this relation field
            // In Go, a field is PRIMARY if:
            // 1. It has explicit @primary directive, OR
            // 2. It's a single-object relation AND the counterpart does NOT have @primary
            // A field is SECONDARY if:
            // 1. It's an array relation, OR
            // 2. The counterpart (target->source) has @primary
            let is_primary = if kind.is_relation() {
                let target_type = &parsed_field.field_type.base_type;
                let source_type = &type_def.name;

                // Check if this field has @primary directive
                let has_primary_directive = parsed_field.directives.is_primary;

                // Check if counterpart has @primary directive
                let counterpart_has_primary = primary_directives
                    .get(&(target_type.clone(), source_type.clone()))
                    .copied()
                    .unwrap_or(false);

                if kind.is_array() {
                    // Arrays are always secondary
                    false
                } else if has_primary_directive {
                    // Explicit @primary makes this primary
                    true
                } else if counterpart_has_primary {
                    // Counterpart has @primary, so this is secondary
                    false
                } else {
                    // Neither has @primary - single-object defaults to primary
                    true
                }
            } else {
                // Non-relation fields: use explicit @primary directive
                parsed_field.directives.is_primary
            };

            // Determine CRDT type: directive overrides > relation defaults > LwwRegister
            // Go uses NONE_CRDT (Typ=0) for ALL relation object fields, not just single-object
            let crdt_type = if let Some(ct) = parsed_field.directives.crdt_type {
                ct
            } else if kind.is_relation() {
                // All relation object fields use NONE_CRDT in Go
                CType::None
            } else {
                CType::LwwRegister
            };

            // Generate field ID using actual kind and CRDT type.
            // Go assigns empty FieldID to:
            // - Secondary (non-primary) relation object fields
            // - Self-referencing relation object fields with empty RelativeID
            //   (Go's Delta() skips them because strconv.Atoi("") fails)
            let field_id = if is_self_ref_relation || (kind.is_relation() && !is_primary) {
                String::new()
            } else {
                generate_field_id(&parsed_field.name, &kind, crdt_type)
            };

            let mut field = FieldDescription::new(&field_id, &parsed_field.name, kind.clone())
                .with_crdt_type(crdt_type);

            // Set is_primary based on our earlier computation
            if is_primary {
                field = field.as_primary();
            }
            if let Some(ref default_value) = parsed_field.directives.default_value {
                field = field.with_default(default_value.clone());
            }
            if let Some(size) = parsed_field.directives.size_constraint {
                field = field.with_size(size);
            }

            // Set relation name - use explicit @relation(name:) if provided, otherwise auto-generate
            if kind.is_relation() {
                let relation_name = parsed_field
                    .directives
                    .relation_name
                    .clone()
                    .unwrap_or_else(|| {
                        // Go uses lexicographic sort of type names for auto-generated relation names
                        generate_relation_name(
                            &type_def.name,
                            &parsed_field.name,
                            &parsed_field.field_type.base_type,
                        )
                    });
                field = field.with_relation_name(relation_name.clone());

                // For single-object relations (not arrays), Go automatically creates an
                // implicit _{field}ID field to store the foreign key.
                // The FK field has the SAME is_primary status as the main relation field:
                // - If main field is PRIMARY, FK field is also PRIMARY (non-empty FieldID)
                // - If main field is SECONDARY, FK field is also SECONDARY (empty FieldID)
                if creates_fk_field {
                    let id_field_name = format!("_{}ID", parsed_field.name);
                    let id_field_kind = FieldKind::doc_id();
                    let id_field_crdt = CType::LwwRegister;

                    // FK field gets a FieldID only if primary.
                    // Go's Delta() skips secondary fields: RelationName.HasValue() && !IsPrimary.
                    // This applies to both self-ref and cross-type secondary FK fields.
                    let id_field_id = if is_primary {
                        generate_field_id(&id_field_name, &id_field_kind, id_field_crdt)
                    } else {
                        String::new()
                    };
                    // FK field has same is_primary status as relation object field
                    let mut id_field =
                        FieldDescription::new(&id_field_id, &id_field_name.clone(), id_field_kind)
                            .with_crdt_type(id_field_crdt)
                            .with_relation_name(relation_name);
                    if is_primary {
                        id_field = id_field.as_primary();

                        // Only create a unique index for true one-to-one relations.
                        // One-to-one means the counterpart type has a non-array field
                        // pointing back to this type. No back-reference (join tables)
                        // or array back-reference (one-to-many) means no unique index.
                        // See Go's ensureOneToOneUniqueIndex() in collection_define.go
                        let is_one_to_one = self
                            .type_defs
                            .get(&parsed_field.field_type.base_type)
                            .map(|target_def| {
                                target_def.fields.iter().any(|f| {
                                    f.field_type.base_type == type_def.name
                                        && !f.field_type.is_list
                                })
                            })
                            .unwrap_or(false);

                        if is_one_to_one {
                            let idx_name = generate_index_name(
                                &type_def.name,
                                &id_field_name,
                                &existing_index_names,
                            );
                            existing_index_names.push(idx_name.clone());
                            indexes.push(IndexDescription {
                                name: idx_name,
                                id: field_id_counter,
                                fields: vec![IndexedFieldDescription {
                                    name: id_field_name.clone(),
                                    descending: false,
                                }],
                                unique: true,
                            });
                            field_id_counter += 1;
                        }
                    }
                    fields.push(id_field);
                    field_id_counter += 1;
                }
            }

            // Handle field-level @index directive
            if let Some(ref idx_config) = parsed_field.directives.index {
                let idx_name = idx_config.name.clone().unwrap_or_else(|| {
                    generate_index_name(
                        &type_def.name,
                        &parsed_field.name,
                        &existing_index_names,
                    )
                });
                existing_index_names.push(idx_name.clone());

                indexes.push(IndexDescription {
                    name: idx_name,
                    id: field_id_counter,
                    fields: vec![IndexedFieldDescription {
                        name: parsed_field.name.clone(),
                        descending: matches!(idx_config.direction, IndexDirection::Desc),
                    }],
                    unique: idx_config.unique,
                });
            }

            fields.push(field);
            field_id_counter += 1;
        }

        // Handle type-level @index directives (composite indexes)
        // Build a set of valid field names for validation
        let valid_field_names: std::collections::HashSet<_> =
            type_def.fields.iter().map(|f| f.name.as_str()).collect();

        for composite_idx in &type_def.directives.indexes {
            // Validate that all referenced fields exist
            for (field_ref, _) in &composite_idx.fields {
                if !valid_field_names.contains(field_ref.as_str()) {
                    return Err(QueryError::parse(format!(
                        "@index on type {} references unknown field '{}'",
                        type_def.name, field_ref
                    )));
                }
            }

            let idx_name = composite_idx.name.clone().unwrap_or_else(|| {
                let first_field = composite_idx
                    .fields
                    .first()
                    .map(|(n, _)| n.as_str())
                    .unwrap_or("unknown");
                generate_index_name(&type_def.name, first_field, &existing_index_names)
            });
            existing_index_names.push(idx_name.clone());

            let indexed_fields: Vec<IndexedFieldDescription> = composite_idx
                .fields
                .iter()
                .map(|(name, descending)| IndexedFieldDescription {
                    name: name.clone(),
                    descending: *descending,
                })
                .collect();

            indexes.push(IndexDescription {
                name: idx_name,
                id: field_id_counter,
                fields: indexed_fields,
                unique: composite_idx.unique,
            });
        }

        // INTEROP CRITICAL: Sort fields alphabetically after _docID (like Go does).
        //
        // Go's collection.go sorts fields so _docID stays at position 0,
        // and remaining fields are sorted alphabetically by name.
        //
        // Field order affects collection CID generation because:
        // 1. Each field gets a priority based on its position (1, 2, 3, ...)
        // 2. Priority is encoded in the field's CRDT delta payload
        // 3. Different priorities = different field CIDs = different collection CID
        //
        // Without this sort, schemas like "type Users { name: String, age: Int }"
        // would have fields [_docID, name, age] in Rust but [_docID, age, name] in Go,
        // causing CID mismatches and P2P topic subscription failures.
        if fields.len() > 1 {
            fields[1..].sort_by(|a, b| a.name.cmp(&b.name));
        }

        // Generate collection ID from type name and fields (like Go, includes field CIDs as links)
        // The headstore simulates Go's prefix collision behavior for deterministic CIDs
        let collection_id = generate_collection_id(&type_def.name, &fields, headstore);

        // Version ID equals collection ID for new schemas (Go behavior)
        let version_id = collection_id.clone();

        let mut collection =
            CollectionVersion::new(&type_def.name, &version_id, &collection_id, fields);
        collection.indexes = indexes;
        collection.is_materialized = type_def.directives.is_materialized;
        collection.is_branchable = type_def.directives.is_branchable;
        if let Some(ref policy_config) = type_def.directives.policy {
            collection.policy = Some(schema::PolicyDescription::new(
                &policy_config.id,
                &policy_config.resource,
            ));
        }

        Ok(collection)
    }

    fn resolve_field_kind(
        &self,
        parsed_type: &ParsedType,
        type_names: &std::collections::HashSet<String>,
        current_type: &str,
        collection_set: &std::collections::HashMap<String, i32>,
        known_collection_ids: &std::collections::HashMap<String, String>,
    ) -> Result<FieldKind> {
        let base = &parsed_type.base_type;

        // Check if it's a scalar type
        if let Some(scalar_kind) = graphql_to_scalar_kind(base) {
            if parsed_type.is_list {
                let array_kind = if parsed_type.element_non_null {
                    scalar_kind.to_array_kind()
                } else {
                    scalar_kind.to_nillable_array_kind()
                };

                return array_kind.map(FieldKind::ScalarArray).ok_or_else(|| {
                    QueryError::parse(format!("scalar type {} cannot be used in arrays", base))
                });
            }
            return Ok(FieldKind::Scalar(scalar_kind));
        }

        // Check if it's a self-reference (same type or "Self" keyword)
        if base == current_type || base == "Self" {
            // For self-references (type pointing to itself), Go ALWAYS uses empty RelativeID
            // The RelativeID indices are only used for cross-type references within a collection set
            return Ok(FieldKind::self_ref("", parsed_type.is_list));
        }

        // Check if it references another type in the schema
        if type_names.contains(base) {
            // This is a relation to another type
            // If the target type is in the collection set (circular relations),
            // use SelfRef with the target's relative index
            if let Some(&target_idx) = collection_set.get(base) {
                return Ok(FieldKind::self_ref(
                    target_idx.to_string(),
                    parsed_type.is_list,
                ));
            }

            // If target type was already processed (alphabetically earlier),
            // use Relation with the known CollectionID (matches Go behavior)
            if let Some(collection_id) = known_collection_ids.get(base) {
                return Ok(FieldKind::relation(
                    collection_id.clone(),
                    parsed_type.is_list,
                ));
            }

            // For non-circular relations where target not yet processed, use Named
            return Ok(FieldKind::named(base, parsed_type.is_list));
        }

        // Unknown type - error for Go compatibility
        Err(QueryError::parse(format!(
            "no type found for given name. Name: {}",
            base
        )))
    }
}

/// Parse a GraphQL type into our ParsedType representation
fn parse_graphql_type(ty: &Type<'_, String>) -> ParsedType {
    match ty {
        Type::NamedType(name) => ParsedType {
            base_type: name.clone(),
            is_list: false,
            is_non_null: false,
            element_non_null: false,
        },
        Type::NonNullType(inner) => {
            let mut parsed = parse_graphql_type(inner);
            parsed.is_non_null = true;
            parsed
        }
        Type::ListType(inner) => {
            let inner_parsed = parse_graphql_type(inner);
            ParsedType {
                base_type: inner_parsed.base_type,
                is_list: true,
                is_non_null: false,
                element_non_null: inner_parsed.is_non_null,
            }
        }
    }
}

/// Convert GraphQL scalar type name to FieldKind's ScalarKind
fn graphql_to_scalar_kind(name: &str) -> Option<ScalarKind> {
    match name {
        "String" => Some(ScalarKind::String),
        "Int" => Some(ScalarKind::Int),
        "Float" | "Float64" => Some(ScalarKind::Float64),
        "Float32" => Some(ScalarKind::Float32),
        "Boolean" => Some(ScalarKind::Bool),
        "ID" => Some(ScalarKind::DocID),
        "DateTime" => Some(ScalarKind::DateTime),
        "JSON" => Some(ScalarKind::Json),
        "Blob" => Some(ScalarKind::Blob),
        _ => None,
    }
}

/// Convert first 8 bytes of a SHA-256 hash to a hex string
fn hash_to_hex(hash: &[u8]) -> String {
    format!(
        "{:x}",
        hash[..8].iter().fold(0u64, |acc, &b| (acc << 8) | b as u64)
    )
}

/// Generate a deterministic collection ID from the type name and fields.
///
/// Uses the same CID format as Go DefraDB for interoperability.
/// Like Go, includes field definition CIDs as links in the collection block.
///
/// IMPORTANT: Go uses priority=1 for ALL fields and the collection block,
/// not incrementing priorities. This was verified by comparing actual AddSchema
/// output with manual CID generation.
///
/// IMPORTANT: Secondary relation fields (where relation_name is set but is_primary is false)
/// are NOT saved to the blockstore in Go and must be excluded from CID generation.
/// See Go's internal/core/crdt/field_definition.go lines 95-98.
///
/// The `headstore` parameter simulates Go's headstore prefix collision behavior.
/// In Go, collection definitions are stored with prefix `/g/<CollectionName>`.
/// When a new collection's headset does a prefix scan, it inadvertently matches
/// entries from other collections whose names share the same prefix (e.g., "Author"
/// prefix-matches "AuthorContact"). This affects the block's priority and heads fields,
/// changing the resulting CID. Passing an empty map produces standard priority=1 behavior.
fn generate_collection_id(
    type_name: &str,
    fields: &[FieldDescription],
    headstore: &HashMap<String, (Cid, u64)>,
) -> String {
    // Sort fields to match Go's order: _docID first, then alphabetically by name
    // Include fields with non-empty FieldID in the CID.
    // Go's Delta() excludes: secondary relations, self-ref with empty relative_id.
    // All excluded fields have empty FieldIDs, so filtering on !id.is_empty() suffices.
    let mut sorted_fields: Vec<&FieldDescription> = fields
        .iter()
        .filter(|f| !f.id.is_empty())
        .collect();
    sorted_fields.sort_by(|a, b| {
        // _docID always comes first
        if a.name == "_docID" {
            std::cmp::Ordering::Less
        } else if b.name == "_docID" {
            std::cmp::Ordering::Greater
        } else {
            a.name.cmp(&b.name)
        }
    });

    // Generate CIDs for each field definition with priority=1 (like Go does)
    // All fields use the same priority=1, not incrementing priorities
    let field_cids: Vec<Cid> = sorted_fields
        .iter()
        .filter_map(|f| {
            schema::generate_field_cid_with_priority(f, 1).ok()
        })
        .collect();

    // Simulate Go's headstore prefix collision:
    // Scan for any existing headstore entry whose name starts with this type's name
    // (Go uses prefix `/g/<name>` which matches `/g/<name>*`)
    let prefix = format!("/g/{}", type_name);
    let mut max_height: u64 = 0;
    let mut head_cids: Vec<Cid> = Vec::new();
    for (key, (cid, height)) in headstore {
        let entry_prefix = format!("/g/{}", key);
        if entry_prefix.starts_with(&prefix) {
            head_cids.push(*cid);
            if *height > max_height {
                max_height = *height;
            }
        }
    }
    head_cids.sort_by_key(|c| c.to_string());

    let priority = max_height + 1;

    match schema::generate_collection_cid_with_priority_and_heads(
        type_name,
        &field_cids,
        priority,
        &head_cids,
    ) {
        Ok(cid) => cid.to_string(),
        Err(_) => {
            // Fallback to simple hash if CID generation fails
            let mut hasher = Sha256::new();
            hasher.update(b"collection:");
            hasher.update(type_name.as_bytes());
            format!("coll_{}", hash_to_hex(&hasher.finalize()))
        }
    }
}

/// Generate a deterministic field ID from collection name and field name.
///
/// Generate a deterministic field ID using the same CID format as Go DefraDB.
///
/// The field ID is a CID derived from the field's delta payload which includes:
/// - Field name
/// - CRDT type
/// - Scalar kind (for scalar fields) or collection ID (for relation fields)
///
/// Uses the same CID format as Go DefraDB for interoperability.
fn generate_field_id(field_name: &str, kind: &FieldKind, crdt_type: CType) -> String {
    // Build a temporary FieldDescription to get the CID
    let field = FieldDescription::new(
        String::new(), // ID will be generated
        field_name.to_string(),
        kind.clone(),
    )
    .with_crdt_type(crdt_type);

    match schema::generate_field_cid(&field) {
        Ok(cid) => cid.to_string(),
        Err(_) => {
            // Fallback to simple hash if CID generation fails
            let mut hasher = Sha256::new();
            hasher.update(b"field:");
            hasher.update(field_name.as_bytes());
            format!("field_{}", hash_to_hex(&hasher.finalize()))
        }
    }
}

/// Generate a deterministic version ID from collection name and fields.
///
/// Uses the same CID format as Go DefraDB for interoperability.
/// In Go, for a new schema the VersionID equals the CollectionID.
fn generate_version_id(name: &str, fields: &[FieldDescription]) -> String {
    // Version ID uses the same logic as collection ID (Go behavior for new schemas)
    generate_collection_id(name, fields, &HashMap::new())
}

/// Generate a relation name following Go DefraDB conventions.
/// Go uses lexicographic sort of type names to create deterministic relation names.
fn generate_relation_name(from_type: &str, _field_name: &str, to_type: &str) -> String {
    // Go's genRelationName simply concatenates the two type names in alphabetical order
    // Format: {type1}_{type2} where type1 < type2 lexicographically
    let t1 = from_type.to_lowercase();
    let t2 = to_type.to_lowercase();
    if t1 < t2 {
        format!("{}_{}", t1, t2)
    } else {
        format!("{}_{}", t2, t1)
    }
}

/// Parse SDL string into CollectionVersion schemas.
///
/// This convenience function discards all warnings. For production use where
/// you need visibility into unknown directives, invalid argument types, or
/// unimplemented features, use [`parse_sdl_with_warnings`] instead.
pub fn parse_sdl(sdl: &str) -> Result<Vec<CollectionVersion>> {
    let mut parser = SdlParser::new(sdl);
    parser.parse()
}

/// Parse SDL with knowledge of existing collection type names.
///
/// External types referenced in the SDL (e.g. `books: [Book]` where `Book` is
/// an existing collection) will be resolved as named relations instead of
/// producing "no type found" errors.
pub fn parse_sdl_with_known_types(
    sdl: &str,
    known_types: std::collections::HashSet<String>,
) -> Result<Vec<CollectionVersion>> {
    let mut parser = SdlParser::new(sdl).with_known_types(known_types);
    parser.parse()
}

/// Parse SDL string into CollectionVersion schemas with warnings.
///
/// Returns both the parsed collections and any warnings encountered during parsing.
/// Warnings are emitted for:
/// - Unknown directives (forward compatibility - ignored but noted)
/// - Unknown arguments on known directives (possible typos)
/// - Invalid argument types (e.g., string where bool expected - default used)
/// - Unimplemented directives (@embedding, @encryptedIndex, field @policy)
///
/// This is the recommended entry point for production use.
pub fn parse_sdl_with_warnings(sdl: &str) -> Result<ParseOutput> {
    let mut parser = SdlParser::new(sdl);
    parser.parse_with_warnings()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_type() {
        let sdl = r#"
            type User {
                name: String
                age: Int
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        assert_eq!(collections.len(), 1);

        let user = &collections[0];
        assert_eq!(user.name, "User");
        // _docID + name + age = 3 fields
        assert_eq!(user.fields.len(), 3);

        let name_field = user.field_by_name("name").unwrap();
        assert_eq!(name_field.kind, FieldKind::string());

        let age_field = user.field_by_name("age").unwrap();
        assert_eq!(age_field.kind, FieldKind::int());
    }

    #[test]
    fn test_parse_non_null_type() {
        let sdl = r#"
            type Post {
                title: String!
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let post = &collections[0];

        let title_field = post.field_by_name("title").unwrap();
        // In DefraDB, all fields are nillable. Non-null is only used for array elements.
        assert_eq!(title_field.kind, FieldKind::string());
    }

    #[test]
    fn test_parse_array_type() {
        let sdl = r#"
            type User {
                tags: [String!]
                scores: [Int]
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let user = &collections[0];

        // [String!] -> non-nillable elements
        let tags = user.field_by_name("tags").unwrap();
        assert_eq!(tags.kind, FieldKind::string_array());

        // [Int] -> nillable elements
        let scores = user.field_by_name("scores").unwrap();
        assert_eq!(scores.kind, FieldKind::nillable_int_array());
    }

    #[test]
    fn test_parse_crdt_directive() {
        let sdl = r#"
            type Counter {
                value: Int @crdt(type: "pncounter")
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let counter = &collections[0];

        let value = counter.field_by_name("value").unwrap();
        assert_eq!(value.crdt_type, CType::PnCounter);
    }

    #[test]
    fn test_parse_primary_directive() {
        let sdl = r#"
            type Post {
                author: User @primary
            }
            type User {
                name: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let post = collections.iter().find(|c| c.name == "Post").unwrap();

        let author = post.field_by_name("author").unwrap();
        assert!(author.is_primary);
    }

    #[test]
    fn test_parse_relation() {
        let sdl = r#"
            type User {
                name: String
                posts: [Post]
            }
            type Post {
                title: String
                author: User
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        assert_eq!(collections.len(), 2);

        let user = collections.iter().find(|c| c.name == "User").unwrap();
        let posts_field = user.field_by_name("posts").unwrap();
        assert!(posts_field.kind.is_relation());
        assert!(posts_field.kind.is_array());

        let post = collections.iter().find(|c| c.name == "Post").unwrap();
        let author_field = post.field_by_name("author").unwrap();
        assert!(author_field.kind.is_relation());
        assert!(!author_field.kind.is_array());
    }

    #[test]
    fn test_parse_self_reference() {
        let sdl = r#"
            type Category {
                name: String
                parent: Category
                children: [Category]
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let category = &collections[0];

        let parent = category.field_by_name("parent").unwrap();
        assert!(matches!(
            parent.kind,
            FieldKind::SelfRef {
                is_array: false,
                ..
            }
        ));

        let children = category.field_by_name("children").unwrap();
        assert!(matches!(
            children.kind,
            FieldKind::SelfRef { is_array: true, .. }
        ));
    }

    #[test]
    fn test_parse_index_directive() {
        let sdl = r#"
            type User {
                email: String @index(unique: true)
                name: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let user = &collections[0];

        assert_eq!(user.indexes.len(), 1);
        let idx = &user.indexes[0];
        assert!(idx.unique);
        assert_eq!(idx.fields.len(), 1);
        assert_eq!(idx.fields[0].name, "email");
    }

    #[test]
    fn test_parse_all_scalar_types() {
        let sdl = r#"
            type AllTypes {
                s: String
                i: Int
                f: Float
                f32: Float32
                f64: Float64
                b: Boolean
                id: ID
                dt: DateTime
                j: JSON
                blob: Blob
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let all = &collections[0];

        assert_eq!(all.field_by_name("s").unwrap().kind, FieldKind::string());
        assert_eq!(all.field_by_name("i").unwrap().kind, FieldKind::int());
        assert_eq!(all.field_by_name("f").unwrap().kind, FieldKind::float64());
        assert_eq!(all.field_by_name("f32").unwrap().kind, FieldKind::float32());
        assert_eq!(all.field_by_name("f64").unwrap().kind, FieldKind::float64());
        assert_eq!(all.field_by_name("b").unwrap().kind, FieldKind::bool());
        assert_eq!(all.field_by_name("id").unwrap().kind, FieldKind::doc_id());
        assert_eq!(all.field_by_name("dt").unwrap().kind, FieldKind::datetime());
        assert_eq!(all.field_by_name("j").unwrap().kind, FieldKind::json());
        assert_eq!(all.field_by_name("blob").unwrap().kind, FieldKind::blob());
    }

    // NOTE: test_parse_issue_example removed - the @primary directive behavior
    // is validated through Go interop tests which are the source of truth for
    // behavioral compatibility.

    #[test]
    fn test_parse_empty_sdl() {
        let sdl = "";
        let collections = parse_sdl(sdl).unwrap();
        assert!(collections.is_empty());
    }

    #[test]
    fn test_parse_invalid_sdl() {
        let sdl = "not valid graphql { {";
        let result = parse_sdl(sdl);
        assert!(result.is_err());
    }

    #[test]
    fn test_doc_id_always_present() {
        let sdl = r#"
            type Simple {
                name: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let simple = &collections[0];

        let doc_id = simple.field_by_name("_docID");
        assert!(doc_id.is_some());
        assert_eq!(doc_id.unwrap().kind, FieldKind::doc_id());
    }

    #[test]
    fn test_collection_and_field_ids_generated() {
        let sdl = r#"
            type User {
                name: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let user = &collections[0];

        // collection_id and version_id should be non-empty
        assert!(!user.collection_id.is_empty());
        assert!(!user.version_id.is_empty());

        // field IDs should be non-empty
        for field in &user.fields {
            assert!(!field.id.is_empty());
        }
    }

    #[test]
    fn test_relation_names_generated() {
        let sdl = r#"
            type Author {
                books: [Book]
            }
            type Book {
                author: Author
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();

        for coll in &collections {
            for field in coll.relation_fields() {
                // Relation fields should have relation names
                assert!(field.relation_name.is_some());
            }
        }
    }

    // =========================================================================
    // Go Compatibility Tests
    // =========================================================================

    #[test]
    fn test_relation_directive_explicit_name() {
        let sdl = r#"
            type User {
                posts: [Post] @relation(name: "user_authored_posts")
            }
            type Post {
                author: User @relation(name: "user_authored_posts") @primary
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();

        let user = collections.iter().find(|c| c.name == "User").unwrap();
        let posts = user.field_by_name("posts").unwrap();
        assert_eq!(
            posts.relation_name.as_deref(),
            Some("user_authored_posts"),
            "explicit @relation name should be used"
        );

        let post = collections.iter().find(|c| c.name == "Post").unwrap();
        let author = post.field_by_name("author").unwrap();
        assert_eq!(
            author.relation_name.as_deref(),
            Some("user_authored_posts"),
            "explicit @relation name should be used"
        );
        assert!(author.is_primary, "@primary should mark the primary side");
    }

    // NOTE: test_relation_name_auto_generation removed - relation naming conventions
    // are validated through Go interop tests which are the source of truth for
    // behavioral compatibility.

    #[test]
    fn test_default_directive_string() {
        let sdl = r#"
            type User {
                role: String @default(string: "member")
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let user = &collections[0];

        let role = user.field_by_name("role").unwrap();
        assert_eq!(
            role.default_value,
            Some(serde_json::Value::String("member".to_string()))
        );
    }

    #[test]
    fn test_default_directive_int() {
        let sdl = r#"
            type Counter {
                count: Int @default(int: 0)
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let counter = &collections[0];

        let count = counter.field_by_name("count").unwrap();
        assert_eq!(
            count.default_value,
            Some(serde_json::Value::Number(0.into()))
        );
    }

    #[test]
    fn test_default_directive_bool() {
        let sdl = r#"
            type Settings {
                enabled: Boolean @default(bool: true)
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let settings = &collections[0];

        let enabled = settings.field_by_name("enabled").unwrap();
        assert_eq!(enabled.default_value, Some(serde_json::Value::Bool(true)));
    }

    #[test]
    fn test_constraints_directive_array_size() {
        let sdl = r#"
            type Article {
                tags: [String!] @constraints(size: 10)
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let article = &collections[0];

        let tags = article.field_by_name("tags").unwrap();
        assert_eq!(tags.size, 10, "@constraints(size:) should set field.size");
    }

    #[test]
    fn test_materialized_directive() {
        let sdl = r#"
            type CachedView @materialized {
                data: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let view = &collections[0];

        assert!(
            view.is_materialized,
            "@materialized should set is_materialized = true"
        );
    }

    #[test]
    fn test_branchable_directive() {
        let sdl = r#"
            type VersionedDoc @branchable {
                content: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let doc = &collections[0];

        assert!(
            doc.is_branchable,
            "@branchable should set is_branchable = true"
        );
    }

    #[test]
    fn test_materialized_directive_with_if_false() {
        let sdl = r#"
            type NotCached @materialized(if: false) {
                data: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let view = &collections[0];

        assert!(
            !view.is_materialized,
            "@materialized(if: false) should set is_materialized = false"
        );
    }

    #[test]
    fn test_float32_scalar_type() {
        let sdl = r#"
            type Sensor {
                temperature: Float32
                values: [Float32!]
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let sensor = &collections[0];

        let temp = sensor.field_by_name("temperature").unwrap();
        assert_eq!(temp.kind, FieldKind::float32());

        let values = sensor.field_by_name("values").unwrap();
        assert_eq!(values.kind, FieldKind::float32_array());
    }

    #[test]
    fn test_multiple_field_indexes() {
        let sdl = r#"
            type User {
                email: String @index(unique: true)
                username: String @index(unique: true)
                age: Int @index
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let user = &collections[0];

        assert_eq!(user.indexes.len(), 3, "should have 3 separate indexes");

        let email_idx = user.indexes.iter().find(|i| i.fields[0].name == "email");
        assert!(email_idx.is_some());
        assert!(email_idx.unwrap().unique);

        let age_idx = user.indexes.iter().find(|i| i.fields[0].name == "age");
        assert!(age_idx.is_some());
        assert!(!age_idx.unwrap().unique);
    }

    #[test]
    fn test_type_level_composite_index() {
        let sdl = r#"
            type User @index(fields: ["firstName", "lastName"], name: "full_name_idx") {
                firstName: String
                lastName: String
                email: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let user = &collections[0];

        assert_eq!(user.indexes.len(), 1);
        let idx = &user.indexes[0];
        assert_eq!(idx.name, "full_name_idx");
        assert_eq!(idx.fields.len(), 2);
        assert_eq!(idx.fields[0].name, "firstName");
        assert_eq!(idx.fields[1].name, "lastName");
    }

    #[test]
    fn test_crdt_directive_variations() {
        let sdl = r#"
            type Counters {
                lww: Int @crdt(type: "lww")
                lwwRegister: Int @crdt(type: "LWW_REGISTER")
                pn: Int @crdt(type: "pncounter")
                pnCounter: Int @crdt(type: "PN_COUNTER")
                p: Int @crdt(type: "pcounter")
                pCounter: Int @crdt(type: "P_COUNTER")
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let counters = &collections[0];

        assert_eq!(
            counters.field_by_name("lww").unwrap().crdt_type,
            CType::LwwRegister
        );
        assert_eq!(
            counters.field_by_name("lwwRegister").unwrap().crdt_type,
            CType::LwwRegister
        );
        assert_eq!(
            counters.field_by_name("pn").unwrap().crdt_type,
            CType::PnCounter
        );
        assert_eq!(
            counters.field_by_name("pnCounter").unwrap().crdt_type,
            CType::PnCounter
        );
        assert_eq!(
            counters.field_by_name("p").unwrap().crdt_type,
            CType::PCounter
        );
        assert_eq!(
            counters.field_by_name("pCounter").unwrap().crdt_type,
            CType::PCounter
        );
    }

    #[test]
    fn test_crdt_validation_fails_for_non_numeric() {
        let sdl = r#"
            type Article {
                title: String @crdt(type: "pncounter")
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let article = &collections[0];

        // Parsing succeeds, but validation should fail
        let result = article.validate();
        assert!(
            result.is_err(),
            "PnCounter on String should fail validation"
        );
    }

    #[test]
    fn test_collection_ids_are_deterministic() {
        // With empty fields (matches Go's behavior for field-less collections)
        let id1 = generate_collection_id("User", &[], &HashMap::new());
        let id2 = generate_collection_id("User", &[], &HashMap::new());
        assert_eq!(id1, id2, "same type name should produce same collection ID");

        let id3 = generate_collection_id("Post", &[], &HashMap::new());
        assert_ne!(
            id1, id3,
            "different type names should produce different IDs"
        );
    }

    #[test]
    fn test_field_ids_are_deterministic() {
        let string_kind = FieldKind::Scalar(ScalarKind::String);
        let id1 = generate_field_id("name", &string_kind, CType::LwwRegister);
        let id2 = generate_field_id("name", &string_kind, CType::LwwRegister);
        assert_eq!(id1, id2, "same field should produce same field ID");

        let id3 = generate_field_id("email", &string_kind, CType::LwwRegister);
        assert_ne!(
            id1, id3,
            "different field names should produce different IDs"
        );

        // Different types should produce different IDs
        let int_kind = FieldKind::Scalar(ScalarKind::Int);
        let id4 = generate_field_id("count", &string_kind, CType::LwwRegister);
        let id5 = generate_field_id("count", &int_kind, CType::LwwRegister);
        assert_ne!(
            id4, id5,
            "different field types should produce different IDs"
        );
    }

    #[test]
    fn test_self_ref_collection_id_matches_go() {
        // Go's TestSchemaSelfReferenceSimple expects this CID for `type User { boss: User }`
        let sdl = r#"
            type User {
                boss: User
            }
        "#;
        let collections = parse_sdl(sdl).unwrap();
        assert_eq!(
            collections[0].collection_id,
            "bafyreicuxpdrri4wwdknhbchhdii6tu4myqlhspv3s2c3pci7jt7qc3zua",
        );
    }

    #[test]
    fn test_self_ref_complex_collection_id_matches_go() {
        // Self-ref schema with multiple relation fields and @primary
        let sdl = r#"
            type User {
                name: String
                age: Int
                boss: User @primary @relation(name: "boss_minion")
                minion: User @relation(name: "boss_minion")
            }
        "#;
        let collections = parse_sdl(sdl).unwrap();
        assert_eq!(
            collections[0].collection_id,
            "bafyreibgdepgcg4y4odgoju4ac6bu5u2jejta6jg6pvzxblm5fnovsa3gi",
        );
    }

    #[test]
    fn test_index_descending_direction() {
        let sdl = r#"
            type Event {
                timestamp: DateTime @index(direction: "DESC")
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let event = &collections[0];

        assert_eq!(event.indexes.len(), 1);
        assert!(event.indexes[0].fields[0].descending);
    }

    // =========================================================================
    // Error Path Tests
    // =========================================================================

    #[test]
    fn test_crdt_directive_unknown_type_returns_error() {
        let sdl = r#"
            type Counter {
                value: Int @crdt(type: "invalid_crdt")
            }
        "#;
        let result = parse_sdl(sdl);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown CRDT type"),
            "error should mention unknown CRDT type: {}",
            err
        );
    }

    #[test]
    fn test_crdt_directive_missing_type_argument_returns_error() {
        let sdl = r#"
            type Counter {
                value: Int @crdt
            }
        "#;
        let result = parse_sdl(sdl);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("requires 'type' argument"),
            "error should mention missing type argument: {}",
            err
        );
    }

    #[test]
    fn test_default_directive_missing_value_returns_error() {
        let sdl = r#"
            type User {
                role: String @default
            }
        "#;
        let result = parse_sdl(sdl);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("requires a value argument"),
            "error should mention missing value: {}",
            err
        );
    }

    #[test]
    fn test_default_directive_unknown_argument_returns_error() {
        let sdl = r#"
            type User {
                role: String @default(invalid_arg: "test")
            }
        "#;
        let result = parse_sdl(sdl);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown @default argument"),
            "error should mention unknown argument: {}",
            err
        );
    }

    #[test]
    fn test_default_directive_invalid_json_returns_error() {
        let sdl = r#"
            type Config {
                settings: JSON @default(json: "{ invalid json }")
            }
        "#;
        let result = parse_sdl(sdl);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("invalid JSON"),
            "error should mention invalid JSON: {}",
            err
        );
    }

    #[test]
    fn test_constraints_directive_negative_size_returns_error() {
        let sdl = r#"
            type Article {
                tags: [String!] @constraints(size: -1)
            }
        "#;
        let result = parse_sdl(sdl);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("non-negative"),
            "error should mention non-negative requirement: {}",
            err
        );
    }

    #[test]
    fn test_default_directive_float() {
        let sdl = r#"
            type Measurement {
                value: Float @default(float: 3.14)
            }
        "#;
        let collections = parse_sdl(sdl).unwrap();
        let m = &collections[0];
        let value = m.field_by_name("value").unwrap();
        assert!(value.default_value.is_some());
        if let Some(serde_json::Value::Number(n)) = &value.default_value {
            assert!((n.as_f64().unwrap() - 3.14).abs() < 0.001);
        } else {
            panic!("expected number default value");
        }
    }

    #[test]
    fn test_default_directive_json() {
        let sdl = r#"
            type Config {
                settings: JSON @default(json: "{\"key\": \"value\"}")
            }
        "#;
        let collections = parse_sdl(sdl).unwrap();
        let config = &collections[0];
        let settings = config.field_by_name("settings").unwrap();
        assert!(settings.default_value.is_some());
        if let Some(serde_json::Value::Object(obj)) = &settings.default_value {
            assert_eq!(obj.get("key").unwrap(), "value");
        } else {
            panic!("expected object default value");
        }
    }

    #[test]
    fn test_default_directive_float32() {
        let sdl = r#"
            type Sensor {
                temp: Float32 @default(float32: 25.5)
            }
        "#;
        let collections = parse_sdl(sdl).unwrap();
        let sensor = &collections[0];
        let temp = sensor.field_by_name("temp").unwrap();
        assert!(temp.default_value.is_some());
        if let Some(serde_json::Value::Number(n)) = &temp.default_value {
            assert!((n.as_f64().unwrap() - 25.5).abs() < 0.001);
        } else {
            panic!("expected number default value");
        }
    }

    #[test]
    fn test_default_directive_datetime() {
        let sdl = r#"
            type Event {
                created: DateTime @default(dateTime: "2024-01-15T10:30:00Z")
            }
        "#;
        let collections = parse_sdl(sdl).unwrap();
        let event = &collections[0];
        let created = event.field_by_name("created").unwrap();
        assert_eq!(
            created.default_value,
            Some(serde_json::Value::String(
                "2024-01-15T10:30:00Z".to_string()
            ))
        );
    }

    #[test]
    fn test_default_directive_blob() {
        let sdl = r#"
            type Document {
                data: Blob @default(blob: "SGVsbG8gV29ybGQ=")
            }
        "#;
        let collections = parse_sdl(sdl).unwrap();
        let doc = &collections[0];
        let data = doc.field_by_name("data").unwrap();
        assert_eq!(
            data.default_value,
            Some(serde_json::Value::String("SGVsbG8gV29ybGQ=".to_string()))
        );
    }

    #[test]
    fn test_whitespace_only_sdl() {
        let sdl = "   \n\t\n   ";
        let collections = parse_sdl(sdl).unwrap();
        assert!(collections.is_empty());
    }

    #[test]
    fn test_branchable_directive_with_if_false() {
        let sdl = r#"
            type Doc @branchable(if: false) {
                content: String
            }
        "#;
        let collections = parse_sdl(sdl).unwrap();
        let doc = &collections[0];
        assert!(!doc.is_branchable);
    }

    // =========================================================================
    // Issue #28 & #29: Warnings and Validation Tests
    // =========================================================================

    #[test]
    fn test_unknown_field_directive_emits_warning() {
        let sdl = r#"
            type User {
                name: String @unknownDirective
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        assert_eq!(output.warnings.len(), 1);

        match &output.warnings[0] {
            ParseWarning::UnknownDirective {
                directive_name,
                location,
                type_name,
                field_name,
            } => {
                assert_eq!(directive_name, "unknownDirective");
                assert_eq!(*location, DirectiveLocation::Field);
                assert_eq!(type_name, "User");
                assert_eq!(field_name.as_deref(), Some("name"));
            }
            other => panic!("expected UnknownDirective warning, got {:?}", other),
        }
    }

    #[test]
    fn test_unknown_type_directive_emits_warning() {
        let sdl = r#"
            type User @futureFeature {
                name: String
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        assert_eq!(output.warnings.len(), 1);

        match &output.warnings[0] {
            ParseWarning::UnknownDirective {
                directive_name,
                location,
                type_name,
                field_name,
            } => {
                assert_eq!(directive_name, "futureFeature");
                assert_eq!(*location, DirectiveLocation::Type);
                assert_eq!(type_name, "User");
                assert!(field_name.is_none());
            }
            other => panic!("expected UnknownDirective warning, got {:?}", other),
        }
    }

    #[test]
    fn test_unknown_directive_argument_emits_warning() {
        let sdl = r#"
            type User {
                email: String @index(unique: true, unknownArg: "value")
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        assert_eq!(output.warnings.len(), 1);

        match &output.warnings[0] {
            ParseWarning::UnknownDirectiveArgument {
                directive_name,
                argument_name,
                type_name,
                field_name,
            } => {
                assert_eq!(directive_name, "index");
                assert_eq!(argument_name, "unknownArg");
                assert_eq!(type_name, "User");
                assert_eq!(field_name.as_deref(), Some("email"));
            }
            other => panic!("expected UnknownDirectiveArgument warning, got {:?}", other),
        }
    }

    #[test]
    fn test_multiple_unknown_directives_emit_multiple_warnings() {
        let sdl = r#"
            type User @futureTypeDirective @anotherUnknown {
                name: String @customDirective
                age: Int @anotherCustom
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        // 2 unknown type directives + 2 unknown field directives
        assert_eq!(output.warnings.len(), 4);
    }

    #[test]
    fn test_known_directives_no_warnings() {
        let sdl = r#"
            type User @materialized @branchable {
                name: String @index(unique: true)
                age: Int @crdt(type: "pncounter")
                role: String @default(string: "user")
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        assert!(
            output.warnings.is_empty(),
            "known directives should not emit warnings: {:?}",
            output.warnings
        );
    }

    #[test]
    fn test_policy_directive_requires_id() {
        let sdl = r#"
            type User @policy(resource: "users") {
                name: String
            }
        "#;

        let result = parse_sdl(sdl);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("policyID must not be empty"),
            "error should mention missing id argument: {}",
            err
        );
    }

    #[test]
    fn test_policy_directive_requires_resource() {
        let sdl = r#"
            type User @policy(id: "policy123") {
                name: String
            }
        "#;

        let result = parse_sdl(sdl);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("resource name must not be empty"),
            "error should mention missing resource argument: {}",
            err
        );
    }

    #[test]
    fn test_policy_directive_valid() {
        let sdl = r#"
            type User @policy(id: "policy123", resource: "users") {
                name: String
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        assert!(output.warnings.is_empty());
    }

    #[test]
    fn test_composite_index_unknown_field_returns_error() {
        let sdl = r#"
            type User @index(fields: ["name", "nonexistent"]) {
                name: String
                age: Int
            }
        "#;

        let result = parse_sdl(sdl);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown field 'nonexistent'"),
            "error should mention unknown field: {}",
            err
        );
    }

    #[test]
    fn test_composite_index_valid_fields() {
        let sdl = r#"
            type User @index(fields: ["name", "age"]) {
                name: String
                age: Int
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        assert!(output.warnings.is_empty());

        let user = &output.collections[0];
        assert_eq!(user.indexes.len(), 1);
        assert_eq!(user.indexes[0].fields.len(), 2);
    }

    #[test]
    fn test_warning_display_format() {
        let warning = ParseWarning::UnknownDirective {
            directive_name: "custom".to_string(),
            location: DirectiveLocation::Field,
            type_name: "User".to_string(),
            field_name: Some("name".to_string()),
        };

        let display = warning.to_string();
        assert!(display.contains("@custom"));
        assert!(display.contains("User.name"));
        assert!(display.contains("forward compatibility"));
    }

    #[test]
    fn test_warning_display_format_type_level() {
        let warning = ParseWarning::UnknownDirective {
            directive_name: "future".to_string(),
            location: DirectiveLocation::Type,
            type_name: "User".to_string(),
            field_name: None,
        };

        let display = warning.to_string();
        assert!(display.contains("@future"));
        assert!(display.contains("type User"));
    }

    #[test]
    fn test_unknown_argument_warning_display() {
        let warning = ParseWarning::UnknownDirectiveArgument {
            directive_name: "index".to_string(),
            argument_name: "badArg".to_string(),
            type_name: "User".to_string(),
            field_name: Some("email".to_string()),
        };

        let display = warning.to_string();
        assert!(display.contains("badArg"));
        assert!(display.contains("@index"));
        assert!(display.contains("User.email"));
    }

    // =========================================================================
    // Additional Test Coverage (PR Review Gaps)
    // =========================================================================

    #[test]
    fn test_unknown_argument_on_type_directive_emits_warning() {
        let sdl = r#"
            type User @materialized(unknownArg: true) {
                name: String
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        assert_eq!(output.warnings.len(), 1);

        match &output.warnings[0] {
            ParseWarning::UnknownDirectiveArgument {
                directive_name,
                argument_name,
                field_name,
                ..
            } => {
                assert_eq!(directive_name, "materialized");
                assert_eq!(argument_name, "unknownArg");
                assert!(field_name.is_none()); // type-level, not field-level
            }
            other => panic!("expected UnknownDirectiveArgument warning, got {:?}", other),
        }
    }

    #[test]
    fn test_unknown_argument_on_branchable_directive() {
        let sdl = r#"
            type User @branchable(if: true, extraArg: "value") {
                name: String
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        assert_eq!(output.warnings.len(), 1);

        match &output.warnings[0] {
            ParseWarning::UnknownDirectiveArgument {
                directive_name,
                argument_name,
                ..
            } => {
                assert_eq!(directive_name, "branchable");
                assert_eq!(argument_name, "extraArg");
            }
            other => panic!("expected UnknownDirectiveArgument warning, got {:?}", other),
        }
    }

    #[test]
    fn test_policy_directive_unknown_argument_emits_warning() {
        let sdl = r#"
            type User @policy(id: "p1", resource: "users", unknownArg: "value") {
                name: String
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        assert_eq!(output.warnings.len(), 1);

        match &output.warnings[0] {
            ParseWarning::UnknownDirectiveArgument {
                directive_name,
                argument_name,
                ..
            } => {
                assert_eq!(directive_name, "policy");
                assert_eq!(argument_name, "unknownArg");
            }
            other => panic!("expected UnknownDirectiveArgument warning, got {:?}", other),
        }
    }

    #[test]
    fn test_embedding_directive_emits_unimplemented_warning() {
        let sdl = r#"
            type Document {
                content: String @embedding(provider: "openai", model: "ada")
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        // @embedding is recognized but not implemented, should emit UnimplementedDirective
        assert_eq!(output.warnings.len(), 1);
        match &output.warnings[0] {
            ParseWarning::UnimplementedDirective {
                directive_name,
                type_name,
                field_name,
            } => {
                assert_eq!(directive_name, "embedding");
                assert_eq!(type_name, "Document");
                assert_eq!(field_name.as_deref(), Some("content"));
            }
            other => panic!("expected UnimplementedDirective, got {:?}", other),
        }
    }

    #[test]
    fn test_embedding_directive_unknown_argument_emits_warning() {
        let sdl = r#"
            type Document {
                content: String @embedding(provider: "openai", unknownArg: "x")
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        // Should have both UnknownDirectiveArgument and UnimplementedDirective
        assert_eq!(output.warnings.len(), 2);

        // Find the UnknownDirectiveArgument warning
        let unknown_arg = output
            .warnings
            .iter()
            .find(|w| matches!(w, ParseWarning::UnknownDirectiveArgument { .. }))
            .expect("should have UnknownDirectiveArgument warning");

        match unknown_arg {
            ParseWarning::UnknownDirectiveArgument {
                directive_name,
                argument_name,
                ..
            } => {
                assert_eq!(directive_name, "embedding");
                assert_eq!(argument_name, "unknownArg");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_encrypted_index_directive_emits_unimplemented_warning() {
        let sdl = r#"
            type Secret {
                data: String @encryptedIndex(type: "match")
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        // @encryptedIndex is recognized but not implemented
        assert_eq!(output.warnings.len(), 1);
        match &output.warnings[0] {
            ParseWarning::UnimplementedDirective {
                directive_name,
                type_name,
                field_name,
            } => {
                assert_eq!(directive_name, "encryptedIndex");
                assert_eq!(type_name, "Secret");
                assert_eq!(field_name.as_deref(), Some("data"));
            }
            other => panic!("expected UnimplementedDirective, got {:?}", other),
        }
    }

    #[test]
    fn test_encrypted_index_unknown_argument_emits_warning() {
        let sdl = r#"
            type Secret {
                data: String @encryptedIndex(type: "match", badArg: true)
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        // Should have both UnknownDirectiveArgument and UnimplementedDirective
        assert_eq!(output.warnings.len(), 2);

        let unknown_arg = output
            .warnings
            .iter()
            .find(|w| matches!(w, ParseWarning::UnknownDirectiveArgument { .. }))
            .expect("should have UnknownDirectiveArgument warning");

        match unknown_arg {
            ParseWarning::UnknownDirectiveArgument {
                directive_name,
                argument_name,
                ..
            } => {
                assert_eq!(directive_name, "encryptedIndex");
                assert_eq!(argument_name, "badArg");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_default_float32_wrong_type_returns_error() {
        let sdl = r#"
            type Sensor {
                temp: Float32 @default(float32: "not a float")
            }
        "#;

        let result = parse_sdl(sdl);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("must be a float"),
            "error should mention type mismatch: {}",
            err
        );
    }

    #[test]
    fn test_default_datetime_wrong_type_returns_error() {
        let sdl = r#"
            type Event {
                created: DateTime @default(dateTime: 12345)
            }
        "#;

        let result = parse_sdl(sdl);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("must be a string"),
            "error should mention type mismatch: {}",
            err
        );
    }

    #[test]
    fn test_default_blob_wrong_type_returns_error() {
        let sdl = r#"
            type Document {
                data: Blob @default(blob: 12345)
            }
        "#;

        let result = parse_sdl(sdl);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("must be a string"),
            "error should mention type mismatch: {}",
            err
        );
    }

    #[test]
    fn test_default_directive_value_alias() {
        let sdl = r#"
            type User {
                role: String @default(value: "member")
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let user = &collections[0];
        let role = user.field_by_name("role").unwrap();
        assert_eq!(
            role.default_value,
            Some(serde_json::Value::String("member".to_string()))
        );
    }

    #[test]
    fn test_type_level_index_unknown_argument_emits_warning() {
        let sdl = r#"
            type User @index(fields: ["name"], unknownArg: "value") {
                name: String
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        assert_eq!(output.warnings.len(), 1);

        match &output.warnings[0] {
            ParseWarning::UnknownDirectiveArgument {
                directive_name,
                argument_name,
                field_name,
                ..
            } => {
                assert_eq!(directive_name, "index");
                assert_eq!(argument_name, "unknownArg");
                assert!(field_name.is_none()); // type-level
            }
            other => panic!("expected UnknownDirectiveArgument, got {:?}", other),
        }
    }

    // =========================================================================
    // InvalidArgumentType Warning Tests
    // =========================================================================

    #[test]
    fn test_invalid_bool_argument_type_emits_warning() {
        let sdl = r#"
            type User @materialized(if: "yes") {
                name: String
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        assert_eq!(output.warnings.len(), 1);

        match &output.warnings[0] {
            ParseWarning::InvalidArgumentType {
                directive_name,
                argument_name,
                expected_type,
                ..
            } => {
                assert_eq!(directive_name, "materialized");
                assert_eq!(argument_name, "if");
                assert_eq!(expected_type, "boolean");
            }
            other => panic!("expected InvalidArgumentType, got {:?}", other),
        }

        // Should still work with default value (true)
        assert!(output.collections[0].is_materialized);
    }

    #[test]
    fn test_invalid_int_argument_type_emits_warning() {
        let sdl = r#"
            type User {
                name: String @constraints(size: "ten")
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        assert_eq!(output.warnings.len(), 1);

        match &output.warnings[0] {
            ParseWarning::InvalidArgumentType {
                directive_name,
                argument_name,
                expected_type,
                ..
            } => {
                assert_eq!(directive_name, "constraints");
                assert_eq!(argument_name, "size");
                assert_eq!(expected_type, "integer");
            }
            other => panic!("expected InvalidArgumentType, got {:?}", other),
        }
    }

    #[test]
    fn test_invalid_argument_type_warning_display() {
        let warning = ParseWarning::InvalidArgumentType {
            directive_name: "index".to_string(),
            argument_name: "unique".to_string(),
            expected_type: "boolean".to_string(),
            type_name: "User".to_string(),
            field_name: Some("email".to_string()),
        };

        let display = warning.to_string();
        assert!(display.contains("unique"));
        assert!(display.contains("@index"));
        assert!(display.contains("User.email"));
        assert!(display.contains("boolean"));
    }

    #[test]
    fn test_unimplemented_directive_warning_display() {
        let warning = ParseWarning::UnimplementedDirective {
            directive_name: "embedding".to_string(),
            type_name: "Document".to_string(),
            field_name: Some("content".to_string()),
        };

        let display = warning.to_string();
        assert!(display.contains("@embedding"));
        assert!(display.contains("Document.content"));
        assert!(display.contains("not yet implemented"));
    }

    #[test]
    fn test_field_policy_directive_emits_unimplemented_warning() {
        let sdl = r#"
            type User {
                name: String @policy(id: "p1", resource: "r1")
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        assert_eq!(output.warnings.len(), 1);

        match &output.warnings[0] {
            ParseWarning::UnimplementedDirective {
                directive_name,
                type_name,
                field_name,
            } => {
                assert_eq!(directive_name, "policy");
                assert_eq!(type_name, "User");
                assert_eq!(field_name.as_deref(), Some("name"));
            }
            other => panic!("expected UnimplementedDirective, got {:?}", other),
        }
    }

    #[test]
    fn test_index_with_includes_argument_no_warning() {
        let sdl = r#"
            type User @index(fields: ["name"], includes: ["email"]) {
                name: String
                email: String
            }
        "#;

        let output = parse_sdl_with_warnings(sdl).unwrap();
        assert_eq!(output.collections.len(), 1);
        // includes is a known argument, should not trigger warning
        assert!(
            output.warnings.is_empty(),
            "includes is a known argument but got warnings: {:?}",
            output.warnings
        );
    }

    // =========================================================================
    // Go Interoperability - Field Ordering Tests
    // =========================================================================

    #[test]
    fn test_fields_sorted_alphabetically_after_docid() {
        // Go sorts fields alphabetically after _docID
        // For "name, age" input order, Go outputs [_docID, age, name]
        let sdl = r#"
            type Users {
                name: String
                age: Int
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let users = &collections[0];

        // Verify field order: _docID first, then alphabetical
        let field_names: Vec<&str> = users.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(
            field_names,
            vec!["_docID", "age", "name"],
            "Fields should be sorted alphabetically after _docID"
        );
    }

    #[test]
    fn test_collection_cid_matches_go_with_sorted_fields() {
        // This CID was generated by Go DefraDB for:
        // type Users { name: String, age: Int }
        // With fields sorted as [_docID, age, name]
        //
        // Go debug output (from running TestDebugUsersCIDGeneration):
        // Collection 'Users' (p=4, 3 field links): bafyreihsneodeja4lfer5puptim3lkwvketyckrmkhfpgxm67ch5wenjwq
        //
        // Note: This CID comes from Go's actual AddSchema behavior, not the debug test
        // which manually specifies field order. The actual AddSchema sorts fields.
        const GO_EXPECTED_CID: &str = "bafyreihsneodeja4lfer5puptim3lkwvketyckrmkhfpgxm67ch5wenjwq";

        let sdl = r#"
            type Users {
                name: String
                age: Int
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let users = &collections[0];

        // Print debug info for diagnosing CID mismatches
        println!(
            "Field order: {:?}",
            users.fields.iter().map(|f| &f.name).collect::<Vec<_>>()
        );
        println!("Collection CID: {}", users.collection_id);
        println!("Expected (Go): {}", GO_EXPECTED_CID);

        assert_eq!(
            users.collection_id, GO_EXPECTED_CID,
            "Collection CID should match Go DefraDB"
        );
    }

    #[test]
    fn test_secondary_relation_is_primary_false() {
        // This tests the exact schema from the failing FFI test
        // TestQueryOneToOne_WithRelationIDFromSecondarySide
        let sdl = r#"
            type Book {
                name: String
                author: Author
            }
            type Author {
                name: String
                published: Book @primary
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();

        let book = collections.iter().find(|c| c.name == "Book").unwrap();
        let author_field = book.field_by_name("author").unwrap();

        // Book.author should be SECONDARY (is_primary = false) because
        // Author.published has @primary directive
        assert!(
            !author_field.is_primary,
            "Book.author should be secondary (is_primary=false) because Author.published has @primary"
        );

        // Verify _authorID field exists on Book (created for all single-object relations)
        let author_id_field = book.field_by_name("_authorID");
        assert!(
            author_id_field.is_some(),
            "Book should have implicit _authorID field"
        );

        // _authorID should also be secondary (empty field_id)
        let author_id_field = author_id_field.unwrap();
        assert!(
            author_id_field.id.is_empty(),
            "_authorID should have empty field_id (secondary)"
        );
        assert!(
            !author_id_field.is_primary,
            "_authorID should be secondary (is_primary=false)"
        );

        // Verify Author.published is primary
        let author = collections.iter().find(|c| c.name == "Author").unwrap();
        let published_field = author.field_by_name("published").unwrap();
        assert!(
            published_field.is_primary,
            "Author.published should be primary (has @primary directive)"
        );
    }
}
