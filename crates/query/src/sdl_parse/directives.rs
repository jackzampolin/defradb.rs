//! Directive definitions and argument helpers for SDL parsing
//!
//! Contains known directive definitions and helper functions for extracting
//! arguments from GraphQL directives.

use crate::error::QueryError;
use graphql_parser::schema::Directive;
use schema::CType;

/// Known field-level directives
pub const KNOWN_FIELD_DIRECTIVES: &[&str] = &[
    "primary",
    "crdt",
    "index",
    "relation",
    "default",
    "constraints",
    "embedding",
    "encryptedIndex",
    "policy", // Go allows @policy on fields in some contexts
];

/// Known type-level directives
pub const KNOWN_TYPE_DIRECTIVES: &[&str] = &["index", "materialized", "branchable", "policy"];

/// Known arguments for each directive
pub fn known_directive_arguments(directive_name: &str) -> &'static [&'static str] {
    match directive_name {
        "primary" => &[],
        "crdt" => &["type"],
        "index" => &["name", "unique", "direction", "fields", "includes"],
        "relation" => &["name"],
        "default" => &[
            "string", "value", "bool", "int", "float", "float32", "float64", "dateTime", "json",
            "blob",
        ],
        "constraints" => &["size"],
        "materialized" => &["if"],
        "branchable" => &["if"],
        "policy" => &["id", "resource"],
        "embedding" => &["provider", "model", "url", "fields", "template"],
        "encryptedIndex" => &["type"],
        _ => &[],
    }
}

// =============================================================================
// Directive argument helpers (standalone functions)
// =============================================================================

/// Find a directive argument by name
pub fn get_directive_arg<'a, 'b>(
    directive: &'a Directive<'b, String>,
    arg_name: &str,
) -> Option<&'a graphql_parser::schema::Value<'b, String>> {
    directive
        .arguments
        .iter()
        .find(|(name, _)| name == arg_name)
        .map(|(_, value)| value)
}

/// Get a string argument from a directive
pub fn get_directive_string(directive: &Directive<'_, String>, arg_name: &str) -> Option<String> {
    match get_directive_arg(directive, arg_name)? {
        graphql_parser::schema::Value::String(s) | graphql_parser::schema::Value::Enum(s) => {
            Some(s.clone())
        }
        _ => None,
    }
}

/// Get a string list argument from a directive
pub fn get_directive_string_list(directive: &Directive<'_, String>, arg_name: &str) -> Vec<String> {
    match get_directive_arg(directive, arg_name) {
        Some(graphql_parser::schema::Value::List(items)) => items
            .iter()
            .filter_map(|v| match v {
                graphql_parser::schema::Value::String(s)
                | graphql_parser::schema::Value::Enum(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Create a type mismatch error for @default directive
pub fn default_type_error(
    arg_name: &str,
    expected: &str,
    actual: &graphql_parser::schema::Value<'_, String>,
) -> QueryError {
    QueryError::parse(format!(
        "@default '{}' argument must be a {}, got {:?}",
        arg_name, expected, actual
    ))
}

/// Parsed directive information from a field
#[derive(Debug, Default, Clone)]
pub struct ParsedDirectives {
    /// Whether this field is the primary side of a relation
    pub is_primary: bool,
    /// CRDT type override (default is LwwRegister)
    pub crdt_type: Option<CType>,
    /// Index configuration
    pub index: Option<IndexConfig>,
    /// Explicit relation name from @relation directive
    pub relation_name: Option<String>,
    /// Default value from @default directive
    pub default_value: Option<serde_json::Value>,
    /// The argument name used in @default (e.g., "int", "bool", "string")
    pub default_arg_name: Option<String>,
    /// Array size constraint from @constraints directive
    pub size_constraint: Option<usize>,
}

/// Index configuration from @index directive
#[derive(Debug, Clone)]
pub struct IndexConfig {
    pub name: Option<String>,
    pub unique: bool,
    pub direction: IndexDirection,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum IndexDirection {
    #[default]
    Asc,
    Desc,
}
