//! Directive definitions and argument helpers for SDL parsing
//!
//! Contains known directive definitions and helper functions for extracting
//! arguments from GraphQL directives.

use graphql_parser::schema::Directive;
use query_types::error::QueryError;
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
    "fulltext",
    "vectorIndex",
    "immutable",
    "policy", // Go allows @policy on fields in some contexts
];

/// Known type-level directives
pub const KNOWN_TYPE_DIRECTIVES: &[&str] = &[
    "index",
    "materialized",
    "downsample",
    "branchable",
    "policy",
];

/// Known arguments for each directive
pub fn known_directive_arguments(directive_name: &str) -> &'static [&'static str] {
    match directive_name {
        "primary" => &[],
        "crdt" => &["type"],
        "index" => &["name", "unique", "direction", "fields", "includes"],
        "relation" => &["name"],
        "default" => &["value"],
        "constraints" => &["size"],
        "materialized" => &["if"],
        "downsample" => &["interval", "timeField", "retention"],
        "branchable" => &["if"],
        "policy" => &["id", "resource"],
        "embedding" => &["provider", "model", "url", "fields", "template"],
        "encryptedIndex" => &["type"],
        "fulltext" => &["language", "k1", "b"],
        "vectorIndex" => &["dimensions", "algorithm", "HNSW"],
        "immutable" => &[],
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

/// Get a non-negative integer argument from a directive.
///
/// `None` for an absent argument; `Some(Err)` for one that is present but not a
/// non-negative integer, because a mistyped parameter is a schema error rather
/// than something to silently default.
pub fn get_directive_u32(
    directive: &Directive<'_, String>,
    arg_name: &str,
) -> Option<Result<u32, String>> {
    let value = get_directive_arg(directive, arg_name)?;
    Some(directive_u32(arg_name, value))
}

/// Reads a non-negative integer out of one AST value.
pub fn directive_u32(
    arg_name: &str,
    value: &graphql_parser::schema::Value<'_, String>,
) -> Result<u32, String> {
    match value {
        graphql_parser::schema::Value::Int(number) => number
            .as_i64()
            .filter(|n| *n >= 0 && *n <= i64::from(u32::MAX))
            .map(|n| n as u32)
            .ok_or_else(|| format!("{arg_name} must be a non-negative 32-bit integer")),
        _ => Err(format!("{arg_name} must be an integer")),
    }
}

/// Create a Go-compatible type mismatch error for an @default directive.
pub fn default_type_error(
    field_name: &str,
    expected: &str,
    actual: &graphql_parser::schema::Value<'_, String>,
) -> QueryError {
    let actual_type = match actual {
        graphql_parser::schema::Value::Variable(_) => "Variable",
        graphql_parser::schema::Value::Int(_) => "Int",
        graphql_parser::schema::Value::Float(_) => "Float",
        graphql_parser::schema::Value::String(_) => "String",
        graphql_parser::schema::Value::Boolean(_) => "Boolean",
        graphql_parser::schema::Value::Null => "Null",
        graphql_parser::schema::Value::Enum(_) => "Enum",
        graphql_parser::schema::Value::List(_) => "List",
        graphql_parser::schema::Value::Object(_) => "Object",
    };
    let value_str = match actual {
        graphql_parser::schema::Value::Variable(value) => format!("${value}"),
        graphql_parser::schema::Value::Int(value) => value
            .as_i64()
            .map(|value| value.to_string())
            .unwrap_or_else(|| format!("{value:?}")),
        graphql_parser::schema::Value::Float(value) => value.to_string(),
        graphql_parser::schema::Value::String(value) => {
            serde_json::to_string(value).unwrap_or_else(|_| format!("{value:?}"))
        }
        graphql_parser::schema::Value::Boolean(value) => value.to_string(),
        graphql_parser::schema::Value::Null => "null".to_string(),
        graphql_parser::schema::Value::Enum(s) => s.clone(),
        other => format!("{:?}", other),
    };
    QueryError::parse(format!(
        "default value is invalid. Field: {}, Expected: {}, Actual: {}, Value: {}",
        field_name, expected, actual_type, value_str
    ))
}

/// Parsed embedding configuration from @embedding directive
#[derive(Debug, Clone)]
pub struct EmbeddingConfig {
    pub provider: String,
    pub model: String,
    pub url: String,
    pub fields: Vec<String>,
    pub template: String,
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
    /// Array size constraint from @constraints directive
    pub size_constraint: Option<usize>,
    /// Whether this field has an encrypted index for searchable encryption
    pub encrypted_index: bool,
    /// Embedding configuration from @embedding directive
    pub embedding: Option<EmbeddingConfig>,
    /// Full-text search configuration from @fulltext directive
    pub fulltext: Option<FullTextConfig>,
    /// Vector index configuration from @vectorIndex directive
    pub vector_index: Option<VectorIndexConfig>,
    /// Whether this field is immutable after document creation
    pub immutable: bool,
}

/// Full-text search configuration from @fulltext directive
#[derive(Debug, Clone)]
pub struct FullTextConfig {
    pub language: Option<String>,
    pub k1: Option<f64>,
    pub b: Option<f64>,
}

/// Vector index configuration from the `@vectorIndex` directive.
///
/// The argument names are Go's, because the SDL is what users and parity tests
/// see. `dimensions` sits at the top level rather than inside the algorithm
/// config because it describes the field, not the algorithm, and the algorithm
/// is chosen by *which* config argument is present.
#[derive(Debug, Clone, Default)]
pub struct VectorIndexConfig {
    /// Vector length. `None` when an `@embedding` on the same field fixes it.
    pub dimensions: Option<u32>,
    /// `None` means HNSW, matching the reference where it is the only value.
    pub algorithm: Option<String>,
    /// Present when the `HNSW` argument was given. Its absence still means
    /// HNSW, with defaults.
    pub hnsw: Option<HnswConfig>,
}

/// The `HNSW` argument of `@vectorIndex`. Every member is optional: an omitted
/// one keeps the default from `schema`, so the directive and the defaults
/// cannot drift apart.
#[derive(Debug, Clone, Default)]
pub struct HnswConfig {
    pub metric: Option<String>,
    pub m: Option<u32>,
    pub ef_construction: Option<u32>,
    pub ef_search: Option<u32>,
}

/// Index configuration from @index directive
#[derive(Debug, Clone)]
pub struct IndexConfig {
    pub name: Option<String>,
    pub unique: bool,
    pub direction: IndexDirection,
    /// Additional fields to include in a composite index (field_name, descending)
    pub includes: Vec<(String, bool)>,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum IndexDirection {
    #[default]
    Asc,
    Desc,
}
