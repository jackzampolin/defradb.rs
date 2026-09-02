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
    "fulltext",
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
        "index" => &[
            "name",
            "kind",
            "ordered",
            "vector",
            "unique",
            "direction",
            "fields",
            "includes",
        ],
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
    /// Ordered index configurations, one per `@index` on the field.
    ///
    /// A list because the reference appends every `@index` directive it finds
    /// on a field rather than keeping one, so a field can carry several.
    pub index: Vec<IndexConfig>,
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
    /// Vector index configurations, one per `@index(vector: {...})` on the
    /// field. Several are how a field carries indexes of different metrics,
    /// which is what a query's `metric` argument then chooses between.
    pub vector_index: Vec<VectorIndexConfig>,
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

/// The `vector` argument of `@index`, which is what selects the vector kind.
///
/// The argument names are Go's, because the SDL is what users and parity tests
/// see. `dimensions` sits here rather than inside the algorithm config because
/// it describes the field, not the algorithm. `metric` does not: it lives
/// inside each algorithm's block, where the reference put it, so a client
/// configuring one algorithm sees only that algorithm's knobs.
#[derive(Debug, Clone, Default)]
pub struct VectorIndexConfig {
    /// Index name from `@index(name:)`. Go's `@vectorIndex` could not name an
    /// index at all; folding into `@index` gives it one for free.
    pub name: Option<String>,
    /// Vector length. Required and greater than zero, matching Go after
    /// sourcenetwork/defradb#5188, which dropped inferring it from an
    /// `@embedding`.
    pub dimensions: Option<u32>,
    /// From `alg:`, which selects an algorithm with its default configuration.
    /// `None` means HNSW, or whichever algorithm block was given.
    pub algorithm: Option<String>,
    /// Present when the `flat` argument was given. Flat has no tuning, so the
    /// block exists to select the algorithm and to give its metric a home:
    /// under this grammar the metric lives in the algorithm's block, so an
    /// algorithm with no block could not be built with anything but cosine.
    pub flat: Option<FlatConfig>,
    /// Present when the `ivfpq` argument was given.
    pub ivfpq: Option<IvfPqConfig>,
    /// Present when the `ivfflat` argument was given.
    pub ivfflat: Option<IvfFlatConfig>,
    /// Present when the `ssg` argument was given.
    pub ssg: Option<SsgConfig>,
    /// Present when the `hnsw` argument was given. Its absence still means
    /// HNSW, with defaults.
    pub hnsw: Option<HnswConfig>,
}

/// The `flat` block, which has only a metric.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlatConfig {
    pub metric: Option<String>,
}

/// The `hnsw` block. Every member is optional: an omitted one keeps the default
/// from `schema`, so the directive and the defaults cannot drift apart.
#[derive(Debug, Clone, Default)]
pub struct HnswConfig {
    pub metric: Option<String>,
    pub m: Option<u32>,
    pub ef_construction: Option<u32>,
    pub ef_search: Option<u32>,
}

/// Index configuration from the ordered kind of `@index`.
#[derive(Debug, Clone, Default)]
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

/// The `ivfpq` block. Every member is optional; an omitted one keeps its
/// default, and a zero is derived from the corpus.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IvfPqConfig {
    pub metric: Option<String>,
    pub nlist: Option<u32>,
    pub nprobe: Option<u32>,
    pub m: Option<u32>,
    pub sample_bytes: Option<u32>,
}

/// The `ivfflat` block. Every member is optional; an omitted one keeps its
/// default, and a zero is derived from the corpus. No `m`: a list holds the
/// full vector, so there is nothing to quantize.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IvfFlatConfig {
    pub metric: Option<String>,
    pub nlist: Option<u32>,
    pub nprobe: Option<u32>,
    pub sample_bytes: Option<u32>,
}

/// The `ssg` block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SsgConfig {
    pub metric: Option<String>,
    pub r: Option<u32>,
    pub angle: Option<u32>,
    pub pool: Option<u32>,
}
