//! SDL parsing helper utilities
//!
//! Module-level helper functions used during SDL parsing:
//! - Preprocessing: `preprocess_empty_types()`
//! - Type parsing: `parse_graphql_type()`, `graphql_to_scalar_kind()`
//! - ID generation: `generate_collection_id()`, `generate_field_id()`, `generate_version_id()`
//! - Formatting: `format_graphql_value()`, `graphql_schema_value_to_json()`
//! - Hashing: `hash_to_hex()`

use cid::Cid;

use crate::error::{QueryError, Result};
use graphql_parser::schema::{Directive, Type};
use schema::{CType, FieldDescription, FieldKind, ScalarKind};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use regex::Regex;

use super::directives::get_directive_arg;
use super::parser::{ParsedType, PolicyConfig, EMPTY_TYPE_PLACEHOLDER};

/// Detect fields with missing type declarations before graphql_parser runs.
///
/// Go's custom SDL parser handles `type User { name: }` with a semantic error
/// "field type not specified. Object: User, Field: name". The graphql_parser
/// crate rejects this at the syntax level with a generic parse error. We detect
/// this pattern first and produce Go-compatible error messages.
pub(super) fn detect_missing_field_types(sdl: &str) -> Result<()> {
    // Match type/interface blocks and scan fields within them
    let type_re =
        Regex::new(r"(?:type|interface)\s+(\w+)(?:\s*@\w+(?:\([^)]*\))?)*\s*\{([^}]*)\}").unwrap();
    // Match field declarations where name is followed by colon then whitespace/newline/}
    // with no type before the next field or closing brace
    let field_re = Regex::new(r"(\w+)\s*:\s*(?:\}|\n|$)").unwrap();

    let mut errors = Vec::new();
    for cap in type_re.captures_iter(sdl) {
        let type_name = &cap[1];
        let body = &cap[2];
        for field_cap in field_re.captures_iter(body) {
            let field_name = &field_cap[1];
            errors.push(format!(
                "field type not specified. Object: {}, Field: {}",
                type_name, field_name
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(QueryError::parse(errors.join("\n")))
    }
}

/// Preprocess SDL to handle empty type/interface definitions.
/// graphql_parser doesn't allow empty types, so we insert a placeholder field.
pub(super) fn preprocess_empty_types(sdl: &str) -> String {
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
pub(super) fn generate_index_name(
    collection_name: &str,
    first_field: &str,
    existing_names: &[String],
) -> String {
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
pub(super) fn graphql_schema_value_to_json(
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

/// Normalize a datetime string to RFC3339 format to match Go's time.Time serialization.
/// Go's time.Time drops trailing zeros in fractional seconds, so:
/// - "2000-07-23T03:00:00.000Z" becomes "2000-07-23T03:00:00Z"
/// - "2000-07-23T03:00:00.123Z" stays "2000-07-23T03:00:00.123Z"
///   If parsing fails, returns the original string (e.g., for special values).
pub(super) fn normalize_datetime_string(s: &str) -> String {
    // Try to parse as RFC3339 variant (ISO 8601)
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        // Format back using RFC3339 which drops trailing zeros in nanoseconds
        // Go uses time.RFC3339Nano which behaves this way
        let nanos = dt.timestamp_subsec_nanos();
        if nanos == 0 {
            // No fractional seconds - output without them
            dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
        } else {
            // Has fractional seconds - output with them, but trim trailing zeros
            let s = dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string();
            // Remove trailing zeros after decimal point (but keep at least one digit)
            let trimmed = s.trim_end_matches('0');
            // If we trimmed all digits after the decimal, we need to handle that
            if trimmed.ends_with('.') {
                format!("{}Z", trimmed.trim_end_matches('.'))
            } else {
                format!("{}Z", trimmed.trim_end_matches('Z'))
            }
        }
    } else {
        // Not a parseable datetime - return as-is (could be a special value)
        s.to_string()
    }
}

/// Parse @policy directive arguments with Go-compatible error messages.
pub(super) fn parse_policy_directive(directive: &Directive<'_, String>) -> Result<PolicyConfig> {
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

    let id_empty = id.as_ref().is_none_or(|s| s.is_empty());
    let resource_empty = resource.as_ref().is_none_or(|s| s.is_empty());

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
pub(super) fn format_graphql_value(value: &graphql_parser::schema::Value<'_, String>) -> String {
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

/// Parse a GraphQL type into our ParsedType representation
pub(super) fn parse_graphql_type(ty: &Type<'_, String>) -> ParsedType {
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
pub(super) fn graphql_to_scalar_kind(name: &str) -> Option<ScalarKind> {
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
pub(super) fn hash_to_hex(hash: &[u8]) -> String {
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
pub(super) fn generate_collection_id(
    type_name: &str,
    fields: &[FieldDescription],
    headstore: &HashMap<String, (Cid, u64)>,
) -> String {
    // Sort fields to match Go's order: _docID first, then alphabetically by name
    // Include fields with non-empty FieldID in the CID.
    // Go's Delta() excludes: secondary relations, self-ref with empty relative_id.
    // All excluded fields have empty FieldIDs, so filtering on !id.is_empty() suffices.
    let mut sorted_fields: Vec<&FieldDescription> =
        fields.iter().filter(|f| !f.id.is_empty()).collect();
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
        .filter_map(|f| schema::generate_field_cid_with_priority(f, 1).ok())
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
    head_cids.sort_by_key(|c| c.to_bytes());

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
pub(super) fn generate_field_id(field_name: &str, kind: &FieldKind, crdt_type: CType) -> String {
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
#[allow(dead_code)]
pub(super) fn generate_version_id(name: &str, fields: &[FieldDescription]) -> String {
    // Version ID uses the same logic as collection ID (Go behavior for new schemas)
    generate_collection_id(name, fields, &HashMap::new())
}

/// Generate a relation name following Go DefraDB conventions.
/// Go uses lexicographic sort of type names to create deterministic relation names.
pub(super) fn generate_relation_name(from_type: &str, _field_name: &str, to_type: &str) -> String {
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
