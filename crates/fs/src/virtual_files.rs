/// Virtual files that appear in collection and root directories.
///
/// Collection-level:
/// - `_schema.graphql`: SDL representation of the collection schema
/// - `_view.json`: All documents as a JSON array (primary grep target)
///
/// Root-level:
/// - `_schema.graphql`: Combined SDL of all collection schemas
/// - `_collections.json`: Collection names and document counts
#[cfg(feature = "fuse")]
use schema::{CollectionVersion, FieldKind, ScalarArrayKind, ScalarKind};
use std::collections::HashMap;

pub const SCHEMA_FILE: &str = "_schema.graphql";
pub const VIEW_FILE: &str = "_view.json";
pub const ROOT_SCHEMA_FILE: &str = "_schema.graphql";
pub const ROOT_COLLECTIONS_FILE: &str = "_collections.json";

/// Generate GraphQL SDL for a single collection schema.
#[cfg(feature = "fuse")]
pub fn generate_sdl(schema: &CollectionVersion) -> String {
    let mut sdl = format!("type {} {{\n", schema.name);

    for field in &schema.fields {
        let gql_type = field_kind_to_graphql(&field.kind);
        sdl.push_str(&format!("  {}: {}\n", field.name, gql_type));
    }

    sdl.push('}');
    sdl
}

/// Generate combined SDL for all collection schemas.
#[cfg(feature = "fuse")]
pub fn generate_root_sdl(schemas: &[&CollectionVersion]) -> String {
    let mut sdl = String::new();
    for (i, schema) in schemas.iter().enumerate() {
        if i > 0 {
            sdl.push_str("\n\n");
        }
        sdl.push_str(&generate_sdl(schema));
    }
    sdl
}

/// Generate a JSON array listing all collections with document counts.
pub fn generate_collections_json(collections: &[(String, usize)]) -> Vec<u8> {
    let entries: Vec<serde_json::Value> = collections
        .iter()
        .map(|(name, count)| {
            serde_json::json!({
                "name": name,
                "documents": count,
            })
        })
        .collect();
    serde_json::to_vec_pretty(&entries).unwrap_or_default()
}

/// Generate a JSON array of all documents in a collection.
pub fn generate_view_json(docs: &[HashMap<String, serde_json::Value>]) -> Vec<u8> {
    serde_json::to_vec_pretty(docs).unwrap_or_default()
}

#[cfg(feature = "fuse")]
fn field_kind_to_graphql(kind: &FieldKind) -> String {
    match kind {
        FieldKind::Scalar(scalar) => scalar_to_graphql(*scalar).to_string(),
        FieldKind::ScalarArray(arr) => format!("[{}]", scalar_array_to_graphql(*arr)),
        FieldKind::Relation {
            collection_id,
            is_array,
        } => {
            if *is_array {
                format!("[{}]", collection_id)
            } else {
                collection_id.clone()
            }
        }
        FieldKind::SelfRef { is_array, .. } | FieldKind::Named { is_array, .. } => {
            if *is_array {
                "[Self]".to_string()
            } else {
                "Self".to_string()
            }
        }
    }
}

#[cfg(feature = "fuse")]
fn scalar_to_graphql(kind: ScalarKind) -> &'static str {
    match kind {
        ScalarKind::None => "None",
        ScalarKind::DocID => "ID",
        ScalarKind::Bool => "Boolean",
        ScalarKind::Int => "Int",
        ScalarKind::Float64 => "Float",
        ScalarKind::Float32 => "Float",
        ScalarKind::DateTime => "DateTime",
        ScalarKind::String => "String",
        ScalarKind::Blob => "Blob",
        ScalarKind::Json => "JSON",
    }
}

#[cfg(feature = "fuse")]
fn scalar_array_to_graphql(kind: ScalarArrayKind) -> &'static str {
    match kind {
        ScalarArrayKind::BoolArray => "Boolean",
        ScalarArrayKind::IntArray => "Int",
        ScalarArrayKind::Float64Array => "Float",
        ScalarArrayKind::Float32Array => "Float",
        ScalarArrayKind::StringArray => "String",
        ScalarArrayKind::NillableBoolArray => "Boolean",
        ScalarArrayKind::NillableIntArray => "Int",
        ScalarArrayKind::NillableFloat64Array => "Float",
        ScalarArrayKind::NillableFloat32Array => "Float",
        ScalarArrayKind::NillableStringArray => "String",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collections_json_format() {
        let cols = vec![("Users".into(), 5), ("Posts".into(), 12)];
        let json = generate_collections_json(&cols);
        let parsed: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["name"], "Users");
        assert_eq!(parsed[0]["documents"], 5);
        assert_eq!(parsed[1]["name"], "Posts");
        assert_eq!(parsed[1]["documents"], 12);
    }

    #[test]
    fn view_json_empty_collection() {
        let docs: Vec<HashMap<String, serde_json::Value>> = vec![];
        let json = generate_view_json(&docs);
        let parsed: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        assert!(parsed.is_empty());
    }
}
