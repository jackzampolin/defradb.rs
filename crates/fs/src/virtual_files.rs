/// Virtual files that appear in each collection directory.
///
/// - `_schema.graphql`: SDL representation of the collection schema
/// - `_view.json`: All documents in the collection as a JSON array
use schema::{CollectionVersion, FieldKind, ScalarArrayKind, ScalarKind};
use std::collections::HashMap;

/// Virtual file names (prefixed with _ to avoid docID collisions).
pub const SCHEMA_FILE: &str = "_schema.graphql";
pub const VIEW_FILE: &str = "_view.json";

/// Generate GraphQL SDL for a collection schema.
pub fn generate_sdl(schema: &CollectionVersion) -> String {
    let mut sdl = format!("type {} {{\n", schema.name);

    for field in &schema.fields {
        let gql_type = field_kind_to_graphql(&field.kind);
        sdl.push_str(&format!("  {}: {}\n", field.name, gql_type));
    }

    sdl.push('}');
    sdl
}

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

/// Generate a JSON array of all documents in a collection.
pub fn generate_view_json(docs: &[HashMap<String, serde_json::Value>]) -> Vec<u8> {
    serde_json::to_vec_pretty(docs).unwrap_or_default()
}

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
