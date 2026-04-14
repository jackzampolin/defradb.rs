mod export;
mod import;

pub use export::export_database;
pub use import::{import_database, ImportStats};

use std::collections::HashMap;

use serde_json::{Map, Value as JsonValue};

use document::Document;
use schema::{CollectionVersion, FieldKind};

/// Classified field information from a collection schema.
pub struct FieldInfo {
    pub name: String,
    pub is_relation: bool,
    pub is_self_ref: bool,
    pub is_array: bool,
    pub is_primary: bool,
}

/// Classify fields from the typed schema, detecting scalar, relation,
/// and self-referencing fields.
pub fn classify_schema_fields(schema: &CollectionVersion) -> Vec<FieldInfo> {
    let collection_id = &schema.collection_id;
    let mut result = Vec::new();

    for field in &schema.fields {
        if field.name == "_docID" {
            continue;
        }

        match &field.kind {
            FieldKind::Scalar(_) | FieldKind::ScalarArray(_) => {
                if field.relation_name.is_none() {
                    result.push(FieldInfo {
                        name: field.name.clone(),
                        is_relation: false,
                        is_self_ref: false,
                        is_array: field.kind.is_array(),
                        is_primary: false,
                    });
                }
            }
            FieldKind::SelfRef { is_array, .. } => {
                result.push(FieldInfo {
                    name: field.name.clone(),
                    is_relation: true,
                    is_self_ref: true,
                    is_array: *is_array,
                    is_primary: field.is_primary,
                });
            }
            FieldKind::Relation {
                collection_id: target_id,
                is_array,
            } => {
                let is_self_ref = target_id == collection_id;
                result.push(FieldInfo {
                    name: field.name.clone(),
                    is_relation: true,
                    is_self_ref,
                    is_array: *is_array,
                    is_primary: field.is_primary,
                });
            }
            FieldKind::Named { is_array, .. } => {
                result.push(FieldInfo {
                    name: field.name.clone(),
                    is_relation: true,
                    is_self_ref: false,
                    is_array: *is_array,
                    is_primary: field.is_primary,
                });
            }
            _ => {}
        }
    }
    result
}

/// Compute `_docIDNew` from current field values.
///
/// Creates a Document from the field values (minus FK fields),
/// sets the collection, and generates the content-addressed ID.
pub fn compute_doc_id_new(
    doc_map: &Map<String, JsonValue>,
    fk_names: &[String],
    schema: &CollectionVersion,
) -> Result<String, String> {
    let mut fields: HashMap<String, JsonValue> = HashMap::new();

    for (key, value) in doc_map {
        if key == "_docID" || key == "_docIDNew" {
            continue;
        }
        if fk_names.contains(key) {
            continue;
        }
        if value.is_null() {
            continue;
        }
        fields.insert(key.clone(), value.clone());
    }

    let mut doc =
        Document::from_map(fields).map_err(|e| format!("failed to create document: {}", e))?;
    doc.set_collection(schema.clone());

    let doc_id = doc
        .generate_doc_id()
        .map_err(|e| format!("failed to generate doc ID: {}", e))?;

    Ok(doc_id.to_string())
}

/// Convert a JSON value to GraphQL input syntax.
///
/// GraphQL uses bare identifiers for object keys (not quoted strings like JSON).
/// This converts: {"name": "Alice", "age": 30} to {name: "Alice", age: 30}
pub fn json_to_graphql_input(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => {
            let escaped = s
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
                .replace('\t', "\\t");
            format!("\"{}\"", escaped)
        }
        JsonValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_to_graphql_input).collect();
            format!("[{}]", items.join(", "))
        }
        JsonValue::Object(obj) => {
            let fields: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("{}: {}", k, json_to_graphql_input(v)))
                .collect();
            format!("{{{}}}", fields.join(", "))
        }
    }
}
