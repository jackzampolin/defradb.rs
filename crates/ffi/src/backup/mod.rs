mod export;
mod import;

pub use export::basic_export;
pub use import::basic_import;

use std::collections::HashMap;

use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value as JsonValue};

use db::Document;
use schema::{CollectionVersion, FieldKind};

/// Deserialize null or missing JSON values as an empty Vec.
/// Go sends `"collections": null` when the slice is nil.
fn null_to_empty_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<Vec<String>>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

/// BackupConfig matches Go's client.BackupConfig.
#[derive(Deserialize)]
pub(crate) struct BackupConfig {
    pub filepath: String,
    #[serde(default)]
    pub pretty: bool,
    #[serde(default, deserialize_with = "null_to_empty_vec")]
    pub collections: Vec<String>,
}

/// Classified field information from a collection schema.
pub(crate) struct FieldInfo {
    pub name: String,
    pub is_relation: bool,
    pub is_self_ref: bool,
    pub is_array: bool,
    pub is_primary: bool,
}

/// Classify fields from the typed schema, detecting scalar, relation,
/// and self-referencing fields.
pub(crate) fn classify_schema_fields(schema: &CollectionVersion) -> Vec<FieldInfo> {
    let collection_id = &schema.collection_id;
    let mut result = Vec::new();

    for field in &schema.fields {
        // Skip internal fields
        if field.name == "_docID" {
            continue;
        }

        match &field.kind {
            FieldKind::Scalar(_) | FieldKind::ScalarArray(_) => {
                // Skip relation backing FK fields (e.g., author_id)
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
                // A relation to the same collection is also self-referencing
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
        }
    }
    result
}

/// Compute `_docIDNew` from current field values.
///
/// Creates a Document from the field values (minus FK fields),
/// sets the collection, and generates the content-addressed ID.
pub(crate) fn compute_doc_id_new(
    doc_map: &Map<String, JsonValue>,
    fk_names: &[String],
    schema: &CollectionVersion,
) -> Result<String, String> {
    let mut fields: HashMap<String, JsonValue> = HashMap::new();

    for (key, value) in doc_map {
        // Skip metadata fields
        if key == "_docID" || key == "_docIDNew" {
            continue;
        }
        // Skip FK fields (relationship metadata, not document data)
        if fk_names.contains(key) {
            continue;
        }
        // Skip null values (they don't contribute to CID)
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
