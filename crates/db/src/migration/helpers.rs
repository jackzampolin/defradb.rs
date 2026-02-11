//! Helper functions for migration placeholder creation and value conversion.

use schema::{CollectionVersion, FieldKind, ScalarKind, ORPHAN_COLLECTION_ID};

/// Create an orphan placeholder collection version.
///
/// Used when a migration references a version that doesn't exist yet.
pub(super) fn create_orphan_placeholder(
    version_id: &str,
    name: &str,
    collection_id: &str,
) -> CollectionVersion {
    let mut placeholder = CollectionVersion {
        version_id: version_id.to_string(),
        collection_id: if collection_id.is_empty() {
            ORPHAN_COLLECTION_ID.to_string()
        } else {
            collection_id.to_string()
        },
        name: name.to_string(),
        is_materialized: true,
        is_placeholder: true,
        ..CollectionVersion::new("", "", "", Vec::new())
    };
    placeholder.is_active = false;
    placeholder
}

/// Create a placeholder with source collection info.
pub(super) fn create_placeholder_with_source(
    version_id: &str,
    source_name: &str,
    source_collection_id: &str,
) -> CollectionVersion {
    let mut placeholder = CollectionVersion {
        name: source_name.to_string(),
        version_id: version_id.to_string(),
        collection_id: source_collection_id.to_string(),
        is_materialized: true,
        is_placeholder: true,
        ..CollectionVersion::new("", "", "", Vec::new())
    };
    placeholder.is_active = false;
    placeholder
}

/// Convert a JSON value to a native NormalValue based on the field's schema type.
///
/// When documents are migrated through lens transforms, they come back as JSON values.
/// This function converts them to the appropriate native type (Int, Float, String, etc.)
/// based on the field's declared type in the schema.
pub fn json_to_native_value(
    value: &serde_json::Value,
    field_name: &str,
    schema: &CollectionVersion,
) -> document::NormalValue {
    if value.is_null() {
        return document::NormalValue::Null;
    }

    let field_kind = schema
        .fields
        .iter()
        .find(|f| f.name == field_name)
        .map(|f| &f.kind);

    if let Some(FieldKind::Scalar(scalar)) = field_kind {
        match scalar {
            ScalarKind::Int => {
                if let Some(n) = value.as_i64() {
                    return document::NormalValue::Int(n);
                }
            }
            ScalarKind::Float64 => {
                if let Some(n) = value.as_f64() {
                    return document::NormalValue::Float64(n);
                }
            }
            ScalarKind::Float32 => {
                if let Some(n) = value.as_f64() {
                    return document::NormalValue::Float32(n as f32);
                }
            }
            ScalarKind::Bool => {
                if let Some(b) = value.as_bool() {
                    return document::NormalValue::Bool(b);
                }
            }
            ScalarKind::String | ScalarKind::DocID => {
                if let Some(s) = value.as_str() {
                    return document::NormalValue::String(s.to_string());
                }
            }
            ScalarKind::Blob => {
                if let Some(s) = value.as_str() {
                    return document::NormalValue::Bytes(s.as_bytes().to_vec());
                }
            }
            ScalarKind::DateTime => {
                if let Some(s) = value.as_str() {
                    return document::NormalValue::String(s.to_string());
                }
            }
            ScalarKind::Json | ScalarKind::None => {}
        }
    }

    document::NormalValue::Json(value.clone())
}
