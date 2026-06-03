//! Value conversion and normalization for index selection.

use document::{JsonLeafValue, JsonPath, JsonScalarValue, NormalValue};
use schema::{FieldKind, ScalarKind};
use serde_json::Value as JsonValue;

/// Convert JSON value to NormalValue.
pub(crate) fn json_to_normal_value(value: &JsonValue) -> Option<NormalValue> {
    match value {
        JsonValue::Null => Some(NormalValue::Null),
        JsonValue::Bool(b) => Some(NormalValue::Bool(*b)),
        JsonValue::Number(n) => n
            .as_i64()
            .map(NormalValue::Int)
            .or_else(|| n.as_f64().map(NormalValue::Float64)),
        JsonValue::String(s) => {
            // Try to parse as DateTime first (RFC3339/ISO8601)
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                return Some(NormalValue::Time(dt));
            }
            Some(NormalValue::String(s.clone()))
        }
        _ => None,
    }
}

/// Convert NormalValue to JsonScalarValue for use in JsonLeafValue.
pub(super) fn normal_value_to_json_scalar(value: &NormalValue) -> Option<JsonScalarValue> {
    match value {
        NormalValue::Null => Some(JsonScalarValue::Null),
        NormalValue::Bool(b) => Some(JsonScalarValue::Bool(*b)),
        NormalValue::Int(i) => Some(JsonScalarValue::Number(*i as f64)),
        NormalValue::Float64(f) => Some(JsonScalarValue::Number(*f)),
        NormalValue::Float32(f) => Some(JsonScalarValue::Number(*f as f64)),
        NormalValue::String(s) => Some(JsonScalarValue::String(s.clone())),
        _ => None,
    }
}

/// Wrap a NormalValue in JsonLeafValue if a JSON path is present.
pub(super) fn wrap_value_for_json_path(
    value: NormalValue,
    json_path: Option<&JsonPath>,
) -> NormalValue {
    match json_path {
        Some(path) => {
            // Top-level null JSON values (empty path) are stored as plain NormalValue::Null
            // in the index (see NormalValue::json_leaves()). Null with non-empty path IS
            // stored as JsonLeafValue { path, value: Null }.
            if matches!(value, NormalValue::Null) && path.is_empty() {
                return value;
            }
            if let Some(scalar) = normal_value_to_json_scalar(&value) {
                NormalValue::JsonLeaf(JsonLeafValue {
                    path: path.clone(),
                    value: scalar,
                })
            } else {
                value
            }
        }
        None => value,
    }
}

/// Wrap multiple values for JSON path (for _in operator).
pub(super) fn wrap_values_for_json_path(
    values: Vec<NormalValue>,
    json_path: Option<&JsonPath>,
) -> Vec<NormalValue> {
    match json_path {
        Some(path) => values
            .into_iter()
            .filter_map(|v| {
                normal_value_to_json_scalar(&v).map(|scalar| {
                    NormalValue::JsonLeaf(JsonLeafValue {
                        path: path.clone(),
                        value: scalar,
                    })
                })
            })
            .collect(),
        None => values,
    }
}

/// Normalize a NormalValue to match the schema field's encoding type.
/// This ensures filter values use the same encoding as stored index values.
/// For example, a Float32 field stores values with `encode_float32_ascending`,
/// so lookup values must also be Float32 (not Float64 or Int).
fn normalize_value_for_field(value: NormalValue, field_kind: &FieldKind) -> NormalValue {
    match (&value, field_kind) {
        // Float64 → Float32 when schema says Float32
        (NormalValue::Float64(f), FieldKind::Scalar(ScalarKind::Float32)) => {
            NormalValue::Float32(*f as f32)
        }
        // Int → Float32 when schema says Float32
        (NormalValue::Int(i), FieldKind::Scalar(ScalarKind::Float32)) => {
            NormalValue::Float32(*i as f32)
        }
        // Int → Float64 when schema says Float64
        (NormalValue::Int(i), FieldKind::Scalar(ScalarKind::Float64)) => {
            NormalValue::Float64(*i as f64)
        }
        _ => value,
    }
}

/// Normalize a NormalValue for a named index field using collection field metadata.
pub(crate) fn normalize_for_index_field(
    value: NormalValue,
    field_name: &str,
    collection_fields: &[schema::FieldDescription],
) -> NormalValue {
    if let Some(field) = collection_fields.iter().find(|f| f.name == field_name) {
        normalize_value_for_field(value, &field.kind)
    } else {
        value
    }
}
