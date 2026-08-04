//! Helper functions for definition validation formatting and type checks.

use crate::{CType, FieldKind, ScalarKind};

/// Check if a CRDT type is supported as a user-specified field type.
/// Matches Go's IsSupportedFieldCType: only None, LwwRegister, PnCounter, PCounter.
/// Object and Composite are internal types, not user-assignable.
pub(super) fn is_crdt_type_supported(crdt: CType) -> bool {
    matches!(
        crdt,
        CType::None | CType::LwwRegister | CType::PnCounter | CType::PCounter
    )
}

/// Check if a field kind is valid for vector embedding storage.
/// Only Float32Array is supported (matches Go's IsVectorEmbeddingCompatible).
pub(super) fn is_valid_embedding_kind(kind: &FieldKind) -> bool {
    matches!(
        kind,
        FieldKind::ScalarArray(crate::ScalarArrayKind::Float32Array)
    )
}

/// Check if a field kind is valid for embedding generation input (string-like).
pub(super) fn is_valid_embedding_generation_kind(kind: &FieldKind) -> bool {
    matches!(
        kind.as_scalar().map(ScalarKind::base_kind),
        Some(
            ScalarKind::String
                | ScalarKind::Int
                | ScalarKind::Float64
                | ScalarKind::Float32
                | ScalarKind::Bool
                | ScalarKind::DateTime
                | ScalarKind::Blob
        )
    )
}

/// Format a CType for error messages (matches Go's CType.String()).
pub(super) fn format_crdt_type(crdt: CType) -> String {
    match crdt {
        CType::None => "none".to_string(),
        CType::LwwRegister => "lww".to_string(),
        CType::Object => "object".to_string(),
        CType::Composite => "composite".to_string(),
        CType::PnCounter => "pncounter".to_string(),
        CType::PCounter => "pcounter".to_string(),
        CType::Unknown(_) => "unknown".to_string(),
    }
}

/// Format a FieldKind for error messages (matches Go's Kind.String()).
pub(super) fn format_field_kind(kind: &FieldKind) -> String {
    match kind {
        FieldKind::Scalar(s) => match s {
            ScalarKind::None => "None".to_string(),
            ScalarKind::DocID => "ID".to_string(),
            ScalarKind::Bool => "Boolean".to_string(),
            ScalarKind::NonNillableBool => "Boolean!".to_string(),
            ScalarKind::Int => "Int".to_string(),
            ScalarKind::NonNillableInt => "Int!".to_string(),
            ScalarKind::Float64 => "Float64".to_string(),
            ScalarKind::NonNillableFloat64 => "Float64!".to_string(),
            ScalarKind::Float32 => "Float32".to_string(),
            ScalarKind::NonNillableFloat32 => "Float32!".to_string(),
            ScalarKind::DateTime => "DateTime".to_string(),
            ScalarKind::NonNillableDateTime => "DateTime!".to_string(),
            ScalarKind::String => "String".to_string(),
            ScalarKind::NonNillableString => "String!".to_string(),
            ScalarKind::Blob => "Blob".to_string(),
            ScalarKind::NonNillableBlob => "Blob!".to_string(),
            ScalarKind::Json => "JSON".to_string(),
            ScalarKind::NonNillableJson => "JSON!".to_string(),
        },
        FieldKind::ScalarArray(a) => match a {
            crate::ScalarArrayKind::BoolArray => "[Boolean!]".to_string(),
            crate::ScalarArrayKind::IntArray => "[Int!]".to_string(),
            crate::ScalarArrayKind::Float64Array => "[Float64!]".to_string(),
            crate::ScalarArrayKind::Float32Array => "[Float32!]".to_string(),
            crate::ScalarArrayKind::StringArray => "[String!]".to_string(),
            crate::ScalarArrayKind::NillableBoolArray => "[Boolean]".to_string(),
            crate::ScalarArrayKind::NillableIntArray => "[Int]".to_string(),
            crate::ScalarArrayKind::NillableFloat64Array => "[Float64]".to_string(),
            crate::ScalarArrayKind::NillableFloat32Array => "[Float32]".to_string(),
            crate::ScalarArrayKind::NillableStringArray => "[String]".to_string(),
            crate::ScalarArrayKind::DateTimeArray => "[DateTime!]".to_string(),
            crate::ScalarArrayKind::NillableDateTimeArray => "[DateTime]".to_string(),
        },
        FieldKind::Relation { .. } | FieldKind::SelfRef { .. } | FieldKind::Named { .. } => {
            "Object".to_string()
        }
    }
}
