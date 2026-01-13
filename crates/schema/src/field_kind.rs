//! Field kind definitions matching Go DefraDB exactly for datastore compatibility.
//!
//! The numeric values here MUST match the Go implementation to ensure
//! Rust and Go can read/write the same datastores.

use serde::{Deserialize, Serialize};

/// Scalar field kinds with numeric values matching Go DefraDB.
///
/// These values are stored in the datastore, so they must match exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ScalarKind {
    None = 0,
    DocID = 1,
    Bool = 2,
    Int = 4,
    Float64 = 6,
    Float32 = 8,
    DateTime = 10,
    String = 11,
    Blob = 13,
    Json = 14,
}

impl ScalarKind {
    /// Returns true if this is a numeric type (Int, Float64, Float32)
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            ScalarKind::Int | ScalarKind::Float64 | ScalarKind::Float32
        )
    }

    /// Get the corresponding array kind for this scalar
    pub fn to_array_kind(&self) -> Option<ScalarArrayKind> {
        match self {
            ScalarKind::Bool => Some(ScalarArrayKind::BoolArray),
            ScalarKind::Int => Some(ScalarArrayKind::IntArray),
            ScalarKind::Float64 => Some(ScalarArrayKind::Float64Array),
            ScalarKind::Float32 => Some(ScalarArrayKind::Float32Array),
            ScalarKind::String => Some(ScalarArrayKind::StringArray),
            _ => None,
        }
    }

    /// Get the corresponding nillable array kind for this scalar
    pub fn to_nillable_array_kind(&self) -> Option<ScalarArrayKind> {
        match self {
            ScalarKind::Bool => Some(ScalarArrayKind::NillableBoolArray),
            ScalarKind::Int => Some(ScalarArrayKind::NillableIntArray),
            ScalarKind::Float64 => Some(ScalarArrayKind::NillableFloat64Array),
            ScalarKind::Float32 => Some(ScalarArrayKind::NillableFloat32Array),
            ScalarKind::String => Some(ScalarArrayKind::NillableStringArray),
            _ => None,
        }
    }
}

/// Array field kinds with numeric values matching Go DefraDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ScalarArrayKind {
    BoolArray = 3,
    IntArray = 5,
    Float64Array = 7,
    Float32Array = 9,
    StringArray = 12,
    // Nillable arrays (elements can be null)
    NillableBoolArray = 18,
    NillableIntArray = 19,
    NillableFloat64Array = 20,
    NillableStringArray = 21,
    NillableFloat32Array = 22,
}

impl ScalarArrayKind {
    /// Returns true if this array contains nillable elements
    pub fn is_nillable(&self) -> bool {
        matches!(
            self,
            ScalarArrayKind::NillableBoolArray
                | ScalarArrayKind::NillableIntArray
                | ScalarArrayKind::NillableFloat64Array
                | ScalarArrayKind::NillableStringArray
                | ScalarArrayKind::NillableFloat32Array
        )
    }

    /// Get the underlying scalar kind for this array
    pub fn element_kind(&self) -> ScalarKind {
        match self {
            ScalarArrayKind::BoolArray | ScalarArrayKind::NillableBoolArray => ScalarKind::Bool,
            ScalarArrayKind::IntArray | ScalarArrayKind::NillableIntArray => ScalarKind::Int,
            ScalarArrayKind::Float64Array | ScalarArrayKind::NillableFloat64Array => {
                ScalarKind::Float64
            }
            ScalarArrayKind::Float32Array | ScalarArrayKind::NillableFloat32Array => {
                ScalarKind::Float32
            }
            ScalarArrayKind::StringArray | ScalarArrayKind::NillableStringArray => {
                ScalarKind::String
            }
        }
    }
}

/// What type a field holds - unified enum for all field kinds.
///
/// This matches Go's FieldKind interface which can be ScalarKind,
/// ScalarArrayKind, CollectionKind, SelfKind, or NamedKind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum FieldKind {
    /// Scalar types (Bool, Int, String, etc.)
    Scalar(ScalarKind),

    /// Array of scalar types
    ScalarArray(ScalarArrayKind),

    /// Relation to another collection
    Relation {
        collection_id: String,
        /// true = one-to-many, false = one-to-one
        is_array: bool,
    },

    /// Self-reference for circular relations
    SelfRef {
        relative_id: String,
        /// true = one-to-many, false = one-to-one
        is_array: bool,
    },

    /// Named reference (temporary state during schema parsing)
    /// Used when referenced collection hasn't been resolved yet
    Named { name: String, is_array: bool },
}

impl FieldKind {
    // Convenience constructors for common scalar types
    pub fn doc_id() -> Self {
        FieldKind::Scalar(ScalarKind::DocID)
    }
    pub fn bool() -> Self {
        FieldKind::Scalar(ScalarKind::Bool)
    }
    pub fn int() -> Self {
        FieldKind::Scalar(ScalarKind::Int)
    }
    pub fn float64() -> Self {
        FieldKind::Scalar(ScalarKind::Float64)
    }
    pub fn float32() -> Self {
        FieldKind::Scalar(ScalarKind::Float32)
    }
    pub fn datetime() -> Self {
        FieldKind::Scalar(ScalarKind::DateTime)
    }
    pub fn string() -> Self {
        FieldKind::Scalar(ScalarKind::String)
    }
    pub fn blob() -> Self {
        FieldKind::Scalar(ScalarKind::Blob)
    }
    pub fn json() -> Self {
        FieldKind::Scalar(ScalarKind::Json)
    }

    // Convenience constructors for array types
    pub fn bool_array() -> Self {
        FieldKind::ScalarArray(ScalarArrayKind::BoolArray)
    }
    pub fn int_array() -> Self {
        FieldKind::ScalarArray(ScalarArrayKind::IntArray)
    }
    pub fn float64_array() -> Self {
        FieldKind::ScalarArray(ScalarArrayKind::Float64Array)
    }
    pub fn float32_array() -> Self {
        FieldKind::ScalarArray(ScalarArrayKind::Float32Array)
    }
    pub fn string_array() -> Self {
        FieldKind::ScalarArray(ScalarArrayKind::StringArray)
    }

    // Nillable array constructors
    pub fn nillable_bool_array() -> Self {
        FieldKind::ScalarArray(ScalarArrayKind::NillableBoolArray)
    }
    pub fn nillable_int_array() -> Self {
        FieldKind::ScalarArray(ScalarArrayKind::NillableIntArray)
    }
    pub fn nillable_float64_array() -> Self {
        FieldKind::ScalarArray(ScalarArrayKind::NillableFloat64Array)
    }
    pub fn nillable_float32_array() -> Self {
        FieldKind::ScalarArray(ScalarArrayKind::NillableFloat32Array)
    }
    pub fn nillable_string_array() -> Self {
        FieldKind::ScalarArray(ScalarArrayKind::NillableStringArray)
    }

    /// Create a relation to another collection
    pub fn relation(collection_id: impl Into<String>, is_array: bool) -> Self {
        FieldKind::Relation {
            collection_id: collection_id.into(),
            is_array,
        }
    }

    /// Create a self-reference
    pub fn self_ref(relative_id: impl Into<String>, is_array: bool) -> Self {
        FieldKind::SelfRef {
            relative_id: relative_id.into(),
            is_array,
        }
    }

    /// Create a named reference (for parsing)
    pub fn named(name: impl Into<String>, is_array: bool) -> Self {
        FieldKind::Named {
            name: name.into(),
            is_array,
        }
    }

    /// Returns true if this kind is numeric (Int, Float64, Float32)
    pub fn is_numeric(&self) -> bool {
        match self {
            FieldKind::Scalar(s) => s.is_numeric(),
            _ => false,
        }
    }

    /// Returns true if this is an array type
    pub fn is_array(&self) -> bool {
        match self {
            FieldKind::ScalarArray(_) => true,
            FieldKind::Relation { is_array, .. } => *is_array,
            FieldKind::SelfRef { is_array, .. } => *is_array,
            FieldKind::Named { is_array, .. } => *is_array,
            FieldKind::Scalar(_) => false,
        }
    }

    /// Returns true if this is a relation type (Relation, SelfRef, or Named)
    pub fn is_relation(&self) -> bool {
        matches!(
            self,
            FieldKind::Relation { .. } | FieldKind::SelfRef { .. } | FieldKind::Named { .. }
        )
    }

    /// Returns true if this is a scalar type
    pub fn is_scalar(&self) -> bool {
        matches!(self, FieldKind::Scalar(_))
    }

    /// Returns true if values of this kind can be nil/null.
    /// In Go DefraDB, all scalar types are nillable by default.
    pub fn is_nillable(&self) -> bool {
        match self {
            FieldKind::Scalar(s) => !matches!(s, ScalarKind::None | ScalarKind::DocID),
            FieldKind::ScalarArray(a) => a.is_nillable(),
            FieldKind::Relation { .. } | FieldKind::SelfRef { .. } | FieldKind::Named { .. } => {
                true
            }
        }
    }

    /// Returns true if this is an object/relation type (matches Go's IsObject)
    pub fn is_object(&self) -> bool {
        self.is_relation()
    }

    /// Get the referenced collection ID if this is a relation
    pub fn relation_collection_id(&self) -> Option<&str> {
        match self {
            FieldKind::Relation { collection_id, .. } => Some(collection_id),
            _ => None,
        }
    }

    /// Get the underlying scalar kind if this is a scalar type
    pub fn as_scalar(&self) -> Option<ScalarKind> {
        match self {
            FieldKind::Scalar(s) => Some(*s),
            _ => None,
        }
    }

    /// Get the underlying array kind if this is an array type
    pub fn as_scalar_array(&self) -> Option<ScalarArrayKind> {
        match self {
            FieldKind::ScalarArray(a) => Some(*a),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_repr_values_match_go() {
        // These values MUST match Go DefraDB for datastore compatibility
        assert_eq!(ScalarKind::None as u8, 0);
        assert_eq!(ScalarKind::DocID as u8, 1);
        assert_eq!(ScalarKind::Bool as u8, 2);
        assert_eq!(ScalarKind::Int as u8, 4);
        assert_eq!(ScalarKind::Float64 as u8, 6);
        assert_eq!(ScalarKind::Float32 as u8, 8);
        assert_eq!(ScalarKind::DateTime as u8, 10);
        assert_eq!(ScalarKind::String as u8, 11);
        assert_eq!(ScalarKind::Blob as u8, 13);
        assert_eq!(ScalarKind::Json as u8, 14);
    }

    #[test]
    fn test_array_repr_values_match_go() {
        assert_eq!(ScalarArrayKind::BoolArray as u8, 3);
        assert_eq!(ScalarArrayKind::IntArray as u8, 5);
        assert_eq!(ScalarArrayKind::Float64Array as u8, 7);
        assert_eq!(ScalarArrayKind::Float32Array as u8, 9);
        assert_eq!(ScalarArrayKind::StringArray as u8, 12);
        assert_eq!(ScalarArrayKind::NillableBoolArray as u8, 18);
        assert_eq!(ScalarArrayKind::NillableIntArray as u8, 19);
        assert_eq!(ScalarArrayKind::NillableFloat64Array as u8, 20);
        assert_eq!(ScalarArrayKind::NillableStringArray as u8, 21);
        assert_eq!(ScalarArrayKind::NillableFloat32Array as u8, 22);
    }

    #[test]
    fn test_is_numeric() {
        assert!(FieldKind::int().is_numeric());
        assert!(FieldKind::float64().is_numeric());
        assert!(FieldKind::float32().is_numeric());
        assert!(!FieldKind::string().is_numeric());
        assert!(!FieldKind::bool().is_numeric());
    }

    #[test]
    fn test_is_array() {
        assert!(FieldKind::int_array().is_array());
        assert!(FieldKind::string_array().is_array());
        assert!(FieldKind::nillable_int_array().is_array());
        assert!(!FieldKind::int().is_array());
        assert!(FieldKind::relation("users", true).is_array());
        assert!(!FieldKind::relation("users", false).is_array());
    }

    #[test]
    fn test_is_relation() {
        assert!(FieldKind::relation("users", false).is_relation());
        assert!(FieldKind::self_ref("parent", false).is_relation());
        assert!(FieldKind::named("User", false).is_relation());
        assert!(!FieldKind::string().is_relation());
    }

    #[test]
    fn test_is_nillable() {
        // Scalars are nillable (except None and DocID)
        assert!(FieldKind::string().is_nillable());
        assert!(FieldKind::int().is_nillable());
        assert!(!FieldKind::doc_id().is_nillable());

        // Nillable arrays have nillable elements
        assert!(FieldKind::nillable_int_array().is_nillable());
        assert!(!FieldKind::int_array().is_nillable());
    }

    #[test]
    fn test_scalar_to_array() {
        assert_eq!(
            ScalarKind::Bool.to_array_kind(),
            Some(ScalarArrayKind::BoolArray)
        );
        assert_eq!(
            ScalarKind::Int.to_array_kind(),
            Some(ScalarArrayKind::IntArray)
        );
        assert_eq!(ScalarKind::DocID.to_array_kind(), None);
    }

    #[test]
    fn test_array_element_kind() {
        assert_eq!(ScalarArrayKind::IntArray.element_kind(), ScalarKind::Int);
        assert_eq!(
            ScalarArrayKind::NillableIntArray.element_kind(),
            ScalarKind::Int
        );
        assert_eq!(
            ScalarArrayKind::Float64Array.element_kind(),
            ScalarKind::Float64
        );
    }

    #[test]
    fn test_serialization_roundtrip() {
        let kinds = vec![
            FieldKind::doc_id(),
            FieldKind::bool(),
            FieldKind::int(),
            FieldKind::float64(),
            FieldKind::float32(),
            FieldKind::string(),
            FieldKind::int_array(),
            FieldKind::nillable_int_array(),
            FieldKind::relation("users", true),
            FieldKind::self_ref("parent", false),
            FieldKind::named("User", false),
        ];

        for kind in kinds {
            let json = serde_json::to_string(&kind).unwrap();
            let parsed: FieldKind = serde_json::from_str(&json).unwrap();
            assert_eq!(kind, parsed);
        }
    }
}
