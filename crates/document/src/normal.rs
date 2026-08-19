//! Normalized value types for documents
//!
//! NormalValue represents all possible field values in a type-safe enum.
//! This avoids runtime type assertions and provides compile-time guarantees.

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};

use crate::json_leaf::{JsonLeafValue, JsonScalarValue};
use crate::json_traverse::{index_traverse_options, traverse_json};

/// Normalized value type representing all possible field values.
///
/// This enum provides a type-safe way to handle document field values,
/// matching Go's NormalValue interface but as a concrete Rust enum.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(untagged)]
#[non_exhaustive]
pub enum NormalValue {
    // === Scalar types ===
    /// Null value
    #[default]
    Null,
    /// Boolean value
    Bool(bool),
    /// 64-bit signed integer
    Int(i64),
    /// 64-bit floating point
    Float64(f64),
    /// 32-bit floating point
    Float32(f32),
    /// String value
    String(String),
    /// Binary data
    Bytes(Vec<u8>),
    /// Timestamp with timezone
    Time(DateTime<FixedOffset>),
    /// Nested document
    Document(Box<crate::Document>),
    /// JSON value (for schemaless fields)
    Json(serde_json::Value),

    // === Nillable scalar types (Option<T>) ===
    /// Nillable boolean
    NillableBool(Option<bool>),
    /// Nillable integer
    NillableInt(Option<i64>),
    /// Nillable 64-bit float
    NillableFloat64(Option<f64>),
    /// Nillable 32-bit float
    NillableFloat32(Option<f32>),
    /// Nillable string
    NillableString(Option<String>),
    /// Nillable bytes
    NillableBytes(Option<Vec<u8>>),
    /// Nillable timestamp
    NillableTime(Option<DateTime<FixedOffset>>),
    /// Nillable document
    NillableDocument(Option<Box<crate::Document>>),

    // === Array types ===
    /// Array of booleans
    BoolArray(Vec<bool>),
    /// Array of integers
    IntArray(Vec<i64>),
    /// Array of 64-bit floats
    Float64Array(Vec<f64>),
    /// Array of 32-bit floats
    Float32Array(Vec<f32>),
    /// Array of strings
    StringArray(Vec<String>),
    /// Array of byte arrays
    BytesArray(Vec<Vec<u8>>),
    /// Array of timestamps
    TimeArray(Vec<DateTime<FixedOffset>>),
    /// Array of documents
    DocumentArray(Vec<crate::Document>),
    /// Array of JSON values
    JsonArray(Vec<serde_json::Value>),

    // === Nillable arrays (the whole array can be null) ===
    /// Nillable array of booleans
    NillableBoolArray(Option<Vec<bool>>),
    /// Nillable array of integers
    NillableIntArray(Option<Vec<i64>>),
    /// Nillable array of 64-bit floats
    NillableFloat64Array(Option<Vec<f64>>),
    /// Nillable array of 32-bit floats
    NillableFloat32Array(Option<Vec<f32>>),
    /// Nillable array of strings
    NillableStringArray(Option<Vec<String>>),
    /// Nillable array of bytes
    NillableBytesArray(Option<Vec<Vec<u8>>>),
    /// Nillable array of timestamps
    NillableTimeArray(Option<Vec<DateTime<FixedOffset>>>),
    /// Nillable array of documents
    NillableDocumentArray(Option<Vec<crate::Document>>),

    // === Arrays with nillable elements ===
    /// Array of nillable booleans
    NillableBoolElementArray(Vec<Option<bool>>),
    /// Array of nillable integers
    NillableIntElementArray(Vec<Option<i64>>),
    /// Array of nillable 64-bit floats
    NillableFloat64ElementArray(Vec<Option<f64>>),
    /// Array of nillable 32-bit floats
    NillableFloat32ElementArray(Vec<Option<f32>>),
    /// Array of nillable strings
    NillableStringElementArray(Vec<Option<String>>),
    /// Array of nillable bytes
    NillableBytesElementArray(Vec<Option<Vec<u8>>>),
    /// Array of nillable timestamps
    NillableTimeElementArray(Vec<Option<DateTime<FixedOffset>>>),
    /// Array of nillable documents
    NillableDocumentElementArray(Vec<Option<crate::Document>>),

    // === JSON indexing types ===
    /// JSON leaf value with path for indexing.
    /// Internal type used during index key generation.
    JsonLeaf(JsonLeafValue),
}

#[derive(Clone, Copy)]
struct NormalValueClassification {
    is_nil: bool,
    is_nillable: bool,
    is_array: bool,
}

macro_rules! normal_value_classifications {
    (
        $value:expr;
        scalar = [$($scalar:ident),+ $(,)?];
        nillable_scalar = [$($nillable_scalar:ident),+ $(,)?];
        array = [$($array:ident),+ $(,)?];
        nillable_array = [$($nillable_array:ident),+ $(,)?];
        nillable_element_array = [$($nillable_element_array:ident),+ $(,)?];
        misc = {
            $($variant:ident $(($pattern:pat))? => $classification:expr),+ $(,)?
        }
    ) => {
        match $value {
            $(NormalValue::$scalar(_) => NormalValueClassification {
                is_nil: false,
                is_nillable: false,
                is_array: false,
            },)+
            $(NormalValue::$nillable_scalar(None) => NormalValueClassification {
                is_nil: true,
                is_nillable: true,
                is_array: false,
            },
            NormalValue::$nillable_scalar(Some(_)) => NormalValueClassification {
                is_nil: false,
                is_nillable: true,
                is_array: false,
            },)+
            $(NormalValue::$array(_) => NormalValueClassification {
                is_nil: false,
                is_nillable: false,
                is_array: true,
            },)+
            $(NormalValue::$nillable_array(None) => NormalValueClassification {
                is_nil: true,
                is_nillable: true,
                is_array: true,
            },
            NormalValue::$nillable_array(Some(_)) => NormalValueClassification {
                is_nil: false,
                is_nillable: true,
                is_array: true,
            },)+
            $(NormalValue::$nillable_element_array(_) => NormalValueClassification {
                is_nil: false,
                is_nillable: true,
                is_array: true,
            },)+
            $(NormalValue::$variant $(($pattern))? => $classification,)+
        }
    };
}

impl NormalValue {
    fn classification(&self) -> NormalValueClassification {
        // Exhaustive by construction: adding a new enum variant must update this table.
        normal_value_classifications!(
            self;
            scalar = [
                Bool,
                Int,
                Float64,
                Float32,
                String,
                Bytes,
                Time,
                Document,
                Json,
            ];
            nillable_scalar = [
                NillableBool,
                NillableInt,
                NillableFloat64,
                NillableFloat32,
                NillableString,
                NillableBytes,
                NillableTime,
                NillableDocument,
            ];
            array = [
                BoolArray,
                IntArray,
                Float64Array,
                Float32Array,
                StringArray,
                BytesArray,
                TimeArray,
                DocumentArray,
                JsonArray,
            ];
            nillable_array = [
                NillableBoolArray,
                NillableIntArray,
                NillableFloat64Array,
                NillableFloat32Array,
                NillableStringArray,
                NillableBytesArray,
                NillableTimeArray,
                NillableDocumentArray,
            ];
            nillable_element_array = [
                NillableBoolElementArray,
                NillableIntElementArray,
                NillableFloat64ElementArray,
                NillableFloat32ElementArray,
                NillableStringElementArray,
                NillableBytesElementArray,
                NillableTimeElementArray,
                NillableDocumentElementArray,
            ];
            misc = {
                Null => NormalValueClassification {
                    is_nil: true,
                    is_nillable: true,
                    is_array: false,
                },
                JsonLeaf(_) => NormalValueClassification {
                    is_nil: false,
                    is_nillable: false,
                    is_array: false,
                }
            }
        )
    }

    /// Returns a reference to self wrapped in Some, or None if this is the Null variant.
    ///
    /// Note: This does NOT unwrap nillable variants like `NillableInt(None)`.
    /// For accessing inner values, use type-specific accessors like `as_int()`, `as_str()`, etc.
    pub fn unwrap(&self) -> Option<&NormalValue> {
        match self {
            NormalValue::Null => None,
            _ => Some(self),
        }
    }

    /// Returns true if this value is nil/null.
    pub fn is_nil(&self) -> bool {
        self.classification().is_nil
    }

    /// Returns true if this value type can be nil.
    pub fn is_nillable(&self) -> bool {
        self.classification().is_nillable
    }

    /// Returns true if this value is an array type.
    pub fn is_array(&self) -> bool {
        self.classification().is_array
    }

    // === Type-specific accessors ===

    /// Get as bool if this is a Bool variant.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            NormalValue::Bool(v) => Some(*v),
            NormalValue::NillableBool(Some(v)) => Some(*v),
            _ => None,
        }
    }

    /// Get as i64 if this is an Int variant.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            NormalValue::Int(v) => Some(*v),
            NormalValue::NillableInt(Some(v)) => Some(*v),
            _ => None,
        }
    }

    /// Get as f64 if this is a Float64 variant.
    pub fn as_float64(&self) -> Option<f64> {
        match self {
            NormalValue::Float64(v) => Some(*v),
            NormalValue::NillableFloat64(Some(v)) => Some(*v),
            _ => None,
        }
    }

    /// Get as f32 if this is a Float32 variant.
    pub fn as_float32(&self) -> Option<f32> {
        match self {
            NormalValue::Float32(v) => Some(*v),
            NormalValue::NillableFloat32(Some(v)) => Some(*v),
            _ => None,
        }
    }

    /// Get as &str if this is a String variant.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            NormalValue::String(v) => Some(v),
            NormalValue::NillableString(Some(v)) => Some(v),
            _ => None,
        }
    }

    /// Get as &[u8] if this is a Bytes variant.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            NormalValue::Bytes(v) => Some(v),
            NormalValue::NillableBytes(Some(v)) => Some(v),
            _ => None,
        }
    }

    /// Get as DateTime if this is a Time variant.
    pub fn as_time(&self) -> Option<&DateTime<FixedOffset>> {
        match self {
            NormalValue::Time(v) => Some(v),
            NormalValue::NillableTime(Some(v)) => Some(v),
            _ => None,
        }
    }

    /// Get as Document if this is a Document variant.
    pub fn as_document(&self) -> Option<&crate::Document> {
        match self {
            NormalValue::Document(v) => Some(v),
            NormalValue::NillableDocument(Some(v)) => Some(v),
            _ => None,
        }
    }

    /// Get as JSON Value if this is a Json variant.
    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            NormalValue::Json(v) => Some(v),
            _ => None,
        }
    }

    /// Get as JsonLeafValue if this is a JsonLeaf variant.
    pub fn as_json_leaf(&self) -> Option<&JsonLeafValue> {
        match self {
            NormalValue::JsonLeaf(v) => Some(v),
            _ => None,
        }
    }

    /// Extract all indexable leaf values from a JSON value.
    ///
    /// JSON values are traversed to find all leaf scalars (null, bool, number, string).
    /// Each leaf is returned as a JsonLeaf variant with its path through the JSON structure.
    ///
    /// # Behavior
    ///
    /// - Scalar JSON (null, bool, number, string): Returns single JsonLeaf with empty path
    /// - Nested objects: Returns JsonLeaf for each leaf with path through properties
    /// - Arrays: Returns JsonLeaf for each element with Index marker in path
    /// - Empty objects/arrays: Returns empty vec (no index entries)
    /// - Null JSON field: Returns single Null entry
    pub fn json_leaves(&self) -> Vec<NormalValue> {
        match self {
            NormalValue::Json(json) => {
                if json.is_null() {
                    return vec![NormalValue::Null];
                }
                extract_json_leaves(json)
            }
            _ => vec![],
        }
    }
}

/// Extract all leaf values from a JSON value for indexing.
fn extract_json_leaves(json: &serde_json::Value) -> Vec<NormalValue> {
    let mut leaves = Vec::new();
    let options = index_traverse_options();

    let _ = traverse_json(
        json,
        |path, value| {
            if let Some(scalar) = JsonScalarValue::from_json_value(value) {
                leaves.push(NormalValue::JsonLeaf(JsonLeafValue::new(
                    path.clone(),
                    scalar,
                )));
            }
            Ok(())
        },
        &options,
    );

    leaves
}
