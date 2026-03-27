//! Field kind definitions matching Go DefraDB exactly for datastore compatibility.
//!
//! The numeric values here MUST match the Go implementation to ensure
//! Rust and Go can read/write the same datastores.
//!
//! # JSON Serialization Format (Go-compatible)
//!
//! This module implements custom serde serialization that matches Go DefraDB's format:
//!
//! - **ScalarKind**: Serialized as just the integer value (e.g., `2` for Bool)
//! - **ScalarArrayKind**: Serialized as just the integer value (e.g., `3` for BoolArray)
//! - **CollectionKind**: Serialized as `{"Array": bool, "CollectionID": string}`
//! - **SelfKind**: Serialized as `{"RelativeID": string, "Array": bool}`
//! - **NamedKind**: Serialized as `{"Name": string, "Array": bool}`
//!
//! For deserialization, the format accepts:
//! - Numbers → ScalarKind or ScalarArrayKind (based on value)
//! - Strings → Mapped to FieldKind using Go's string mappings or NamedKind
//! - Objects → CollectionKind, SelfKind, or NamedKind based on fields present

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

/// Scalar field kinds with numeric values matching Go DefraDB.
///
/// These values are stored in the datastore, so they must match exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[non_exhaustive]
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
#[non_exhaustive]
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
    /// Returns true if this array contains elements that can be null.
    /// For example, NillableIntArray can contain null values, but IntArray cannot.
    pub fn has_nillable_elements(&self) -> bool {
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
///
/// Uses custom serde implementation for Go compatibility.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
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

impl Default for FieldKind {
    fn default() -> Self {
        FieldKind::Scalar(ScalarKind::None)
    }
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
    /// In Go DefraDB, ALL types return true for IsNillable().
    pub fn is_nillable(&self) -> bool {
        true
    }

    /// Returns true if this array type has nillable elements.
    /// This is different from is_nillable() which checks if the field itself can be null.
    pub fn has_nillable_elements(&self) -> bool {
        match self {
            FieldKind::ScalarArray(a) => a.has_nillable_elements(),
            _ => false,
        }
    }

    /// Returns true if this is an object/relation type (matches Go's IsObject)
    pub fn is_object(&self) -> bool {
        self.is_relation()
    }

    /// Get the referenced collection ID/name if this is a relation
    ///
    /// Returns the target collection identifier for relation types:
    /// - `Relation`: Returns the `collection_id`
    /// - `SelfRef`: Returns the `relative_id` (typically the same collection)
    /// - `Named`: Returns the `name` (unresolved collection reference)
    /// - Other types: Returns `None`
    pub fn relation_collection_id(&self) -> Option<&str> {
        match self {
            FieldKind::Relation { collection_id, .. } => Some(collection_id),
            FieldKind::SelfRef { relative_id, .. } => Some(relative_id),
            FieldKind::Named { name, .. } => Some(name),
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

    /// Get the GraphQL type name for this field kind
    pub fn graphql_type_name(&self) -> &'static str {
        match self {
            FieldKind::Scalar(s) => match s {
                ScalarKind::None => "String",
                ScalarKind::DocID => "ID",
                ScalarKind::Bool => "Boolean",
                ScalarKind::Int => "Int",
                ScalarKind::Float64 | ScalarKind::Float32 => "Float",
                ScalarKind::DateTime => "DateTime",
                ScalarKind::String => "String",
                ScalarKind::Blob => "Blob",
                ScalarKind::Json => "JSON",
            },
            FieldKind::ScalarArray(a) => match a {
                ScalarArrayKind::BoolArray | ScalarArrayKind::NillableBoolArray => "[Boolean]",
                ScalarArrayKind::IntArray | ScalarArrayKind::NillableIntArray => "[Int]",
                ScalarArrayKind::Float64Array
                | ScalarArrayKind::Float32Array
                | ScalarArrayKind::NillableFloat64Array
                | ScalarArrayKind::NillableFloat32Array => "[Float]",
                ScalarArrayKind::StringArray | ScalarArrayKind::NillableStringArray => "[String]",
            },
            FieldKind::Relation { is_array, .. } => {
                if *is_array {
                    "[Object]"
                } else {
                    "Object"
                }
            }
            FieldKind::SelfRef { is_array, .. } => {
                if *is_array {
                    "[Object]"
                } else {
                    "Object"
                }
            }
            FieldKind::Named { is_array, .. } => {
                if *is_array {
                    "[Object]"
                } else {
                    "Object"
                }
            }
        }
    }
}

// ============================================================================
// Go-compatible JSON serialization
// ============================================================================

impl Serialize for FieldKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;

        match self {
            // Scalars serialize as just the integer value
            FieldKind::Scalar(kind) => serializer.serialize_u8(*kind as u8),

            // Arrays serialize as just the integer value
            FieldKind::ScalarArray(kind) => serializer.serialize_u8(*kind as u8),

            // Relation serializes as {"Array": bool, "CollectionID": string}
            FieldKind::Relation {
                collection_id,
                is_array,
            } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("Array", is_array)?;
                map.serialize_entry("CollectionID", collection_id)?;
                map.end()
            }

            // SelfRef serializes as {"RelativeID": string, "Array": bool}
            FieldKind::SelfRef {
                relative_id,
                is_array,
            } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("RelativeID", relative_id)?;
                map.serialize_entry("Array", is_array)?;
                map.end()
            }

            // Named serializes as {"Name": string, "Array": bool}
            FieldKind::Named { name, is_array } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("Name", name)?;
                map.serialize_entry("Array", is_array)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for FieldKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Use an untagged approach: try different formats
        let value = serde_json::Value::deserialize(deserializer)?;

        match &value {
            // Number → ScalarKind or ScalarArrayKind
            serde_json::Value::Number(n) => {
                let kind = n.as_u64().ok_or_else(|| {
                    de::Error::custom("FieldKind integer must be a positive number")
                })? as u8;
                Ok(int_to_field_kind(kind))
            }

            // String → Use Go's string mapping or NamedKind
            serde_json::Value::String(s) => parse_string_kind(s).map_err(de::Error::custom),

            // Object → CollectionKind, SelfKind, or NamedKind
            serde_json::Value::Object(map) => {
                // Check for CollectionID → Relation
                if map.contains_key("CollectionID") {
                    let collection_id = map
                        .get("CollectionID")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let is_array = map.get("Array").and_then(|v| v.as_bool()).unwrap_or(false);
                    return Ok(FieldKind::Relation {
                        collection_id,
                        is_array,
                    });
                }

                // Check for RelativeID → SelfRef
                if map.contains_key("RelativeID") {
                    let relative_id = map
                        .get("RelativeID")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let is_array = map.get("Array").and_then(|v| v.as_bool()).unwrap_or(false);
                    return Ok(FieldKind::SelfRef {
                        relative_id,
                        is_array,
                    });
                }

                // Check for Name → Named
                if map.contains_key("Name") {
                    let name = map
                        .get("Name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let is_array = map.get("Array").and_then(|v| v.as_bool()).unwrap_or(false);
                    return Ok(FieldKind::Named { name, is_array });
                }

                Err(de::Error::custom(
                    "Unknown FieldKind object format: expected CollectionID, RelativeID, or Name",
                ))
            }

            // Null → None scalar
            serde_json::Value::Null => Ok(FieldKind::Scalar(ScalarKind::None)),

            _ => Err(de::Error::custom(format!(
                "Invalid FieldKind format: expected number, string, or object, got {:?}",
                value
            ))),
        }
    }
}

/// Convert an integer to a FieldKind (matches Go's IntToFieldKind)
fn int_to_field_kind(kind: u8) -> FieldKind {
    // Array kinds
    match kind {
        3 => FieldKind::ScalarArray(ScalarArrayKind::BoolArray),
        5 => FieldKind::ScalarArray(ScalarArrayKind::IntArray),
        7 => FieldKind::ScalarArray(ScalarArrayKind::Float64Array),
        9 => FieldKind::ScalarArray(ScalarArrayKind::Float32Array),
        12 => FieldKind::ScalarArray(ScalarArrayKind::StringArray),
        18 => FieldKind::ScalarArray(ScalarArrayKind::NillableBoolArray),
        19 => FieldKind::ScalarArray(ScalarArrayKind::NillableIntArray),
        20 => FieldKind::ScalarArray(ScalarArrayKind::NillableFloat64Array),
        21 => FieldKind::ScalarArray(ScalarArrayKind::NillableStringArray),
        22 => FieldKind::ScalarArray(ScalarArrayKind::NillableFloat32Array),
        // Scalar kinds
        0 => FieldKind::Scalar(ScalarKind::None),
        1 => FieldKind::Scalar(ScalarKind::DocID),
        2 => FieldKind::Scalar(ScalarKind::Bool),
        4 => FieldKind::Scalar(ScalarKind::Int),
        6 => FieldKind::Scalar(ScalarKind::Float64),
        8 => FieldKind::Scalar(ScalarKind::Float32),
        10 => FieldKind::Scalar(ScalarKind::DateTime),
        11 => FieldKind::Scalar(ScalarKind::String),
        13 => FieldKind::Scalar(ScalarKind::Blob),
        14 => FieldKind::Scalar(ScalarKind::Json),
        // Unknown → treat as scalar
        _ => FieldKind::Scalar(ScalarKind::None),
    }
}

/// Parse a string to FieldKind (matches Go's FieldKindStringToEnumMapping)
fn parse_string_kind(s: &str) -> Result<FieldKind, String> {
    // Go's FieldKindStringToEnumMapping
    match s {
        "ID" => Ok(FieldKind::Scalar(ScalarKind::DocID)),
        "Boolean" => Ok(FieldKind::Scalar(ScalarKind::Bool)),
        "Int" => Ok(FieldKind::Scalar(ScalarKind::Int)),
        "DateTime" => Ok(FieldKind::Scalar(ScalarKind::DateTime)),
        "Float" | "Float64" => Ok(FieldKind::Scalar(ScalarKind::Float64)),
        "Float32" => Ok(FieldKind::Scalar(ScalarKind::Float32)),
        "String" => Ok(FieldKind::Scalar(ScalarKind::String)),
        "Blob" => Ok(FieldKind::Scalar(ScalarKind::Blob)),
        "JSON" => Ok(FieldKind::Scalar(ScalarKind::Json)),
        // Arrays
        "[Boolean]" => Ok(FieldKind::ScalarArray(ScalarArrayKind::NillableBoolArray)),
        "[Boolean!]" => Ok(FieldKind::ScalarArray(ScalarArrayKind::BoolArray)),
        "[Int]" => Ok(FieldKind::ScalarArray(ScalarArrayKind::NillableIntArray)),
        "[Int!]" => Ok(FieldKind::ScalarArray(ScalarArrayKind::IntArray)),
        "[Float]" | "[Float64]" => Ok(FieldKind::ScalarArray(
            ScalarArrayKind::NillableFloat64Array,
        )),
        "[Float!]" | "[Float64!]" => Ok(FieldKind::ScalarArray(ScalarArrayKind::Float64Array)),
        "[Float32]" => Ok(FieldKind::ScalarArray(
            ScalarArrayKind::NillableFloat32Array,
        )),
        "[Float32!]" => Ok(FieldKind::ScalarArray(ScalarArrayKind::Float32Array)),
        "[String]" => Ok(FieldKind::ScalarArray(ScalarArrayKind::NillableStringArray)),
        "[String!]" => Ok(FieldKind::ScalarArray(ScalarArrayKind::StringArray)),
        // Self reference (Go uses "Self" from request.SelfTypeName)
        "Self" => Ok(FieldKind::SelfRef {
            relative_id: String::new(),
            is_array: false,
        }),
        "[Self]" => Ok(FieldKind::SelfRef {
            relative_id: String::new(),
            is_array: true,
        }),
        // Otherwise treat as named reference (with array check)
        _ => {
            let is_array = s.starts_with('[') && s.ends_with(']');
            let name = if is_array {
                s[1..s.len() - 1].to_string()
            } else {
                s.to_string()
            };
            Ok(FieldKind::Named { name, is_array })
        }
    }
}
