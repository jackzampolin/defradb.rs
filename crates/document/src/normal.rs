//! Normalized value types for documents
//!
//! NormalValue represents all possible field values in a type-safe enum.
//! This avoids runtime type assertions and provides compile-time guarantees.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Normalized value type representing all possible field values.
///
/// This enum provides a type-safe way to handle document field values,
/// matching Go's NormalValue interface but as a concrete Rust enum.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(untagged)]
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
    Time(DateTime<Utc>),
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
    NillableTime(Option<DateTime<Utc>>),
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
    TimeArray(Vec<DateTime<Utc>>),
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
    NillableTimeArray(Option<Vec<DateTime<Utc>>>),
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
    NillableTimeElementArray(Vec<Option<DateTime<Utc>>>),
    /// Array of nillable documents
    NillableDocumentElementArray(Vec<Option<crate::Document>>),
}

impl NormalValue {
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
        matches!(self, NormalValue::Null)
            || matches!(self, NormalValue::NillableBool(None))
            || matches!(self, NormalValue::NillableInt(None))
            || matches!(self, NormalValue::NillableFloat64(None))
            || matches!(self, NormalValue::NillableFloat32(None))
            || matches!(self, NormalValue::NillableString(None))
            || matches!(self, NormalValue::NillableBytes(None))
            || matches!(self, NormalValue::NillableTime(None))
            || matches!(self, NormalValue::NillableDocument(None))
            || matches!(self, NormalValue::NillableBoolArray(None))
            || matches!(self, NormalValue::NillableIntArray(None))
            || matches!(self, NormalValue::NillableFloat64Array(None))
            || matches!(self, NormalValue::NillableFloat32Array(None))
            || matches!(self, NormalValue::NillableStringArray(None))
            || matches!(self, NormalValue::NillableBytesArray(None))
            || matches!(self, NormalValue::NillableTimeArray(None))
            || matches!(self, NormalValue::NillableDocumentArray(None))
    }

    /// Returns true if this value type can be nil.
    pub fn is_nillable(&self) -> bool {
        matches!(
            self,
            NormalValue::Null
                | NormalValue::NillableBool(_)
                | NormalValue::NillableInt(_)
                | NormalValue::NillableFloat64(_)
                | NormalValue::NillableFloat32(_)
                | NormalValue::NillableString(_)
                | NormalValue::NillableBytes(_)
                | NormalValue::NillableTime(_)
                | NormalValue::NillableDocument(_)
                | NormalValue::NillableBoolArray(_)
                | NormalValue::NillableIntArray(_)
                | NormalValue::NillableFloat64Array(_)
                | NormalValue::NillableFloat32Array(_)
                | NormalValue::NillableStringArray(_)
                | NormalValue::NillableBytesArray(_)
                | NormalValue::NillableTimeArray(_)
                | NormalValue::NillableDocumentArray(_)
                | NormalValue::NillableBoolElementArray(_)
                | NormalValue::NillableIntElementArray(_)
                | NormalValue::NillableFloat64ElementArray(_)
                | NormalValue::NillableFloat32ElementArray(_)
                | NormalValue::NillableStringElementArray(_)
                | NormalValue::NillableBytesElementArray(_)
                | NormalValue::NillableTimeElementArray(_)
                | NormalValue::NillableDocumentElementArray(_)
        )
    }

    /// Returns true if this value is an array type.
    pub fn is_array(&self) -> bool {
        matches!(
            self,
            NormalValue::BoolArray(_)
                | NormalValue::IntArray(_)
                | NormalValue::Float64Array(_)
                | NormalValue::Float32Array(_)
                | NormalValue::StringArray(_)
                | NormalValue::BytesArray(_)
                | NormalValue::TimeArray(_)
                | NormalValue::DocumentArray(_)
                | NormalValue::JsonArray(_)
                | NormalValue::NillableBoolArray(_)
                | NormalValue::NillableIntArray(_)
                | NormalValue::NillableFloat64Array(_)
                | NormalValue::NillableFloat32Array(_)
                | NormalValue::NillableStringArray(_)
                | NormalValue::NillableBytesArray(_)
                | NormalValue::NillableTimeArray(_)
                | NormalValue::NillableDocumentArray(_)
                | NormalValue::NillableBoolElementArray(_)
                | NormalValue::NillableIntElementArray(_)
                | NormalValue::NillableFloat64ElementArray(_)
                | NormalValue::NillableFloat32ElementArray(_)
                | NormalValue::NillableStringElementArray(_)
                | NormalValue::NillableBytesElementArray(_)
                | NormalValue::NillableTimeElementArray(_)
                | NormalValue::NillableDocumentElementArray(_)
        )
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
    pub fn as_time(&self) -> Option<&DateTime<Utc>> {
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
}

// === From implementations for convenient construction ===

impl From<bool> for NormalValue {
    fn from(v: bool) -> Self {
        NormalValue::Bool(v)
    }
}

impl From<i64> for NormalValue {
    fn from(v: i64) -> Self {
        NormalValue::Int(v)
    }
}

impl From<i32> for NormalValue {
    fn from(v: i32) -> Self {
        NormalValue::Int(v as i64)
    }
}

impl From<f64> for NormalValue {
    fn from(v: f64) -> Self {
        NormalValue::Float64(v)
    }
}

impl From<f32> for NormalValue {
    fn from(v: f32) -> Self {
        NormalValue::Float32(v)
    }
}

impl From<String> for NormalValue {
    fn from(v: String) -> Self {
        NormalValue::String(v)
    }
}

impl From<&str> for NormalValue {
    fn from(v: &str) -> Self {
        NormalValue::String(v.to_string())
    }
}

impl From<Vec<u8>> for NormalValue {
    fn from(v: Vec<u8>) -> Self {
        NormalValue::Bytes(v)
    }
}

impl From<DateTime<Utc>> for NormalValue {
    fn from(v: DateTime<Utc>) -> Self {
        NormalValue::Time(v)
    }
}

impl From<serde_json::Value> for NormalValue {
    fn from(v: serde_json::Value) -> Self {
        NormalValue::Json(v)
    }
}

impl From<Vec<String>> for NormalValue {
    fn from(v: Vec<String>) -> Self {
        NormalValue::StringArray(v)
    }
}

impl From<Vec<i64>> for NormalValue {
    fn from(v: Vec<i64>) -> Self {
        NormalValue::IntArray(v)
    }
}

impl From<Vec<bool>> for NormalValue {
    fn from(v: Vec<bool>) -> Self {
        NormalValue::BoolArray(v)
    }
}

impl From<Vec<f64>> for NormalValue {
    fn from(v: Vec<f64>) -> Self {
        NormalValue::Float64Array(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_nil() {
        assert!(NormalValue::Null.is_nil());
        assert!(NormalValue::NillableBool(None).is_nil());
        assert!(NormalValue::NillableString(None).is_nil());
        assert!(!NormalValue::Bool(true).is_nil());
        assert!(!NormalValue::NillableBool(Some(true)).is_nil());
    }

    #[test]
    fn test_is_nillable() {
        assert!(NormalValue::Null.is_nillable());
        assert!(NormalValue::NillableBool(Some(true)).is_nillable());
        assert!(!NormalValue::Bool(true).is_nillable());
        assert!(!NormalValue::Int(42).is_nillable());
    }

    #[test]
    fn test_is_array() {
        assert!(NormalValue::IntArray(vec![1, 2, 3]).is_array());
        assert!(NormalValue::StringArray(vec!["a".into()]).is_array());
        assert!(!NormalValue::Int(42).is_array());
        assert!(!NormalValue::String("hello".into()).is_array());
    }

    #[test]
    fn test_as_bool() {
        assert_eq!(NormalValue::Bool(true).as_bool(), Some(true));
        assert_eq!(
            NormalValue::NillableBool(Some(false)).as_bool(),
            Some(false)
        );
        assert_eq!(NormalValue::NillableBool(None).as_bool(), None);
        assert_eq!(NormalValue::Int(1).as_bool(), None);
    }

    #[test]
    fn test_as_int() {
        assert_eq!(NormalValue::Int(42).as_int(), Some(42));
        assert_eq!(NormalValue::NillableInt(Some(100)).as_int(), Some(100));
        assert_eq!(NormalValue::NillableInt(None).as_int(), None);
        assert_eq!(NormalValue::String("42".into()).as_int(), None);
    }

    #[test]
    fn test_as_str() {
        assert_eq!(NormalValue::String("hello".into()).as_str(), Some("hello"));
        assert_eq!(
            NormalValue::NillableString(Some("world".into())).as_str(),
            Some("world")
        );
        assert_eq!(NormalValue::NillableString(None).as_str(), None);
        assert_eq!(NormalValue::Int(42).as_str(), None);
    }

    #[test]
    fn test_from_implementations() {
        assert_eq!(NormalValue::from(true), NormalValue::Bool(true));
        assert_eq!(NormalValue::from(42i64), NormalValue::Int(42));
        assert_eq!(NormalValue::from(3.14f64), NormalValue::Float64(3.14));
        assert_eq!(
            NormalValue::from("hello"),
            NormalValue::String("hello".into())
        );
    }

    #[test]
    fn test_default() {
        assert_eq!(NormalValue::default(), NormalValue::Null);
    }
}
