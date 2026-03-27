//! From implementations for convenient NormalValue construction.

use chrono::{DateTime, FixedOffset};

use crate::json_leaf::JsonLeafValue;
use crate::NormalValue;

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

impl From<DateTime<FixedOffset>> for NormalValue {
    fn from(v: DateTime<FixedOffset>) -> Self {
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

impl From<JsonLeafValue> for NormalValue {
    fn from(v: JsonLeafValue) -> Self {
        NormalValue::JsonLeaf(v)
    }
}
