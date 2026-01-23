//! JSON conversion utilities for NormalValue types.
//!
//! This module handles converting document field values to JSON format.

use chrono::SecondsFormat;
use document::NormalValue;
use serde_json::Value as JsonValue;
use std::fmt::Write;

use crate::error::{QueryError, Result};

/// Convert NormalValue to JSON.
pub fn normal_value_to_json(value: &NormalValue) -> Result<JsonValue> {
    match value {
        NormalValue::Null => Ok(JsonValue::Null),
        NormalValue::Bool(b) => Ok(JsonValue::Bool(*b)),
        NormalValue::Int(i) => Ok(JsonValue::Number((*i).into())),
        NormalValue::Float64(f) => float64_to_json(*f),
        NormalValue::Float32(f) => float32_to_json(*f),
        NormalValue::String(s) => Ok(JsonValue::String(s.clone())),
        NormalValue::Bytes(b) => bytes_to_json(b),
        NormalValue::Time(t) => Ok(JsonValue::String(
            t.to_rfc3339_opts(SecondsFormat::Secs, true),
        )),
        NormalValue::Json(j) => Ok(j.clone()),
        NormalValue::IntArray(arr) => Ok(JsonValue::Array(
            arr.iter().map(|i| JsonValue::Number((*i).into())).collect(),
        )),
        NormalValue::StringArray(arr) => Ok(JsonValue::Array(
            arr.iter().map(|s| JsonValue::String(s.clone())).collect(),
        )),
        NormalValue::BoolArray(arr) => Ok(JsonValue::Array(
            arr.iter().map(|b| JsonValue::Bool(*b)).collect(),
        )),
        NormalValue::Float64Array(arr) => float64_array_to_json(arr),
        NormalValue::Float32Array(arr) => float32_array_to_json(arr),
        NormalValue::NillableBool(opt) => Ok(opt.map(JsonValue::Bool).unwrap_or(JsonValue::Null)),
        NormalValue::NillableInt(opt) => Ok(opt
            .map(|i| JsonValue::Number(i.into()))
            .unwrap_or(JsonValue::Null)),
        NormalValue::NillableString(opt) => Ok(opt
            .as_ref()
            .map(|s| JsonValue::String(s.clone()))
            .unwrap_or(JsonValue::Null)),
        NormalValue::NillableFloat64(opt) => nillable_float64_to_json(*opt),
        NormalValue::NillableFloat32(opt) => nillable_float32_to_json(*opt),
        NormalValue::NillableTime(opt) => Ok(opt
            .as_ref()
            .map(|t| JsonValue::String(t.to_rfc3339_opts(SecondsFormat::Secs, true)))
            .unwrap_or(JsonValue::Null)),
        NormalValue::Document(doc) => Ok(JsonValue::String(format!("<document:{:?}>", doc.id()))),
        NormalValue::DocumentArray(docs) => Ok(JsonValue::Array(
            docs.iter()
                .map(|d| JsonValue::String(format!("<document:{:?}>", d.id())))
                .collect(),
        )),
        NormalValue::NillableBytes(opt) => nillable_bytes_to_json(opt.as_ref()),
        NormalValue::NillableDocument(opt) => Ok(opt
            .as_ref()
            .map(|d| JsonValue::String(format!("<document:{:?}>", d.id())))
            .unwrap_or(JsonValue::Null)),
        NormalValue::BytesArray(arr) => bytes_array_to_json(arr),
        NormalValue::TimeArray(arr) => Ok(JsonValue::Array(
            arr.iter()
                .map(|t| JsonValue::String(t.to_rfc3339_opts(SecondsFormat::Secs, true)))
                .collect(),
        )),
        NormalValue::JsonArray(arr) => Ok(JsonValue::Array(arr.clone())),
        NormalValue::NillableIntArray(opt) => Ok(opt
            .as_ref()
            .map(|arr| {
                JsonValue::Array(arr.iter().map(|i| JsonValue::Number((*i).into())).collect())
            })
            .unwrap_or(JsonValue::Null)),
        NormalValue::NillableStringArray(opt) => Ok(opt
            .as_ref()
            .map(|arr| JsonValue::Array(arr.iter().map(|s| JsonValue::String(s.clone())).collect()))
            .unwrap_or(JsonValue::Null)),
        NormalValue::NillableBoolArray(opt) => Ok(opt
            .as_ref()
            .map(|arr| JsonValue::Array(arr.iter().map(|b| JsonValue::Bool(*b)).collect()))
            .unwrap_or(JsonValue::Null)),
        NormalValue::NillableFloat64Array(opt) => nillable_float64_array_to_json(opt.as_ref()),
        NormalValue::NillableFloat32Array(opt) => nillable_float32_array_to_json(opt.as_ref()),
        NormalValue::NillableBytesArray(opt) => nillable_bytes_array_to_json(opt.as_ref()),
        NormalValue::NillableTimeArray(opt) => Ok(opt
            .as_ref()
            .map(|arr| {
                JsonValue::Array(
                    arr.iter()
                        .map(|t| JsonValue::String(t.to_rfc3339_opts(SecondsFormat::Secs, true)))
                        .collect(),
                )
            })
            .unwrap_or(JsonValue::Null)),
        NormalValue::NillableDocumentArray(opt) => Ok(opt
            .as_ref()
            .map(|arr| {
                JsonValue::Array(
                    arr.iter()
                        .map(|d| JsonValue::String(format!("<document:{:?}>", d.id())))
                        .collect(),
                )
            })
            .unwrap_or(JsonValue::Null)),
        NormalValue::NillableBoolElementArray(arr) => Ok(JsonValue::Array(
            arr.iter()
                .map(|opt| opt.map(JsonValue::Bool).unwrap_or(JsonValue::Null))
                .collect(),
        )),
        NormalValue::NillableIntElementArray(arr) => Ok(JsonValue::Array(
            arr.iter()
                .map(|opt| {
                    opt.map(|i| JsonValue::Number(i.into()))
                        .unwrap_or(JsonValue::Null)
                })
                .collect(),
        )),
        NormalValue::NillableFloat64ElementArray(arr) => {
            nillable_float64_element_array_to_json(arr)
        }
        NormalValue::NillableFloat32ElementArray(arr) => {
            nillable_float32_element_array_to_json(arr)
        }
        NormalValue::NillableStringElementArray(arr) => Ok(JsonValue::Array(
            arr.iter()
                .map(|opt| {
                    opt.as_ref()
                        .map(|s| JsonValue::String(s.clone()))
                        .unwrap_or(JsonValue::Null)
                })
                .collect(),
        )),
        NormalValue::NillableBytesElementArray(arr) => nillable_bytes_element_array_to_json(arr),
        NormalValue::NillableTimeElementArray(arr) => Ok(JsonValue::Array(
            arr.iter()
                .map(|opt| {
                    opt.as_ref()
                        .map(|t| JsonValue::String(t.to_rfc3339_opts(SecondsFormat::Secs, true)))
                        .unwrap_or(JsonValue::Null)
                })
                .collect(),
        )),
        NormalValue::NillableDocumentElementArray(arr) => Ok(JsonValue::Array(
            arr.iter()
                .map(|opt| {
                    opt.as_ref()
                        .map(|d| JsonValue::String(format!("<document:{:?}>", d.id())))
                        .unwrap_or(JsonValue::Null)
                })
                .collect(),
        )),
    }
}

fn float64_to_json(f: f64) -> Result<JsonValue> {
    serde_json::Number::from_f64(f)
        .map(JsonValue::Number)
        .ok_or_else(|| {
            QueryError::execution(format!(
                "cannot serialize non-finite float64 value '{}' to JSON",
                f
            ))
        })
}

fn float32_to_json(f: f32) -> Result<JsonValue> {
    serde_json::Number::from_f64(f as f64)
        .map(JsonValue::Number)
        .ok_or_else(|| {
            QueryError::execution(format!(
                "cannot serialize non-finite float32 value '{}' to JSON",
                f
            ))
        })
}

fn nillable_float64_to_json(opt: Option<f64>) -> Result<JsonValue> {
    match opt {
        Some(f) => float64_to_json(f),
        None => Ok(JsonValue::Null),
    }
}

fn nillable_float32_to_json(opt: Option<f32>) -> Result<JsonValue> {
    match opt {
        Some(f) => float32_to_json(f),
        None => Ok(JsonValue::Null),
    }
}

fn float64_array_to_json(arr: &[f64]) -> Result<JsonValue> {
    let values: Result<Vec<_>> = arr.iter().map(|f| float64_to_json(*f)).collect();
    Ok(JsonValue::Array(values?))
}

fn float32_array_to_json(arr: &[f32]) -> Result<JsonValue> {
    let values: Result<Vec<_>> = arr.iter().map(|f| float32_to_json(*f)).collect();
    Ok(JsonValue::Array(values?))
}

fn nillable_float64_array_to_json(opt: Option<&Vec<f64>>) -> Result<JsonValue> {
    match opt {
        Some(arr) => float64_array_to_json(arr),
        None => Ok(JsonValue::Null),
    }
}

fn nillable_float32_array_to_json(opt: Option<&Vec<f32>>) -> Result<JsonValue> {
    match opt {
        Some(arr) => float32_array_to_json(arr),
        None => Ok(JsonValue::Null),
    }
}

fn nillable_float64_element_array_to_json(arr: &[Option<f64>]) -> Result<JsonValue> {
    let values: Result<Vec<_>> = arr
        .iter()
        .map(|opt| match opt {
            Some(f) => float64_to_json(*f),
            None => Ok(JsonValue::Null),
        })
        .collect();
    Ok(JsonValue::Array(values?))
}

fn nillable_float32_element_array_to_json(arr: &[Option<f32>]) -> Result<JsonValue> {
    let values: Result<Vec<_>> = arr
        .iter()
        .map(|opt| match opt {
            Some(f) => float32_to_json(*f),
            None => Ok(JsonValue::Null),
        })
        .collect();
    Ok(JsonValue::Array(values?))
}

fn bytes_to_json(b: &[u8]) -> Result<JsonValue> {
    let mut buf = String::with_capacity(b.len() * 2);
    for byte in b {
        write!(buf, "{:02x}", byte)
            .map_err(|e| QueryError::execution(format!("failed to encode bytes: {}", e)))?;
    }
    Ok(JsonValue::String(buf))
}

fn nillable_bytes_to_json(opt: Option<&Vec<u8>>) -> Result<JsonValue> {
    match opt {
        Some(b) => bytes_to_json(b),
        None => Ok(JsonValue::Null),
    }
}

fn bytes_array_to_json(arr: &[Vec<u8>]) -> Result<JsonValue> {
    let values: Result<Vec<_>> = arr.iter().map(|b| bytes_to_json(b)).collect();
    Ok(JsonValue::Array(values?))
}

fn nillable_bytes_array_to_json(opt: Option<&Vec<Vec<u8>>>) -> Result<JsonValue> {
    match opt {
        Some(arr) => bytes_array_to_json(arr),
        None => Ok(JsonValue::Null),
    }
}

fn nillable_bytes_element_array_to_json(arr: &[Option<Vec<u8>>]) -> Result<JsonValue> {
    let values: Result<Vec<_>> = arr
        .iter()
        .map(|opt| match opt {
            Some(b) => bytes_to_json(b),
            None => Ok(JsonValue::Null),
        })
        .collect();
    Ok(JsonValue::Array(values?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_float64_to_json_valid() {
        assert_eq!(float64_to_json(3.14).unwrap(), serde_json::json!(3.14));
        assert_eq!(float64_to_json(0.0).unwrap(), serde_json::json!(0.0));
        assert_eq!(float64_to_json(-42.5).unwrap(), serde_json::json!(-42.5));
    }

    #[test]
    fn test_float64_to_json_nan_returns_error() {
        let result = float64_to_json(f64::NAN);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-finite"));
    }

    #[test]
    fn test_float64_to_json_infinity_returns_error() {
        let result = float64_to_json(f64::INFINITY);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-finite"));
    }

    #[test]
    fn test_float64_to_json_neg_infinity_returns_error() {
        let result = float64_to_json(f64::NEG_INFINITY);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-finite"));
    }

    #[test]
    fn test_float64_array_with_nan_returns_error() {
        let arr = vec![1.0, f64::NAN, 3.0];
        let result = float64_array_to_json(&arr);
        assert!(result.is_err());
    }

    #[test]
    fn test_bytes_to_json() {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        let result = bytes_to_json(&bytes).unwrap();
        assert_eq!(result, JsonValue::String("deadbeef".to_string()));
    }

    #[test]
    fn test_normal_value_null() {
        assert_eq!(
            normal_value_to_json(&NormalValue::Null).unwrap(),
            JsonValue::Null
        );
    }

    #[test]
    fn test_normal_value_bool() {
        assert_eq!(
            normal_value_to_json(&NormalValue::Bool(true)).unwrap(),
            JsonValue::Bool(true)
        );
    }

    #[test]
    fn test_normal_value_int() {
        assert_eq!(
            normal_value_to_json(&NormalValue::Int(42)).unwrap(),
            serde_json::json!(42)
        );
    }

    #[test]
    fn test_normal_value_string() {
        assert_eq!(
            normal_value_to_json(&NormalValue::String("hello".to_string())).unwrap(),
            JsonValue::String("hello".to_string())
        );
    }
}
