// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Encoding helpers for JSON and CBOR conversion

use crate::error::{Error, Result};
use crate::NormalValue;

/// Convert a JSON value to a NormalValue.
///
/// Returns an error if a JSON number cannot be represented as i64 or f64.
pub fn json_to_normal_value(value: serde_json::Value) -> Result<NormalValue> {
    match value {
        serde_json::Value::Null => Ok(NormalValue::Null),
        serde_json::Value::Bool(b) => Ok(NormalValue::Bool(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(NormalValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(NormalValue::Float64(f))
            } else {
                Err(Error::JsonNumberOutOfRange(n.to_string()))
            }
        }
        serde_json::Value::String(s) => Ok(NormalValue::String(s)),
        serde_json::Value::Array(arr) => {
            // Try to infer array type from first element
            if arr.is_empty() {
                return Ok(NormalValue::JsonArray(vec![]));
            }

            match &arr[0] {
                serde_json::Value::Bool(_) => {
                    let mut bools = Vec::with_capacity(arr.len());
                    for v in &arr {
                        match v {
                            serde_json::Value::Bool(b) => bools.push(*b),
                            _ => return Ok(NormalValue::JsonArray(arr)), // Mixed types, preserve full array
                        }
                    }
                    Ok(NormalValue::BoolArray(bools))
                }
                serde_json::Value::Number(n) if n.is_i64() => {
                    let mut ints = Vec::with_capacity(arr.len());
                    for v in &arr {
                        if let Some(i) = v.as_i64() {
                            ints.push(i);
                        } else {
                            // Mixed types or out of range, preserve full array
                            return Ok(NormalValue::JsonArray(arr));
                        }
                    }
                    Ok(NormalValue::IntArray(ints))
                }
                serde_json::Value::Number(_) => {
                    let mut floats = Vec::with_capacity(arr.len());
                    for v in &arr {
                        if let Some(f) = v.as_f64() {
                            floats.push(f);
                        } else {
                            return Ok(NormalValue::JsonArray(arr));
                        }
                    }
                    Ok(NormalValue::Float64Array(floats))
                }
                serde_json::Value::String(_) => {
                    let mut strings = Vec::with_capacity(arr.len());
                    for v in &arr {
                        if let Some(s) = v.as_str() {
                            strings.push(s.to_string());
                        } else {
                            return Ok(NormalValue::JsonArray(arr));
                        }
                    }
                    Ok(NormalValue::StringArray(strings))
                }
                _ => {
                    // Complex types, keep as JSON array
                    Ok(NormalValue::JsonArray(arr))
                }
            }
        }
        serde_json::Value::Object(_) => {
            // Store complex objects as JSON
            Ok(NormalValue::Json(value))
        }
    }
}

/// Convert a NormalValue to a JSON value.
///
/// Returns an error if the value contains non-finite floats (NaN, Infinity),
/// matching Go's encoding/json behavior which rejects these values.
pub fn normal_value_to_json(value: &NormalValue) -> Result<serde_json::Value> {
    match value {
        NormalValue::Null => Ok(serde_json::Value::Null),
        NormalValue::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        NormalValue::Int(i) => Ok(serde_json::Value::Number((*i).into())),
        NormalValue::Float64(f) => float64_to_json(*f),
        NormalValue::Float32(f) => float64_to_json(*f as f64),
        NormalValue::String(s) => Ok(serde_json::Value::String(s.clone())),
        NormalValue::Bytes(b) => {
            // Encode bytes as base64
            Ok(serde_json::Value::String(base64_encode(b)))
        }
        NormalValue::Time(t) => Ok(serde_json::Value::String(t.to_rfc3339())),
        NormalValue::Json(v) => Ok(v.clone()),
        NormalValue::IntArray(arr) => Ok(serde_json::Value::Array(
            arr.iter()
                .map(|i| serde_json::Value::Number((*i).into()))
                .collect(),
        )),
        NormalValue::StringArray(arr) => Ok(serde_json::Value::Array(
            arr.iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        )),
        NormalValue::BoolArray(arr) => Ok(serde_json::Value::Array(
            arr.iter().map(|b| serde_json::Value::Bool(*b)).collect(),
        )),
        NormalValue::Float64Array(arr) => {
            let mut result = Vec::with_capacity(arr.len());
            for f in arr {
                result.push(float64_to_json(*f)?);
            }
            Ok(serde_json::Value::Array(result))
        }
        NormalValue::Float32Array(arr) => {
            let mut result = Vec::with_capacity(arr.len());
            for f in arr {
                result.push(float64_to_json(*f as f64)?);
            }
            Ok(serde_json::Value::Array(result))
        }
        NormalValue::JsonArray(arr) => Ok(serde_json::Value::Array(arr.clone())),
        // Nillable variants
        NormalValue::NillableBool(opt) => Ok(opt
            .map(serde_json::Value::Bool)
            .unwrap_or(serde_json::Value::Null)),
        NormalValue::NillableInt(opt) => Ok(opt
            .map(|i| serde_json::Value::Number(i.into()))
            .unwrap_or(serde_json::Value::Null)),
        NormalValue::NillableFloat64(opt) => match opt {
            Some(f) => float64_to_json(*f),
            None => Ok(serde_json::Value::Null),
        },
        NormalValue::NillableFloat32(opt) => match opt {
            Some(f) => float64_to_json(*f as f64),
            None => Ok(serde_json::Value::Null),
        },
        NormalValue::NillableString(opt) => Ok(opt
            .as_ref()
            .map(|s| serde_json::Value::String(s.clone()))
            .unwrap_or(serde_json::Value::Null)),
        // For other types, use JSON serialization
        _ => serde_json::to_value(value).map_err(|e| Error::CborEncode(e.to_string())),
    }
}

/// Convert a f64 to a JSON value, returning an error for non-finite values.
///
/// This matches Go's encoding/json behavior which rejects NaN and Infinity.
fn float64_to_json(f: f64) -> Result<serde_json::Value> {
    if !f.is_finite() {
        return Err(Error::NonFiniteFloat(format!("{}", f)));
    }
    serde_json::Number::from_f64(f)
        .map(serde_json::Value::Number)
        .ok_or_else(|| Error::NonFiniteFloat(format!("{}", f)))
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Convert a NormalValue to a ciborium::Value for CBOR encoding.
///
/// Returns an error if nested document encoding fails.
pub fn normal_value_to_cbor(value: &NormalValue) -> Result<ciborium::Value> {
    match value {
        NormalValue::Null => Ok(ciborium::Value::Null),
        NormalValue::Bool(b) => Ok(ciborium::Value::Bool(*b)),
        NormalValue::Int(i) => Ok(ciborium::Value::Integer((*i).into())),
        NormalValue::Float64(f) => Ok(ciborium::Value::Float(*f)),
        NormalValue::Float32(f) => Ok(ciborium::Value::Float(*f as f64)),
        NormalValue::String(s) => Ok(ciborium::Value::Text(s.clone())),
        NormalValue::Bytes(b) => Ok(ciborium::Value::Bytes(b.clone())),
        NormalValue::Time(t) => Ok(ciborium::Value::Text(t.to_rfc3339())),
        NormalValue::Json(v) => json_to_cbor_value(v),
        NormalValue::IntArray(arr) => Ok(ciborium::Value::Array(
            arr.iter()
                .map(|i| ciborium::Value::Integer((*i).into()))
                .collect(),
        )),
        NormalValue::StringArray(arr) => Ok(ciborium::Value::Array(
            arr.iter()
                .map(|s| ciborium::Value::Text(s.clone()))
                .collect(),
        )),
        NormalValue::BoolArray(arr) => Ok(ciborium::Value::Array(
            arr.iter().map(|b| ciborium::Value::Bool(*b)).collect(),
        )),
        NormalValue::Float64Array(arr) => Ok(ciborium::Value::Array(
            arr.iter().map(|f| ciborium::Value::Float(*f)).collect(),
        )),
        NormalValue::Float32Array(arr) => Ok(ciborium::Value::Array(
            arr.iter()
                .map(|f| ciborium::Value::Float(*f as f64))
                .collect(),
        )),
        NormalValue::JsonArray(arr) => {
            let cbor_values: Result<Vec<ciborium::Value>> =
                arr.iter().map(json_to_cbor_value).collect();
            Ok(ciborium::Value::Array(cbor_values?))
        }
        // Nillable variants - encode as value or null
        NormalValue::NillableBool(opt) => Ok(opt
            .map(ciborium::Value::Bool)
            .unwrap_or(ciborium::Value::Null)),
        NormalValue::NillableInt(opt) => Ok(opt
            .map(|i| ciborium::Value::Integer(i.into()))
            .unwrap_or(ciborium::Value::Null)),
        NormalValue::NillableFloat64(opt) => Ok(opt
            .map(ciborium::Value::Float)
            .unwrap_or(ciborium::Value::Null)),
        NormalValue::NillableFloat32(opt) => Ok(opt
            .map(|f| ciborium::Value::Float(f as f64))
            .unwrap_or(ciborium::Value::Null)),
        NormalValue::NillableString(opt) => Ok(opt
            .as_ref()
            .map(|s| ciborium::Value::Text(s.clone()))
            .unwrap_or(ciborium::Value::Null)),
        NormalValue::NillableBytes(opt) => Ok(opt
            .as_ref()
            .map(|b| ciborium::Value::Bytes(b.clone()))
            .unwrap_or(ciborium::Value::Null)),
        NormalValue::NillableTime(opt) => Ok(opt
            .map(|t| ciborium::Value::Text(t.to_rfc3339()))
            .unwrap_or(ciborium::Value::Null)),
        // Document value - propagate errors instead of silently converting to null
        NormalValue::Document(doc) => {
            let bytes = doc.to_cbor()?;
            ciborium::from_reader(&bytes[..])
                .map_err(|e| Error::CborDecode(format!("nested document: {}", e)))
        }
        NormalValue::NillableDocument(opt) => match opt {
            Some(doc) => {
                let bytes = doc.to_cbor()?;
                ciborium::from_reader(&bytes[..])
                    .map_err(|e| Error::CborDecode(format!("nested document: {}", e)))
            }
            None => Ok(ciborium::Value::Null),
        },
        // Additional array types
        NormalValue::BytesArray(arr) => Ok(ciborium::Value::Array(
            arr.iter()
                .map(|b| ciborium::Value::Bytes(b.clone()))
                .collect(),
        )),
        NormalValue::TimeArray(arr) => Ok(ciborium::Value::Array(
            arr.iter()
                .map(|t| ciborium::Value::Text(t.to_rfc3339()))
                .collect(),
        )),
        NormalValue::DocumentArray(arr) => {
            let mut result = Vec::with_capacity(arr.len());
            for doc in arr {
                let bytes = doc.to_cbor()?;
                let cbor_val: ciborium::Value = ciborium::from_reader(&bytes[..])
                    .map_err(|e| Error::CborDecode(format!("document array element: {}", e)))?;
                result.push(cbor_val);
            }
            Ok(ciborium::Value::Array(result))
        }
        // Nillable array types
        NormalValue::NillableBoolArray(opt) => Ok(opt
            .as_ref()
            .map(|arr| {
                ciborium::Value::Array(arr.iter().map(|b| ciborium::Value::Bool(*b)).collect())
            })
            .unwrap_or(ciborium::Value::Null)),
        NormalValue::NillableIntArray(opt) => Ok(opt
            .as_ref()
            .map(|arr| {
                ciborium::Value::Array(
                    arr.iter()
                        .map(|i| ciborium::Value::Integer((*i).into()))
                        .collect(),
                )
            })
            .unwrap_or(ciborium::Value::Null)),
        NormalValue::NillableFloat64Array(opt) => Ok(opt
            .as_ref()
            .map(|arr| {
                ciborium::Value::Array(arr.iter().map(|f| ciborium::Value::Float(*f)).collect())
            })
            .unwrap_or(ciborium::Value::Null)),
        NormalValue::NillableFloat32Array(opt) => Ok(opt
            .as_ref()
            .map(|arr| {
                ciborium::Value::Array(
                    arr.iter()
                        .map(|f| ciborium::Value::Float(*f as f64))
                        .collect(),
                )
            })
            .unwrap_or(ciborium::Value::Null)),
        NormalValue::NillableStringArray(opt) => Ok(opt
            .as_ref()
            .map(|arr| {
                ciborium::Value::Array(
                    arr.iter()
                        .map(|s| ciborium::Value::Text(s.clone()))
                        .collect(),
                )
            })
            .unwrap_or(ciborium::Value::Null)),
        NormalValue::NillableBytesArray(opt) => Ok(opt
            .as_ref()
            .map(|arr| {
                ciborium::Value::Array(
                    arr.iter()
                        .map(|b| ciborium::Value::Bytes(b.clone()))
                        .collect(),
                )
            })
            .unwrap_or(ciborium::Value::Null)),
        NormalValue::NillableTimeArray(opt) => Ok(opt
            .as_ref()
            .map(|arr| {
                ciborium::Value::Array(
                    arr.iter()
                        .map(|t| ciborium::Value::Text(t.to_rfc3339()))
                        .collect(),
                )
            })
            .unwrap_or(ciborium::Value::Null)),
        // Arrays with nillable elements
        NormalValue::NillableBoolElementArray(arr) => Ok(ciborium::Value::Array(
            arr.iter()
                .map(|opt| {
                    opt.map(ciborium::Value::Bool)
                        .unwrap_or(ciborium::Value::Null)
                })
                .collect(),
        )),
        NormalValue::NillableIntElementArray(arr) => Ok(ciborium::Value::Array(
            arr.iter()
                .map(|opt| {
                    opt.map(|i| ciborium::Value::Integer(i.into()))
                        .unwrap_or(ciborium::Value::Null)
                })
                .collect(),
        )),
        NormalValue::NillableFloat64ElementArray(arr) => Ok(ciborium::Value::Array(
            arr.iter()
                .map(|opt| {
                    opt.map(ciborium::Value::Float)
                        .unwrap_or(ciborium::Value::Null)
                })
                .collect(),
        )),
        NormalValue::NillableFloat32ElementArray(arr) => Ok(ciborium::Value::Array(
            arr.iter()
                .map(|opt| {
                    opt.map(|f| ciborium::Value::Float(f as f64))
                        .unwrap_or(ciborium::Value::Null)
                })
                .collect(),
        )),
        NormalValue::NillableStringElementArray(arr) => Ok(ciborium::Value::Array(
            arr.iter()
                .map(|opt| {
                    opt.as_ref()
                        .map(|s| ciborium::Value::Text(s.clone()))
                        .unwrap_or(ciborium::Value::Null)
                })
                .collect(),
        )),
        NormalValue::NillableBytesElementArray(arr) => Ok(ciborium::Value::Array(
            arr.iter()
                .map(|opt| {
                    opt.as_ref()
                        .map(|b| ciborium::Value::Bytes(b.clone()))
                        .unwrap_or(ciborium::Value::Null)
                })
                .collect(),
        )),
        NormalValue::NillableTimeElementArray(arr) => Ok(ciborium::Value::Array(
            arr.iter()
                .map(|opt| {
                    opt.map(|t| ciborium::Value::Text(t.to_rfc3339()))
                        .unwrap_or(ciborium::Value::Null)
                })
                .collect(),
        )),
        NormalValue::NillableDocumentElementArray(arr) => {
            let mut result = Vec::with_capacity(arr.len());
            for opt in arr {
                let cbor_val = match opt {
                    Some(doc) => {
                        let bytes = doc.to_cbor()?;
                        ciborium::from_reader(&bytes[..]).map_err(|e| {
                            Error::CborDecode(format!("document array element: {}", e))
                        })?
                    }
                    None => ciborium::Value::Null,
                };
                result.push(cbor_val);
            }
            Ok(ciborium::Value::Array(result))
        }
        NormalValue::NillableDocumentArray(opt) => match opt {
            Some(arr) => {
                let mut result = Vec::with_capacity(arr.len());
                for doc in arr {
                    let bytes = doc.to_cbor()?;
                    let cbor_val: ciborium::Value = ciborium::from_reader(&bytes[..])
                        .map_err(|e| Error::CborDecode(format!("document array element: {}", e)))?;
                    result.push(cbor_val);
                }
                Ok(ciborium::Value::Array(result))
            }
            None => Ok(ciborium::Value::Null),
        },
    }
}

/// Convert a JSON value to a ciborium::Value.
fn json_to_cbor_value(value: &serde_json::Value) -> Result<ciborium::Value> {
    match value {
        serde_json::Value::Null => Ok(ciborium::Value::Null),
        serde_json::Value::Bool(b) => Ok(ciborium::Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ciborium::Value::Integer(i.into()))
            } else if let Some(f) = n.as_f64() {
                Ok(ciborium::Value::Float(f))
            } else {
                Err(Error::JsonNumberOutOfRange(n.to_string()))
            }
        }
        serde_json::Value::String(s) => Ok(ciborium::Value::Text(s.clone())),
        serde_json::Value::Array(arr) => {
            let mut result = Vec::with_capacity(arr.len());
            for v in arr {
                result.push(json_to_cbor_value(v)?);
            }
            Ok(ciborium::Value::Array(result))
        }
        serde_json::Value::Object(obj) => {
            let mut entries = Vec::with_capacity(obj.len());
            for (k, v) in obj {
                entries.push((ciborium::Value::Text(k.clone()), json_to_cbor_value(v)?));
            }
            Ok(ciborium::Value::Map(entries))
        }
    }
}

/// Canonical CBOR key ordering (RFC 7049 Section 3.9, RFC 8949).
/// Keys are sorted by:
/// 1. Length first (shorter keys come first)
/// 2. Lexicographically (bytewise) within same length
pub fn canonical_cbor_key_order(a: &&str, b: &&str) -> std::cmp::Ordering {
    match a.len().cmp(&b.len()) {
        std::cmp::Ordering::Equal => a.cmp(b),
        other => other,
    }
}

/// Convert a ciborium::Value to a NormalValue.
///
/// This is used for decoding CBOR bytes back into document values.
pub fn cbor_to_normal_value(value: ciborium::Value) -> Result<NormalValue> {
    match value {
        ciborium::Value::Null => Ok(NormalValue::Null),
        ciborium::Value::Bool(b) => Ok(NormalValue::Bool(b)),
        ciborium::Value::Integer(i) => {
            let val: i128 = i.into();
            if val >= i64::MIN as i128 && val <= i64::MAX as i128 {
                Ok(NormalValue::Int(val as i64))
            } else {
                Err(Error::CborDecode(format!(
                    "integer out of i64 range: {}",
                    val
                )))
            }
        }
        ciborium::Value::Float(f) => Ok(NormalValue::Float64(f)),
        ciborium::Value::Text(s) => Ok(NormalValue::String(s)),
        ciborium::Value::Bytes(b) => Ok(NormalValue::Bytes(b)),
        ciborium::Value::Array(arr) => {
            if arr.is_empty() {
                return Ok(NormalValue::JsonArray(vec![]));
            }
            // Try to infer array type from first element
            match &arr[0] {
                ciborium::Value::Bool(_) => {
                    let mut bools = Vec::with_capacity(arr.len());
                    for v in arr {
                        match v {
                            ciborium::Value::Bool(b) => bools.push(b),
                            _ => {
                                return cbor_array_to_json_array(
                                    std::iter::once(ciborium::Value::Bool(bools.pop().unwrap()))
                                        .chain(std::iter::once(v))
                                        .chain(std::iter::empty()),
                                )
                            }
                        }
                    }
                    Ok(NormalValue::BoolArray(bools))
                }
                ciborium::Value::Integer(_) => {
                    let mut ints = Vec::with_capacity(arr.len());
                    for v in arr {
                        match v {
                            ciborium::Value::Integer(i) => {
                                let val: i128 = i.into();
                                if val >= i64::MIN as i128 && val <= i64::MAX as i128 {
                                    ints.push(val as i64);
                                } else {
                                    return Err(Error::CborDecode(format!(
                                        "integer out of i64 range: {}",
                                        val
                                    )));
                                }
                            }
                            _ => {
                                // Mixed types - convert to JSON array
                                let mut json_arr = Vec::with_capacity(ints.len() + 1);
                                for i in ints {
                                    json_arr.push(serde_json::json!(i));
                                }
                                json_arr.push(cbor_value_to_json(&v)?);
                                return Ok(NormalValue::JsonArray(json_arr));
                            }
                        }
                    }
                    Ok(NormalValue::IntArray(ints))
                }
                ciborium::Value::Float(_) => {
                    let mut floats = Vec::with_capacity(arr.len());
                    for v in arr {
                        match v {
                            ciborium::Value::Float(f) => floats.push(f),
                            _ => {
                                let mut json_arr = Vec::with_capacity(floats.len() + 1);
                                for f in floats {
                                    json_arr.push(serde_json::json!(f));
                                }
                                json_arr.push(cbor_value_to_json(&v)?);
                                return Ok(NormalValue::JsonArray(json_arr));
                            }
                        }
                    }
                    Ok(NormalValue::Float64Array(floats))
                }
                ciborium::Value::Text(_) => {
                    let mut strings = Vec::with_capacity(arr.len());
                    for v in arr {
                        match v {
                            ciborium::Value::Text(s) => strings.push(s),
                            _ => {
                                let mut json_arr = Vec::with_capacity(strings.len() + 1);
                                for s in strings {
                                    json_arr.push(serde_json::json!(s));
                                }
                                json_arr.push(cbor_value_to_json(&v)?);
                                return Ok(NormalValue::JsonArray(json_arr));
                            }
                        }
                    }
                    Ok(NormalValue::StringArray(strings))
                }
                _ => {
                    // Complex array - convert to JSON array
                    let mut json_arr = Vec::with_capacity(arr.len());
                    for v in arr {
                        json_arr.push(cbor_value_to_json(&v)?);
                    }
                    Ok(NormalValue::JsonArray(json_arr))
                }
            }
        }
        ciborium::Value::Map(map) => {
            // Convert map to JSON object
            let mut json_obj = serde_json::Map::new();
            for (k, v) in map {
                let key = match k {
                    ciborium::Value::Text(s) => s,
                    _ => return Err(Error::CborDecode("map key must be text".into())),
                };
                json_obj.insert(key, cbor_value_to_json(&v)?);
            }
            Ok(NormalValue::Json(serde_json::Value::Object(json_obj)))
        }
        ciborium::Value::Tag(_, _) => Err(Error::CborDecode("CBOR tags not supported".into())),
        _ => Err(Error::CborDecode("unsupported CBOR value type".into())),
    }
}

/// Helper to convert CBOR array to JSON array when types are mixed.
fn cbor_array_to_json_array<I: Iterator<Item = ciborium::Value>>(iter: I) -> Result<NormalValue> {
    let mut json_arr = Vec::new();
    for v in iter {
        json_arr.push(cbor_value_to_json(&v)?);
    }
    Ok(NormalValue::JsonArray(json_arr))
}

/// Convert a ciborium::Value to a serde_json::Value.
fn cbor_value_to_json(value: &ciborium::Value) -> Result<serde_json::Value> {
    match value {
        ciborium::Value::Null => Ok(serde_json::Value::Null),
        ciborium::Value::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        ciborium::Value::Integer(i) => {
            let val: i128 = (*i).into();
            if val >= i64::MIN as i128 && val <= i64::MAX as i128 {
                Ok(serde_json::json!(val as i64))
            } else {
                Err(Error::CborDecode(format!(
                    "integer out of i64 range: {}",
                    val
                )))
            }
        }
        ciborium::Value::Float(f) => {
            if f.is_finite() {
                Ok(serde_json::json!(f))
            } else {
                // NaN/Infinity can't be represented in JSON
                Err(Error::NonFiniteFloat(format!("{}", f)))
            }
        }
        ciborium::Value::Text(s) => Ok(serde_json::Value::String(s.clone())),
        ciborium::Value::Bytes(b) => Ok(serde_json::Value::String(base64_encode(b))),
        ciborium::Value::Array(arr) => {
            let mut json_arr = Vec::with_capacity(arr.len());
            for v in arr {
                json_arr.push(cbor_value_to_json(v)?);
            }
            Ok(serde_json::Value::Array(json_arr))
        }
        ciborium::Value::Map(map) => {
            let mut json_obj = serde_json::Map::new();
            for (k, v) in map {
                let key = match k {
                    ciborium::Value::Text(s) => s.clone(),
                    _ => return Err(Error::CborDecode("map key must be text".into())),
                };
                json_obj.insert(key, cbor_value_to_json(v)?);
            }
            Ok(serde_json::Value::Object(json_obj))
        }
        _ => Err(Error::CborDecode(
            "unsupported CBOR value type for JSON conversion".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_number_i64() {
        let val = json_to_normal_value(serde_json::json!(42)).unwrap();
        assert_eq!(val.as_int(), Some(42));
    }

    #[test]
    fn test_json_number_f64() {
        let val = json_to_normal_value(serde_json::json!(3.14)).unwrap();
        assert_eq!(val.as_float64(), Some(3.14));
    }

    #[test]
    fn test_json_string() {
        let val = json_to_normal_value(serde_json::json!("hello")).unwrap();
        assert_eq!(val.as_str(), Some("hello"));
    }

    #[test]
    fn test_json_null() {
        let val = json_to_normal_value(serde_json::Value::Null).unwrap();
        assert!(val.is_nil());
    }

    #[test]
    fn test_json_bool() {
        let val = json_to_normal_value(serde_json::json!(true)).unwrap();
        assert_eq!(val.as_bool(), Some(true));
    }

    #[test]
    fn test_json_int_array() {
        let val = json_to_normal_value(serde_json::json!([1, 2, 3])).unwrap();
        assert!(val.is_array());
        match val {
            NormalValue::IntArray(arr) => assert_eq!(arr, vec![1, 2, 3]),
            _ => panic!("expected IntArray"),
        }
    }

    #[test]
    fn test_json_string_array() {
        let val = json_to_normal_value(serde_json::json!(["a", "b"])).unwrap();
        match val {
            NormalValue::StringArray(arr) => assert_eq!(arr, vec!["a", "b"]),
            _ => panic!("expected StringArray"),
        }
    }

    #[test]
    fn test_json_empty_array() {
        let val = json_to_normal_value(serde_json::json!([])).unwrap();
        match val {
            NormalValue::JsonArray(arr) => assert!(arr.is_empty()),
            _ => panic!("expected JsonArray"),
        }
    }

    #[test]
    fn test_cbor_encoding_simple() {
        let val = NormalValue::Int(42);
        let cbor = normal_value_to_cbor(&val).unwrap();
        assert!(matches!(cbor, ciborium::Value::Integer(_)));
    }

    #[test]
    fn test_cbor_encoding_string() {
        let val = NormalValue::String("hello".into());
        let cbor = normal_value_to_cbor(&val).unwrap();
        match cbor {
            ciborium::Value::Text(s) => assert_eq!(s, "hello"),
            _ => panic!("expected Text"),
        }
    }

    #[test]
    fn test_canonical_key_order() {
        let mut keys = vec!["ab", "z", "aa"];
        keys.sort_by(canonical_cbor_key_order);
        assert_eq!(keys, vec!["z", "aa", "ab"]);
    }

    #[test]
    fn test_json_to_cbor_valid_numbers() {
        // Valid numbers should convert correctly
        let json_val = serde_json::json!({"key": 123});
        let cbor = json_to_cbor_value(&json_val).unwrap();
        assert!(matches!(cbor, ciborium::Value::Map(_)));
    }

    // === Mixed-type array tests (Go compatibility) ===

    #[test]
    fn test_json_mixed_array_int_string_preserves_all() {
        // Mixed int+string array should preserve all elements
        let val = json_to_normal_value(serde_json::json!([1, "text", 3])).unwrap();
        match val {
            NormalValue::JsonArray(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], serde_json::json!(1));
                assert_eq!(arr[1], serde_json::json!("text"));
                assert_eq!(arr[2], serde_json::json!(3));
            }
            _ => panic!("expected JsonArray for mixed types"),
        }
    }

    #[test]
    fn test_json_mixed_array_bool_int_preserves_all() {
        // Mixed bool+int array should preserve all elements
        let val = json_to_normal_value(serde_json::json!([true, 42, false])).unwrap();
        match val {
            NormalValue::JsonArray(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], serde_json::json!(true));
                assert_eq!(arr[1], serde_json::json!(42));
                assert_eq!(arr[2], serde_json::json!(false));
            }
            _ => panic!("expected JsonArray for mixed types"),
        }
    }

    #[test]
    fn test_json_mixed_array_string_null_preserves_all() {
        // Mixed string+null array should preserve all elements
        let val = json_to_normal_value(serde_json::json!(["a", null, "b"])).unwrap();
        match val {
            NormalValue::JsonArray(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], serde_json::json!("a"));
                assert_eq!(arr[1], serde_json::Value::Null);
                assert_eq!(arr[2], serde_json::json!("b"));
            }
            _ => panic!("expected JsonArray for mixed types"),
        }
    }

    #[test]
    fn test_json_mixed_array_int_float_preserves_all() {
        // Array starting with int but containing float should preserve all
        // Note: [1, 3.14] - first element looks like int, second is float
        let val = json_to_normal_value(serde_json::json!([1, 3.14])).unwrap();
        // This might become IntArray if 3.14 coerces to int, or JsonArray if not
        // The key is that ALL elements are preserved
        match &val {
            NormalValue::IntArray(arr) => {
                assert_eq!(arr.len(), 2);
            }
            NormalValue::JsonArray(arr) => {
                assert_eq!(arr.len(), 2);
            }
            NormalValue::Float64Array(arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("unexpected type: {:?}", val),
        }
    }

    #[test]
    fn test_json_homogeneous_int_array() {
        // Pure int array should become IntArray
        let val = json_to_normal_value(serde_json::json!([1, 2, 3])).unwrap();
        match val {
            NormalValue::IntArray(arr) => {
                assert_eq!(arr, vec![1, 2, 3]);
            }
            _ => panic!("expected IntArray"),
        }
    }

    #[test]
    fn test_json_homogeneous_string_array() {
        // Pure string array should become StringArray
        let val = json_to_normal_value(serde_json::json!(["a", "b", "c"])).unwrap();
        match val {
            NormalValue::StringArray(arr) => {
                assert_eq!(arr, vec!["a", "b", "c"]);
            }
            _ => panic!("expected StringArray"),
        }
    }

    #[test]
    fn test_json_homogeneous_bool_array() {
        // Pure bool array should become BoolArray
        let val = json_to_normal_value(serde_json::json!([true, false, true])).unwrap();
        match val {
            NormalValue::BoolArray(arr) => {
                assert_eq!(arr, vec![true, false, true]);
            }
            _ => panic!("expected BoolArray"),
        }
    }

    // === Non-finite float tests (Go compatibility) ===

    #[test]
    fn test_normal_value_to_json_nan_error() {
        let val = NormalValue::Float64(f64::NAN);
        let result = normal_value_to_json(&val);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NonFiniteFloat(_)));
    }

    #[test]
    fn test_normal_value_to_json_infinity_error() {
        let val = NormalValue::Float64(f64::INFINITY);
        let result = normal_value_to_json(&val);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NonFiniteFloat(_)));
    }

    #[test]
    fn test_normal_value_to_json_neg_infinity_error() {
        let val = NormalValue::Float64(f64::NEG_INFINITY);
        let result = normal_value_to_json(&val);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NonFiniteFloat(_)));
    }

    #[test]
    fn test_normal_value_to_json_float32_nan_error() {
        let val = NormalValue::Float32(f32::NAN);
        let result = normal_value_to_json(&val);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NonFiniteFloat(_)));
    }

    #[test]
    fn test_normal_value_to_json_float_array_nan_error() {
        let val = NormalValue::Float64Array(vec![1.0, f64::NAN, 3.0]);
        let result = normal_value_to_json(&val);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NonFiniteFloat(_)));
    }

    #[test]
    fn test_normal_value_to_json_float32_array_infinity_error() {
        let val = NormalValue::Float32Array(vec![1.0, f32::INFINITY]);
        let result = normal_value_to_json(&val);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NonFiniteFloat(_)));
    }

    #[test]
    fn test_normal_value_to_json_nillable_float_nan_error() {
        let val = NormalValue::NillableFloat64(Some(f64::NAN));
        let result = normal_value_to_json(&val);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NonFiniteFloat(_)));
    }

    #[test]
    fn test_normal_value_to_json_nillable_float_none_ok() {
        // None should convert to null successfully
        let val = NormalValue::NillableFloat64(None);
        let result = normal_value_to_json(&val);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), serde_json::Value::Null);
    }

    #[test]
    fn test_normal_value_to_json_finite_float_ok() {
        // Normal finite floats should work fine
        let val = NormalValue::Float64(3.14);
        let result = normal_value_to_json(&val);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cbor_nan_preserved() {
        // CBOR supports NaN, so this should succeed
        let val = NormalValue::Float64(f64::NAN);
        let result = normal_value_to_cbor(&val);
        assert!(result.is_ok());
        match result.unwrap() {
            ciborium::Value::Float(f) => assert!(f.is_nan()),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn test_cbor_infinity_preserved() {
        // CBOR supports Infinity, so this should succeed
        let val = NormalValue::Float64(f64::INFINITY);
        let result = normal_value_to_cbor(&val);
        assert!(result.is_ok());
        match result.unwrap() {
            ciborium::Value::Float(f) => assert!(f.is_infinite() && f.is_sign_positive()),
            _ => panic!("expected Float"),
        }
    }

    // === RFC3339 time format tests (Go compatibility) ===

    #[test]
    fn test_time_to_json_rfc3339_format() {
        use chrono::{TimeZone, Utc};
        // Timestamp without fractional seconds
        let t = Utc.with_ymd_and_hms(2025, 1, 14, 12, 30, 45).unwrap();
        let val = NormalValue::Time(t);
        let json = normal_value_to_json(&val).unwrap();
        // Should be valid RFC3339 format
        let s = json.as_str().unwrap();
        assert!(s.contains("2025-01-14"));
        assert!(s.contains("12:30:45"));
        // Verify it can be parsed back
        chrono::DateTime::parse_from_rfc3339(s).expect("should be valid RFC3339");
    }

    #[test]
    fn test_time_to_json_with_nanoseconds() {
        use chrono::{TimeZone, Timelike, Utc};
        // Timestamp with nanoseconds
        let t = Utc
            .with_ymd_and_hms(2025, 1, 14, 12, 30, 45)
            .unwrap()
            .with_nanosecond(123456789)
            .unwrap();
        let val = NormalValue::Time(t);
        let json = normal_value_to_json(&val).unwrap();
        let s = json.as_str().unwrap();
        // Should include fractional seconds
        assert!(s.contains("."));
        // Verify roundtrip preserves nanoseconds
        let parsed = chrono::DateTime::parse_from_rfc3339(s).unwrap();
        assert_eq!(parsed.timestamp_nanos_opt(), t.timestamp_nanos_opt());
    }

    #[test]
    fn test_time_to_cbor_matches_json() {
        use chrono::{TimeZone, Timelike, Utc};
        let t = Utc
            .with_ymd_and_hms(2025, 1, 14, 12, 30, 45)
            .unwrap()
            .with_nanosecond(500000000)
            .unwrap();
        let val = NormalValue::Time(t);

        // Get both encodings
        let json = normal_value_to_json(&val).unwrap();
        let cbor = normal_value_to_cbor(&val).unwrap();

        // CBOR should encode as text with same value as JSON
        match cbor {
            ciborium::Value::Text(cbor_str) => {
                assert_eq!(cbor_str, json.as_str().unwrap());
            }
            _ => panic!("expected Text for time in CBOR"),
        }
    }
}
