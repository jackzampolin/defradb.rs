//! JSON-to-NormalValue conversion utilities for document mutations.

use chrono::{DateTime, FixedOffset, Utc};
use schema::{FieldKind, ScalarArrayKind, ScalarKind};
use serde_json::Value as JsonValue;

use query_types::error::{QueryError, Result};

/// Convert a JSON value to a document NormalValue.
pub fn json_to_normal_value(value: &JsonValue) -> Result<document::NormalValue> {
    use document::NormalValue;

    match value {
        JsonValue::Null => Ok(NormalValue::Null),
        JsonValue::Bool(b) => Ok(NormalValue::Bool(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(NormalValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(NormalValue::Float64(f))
            } else {
                Err(QueryError::execution("Invalid number value"))
            }
        }
        JsonValue::String(s) => Ok(NormalValue::String(s.clone())),
        JsonValue::Array(arr) => {
            // Empty arrays default to empty string array
            if arr.is_empty() {
                return Ok(NormalValue::StringArray(Vec::new()));
            }

            // Determine array type from first non-null element
            let first_non_null = arr.iter().find(|v| !v.is_null());

            match first_non_null {
                Some(JsonValue::Bool(_)) => {
                    let mut bools = Vec::with_capacity(arr.len());
                    for (i, v) in arr.iter().enumerate() {
                        match v {
                            JsonValue::Bool(b) => bools.push(*b),
                            JsonValue::Null => bools.push(false),
                            _ => {
                                return Err(QueryError::execution(format!(
                                    "Array element at index {} is not a boolean (found {:?})",
                                    i, v
                                )))
                            }
                        }
                    }
                    Ok(NormalValue::BoolArray(bools))
                }
                Some(JsonValue::Number(n)) if n.is_i64() => {
                    let mut ints = Vec::with_capacity(arr.len());
                    for (i, v) in arr.iter().enumerate() {
                        match v {
                            JsonValue::Number(n) if n.as_i64().is_some() => {
                                ints.push(n.as_i64().unwrap())
                            }
                            JsonValue::Null => ints.push(0),
                            _ => {
                                return Err(QueryError::execution(format!(
                                    "Array element at index {} is not an integer (found {:?})",
                                    i, v
                                )))
                            }
                        }
                    }
                    Ok(NormalValue::IntArray(ints))
                }
                Some(JsonValue::Number(_)) => {
                    let mut floats = Vec::with_capacity(arr.len());
                    for (i, v) in arr.iter().enumerate() {
                        match v {
                            JsonValue::Number(n) => floats.push(n.as_f64().unwrap_or(0.0)),
                            JsonValue::Null => floats.push(0.0),
                            _ => {
                                return Err(QueryError::execution(format!(
                                    "Array element at index {} is not a number (found {:?})",
                                    i, v
                                )))
                            }
                        }
                    }
                    Ok(NormalValue::Float64Array(floats))
                }
                Some(JsonValue::String(_)) => {
                    let mut strings = Vec::with_capacity(arr.len());
                    for (i, v) in arr.iter().enumerate() {
                        match v {
                            JsonValue::String(s) => strings.push(s.clone()),
                            JsonValue::Null => strings.push(String::new()),
                            _ => {
                                return Err(QueryError::execution(format!(
                                    "Array element at index {} is not a string (found {:?})",
                                    i, v
                                )))
                            }
                        }
                    }
                    Ok(NormalValue::StringArray(strings))
                }
                // Array contains only nulls - default to empty strings
                None => {
                    let strings: Vec<String> = arr.iter().map(|_| String::new()).collect();
                    Ok(NormalValue::StringArray(strings))
                }
                // Nested arrays or objects - store as JSON
                Some(_) => Ok(NormalValue::Json(JsonValue::Array(arr.clone()))),
            }
        }
        JsonValue::Object(_) => {
            // Nested objects could be sub-documents - for now, store as JSON
            Ok(NormalValue::Json(value.clone()))
        }
    }
}

/// Convert a JSON value to a document NormalValue with schema-aware type coercion.
#[allow(dead_code)]
pub fn json_to_normal_value_with_kind(
    value: &JsonValue,
    field_kind: Option<&FieldKind>,
) -> Result<document::NormalValue> {
    json_to_normal_value_with_kind_and_time(value, field_kind, None)
}

/// Convert a JSON value to a document NormalValue with schema-aware type coercion
/// and an optional pre-computed request time for UTC_NOW resolution.
pub fn json_to_normal_value_with_kind_and_time(
    value: &JsonValue,
    field_kind: Option<&FieldKind>,
    request_time: Option<DateTime<FixedOffset>>,
) -> Result<document::NormalValue> {
    use document::NormalValue;

    // Handle null regardless of expected type
    if value.is_null() {
        return Ok(NormalValue::Null);
    }

    // If we have schema information, use it for type coercion
    if let Some(kind) = field_kind {
        match kind {
            FieldKind::Scalar(scalar) => match scalar.base_kind() {
                // JSON fields: wrap ALL values as JSON
                ScalarKind::Json => Ok(NormalValue::Json(value.clone())),
                // DateTime fields: parse RFC 3339 strings or special values like UTC_NOW.
                // The string/number → Time mapping is delegated to the shared `document`
                // converter so the create and reindex paths cannot drift.
                ScalarKind::DateTime => {
                    if let JsonValue::String(s) = value {
                        if s == "UTC_NOW" {
                            let time = request_time.unwrap_or_else(|| {
                                let utc_offset = FixedOffset::east_opt(0).unwrap();
                                Utc::now().with_timezone(&utc_offset)
                            });
                            return Ok(NormalValue::Time(time));
                        }
                    }
                    document::encoding::json_to_normal_value_for_kind(value, scalar).ok_or_else(
                        || match value {
                            JsonValue::String(s) => QueryError::execution(format!(
                                "Invalid DateTime format '{}': expected RFC 3339 (e.g., '2024-01-15T10:30:00Z')",
                                s
                            )),
                            JsonValue::Number(_) => QueryError::execution(format!(
                                "Expected DateTime string or Unix timestamp, got: {:?}",
                                value
                            )),
                            _ => QueryError::execution(format!(
                                "Expected DateTime string, got: {:?}",
                                value
                            )),
                        },
                    )
                }
                // Float fields: convert integers to the declared float representation.
                ScalarKind::Float64 | ScalarKind::Float32 => {
                    document::encoding::json_to_normal_value_for_kind(value, scalar).ok_or_else(
                        || {
                            QueryError::execution(format!(
                                "Expected number for {} field, got: {:?}",
                                if scalar.base_kind() == ScalarKind::Float64 {
                                    "Float64"
                                } else {
                                    "Float32"
                                },
                                value
                            ))
                        },
                    )
                }
                // Blob: production stores hex text as String (Go parity). When
                // DEFRA_TEST_BLOB_AS_BYTES is set, hex-decode into Bytes so e2e
                // tests can exercise NormalValue::Bytes JSON converters (#1294).
                ScalarKind::Blob => {
                    if document::encoding::test_blob_as_bytes_enabled() {
                        let s = value.as_str().ok_or_else(|| {
                            QueryError::execution(format!(
                                "Expected hex string for Blob field, got: {:?}",
                                value
                            ))
                        })?;
                        let bytes = document::encoding::decode_hex_blob(s).ok_or_else(|| {
                            QueryError::execution(format!(
                                "Invalid hex Blob value '{}': expected even-length hex",
                                s
                            ))
                        })?;
                        Ok(NormalValue::Bytes(bytes))
                    } else {
                        json_to_normal_value(value)
                    }
                }
                // For other scalar types, fall through to default conversion.
                _ => json_to_normal_value(value),
            },
            // ScalarArray fields
            FieldKind::ScalarArray(array_kind) => match value {
                JsonValue::Array(arr) => json_array_to_normal_value_with_kind(arr, *array_kind),
                _ => Err(QueryError::execution(format!(
                    "Expected array, got: {:?}",
                    value
                ))),
            },
            _ => json_to_normal_value(value),
        }
    } else {
        // No schema info - use default conversion
        json_to_normal_value(value)
    }
}

/// Convert a JSON array to NormalValue using schema-aware type coercion.
fn json_array_to_normal_value_with_kind(
    arr: &[JsonValue],
    array_kind: ScalarArrayKind,
) -> Result<document::NormalValue> {
    use document::NormalValue;

    match array_kind {
        ScalarArrayKind::BoolArray => {
            let mut bools = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Bool(b) => bools.push(*b),
                    JsonValue::Null => bools.push(false),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a boolean (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::BoolArray(bools))
        }
        ScalarArrayKind::NillableBoolArray => {
            let mut bools = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Bool(b) => bools.push(Some(*b)),
                    JsonValue::Null => bools.push(None),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a boolean (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::NillableBoolElementArray(bools))
        }
        ScalarArrayKind::IntArray => {
            let mut ints = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Number(n) if n.as_i64().is_some() => ints.push(n.as_i64().unwrap()),
                    JsonValue::Null => ints.push(0),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not an integer (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::IntArray(ints))
        }
        ScalarArrayKind::NillableIntArray => {
            let mut ints = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Number(n) if n.as_i64().is_some() => {
                        ints.push(Some(n.as_i64().unwrap()))
                    }
                    JsonValue::Null => ints.push(None),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not an integer (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::NillableIntElementArray(ints))
        }
        ScalarArrayKind::Float64Array => {
            let mut floats = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Number(n) => floats.push(n.as_f64().unwrap_or(0.0)),
                    JsonValue::Null => floats.push(0.0),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a number (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::Float64Array(floats))
        }
        ScalarArrayKind::NillableFloat64Array => {
            let mut floats = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Number(n) => floats.push(Some(n.as_f64().unwrap_or(0.0))),
                    JsonValue::Null => floats.push(None),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a number (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::NillableFloat64ElementArray(floats))
        }
        ScalarArrayKind::Float32Array => {
            let mut floats = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Number(n) => floats.push(n.as_f64().unwrap_or(0.0) as f32),
                    JsonValue::Null => floats.push(0.0),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a number (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::Float32Array(floats))
        }
        ScalarArrayKind::NillableFloat32Array => {
            let mut floats = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Number(n) => floats.push(Some(n.as_f64().unwrap_or(0.0) as f32)),
                    JsonValue::Null => floats.push(None),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a number (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::NillableFloat32ElementArray(floats))
        }
        ScalarArrayKind::StringArray => {
            let mut strings = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::String(s) => strings.push(s.clone()),
                    JsonValue::Null => strings.push(String::new()),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a string (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::StringArray(strings))
        }
        ScalarArrayKind::NillableStringArray => {
            let mut strings = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::String(s) => strings.push(Some(s.clone())),
                    JsonValue::Null => strings.push(None),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a string (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::NillableStringElementArray(strings))
        }
        ScalarArrayKind::DateTimeArray => {
            let times = arr
                .iter()
                .enumerate()
                .map(|(i, value)| match value {
                    JsonValue::String(value) => DateTime::parse_from_rfc3339(value).map_err(|_| {
                        QueryError::execution(format!(
                            "Array element at index {} is not a valid DateTime",
                            i
                        ))
                    }),
                    _ => Err(QueryError::execution(format!(
                        "Array element at index {} is not a DateTime string (found {:?})",
                        i, value
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(NormalValue::TimeArray(times))
        }
        ScalarArrayKind::NillableDateTimeArray => {
            let times = arr
                .iter()
                .enumerate()
                .map(|(i, value)| match value {
                    JsonValue::String(value) => {
                        DateTime::parse_from_rfc3339(value).map(Some).map_err(|_| {
                            QueryError::execution(format!(
                                "Array element at index {} is not a valid DateTime",
                                i
                            ))
                        })
                    }
                    JsonValue::Null => Ok(None),
                    _ => Err(QueryError::execution(format!(
                        "Array element at index {} is not a DateTime string (found {:?})",
                        i, value
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(NormalValue::NillableTimeElementArray(times))
        }
        _ => Err(QueryError::Execution(format!(
            "unsupported array kind: {:?}",
            array_kind
        ))),
    }
}

/// Convert a NormalValue to JsonValue for plan Doc storage.
pub fn normal_value_to_json(value: &document::NormalValue) -> JsonValue {
    use document::NormalValue;

    match value {
        NormalValue::Null => JsonValue::Null,
        NormalValue::Bool(b) => JsonValue::Bool(*b),
        NormalValue::Int(i) => JsonValue::Number((*i).into()),
        NormalValue::Float64(f) => {
            if f.is_nan() || f.is_infinite() {
                tracing::warn!(value = %f, "Float64 value cannot be represented in JSON, using null");
                JsonValue::Null
            } else {
                serde_json::Number::from_f64(*f)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null)
            }
        }
        NormalValue::Float32(f) => {
            if f.is_nan() || f.is_infinite() {
                tracing::warn!(value = %f, "Float32 value cannot be represented in JSON, using null");
                JsonValue::Null
            } else {
                serde_json::Number::from_f64(*f as f64)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null)
            }
        }
        NormalValue::String(s) => JsonValue::String(s.clone()),
        NormalValue::Bytes(b) => document::encoding::bytes_to_json(b),
        NormalValue::Json(j) => j.clone(),
        NormalValue::BoolArray(arr) => {
            JsonValue::Array(arr.iter().map(|b| JsonValue::Bool(*b)).collect())
        }
        NormalValue::IntArray(arr) => {
            JsonValue::Array(arr.iter().map(|i| JsonValue::Number((*i).into())).collect())
        }
        NormalValue::Float64Array(arr) => JsonValue::Array(
            arr.iter()
                .map(|f| {
                    if f.is_nan() || f.is_infinite() {
                        tracing::warn!(value = %f, "Float64 array element cannot be represented in JSON, using null");
                        JsonValue::Null
                    } else {
                        serde_json::Number::from_f64(*f)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null)
                    }
                })
                .collect(),
        ),
        NormalValue::StringArray(arr) => {
            JsonValue::Array(arr.iter().map(|s| JsonValue::String(s.clone())).collect())
        }
        NormalValue::Float32Array(arr) => JsonValue::Array(
            arr.iter()
                .map(|f| {
                    if f.is_nan() || f.is_infinite() {
                        tracing::warn!(value = %f, "Float32 array element cannot be represented in JSON, using null");
                        JsonValue::Null
                    } else {
                        serde_json::Number::from_f64(*f as f64)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null)
                    }
                })
                .collect(),
        ),
        NormalValue::BytesArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|bytes| document::encoding::bytes_to_json(bytes))
                .collect(),
        ),
        NormalValue::Time(t) => {
            JsonValue::String(t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
        }
        NormalValue::NillableTime(Some(t)) => {
            JsonValue::String(t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
        }
        NormalValue::NillableTime(None) => JsonValue::Null,
        NormalValue::TimeArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|t| JsonValue::String(t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)))
                .collect(),
        ),
        NormalValue::NillableTimeArray(arr) => arr
            .as_ref()
            .map(|arr| {
                JsonValue::Array(
                    arr.iter()
                        .map(|time| {
                            JsonValue::String(
                                time.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                            )
                        })
                        .collect(),
                )
            })
            .unwrap_or(JsonValue::Null),
        NormalValue::NillableBoolElementArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|opt| match opt {
                    Some(b) => JsonValue::Bool(*b),
                    None => JsonValue::Null,
                })
                .collect(),
        ),
        NormalValue::NillableIntElementArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|opt| match opt {
                    Some(i) => JsonValue::Number((*i).into()),
                    None => JsonValue::Null,
                })
                .collect(),
        ),
        NormalValue::NillableFloat64ElementArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|opt| match opt {
                    Some(f) => {
                        if f.is_nan() || f.is_infinite() {
                            JsonValue::Null
                        } else {
                            serde_json::Number::from_f64(*f)
                                .map(JsonValue::Number)
                                .unwrap_or(JsonValue::Null)
                        }
                    }
                    None => JsonValue::Null,
                })
                .collect(),
        ),
        NormalValue::NillableFloat32ElementArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|opt| match opt {
                    Some(f) => {
                        if f.is_nan() || f.is_infinite() {
                            JsonValue::Null
                        } else {
                            serde_json::Number::from_f64(*f as f64)
                                .map(JsonValue::Number)
                                .unwrap_or(JsonValue::Null)
                        }
                    }
                    None => JsonValue::Null,
                })
                .collect(),
        ),
        NormalValue::NillableStringElementArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|opt| match opt {
                    Some(s) => JsonValue::String(s.clone()),
                    None => JsonValue::Null,
                })
                .collect(),
        ),
        NormalValue::NillableTimeElementArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|time| {
                    time.as_ref()
                        .map(|time| {
                            JsonValue::String(
                                time.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                            )
                        })
                        .unwrap_or(JsonValue::Null)
                })
                .collect(),
        ),
        other => {
            tracing::warn!("Unexpected NormalValue variant encountered, converting to null: {:?}", other);
            JsonValue::Null
        }
    }
}
