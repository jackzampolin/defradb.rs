// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Field value encoding for secondary indexes
//!
//! This module provides encoding/decoding of document field values
//! using order-preserving encoding for use in secondary index keys.

use chrono::{TimeZone, Utc};
use document::NormalValue;
use schema::FieldKind;

use crate::corekv::Result;
use crate::encoding::{self, EncodedType};

/// An indexed field with value and sort direction.
#[derive(Debug, Clone)]
pub struct IndexedField {
    /// The field value
    pub value: NormalValue,
    /// Whether this field is indexed in descending order
    pub descending: bool,
}

impl IndexedField {
    /// Create a new indexed field
    pub fn new(value: NormalValue, descending: bool) -> Self {
        Self { value, descending }
    }

    /// Create an ascending indexed field
    pub fn ascending(value: NormalValue) -> Self {
        Self {
            value,
            descending: false,
        }
    }

    /// Create a descending indexed field
    pub fn descending(value: NormalValue) -> Self {
        Self {
            value,
            descending: true,
        }
    }
}

/// Encode a NormalValue to bytes using order-preserving encoding.
///
/// This matches Go's `EncodeFieldValue` in encoding/field_value.go
pub fn encode_field_value(buf: Vec<u8>, val: &NormalValue, descending: bool) -> Vec<u8> {
    if val.is_nil() {
        return if descending {
            encoding::encode_null_descending(buf)
        } else {
            encoding::encode_null_ascending(buf)
        };
    }

    match val {
        NormalValue::Bool(v) => {
            if descending {
                encoding::encode_bool_descending(buf, *v)
            } else {
                encoding::encode_bool_ascending(buf, *v)
            }
        }
        NormalValue::NillableBool(Some(v)) => {
            if descending {
                encoding::encode_bool_descending(buf, *v)
            } else {
                encoding::encode_bool_ascending(buf, *v)
            }
        }
        NormalValue::Int(v) => {
            if descending {
                encoding::encode_varint_descending(buf, *v)
            } else {
                encoding::encode_varint_ascending(buf, *v)
            }
        }
        NormalValue::NillableInt(Some(v)) => {
            if descending {
                encoding::encode_varint_descending(buf, *v)
            } else {
                encoding::encode_varint_ascending(buf, *v)
            }
        }
        NormalValue::Float32(v) => {
            if descending {
                encoding::encode_float32_descending(buf, *v)
            } else {
                encoding::encode_float32_ascending(buf, *v)
            }
        }
        NormalValue::NillableFloat32(Some(v)) => {
            if descending {
                encoding::encode_float32_descending(buf, *v)
            } else {
                encoding::encode_float32_ascending(buf, *v)
            }
        }
        NormalValue::Float64(v) => {
            if descending {
                encoding::encode_float64_descending(buf, *v)
            } else {
                encoding::encode_float64_ascending(buf, *v)
            }
        }
        NormalValue::NillableFloat64(Some(v)) => {
            if descending {
                encoding::encode_float64_descending(buf, *v)
            } else {
                encoding::encode_float64_ascending(buf, *v)
            }
        }
        NormalValue::String(v) => {
            if descending {
                encoding::encode_string_descending(buf, v)
            } else {
                encoding::encode_string_ascending(buf, v)
            }
        }
        NormalValue::NillableString(Some(v)) => {
            if descending {
                encoding::encode_string_descending(buf, v)
            } else {
                encoding::encode_string_ascending(buf, v)
            }
        }
        NormalValue::Bytes(v) => {
            if descending {
                encoding::encode_bytes_descending(buf, v)
            } else {
                encoding::encode_bytes_ascending(buf, v)
            }
        }
        NormalValue::NillableBytes(Some(v)) => {
            if descending {
                encoding::encode_bytes_descending(buf, v)
            } else {
                encoding::encode_bytes_ascending(buf, v)
            }
        }
        NormalValue::Time(v) => {
            // Convert DateTime to nanoseconds since Unix epoch
            let nanos = v.timestamp_nanos_opt().unwrap_or(0);
            if descending {
                encoding::encode_time_descending(buf, nanos)
            } else {
                encoding::encode_time_ascending(buf, nanos)
            }
        }
        NormalValue::NillableTime(Some(v)) => {
            let nanos = v.timestamp_nanos_opt().unwrap_or(0);
            if descending {
                encoding::encode_time_descending(buf, nanos)
            } else {
                encoding::encode_time_ascending(buf, nanos)
            }
        }
        // For types we don't support yet, encode as null
        _ => {
            if descending {
                encoding::encode_null_descending(buf)
            } else {
                encoding::encode_null_ascending(buf)
            }
        }
    }
}

/// Decode a field value from bytes.
///
/// This matches Go's `DecodeFieldValue` in encoding/field_value.go
pub fn decode_field_value<'a>(
    buf: &'a [u8],
    descending: bool,
    kind: &FieldKind,
) -> Result<(&'a [u8], NormalValue)> {
    use schema::ScalarKind;

    let typ = encoding::peek_type(buf);

    match typ {
        EncodedType::Null => {
            let (rest, _) = encoding::decode_if_null(buf);
            Ok((rest, NormalValue::Null))
        }
        EncodedType::Bool => {
            let (rest, v) = if descending {
                encoding::decode_bool_descending(buf)?
            } else {
                encoding::decode_bool_ascending(buf)?
            };
            Ok((rest, NormalValue::Bool(v)))
        }
        EncodedType::Int => {
            let (rest, v) = if descending {
                encoding::decode_varint_descending(buf)?
            } else {
                encoding::decode_varint_ascending(buf)?
            };
            Ok((rest, NormalValue::Int(v)))
        }
        EncodedType::Float32 => {
            let (rest, v) = if descending {
                encoding::decode_float32_descending(buf)?
            } else {
                encoding::decode_float32_ascending(buf)?
            };
            Ok((rest, NormalValue::Float32(v)))
        }
        EncodedType::Float64 => {
            let (rest, v) = if descending {
                encoding::decode_float64_descending(buf)?
            } else {
                encoding::decode_float64_ascending(buf)?
            };
            Ok((rest, NormalValue::Float64(v)))
        }
        EncodedType::Bytes | EncodedType::BytesDesc => {
            let (rest, v) = if descending {
                encoding::decode_bytes_descending(buf)?
            } else {
                encoding::decode_bytes_ascending(buf)?
            };
            // Determine if this should be a String based on field kind
            let is_string = matches!(
                kind,
                FieldKind::Scalar(ScalarKind::String)
            );
            if is_string {
                let s = String::from_utf8(v).map_err(|e| {
                    crate::corekv::Error::Other(format!("invalid utf-8: {}", e))
                })?;
                Ok((rest, NormalValue::String(s)))
            } else {
                Ok((rest, NormalValue::Bytes(v)))
            }
        }
        EncodedType::Time => {
            let (rest, nanos) = if descending {
                encoding::decode_time_descending(buf)?
            } else {
                encoding::decode_time_ascending(buf)?
            };
            // Convert nanoseconds to DateTime
            let secs = nanos / 1_000_000_000;
            let nsecs = (nanos % 1_000_000_000) as u32;
            let dt = Utc.timestamp_opt(secs, nsecs).single().ok_or_else(|| {
                crate::corekv::Error::Other("invalid timestamp".to_string())
            })?;
            Ok((rest, NormalValue::Time(dt)))
        }
        _ => Err(crate::corekv::Error::Other(format!(
            "cannot decode field value: unknown type {:?}",
            typ
        ))),
    }
}

/// Encode an IndexedField to bytes
pub fn encode_indexed_field(buf: Vec<u8>, field: &IndexedField) -> Vec<u8> {
    encode_field_value(buf, &field.value, field.descending)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_encode_decode_bool() {
        let test_cases = vec![true, false];

        for v in test_cases {
            let val = NormalValue::Bool(v);
            let buf = encode_field_value(vec![], &val, false);
            let (_, decoded) = decode_field_value(&buf, false, &FieldKind::bool()).unwrap();
            assert_eq!(decoded, val);

            // Test descending
            let buf = encode_field_value(vec![], &val, true);
            let (_, decoded) = decode_field_value(&buf, true, &FieldKind::bool()).unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_encode_decode_int() {
        let test_cases = vec![-1000i64, -1, 0, 1, 1000, i64::MIN, i64::MAX];

        for v in test_cases {
            let val = NormalValue::Int(v);
            let buf = encode_field_value(vec![], &val, false);
            let (_, decoded) = decode_field_value(&buf, false, &FieldKind::int()).unwrap();
            assert_eq!(decoded, val);

            // Test descending
            let buf = encode_field_value(vec![], &val, true);
            let (_, decoded) = decode_field_value(&buf, true, &FieldKind::int()).unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_encode_decode_float64() {
        let test_cases = vec![-1.5, 0.0, 1.5, f64::MIN, f64::MAX];

        for v in test_cases {
            let val = NormalValue::Float64(v);
            let buf = encode_field_value(vec![], &val, false);
            let (_, decoded) = decode_field_value(&buf, false, &FieldKind::float64()).unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_encode_decode_string() {
        let test_cases = vec!["", "hello", "world", "test string"];

        for v in test_cases {
            let val = NormalValue::String(v.to_string());
            let buf = encode_field_value(vec![], &val, false);
            let (_, decoded) = decode_field_value(&buf, false, &FieldKind::string()).unwrap();
            assert_eq!(decoded, val);

            // Test descending
            let buf = encode_field_value(vec![], &val, true);
            let (_, decoded) = decode_field_value(&buf, true, &FieldKind::string()).unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_encode_decode_time() {
        let dt = Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap();
        let val = NormalValue::Time(dt);

        let buf = encode_field_value(vec![], &val, false);
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::datetime()).unwrap();
        assert_eq!(decoded.as_time(), val.as_time());
    }

    #[test]
    fn test_encode_decode_null() {
        let val = NormalValue::Null;

        let buf = encode_field_value(vec![], &val, false);
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::int()).unwrap();
        assert!(decoded.is_nil());

        // Test descending null
        let buf = encode_field_value(vec![], &val, true);
        let (_, decoded) = decode_field_value(&buf, true, &FieldKind::int()).unwrap();
        assert!(decoded.is_nil());
    }

    #[test]
    fn test_sort_order_int_ascending() {
        let values: Vec<i64> = vec![-100, -1, 0, 1, 100];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|v| encode_field_value(vec![], &NormalValue::Int(*v), false))
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: {} should be < {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_sort_order_int_descending() {
        let values: Vec<i64> = vec![-100, -1, 0, 1, 100];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|v| encode_field_value(vec![], &NormalValue::Int(*v), true))
            .collect();

        // In descending, larger values should have smaller byte sequences
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] > encoded[i + 1],
                "descending sort order violated: {} should be > {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_sort_order_string_ascending() {
        let values = vec!["", "a", "aa", "ab", "b", "ba"];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|v| encode_field_value(vec![], &NormalValue::String(v.to_string()), false))
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: {:?} should be < {:?}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_indexed_field() {
        let field = IndexedField::ascending(NormalValue::Int(42));
        let buf = encode_indexed_field(vec![], &field);
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::int()).unwrap();
        assert_eq!(decoded, NormalValue::Int(42));

        let field = IndexedField::descending(NormalValue::Int(42));
        let buf = encode_indexed_field(vec![], &field);
        let (_, decoded) = decode_field_value(&buf, true, &FieldKind::int()).unwrap();
        assert_eq!(decoded, NormalValue::Int(42));
    }
}
