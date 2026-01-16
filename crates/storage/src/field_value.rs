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

// Re-export IndexedField from its canonical location
pub use crate::keys::datastore::IndexedField;

/// Encode a NormalValue to bytes using order-preserving encoding.
pub fn encode_field_value(buf: Vec<u8>, val: &NormalValue, descending: bool) -> Result<Vec<u8>> {
    if val.is_nil() {
        return Ok(if descending {
            encoding::encode_null_descending(buf)
        } else {
            encoding::encode_null_ascending(buf)
        });
    }

    match val {
        NormalValue::Bool(v) => Ok(if descending {
            encoding::encode_bool_descending(buf, *v)
        } else {
            encoding::encode_bool_ascending(buf, *v)
        }),
        NormalValue::NillableBool(Some(v)) => Ok(if descending {
            encoding::encode_bool_descending(buf, *v)
        } else {
            encoding::encode_bool_ascending(buf, *v)
        }),
        NormalValue::Int(v) => Ok(if descending {
            encoding::encode_varint_descending(buf, *v)
        } else {
            encoding::encode_varint_ascending(buf, *v)
        }),
        NormalValue::NillableInt(Some(v)) => Ok(if descending {
            encoding::encode_varint_descending(buf, *v)
        } else {
            encoding::encode_varint_ascending(buf, *v)
        }),
        NormalValue::Float32(v) => Ok(if descending {
            encoding::encode_float32_descending(buf, *v)
        } else {
            encoding::encode_float32_ascending(buf, *v)
        }),
        NormalValue::NillableFloat32(Some(v)) => Ok(if descending {
            encoding::encode_float32_descending(buf, *v)
        } else {
            encoding::encode_float32_ascending(buf, *v)
        }),
        NormalValue::Float64(v) => Ok(if descending {
            encoding::encode_float64_descending(buf, *v)
        } else {
            encoding::encode_float64_ascending(buf, *v)
        }),
        NormalValue::NillableFloat64(Some(v)) => Ok(if descending {
            encoding::encode_float64_descending(buf, *v)
        } else {
            encoding::encode_float64_ascending(buf, *v)
        }),
        NormalValue::String(v) => Ok(if descending {
            encoding::encode_string_descending(buf, v)
        } else {
            encoding::encode_string_ascending(buf, v)
        }),
        NormalValue::NillableString(Some(v)) => Ok(if descending {
            encoding::encode_string_descending(buf, v)
        } else {
            encoding::encode_string_ascending(buf, v)
        }),
        NormalValue::Bytes(v) => Ok(if descending {
            encoding::encode_bytes_descending(buf, v)
        } else {
            encoding::encode_bytes_ascending(buf, v)
        }),
        NormalValue::NillableBytes(Some(v)) => Ok(if descending {
            encoding::encode_bytes_descending(buf, v)
        } else {
            encoding::encode_bytes_ascending(buf, v)
        }),
        NormalValue::Time(v) => {
            let nanos = v.timestamp_nanos_opt().ok_or_else(|| {
                crate::corekv::Error::Other(format!(
                    "timestamp {} cannot be encoded as nanoseconds (overflow)",
                    v
                ))
            })?;
            Ok(if descending {
                encoding::encode_time_descending(buf, nanos)
            } else {
                encoding::encode_time_ascending(buf, nanos)
            })
        }
        NormalValue::NillableTime(Some(v)) => {
            let nanos = v.timestamp_nanos_opt().ok_or_else(|| {
                crate::corekv::Error::Other(format!(
                    "timestamp {} cannot be encoded as nanoseconds (overflow)",
                    v
                ))
            })?;
            Ok(if descending {
                encoding::encode_time_descending(buf, nanos)
            } else {
                encoding::encode_time_ascending(buf, nanos)
            })
        }
        // Nillable None variants are handled by is_nil() check above
        // Unsupported types return an error
        _ => Err(crate::corekv::Error::Other(format!(
            "unsupported field type for indexing: {:?}",
            val
        ))),
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
            let is_string = matches!(kind, FieldKind::Scalar(ScalarKind::String));
            if is_string {
                let s = String::from_utf8(v)
                    .map_err(|e| crate::corekv::Error::Other(format!("invalid utf-8: {}", e)))?;
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
            // Convert nanoseconds to DateTime, handling negative timestamps correctly
            let (secs, nsecs) = if nanos >= 0 {
                (nanos / 1_000_000_000, (nanos % 1_000_000_000) as u32)
            } else {
                // For negative nanoseconds, adjust to get positive subsecond component
                let secs = (nanos - 999_999_999) / 1_000_000_000;
                let nsecs = ((nanos % 1_000_000_000) + 1_000_000_000) % 1_000_000_000;
                (secs, nsecs as u32)
            };
            let dt = Utc.timestamp_opt(secs, nsecs).single().ok_or_else(|| {
                crate::corekv::Error::Other(format!(
                    "invalid timestamp: cannot construct DateTime from secs={}, nsecs={}",
                    secs, nsecs
                ))
            })?;
            Ok((rest, NormalValue::Time(dt)))
        }
        _ => Err(crate::corekv::Error::Other(format!(
            "cannot decode field value: unknown type {:?} (marker byte: 0x{:02x}, buffer len: {})",
            typ,
            buf.first().unwrap_or(&0),
            buf.len()
        ))),
    }
}

/// Encode an IndexedField to bytes
pub fn encode_indexed_field(buf: Vec<u8>, field: &IndexedField) -> Result<Vec<u8>> {
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
            let buf = encode_field_value(vec![], &val, false).unwrap();
            let (_, decoded) = decode_field_value(&buf, false, &FieldKind::bool()).unwrap();
            assert_eq!(decoded, val);

            // Test descending
            let buf = encode_field_value(vec![], &val, true).unwrap();
            let (_, decoded) = decode_field_value(&buf, true, &FieldKind::bool()).unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_encode_decode_int() {
        let test_cases = vec![-1000i64, -1, 0, 1, 1000, i64::MIN, i64::MAX];

        for v in test_cases {
            let val = NormalValue::Int(v);
            let buf = encode_field_value(vec![], &val, false).unwrap();
            let (_, decoded) = decode_field_value(&buf, false, &FieldKind::int()).unwrap();
            assert_eq!(decoded, val);

            // Test descending
            let buf = encode_field_value(vec![], &val, true).unwrap();
            let (_, decoded) = decode_field_value(&buf, true, &FieldKind::int()).unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_encode_decode_float64() {
        let test_cases = vec![-1.5, 0.0, 1.5, f64::MIN, f64::MAX];

        for v in test_cases {
            let val = NormalValue::Float64(v);
            let buf = encode_field_value(vec![], &val, false).unwrap();
            let (_, decoded) = decode_field_value(&buf, false, &FieldKind::float64()).unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_encode_decode_string() {
        let test_cases = vec!["", "hello", "world", "test string"];

        for v in test_cases {
            let val = NormalValue::String(v.to_string());
            let buf = encode_field_value(vec![], &val, false).unwrap();
            let (_, decoded) = decode_field_value(&buf, false, &FieldKind::string()).unwrap();
            assert_eq!(decoded, val);

            // Test descending
            let buf = encode_field_value(vec![], &val, true).unwrap();
            let (_, decoded) = decode_field_value(&buf, true, &FieldKind::string()).unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_encode_decode_time() {
        let dt = Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap();
        let val = NormalValue::Time(dt);

        let buf = encode_field_value(vec![], &val, false).unwrap();
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::datetime()).unwrap();
        assert_eq!(decoded.as_time(), val.as_time());
    }

    #[test]
    fn test_encode_decode_null() {
        let val = NormalValue::Null;

        let buf = encode_field_value(vec![], &val, false).unwrap();
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::int()).unwrap();
        assert!(decoded.is_nil());

        // Test descending null
        let buf = encode_field_value(vec![], &val, true).unwrap();
        let (_, decoded) = decode_field_value(&buf, true, &FieldKind::int()).unwrap();
        assert!(decoded.is_nil());
    }

    #[test]
    fn test_sort_order_int_ascending() {
        let values: Vec<i64> = vec![-100, -1, 0, 1, 100];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|v| encode_field_value(vec![], &NormalValue::Int(*v), false).unwrap())
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
            .map(|v| encode_field_value(vec![], &NormalValue::Int(*v), true).unwrap())
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
            .map(|v| {
                encode_field_value(vec![], &NormalValue::String(v.to_string()), false).unwrap()
            })
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
        let buf = encode_indexed_field(vec![], &field).unwrap();
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::int()).unwrap();
        assert_eq!(decoded, NormalValue::Int(42));

        let field = IndexedField::descending(NormalValue::Int(42));
        let buf = encode_indexed_field(vec![], &field).unwrap();
        let (_, decoded) = decode_field_value(&buf, true, &FieldKind::int()).unwrap();
        assert_eq!(decoded, NormalValue::Int(42));
    }

    #[test]
    fn test_pre_epoch_timestamp() {
        // Test a date before Unix epoch (1969-06-15)
        let dt = Utc.with_ymd_and_hms(1969, 6, 15, 12, 30, 45).unwrap();
        let val = NormalValue::Time(dt);

        let buf = encode_field_value(vec![], &val, false).unwrap();
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::datetime()).unwrap();
        assert_eq!(decoded.as_time(), val.as_time());

        // Test descending
        let buf = encode_field_value(vec![], &val, true).unwrap();
        let (_, decoded) = decode_field_value(&buf, true, &FieldKind::datetime()).unwrap();
        assert_eq!(decoded.as_time(), val.as_time());
    }

    #[test]
    fn test_nillable_variants() {
        // Test NillableBool
        let val = NormalValue::NillableBool(Some(true));
        let buf = encode_field_value(vec![], &val, false).unwrap();
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::bool()).unwrap();
        assert_eq!(decoded, NormalValue::Bool(true));

        // Test NillableInt
        let val = NormalValue::NillableInt(Some(42));
        let buf = encode_field_value(vec![], &val, false).unwrap();
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::int()).unwrap();
        assert_eq!(decoded, NormalValue::Int(42));

        // Test NillableFloat32
        let val = NormalValue::NillableFloat32(Some(3.14));
        let buf = encode_field_value(vec![], &val, false).unwrap();
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::float32()).unwrap();
        assert_eq!(decoded, NormalValue::Float32(3.14));

        // Test NillableFloat64
        let val = NormalValue::NillableFloat64(Some(3.14159));
        let buf = encode_field_value(vec![], &val, false).unwrap();
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::float64()).unwrap();
        assert_eq!(decoded, NormalValue::Float64(3.14159));

        // Test NillableString
        let val = NormalValue::NillableString(Some("hello".to_string()));
        let buf = encode_field_value(vec![], &val, false).unwrap();
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::string()).unwrap();
        assert_eq!(decoded, NormalValue::String("hello".to_string()));

        // Test NillableBytes
        let val = NormalValue::NillableBytes(Some(vec![1, 2, 3]));
        let buf = encode_field_value(vec![], &val, false).unwrap();
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::blob()).unwrap();
        assert_eq!(decoded, NormalValue::Bytes(vec![1, 2, 3]));

        // Test NillableTime
        let dt = Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap();
        let val = NormalValue::NillableTime(Some(dt));
        let buf = encode_field_value(vec![], &val, false).unwrap();
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::datetime()).unwrap();
        assert_eq!(decoded.as_time(), Some(&dt));

        // Test Nillable None variants encode as null
        let val = NormalValue::NillableInt(None);
        let buf = encode_field_value(vec![], &val, false).unwrap();
        let (_, decoded) = decode_field_value(&buf, false, &FieldKind::int()).unwrap();
        assert!(decoded.is_nil());
    }
}
