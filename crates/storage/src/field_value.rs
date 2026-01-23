
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
///
/// # Supported Types
///
/// - Bool, NillableBool
/// - Int, NillableInt
/// - Float32, NillableFloat32
/// - Float64, NillableFloat64
/// - String, NillableString
/// - Bytes, NillableBytes
/// - Time, NillableTime
/// - Null (and Nillable*None variants)
///
/// # Unsupported Types
///
/// Arrays, nested objects, and other complex types are not supported for indexing.
/// These will return an error with details about the unsupported type.
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
        // Unsupported types return an error with helpful context
        _ => Err(crate::corekv::Error::Other(format!(
            "unsupported field type for indexing: {:?}. Supported types: Bool, Int, Float32, \
             Float64, String, Bytes, Time (and their Nillable variants). Arrays and nested \
             objects cannot be indexed.",
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

// Tests extracted to crates/storage/tests/field_value_tests.rs
