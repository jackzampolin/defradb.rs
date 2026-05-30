//! Encoding helpers for JSON and CBOR conversion

use chrono::{DateTime, FixedOffset, SecondsFormat, Timelike};

use crate::error::{Error, Result};
use crate::NormalValue;

// Re-export CBOR encoding functions
pub use crate::encoding_cbor::{cbor_to_normal_value, normal_value_to_cbor};

/// Format a DateTime to RFC3339 matching Go's time.RFC3339Nano behavior.
///
/// Go's RFC3339Nano format omits fractional seconds when nanoseconds are zero,
/// but includes all 9 digits when non-zero. This is critical for docID compatibility
/// since the time string is embedded in the CBOR encoding used to compute the hash.
///
/// Examples:
/// - No nanoseconds: "2017-07-23T03:46:56-05:00"
/// - With nanoseconds: "2017-07-23T03:46:56.123456789-05:00"
pub fn format_time_rfc3339_nano(t: &DateTime<FixedOffset>) -> String {
    if t.nanosecond() == 0 {
        // No fractional seconds - use Secs format
        t.to_rfc3339_opts(SecondsFormat::Secs, true)
    } else {
        // Has fractional seconds - format with nanos then trim trailing zeros
        // to match Go's time.RFC3339Nano which omits unnecessary trailing zeros.
        // e.g., ".123000000" becomes ".123"
        let s = t.to_rfc3339_opts(SecondsFormat::Nanos, true);
        trim_rfc3339_trailing_zeros(&s)
    }
}

/// Trim trailing zeros from the fractional seconds portion of an RFC3339 string.
/// "2024-01-01T00:00:00.123000000Z" -> "2024-01-01T00:00:00.123Z"
/// "2024-01-01T00:00:00.100000000Z" -> "2024-01-01T00:00:00.1Z"
pub fn trim_rfc3339_trailing_zeros(s: &str) -> String {
    // Find the '.' that starts fractional seconds
    if let Some(dot_pos) = s.rfind('.') {
        // Find where the fractional digits end (before timezone Z or +/-)
        let tz_start = s[dot_pos..].find(['Z', '+', '-']);
        if let Some(tz_offset) = tz_start {
            let tz_pos = dot_pos + tz_offset;
            let frac = &s[dot_pos + 1..tz_pos];
            let trimmed = frac.trim_end_matches('0');
            if trimmed.is_empty() {
                // All zeros — drop the dot entirely
                format!("{}{}", &s[..dot_pos], &s[tz_pos..])
            } else {
                format!("{}.{}{}", &s[..dot_pos], trimmed, &s[tz_pos..])
            }
        } else {
            s.to_string()
        }
    } else {
        s.to_string()
    }
}

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

/// Re-type a value read back from document storage against its declared scalar
/// kind, repairing the schema-blind CBOR round-trip.
///
/// Document storage encodes a `DateTime` as an *untagged* CBOR text string
/// (`encoding_cbor`: `Time` -> `Text`) and decodes every text back to
/// `NormalValue::String` — the schema is not consulted. So any value loaded via
/// `Document::from_cbor` carries a `DateTime` field as a `String`. Fed to the
/// secondary-index encoder, that `String` is `encode_string_*` instead of
/// `encode_time_*`: a different type marker and byte magnitude, landing in a
/// disjoint range from live-written `Time` entries — so the rows silently
/// vanish from `DateTime` cursor/range queries (the "#72" hidden-rows bug, and
/// why a plain reindex re-encodes the whole index as String).
///
/// Coercing here makes index entries identical whether a document came from a
/// live write (`Time`) or a storage round-trip (`String`). It is a no-op for
/// already-correct values and for every non-`DateTime` kind.
pub fn coerce_stored_value_for_kind(value: NormalValue, kind: &schema::ScalarKind) -> NormalValue {
    use schema::ScalarKind;
    match (kind, &value) {
        (ScalarKind::DateTime, NormalValue::String(s)) => match DateTime::parse_from_rfc3339(s) {
            Ok(dt) => NormalValue::Time(dt),
            Err(_) => value,
        },
        _ => value,
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
        NormalValue::Time(t) => Ok(serde_json::Value::String(format_time_rfc3339_nano(t))),
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
    // Match Go's json.Marshal behavior: float64 values that are whole numbers
    // are serialized without a decimal point (e.g., float64(21.0) → "21").
    if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
        return Ok(serde_json::Value::Number(serde_json::Number::from(
            f as i64,
        )));
    }
    serde_json::Number::from_f64(f)
        .map(serde_json::Value::Number)
        .ok_or_else(|| Error::NonFiniteFloat(format!("{}", f)))
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
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

#[cfg(test)]
mod coerce_stored_value_tests {
    use super::*;
    use schema::ScalarKind;

    #[test]
    fn datetime_string_coerces_to_time() {
        let v = NormalValue::String("2026-05-29T13:06:28Z".to_string());
        let got = coerce_stored_value_for_kind(v, &ScalarKind::DateTime);
        let expected = DateTime::parse_from_rfc3339("2026-05-29T13:06:28Z").unwrap();
        assert_eq!(got, NormalValue::Time(expected));
    }

    #[test]
    fn noop_for_string_field_and_correct_values() {
        let s = NormalValue::String("2026-05-29T13:06:28Z".to_string());
        assert_eq!(coerce_stored_value_for_kind(s.clone(), &ScalarKind::String), s);
        let t = NormalValue::Time(DateTime::parse_from_rfc3339("2026-05-29T13:06:28Z").unwrap());
        assert_eq!(
            coerce_stored_value_for_kind(t.clone(), &ScalarKind::DateTime),
            t
        );
        let bad = NormalValue::String("not-a-date".to_string());
        assert_eq!(
            coerce_stored_value_for_kind(bad.clone(), &ScalarKind::DateTime),
            bad
        );
    }
}
