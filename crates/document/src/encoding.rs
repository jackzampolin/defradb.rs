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

/// Coerce a JSON value to a `NormalValue` according to a declared scalar field
/// kind. This is the single source of truth shared by the mutation-create path
/// and the migration/reindex path so the two cannot drift on scalar encoding.
///
/// Critically, a `DateTime` field becomes `NormalValue::Time` (not a raw
/// RFC3339 string) so that secondary-index entries are byte-identical whether a
/// document is freshly written or rebuilt by a reindex. Divergence here
/// silently corrupts index-seek pagination and range filters on the field: the
/// index stores `encode_time_*` (nanoseconds) on the write path but would store
/// `encode_string_*` (UTF-8) on the reindex path, so seeks land in the wrong
/// place.
///
/// Returns `None` when `value` cannot be represented in `kind`; callers decide
/// whether that is a hard error (strict create path) or a best-effort fallback
/// (reindex path). Request-time sentinels (e.g. `UTC_NOW`) are resolved upstream
/// and are not handled here.
pub fn json_to_normal_value_for_kind(
    value: &serde_json::Value,
    kind: &schema::ScalarKind,
) -> Option<NormalValue> {
    use schema::ScalarKind;
    use serde_json::Value as JsonValue;

    match kind.base_kind() {
        ScalarKind::Int => value.as_i64().map(NormalValue::Int),
        ScalarKind::Float64 => value.as_f64().map(NormalValue::Float64),
        ScalarKind::Float32 => value.as_f64().map(|f| NormalValue::Float32(f as f32)),
        ScalarKind::Bool => value.as_bool().map(NormalValue::Bool),
        ScalarKind::String | ScalarKind::DocID => {
            value.as_str().map(|s| NormalValue::String(s.to_string()))
        }
        ScalarKind::Blob => value
            .as_str()
            .map(|s| NormalValue::Bytes(s.as_bytes().to_vec())),
        ScalarKind::DateTime => match value {
            JsonValue::String(s) => DateTime::parse_from_rfc3339(s).ok().map(NormalValue::Time),
            JsonValue::Number(n) => n.as_i64().and_then(|ts| {
                DateTime::from_timestamp(ts, 0).map(|dt| {
                    NormalValue::Time(dt.with_timezone(&FixedOffset::east_opt(0).unwrap()))
                })
            }),
            _ => None,
        },
        ScalarKind::Json | ScalarKind::None => None,
        _ => None,
    }
}

/// Coerce a JSON array to a `NormalValue` according to its declared scalar-array kind.
///
/// This mirrors the mutation write path's array representation so materialized documents and
/// rebuilt indexes do not fall back to `NormalValue::Json` and encode differently from fresh
/// writes.
pub fn json_to_normal_value_for_array_kind(
    value: &serde_json::Value,
    kind: &schema::ScalarArrayKind,
) -> Option<NormalValue> {
    use schema::ScalarArrayKind;
    use serde_json::Value as JsonValue;

    let values = value.as_array()?;

    match kind {
        ScalarArrayKind::BoolArray => values
            .iter()
            .map(|value| match value {
                JsonValue::Bool(value) => Some(*value),
                JsonValue::Null => None,
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(NormalValue::BoolArray),
        ScalarArrayKind::NillableBoolArray => values
            .iter()
            .map(|value| match value {
                JsonValue::Bool(value) => Some(Some(*value)),
                JsonValue::Null => Some(None),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(NormalValue::NillableBoolElementArray),
        ScalarArrayKind::IntArray => values
            .iter()
            .map(|value| match value {
                JsonValue::Number(value) => value.as_i64(),
                JsonValue::Null => None,
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(NormalValue::IntArray),
        ScalarArrayKind::NillableIntArray => values
            .iter()
            .map(|value| match value {
                JsonValue::Number(value) => value.as_i64().map(Some),
                JsonValue::Null => Some(None),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(NormalValue::NillableIntElementArray),
        ScalarArrayKind::Float64Array => values
            .iter()
            .map(|value| match value {
                JsonValue::Number(value) => value.as_f64(),
                JsonValue::Null => None,
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(NormalValue::Float64Array),
        ScalarArrayKind::NillableFloat64Array => values
            .iter()
            .map(|value| match value {
                JsonValue::Number(value) => value.as_f64().map(Some),
                JsonValue::Null => Some(None),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(NormalValue::NillableFloat64ElementArray),
        ScalarArrayKind::Float32Array => values
            .iter()
            .map(|value| match value {
                JsonValue::Number(value) => value.as_f64().map(|value| value as f32),
                JsonValue::Null => None,
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(NormalValue::Float32Array),
        ScalarArrayKind::NillableFloat32Array => values
            .iter()
            .map(|value| match value {
                JsonValue::Number(value) => value.as_f64().map(|value| Some(value as f32)),
                JsonValue::Null => Some(None),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(NormalValue::NillableFloat32ElementArray),
        ScalarArrayKind::StringArray => values
            .iter()
            .map(|value| match value {
                JsonValue::String(value) => Some(value.clone()),
                JsonValue::Null => None,
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(NormalValue::StringArray),
        ScalarArrayKind::NillableStringArray => values
            .iter()
            .map(|value| match value {
                JsonValue::String(value) => Some(Some(value.clone())),
                JsonValue::Null => Some(None),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(NormalValue::NillableStringElementArray),
        ScalarArrayKind::DateTimeArray => values
            .iter()
            .map(|value| match value {
                JsonValue::String(value) => DateTime::parse_from_rfc3339(value).ok(),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(NormalValue::TimeArray),
        ScalarArrayKind::NillableDateTimeArray => values
            .iter()
            .map(|value| match value {
                JsonValue::String(value) => DateTime::parse_from_rfc3339(value).ok().map(Some),
                JsonValue::Null => Some(None),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(NormalValue::NillableTimeElementArray),
        _ => None,
    }
}

/// Re-type a value read back from document storage against its declared scalar
/// kind, repairing the schema-blind CBOR round-trip.
///
/// Document storage encodes a `DateTime` as an *untagged* CBOR text string
/// (`encoding_cbor`: `Time` → `Text`) and decodes every text back to
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
/// already-correct values and for every non-`DateTime` kind (`Int`, `Float`,
/// `Bytes`, etc. survive the CBOR round-trip losslessly; only `Time` downgrades).
pub fn coerce_stored_value_for_kind(value: NormalValue, kind: &schema::ScalarKind) -> NormalValue {
    use schema::ScalarKind;
    match (kind.base_kind(), &value) {
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

/// Env var for the #1294 e2e harness: store Blob inputs as hex-decoded
/// [`NormalValue::Bytes`] so create/query response converters are exercised.
///
/// Production paths leave this unset; Blob fields remain hex **strings**.
pub const TEST_BLOB_AS_BYTES_ENV: &str = "DEFRA_TEST_BLOB_AS_BYTES";

/// True when [`TEST_BLOB_AS_BYTES_ENV`] is `1` or `true`.
pub fn test_blob_as_bytes_enabled() -> bool {
    matches!(
        std::env::var(TEST_BLOB_AS_BYTES_ENV).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

/// Decode a hex string (mixed case, even length) to raw bytes.
pub fn decode_hex_blob(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = from_hex_digit(bytes[i])?;
        let lo = from_hex_digit(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Some(out)
}

fn from_hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
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
mod json_to_normal_value_for_kind_tests {
    use super::*;
    use schema::{ScalarArrayKind, ScalarKind};
    use serde_json::json;

    #[test]
    fn datetime_string_becomes_time_not_string() {
        // The regression: a DateTime field MUST coerce to Time so reindexed
        // index entries match freshly-written ones (encode_time, not encode_string).
        let nv =
            json_to_normal_value_for_kind(&json!("2026-05-29T13:06:28Z"), &ScalarKind::DateTime);
        let expected = DateTime::parse_from_rfc3339("2026-05-29T13:06:28Z").unwrap();
        assert_eq!(nv, Some(NormalValue::Time(expected)));
    }

    #[test]
    fn datetime_unix_timestamp_becomes_time() {
        let nv = json_to_normal_value_for_kind(&json!(1_764_421_588_i64), &ScalarKind::DateTime);
        match nv {
            Some(NormalValue::Time(_)) => {}
            other => panic!("expected Time, got {other:?}"),
        }
    }

    #[test]
    fn scalar_kinds_coerce_as_expected() {
        assert_eq!(
            json_to_normal_value_for_kind(&json!(42), &ScalarKind::Int),
            Some(NormalValue::Int(42))
        );
        assert_eq!(
            json_to_normal_value_for_kind(&json!(1.5), &ScalarKind::Float64),
            Some(NormalValue::Float64(1.5))
        );
        assert_eq!(
            json_to_normal_value_for_kind(&json!(1.5), &ScalarKind::Float32),
            Some(NormalValue::Float32(1.5))
        );
        assert_eq!(
            json_to_normal_value_for_kind(&json!(true), &ScalarKind::Bool),
            Some(NormalValue::Bool(true))
        );
        assert_eq!(
            json_to_normal_value_for_kind(&json!("hi"), &ScalarKind::String),
            Some(NormalValue::String("hi".to_string()))
        );
    }

    #[test]
    fn non_nillable_arrays_reject_null_elements() {
        let cases = [
            (json!([true, null]), ScalarArrayKind::BoolArray),
            (json!([1, null]), ScalarArrayKind::IntArray),
            (json!([1.5, null]), ScalarArrayKind::Float64Array),
            (json!([1.5, null]), ScalarArrayKind::Float32Array),
            (json!(["value", null]), ScalarArrayKind::StringArray),
            (
                json!(["2026-05-29T13:06:28Z", null]),
                ScalarArrayKind::DateTimeArray,
            ),
        ];

        for (value, kind) in cases {
            assert_eq!(
                json_to_normal_value_for_array_kind(&value, &kind),
                None,
                "{kind:?} must not invent a default for a null element"
            );
        }
    }

    #[test]
    fn nillable_array_preserves_null_elements() {
        assert_eq!(
            json_to_normal_value_for_array_kind(
                &json!([true, null]),
                &ScalarArrayKind::NillableBoolArray,
            ),
            Some(NormalValue::NillableBoolElementArray(vec![
                Some(true),
                None,
            ]))
        );
        assert_eq!(
            json_to_normal_value_for_array_kind(
                &json!(["2026-05-29T13:06:28Z", null]),
                &ScalarArrayKind::NillableDateTimeArray,
            ),
            Some(NormalValue::NillableTimeElementArray(vec![
                Some(DateTime::parse_from_rfc3339("2026-05-29T13:06:28Z").unwrap()),
                None,
            ]))
        );
    }

    #[test]
    fn coerce_stored_datetime_string_to_time() {
        // The #72 repair: a DateTime read back from CBOR storage is a String;
        // it must coerce to Time so the index encoder matches live-write entries.
        let v = NormalValue::String("2026-05-29T13:06:28Z".to_string());
        let got = coerce_stored_value_for_kind(v, &ScalarKind::DateTime);
        let expected = DateTime::parse_from_rfc3339("2026-05-29T13:06:28Z").unwrap();
        assert_eq!(got, NormalValue::Time(expected));
    }

    #[test]
    fn coerce_stored_value_is_noop_for_non_datetime_and_correct_values() {
        // A genuine String field is untouched (even if it looks date-like).
        let s = NormalValue::String("2026-05-29T13:06:28Z".to_string());
        assert_eq!(
            coerce_stored_value_for_kind(s.clone(), &ScalarKind::String),
            s
        );
        // An already-correct Time is untouched.
        let t = NormalValue::Time(DateTime::parse_from_rfc3339("2026-05-29T13:06:28Z").unwrap());
        assert_eq!(
            coerce_stored_value_for_kind(t.clone(), &ScalarKind::DateTime),
            t
        );
        // A non-RFC3339 string for a DateTime field is left as-is (no panic).
        let bad = NormalValue::String("not-a-date".to_string());
        assert_eq!(
            coerce_stored_value_for_kind(bad.clone(), &ScalarKind::DateTime),
            bad
        );
    }

    #[test]
    fn unrepresentable_value_returns_none() {
        // A non-RFC3339 string for a DateTime field cannot be coerced; the caller
        // decides whether that's an error (create) or a fallback (reindex).
        assert_eq!(
            json_to_normal_value_for_kind(&json!("not-a-date"), &ScalarKind::DateTime),
            None
        );
        // Json/None kinds are handled by callers, not here.
        assert_eq!(
            json_to_normal_value_for_kind(&json!({"a": 1}), &ScalarKind::Json),
            None
        );
    }
}
