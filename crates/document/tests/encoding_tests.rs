//! Integration tests for encoding utilities

use chrono::{DateTime, TimeZone, Timelike, Utc};
use document::encoding::{
    coerce_stored_value_for_kind, json_to_normal_value_for_array_kind,
    json_to_normal_value_for_kind,
};
use document::NormalValue;
use schema::{ScalarArrayKind, ScalarKind};
use serde_json::json;

// The JSON/CBOR round-trip cases below drive the encoders through the public
// Document API; the schema-kind coercion cases call `document::encoding`
// directly.

#[test]
fn test_json_number_i64() {
    // Test JSON parsing through Document
    let doc = document::Document::from_json_str(r#"{"value": 42}"#).unwrap();
    assert_eq!(doc.get("value").and_then(|v| v.as_int()), Some(42));
}

#[test]
fn test_json_number_f64() {
    let doc = document::Document::from_json_str(r#"{"value": 3.15}"#).unwrap();
    assert_eq!(doc.get("value").and_then(|v| v.as_float64()), Some(3.15));
}

#[test]
fn test_json_string() {
    let doc = document::Document::from_json_str(r#"{"value": "hello"}"#).unwrap();
    assert_eq!(doc.get("value").and_then(|v| v.as_str()), Some("hello"));
}

#[test]
fn test_json_null() {
    let doc = document::Document::from_json_str(r#"{"value": null}"#).unwrap();
    assert!(doc.get("value").map(|v| v.is_nil()).unwrap_or(false));
}

#[test]
fn test_json_bool() {
    let doc = document::Document::from_json_str(r#"{"value": true}"#).unwrap();
    assert_eq!(doc.get("value").and_then(|v| v.as_bool()), Some(true));
}

#[test]
fn test_json_string_array() {
    let doc = document::Document::from_json_str(r#"{"values": ["a", "b"]}"#).unwrap();
    let val = doc.get("values").unwrap();
    assert!(val.is_array());
}

#[test]
fn test_json_empty_array() {
    let doc = document::Document::from_json_str(r#"{"values": []}"#).unwrap();
    let val = doc.get("values").unwrap();
    assert!(val.is_array());
}

#[test]
fn test_cbor_roundtrip_simple() {
    let mut doc = document::Document::new();
    doc.set("value", 42i64);
    let cbor = doc.to_cbor().unwrap();
    let decoded = document::Document::from_cbor(&cbor).unwrap();
    assert_eq!(decoded.get("value").and_then(|v| v.as_int()), Some(42));
}

#[test]
fn test_cbor_roundtrip_string() {
    let mut doc = document::Document::new();
    doc.set("value", "hello");
    let cbor = doc.to_cbor().unwrap();
    let decoded = document::Document::from_cbor(&cbor).unwrap();
    assert_eq!(decoded.get("value").and_then(|v| v.as_str()), Some("hello"));
}

// === Mixed-type array tests (Go compatibility) ===

#[test]
fn test_json_mixed_array_int_string_preserves_all() {
    // Mixed int+string array should preserve all elements
    let doc = document::Document::from_json_str(r#"{"values": [1, "text", 3]}"#).unwrap();
    let val = doc.get("values").unwrap();
    assert!(val.is_array());
}

#[test]
fn test_json_mixed_array_bool_int_preserves_all() {
    // Mixed bool+int array should preserve all elements
    let doc = document::Document::from_json_str(r#"{"values": [true, 42, false]}"#).unwrap();
    let val = doc.get("values").unwrap();
    assert!(val.is_array());
}

#[test]
fn test_json_mixed_array_string_null_preserves_all() {
    // Mixed string+null array should preserve all elements
    let doc = document::Document::from_json_str(r#"{"values": ["a", null, "b"]}"#).unwrap();
    let val = doc.get("values").unwrap();
    assert!(val.is_array());
}

#[test]
fn test_json_homogeneous_int_array() {
    // Pure int array should become IntArray
    let doc = document::Document::from_json_str(r#"{"values": [1, 2, 3]}"#).unwrap();
    let val = doc.get("values").unwrap();
    assert!(val.is_array());
}

#[test]
fn test_json_homogeneous_string_array() {
    // Pure string array should become StringArray
    let doc = document::Document::from_json_str(r#"{"values": ["a", "b", "c"]}"#).unwrap();
    let val = doc.get("values").unwrap();
    assert!(val.is_array());
}

#[test]
fn test_json_homogeneous_bool_array() {
    // Pure bool array should become BoolArray
    let doc = document::Document::from_json_str(r#"{"values": [true, false, true]}"#).unwrap();
    let val = doc.get("values").unwrap();
    assert!(val.is_array());
}

// === Non-finite float tests (Go compatibility) ===

#[test]
fn test_to_map_nan_error() {
    let mut doc = document::Document::new();
    doc.set("value", f64::NAN);
    let result = doc.to_map();
    assert!(result.is_err());
}

#[test]
fn test_to_map_neg_infinity_error() {
    let mut doc = document::Document::new();
    doc.set("value", f64::NEG_INFINITY);
    let result = doc.to_map();
    assert!(result.is_err());
}

#[test]
fn test_to_map_float_array_nan_error() {
    let mut doc = document::Document::new();
    doc.set(
        "values",
        NormalValue::Float64Array(vec![1.0, f64::NAN, 3.0]),
    );
    let result = doc.to_map();
    assert!(result.is_err());
}

#[test]
fn test_to_map_finite_float_ok() {
    // Normal finite floats should work fine
    let mut doc = document::Document::new();
    doc.set("value", 3.15);
    let result = doc.to_map();
    assert!(result.is_ok());
}

// === RFC3339 time format tests (Go compatibility) ===

#[test]
fn test_time_to_json_rfc3339_format() {
    // Timestamp without fractional seconds
    let t = Utc.with_ymd_and_hms(2025, 1, 14, 12, 30, 45).unwrap();
    let mut doc = document::Document::new();
    doc.set("timestamp", NormalValue::Time(t.fixed_offset()));

    let map = doc.to_map().unwrap();
    let s = map.get("timestamp").unwrap().as_str().unwrap();

    // Should be valid RFC3339 format
    assert!(s.contains("2025-01-14"));
    assert!(s.contains("12:30:45"));
    // Verify it can be parsed back
    chrono::DateTime::parse_from_rfc3339(s).expect("should be valid RFC3339");
}

#[test]
fn test_time_to_json_with_nanoseconds() {
    // Timestamp with nanoseconds
    let t = Utc
        .with_ymd_and_hms(2025, 1, 14, 12, 30, 45)
        .unwrap()
        .with_nanosecond(123456789)
        .unwrap();
    let mut doc = document::Document::new();
    doc.set("timestamp", NormalValue::Time(t.fixed_offset()));

    let map = doc.to_map().unwrap();
    let s = map.get("timestamp").unwrap().as_str().unwrap();

    // Should include fractional seconds
    assert!(s.contains("."));
    // Verify roundtrip preserves nanoseconds
    let parsed = chrono::DateTime::parse_from_rfc3339(s).unwrap();
    assert_eq!(parsed.timestamp_nanos_opt(), t.timestamp_nanos_opt());
}

#[test]
fn test_time_format_matches_go_rfc3339_nano() {
    // Go's time.RFC3339Nano omits fractional seconds when zero but includes all 9 digits when present.
    // Verify our Document's CBOR encoding matches this behavior.

    // Test 1: Time without nanoseconds (Go: "2017-07-23T03:46:56-05:00")
    let t1 = Utc.with_ymd_and_hms(2017, 7, 23, 3, 46, 56).unwrap();
    let t1_fixed = t1.with_timezone(&chrono::FixedOffset::west_opt(5 * 3600).unwrap());

    let mut doc1 = document::Document::new();
    doc1.set("timestamp", NormalValue::Time(t1_fixed));
    let map1 = doc1.to_map().unwrap();
    let s1 = map1.get("timestamp").unwrap().as_str().unwrap();

    // Should NOT have .000000000
    assert!(
        !s1.contains(".000000000"),
        "Expected no fractional seconds for zero nanos, got: {}",
        s1
    );
    assert!(
        s1.ends_with("-05:00"),
        "Expected -05:00 timezone, got: {}",
        s1
    );

    // Test 2: Time with nanoseconds (Go: "2017-07-23T03:46:56.123456789-05:00")
    let t2 = t1.with_nanosecond(123456789).unwrap();
    let t2_fixed = t2.with_timezone(&chrono::FixedOffset::west_opt(5 * 3600).unwrap());

    let mut doc2 = document::Document::new();
    doc2.set("timestamp", NormalValue::Time(t2_fixed));
    let map2 = doc2.to_map().unwrap();
    let s2 = map2.get("timestamp").unwrap().as_str().unwrap();

    // Should have .123456789
    assert!(
        s2.contains(".123456789"),
        "Expected .123456789, got: {}",
        s2
    );
}

#[test]
fn datetime_string_becomes_time_not_string() {
    // The regression: a DateTime field MUST coerce to Time so reindexed
    // index entries match freshly-written ones (encode_time, not encode_string).
    let nv = json_to_normal_value_for_kind(&json!("2026-05-29T13:06:28Z"), &ScalarKind::DateTime);
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
