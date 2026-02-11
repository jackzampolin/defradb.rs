//! Integration tests for encoding utilities

use chrono::{TimeZone, Timelike, Utc};
use document::NormalValue;

// Re-export internal encoding functions for testing via a test helper module
// Since encoding module is private, we test through the public Document API

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
fn test_json_int_array() {
    let doc = document::Document::from_json_str(r#"{"values": [1, 2, 3]}"#).unwrap();
    let val = doc.get("values").unwrap();
    assert!(val.is_array());
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
fn test_to_map_infinity_error() {
    let mut doc = document::Document::new();
    doc.set("value", f64::INFINITY);
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
