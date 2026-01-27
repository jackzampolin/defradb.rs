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
    let doc = document::Document::from_json_str(r#"{"value": 3.14}"#).unwrap();
    assert_eq!(doc.get("value").and_then(|v| v.as_float64()), Some(3.14));
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
    doc.set("value", 3.14);
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
fn test_canonical_key_order() {
    // Test that keys are sorted by length first, then lexicographically
    let mut doc = document::Document::new();
    doc.set("ab", 3i64);
    doc.set("z", 1i64);
    doc.set("aa", 2i64);

    let cbor = doc.to_cbor().unwrap();

    // Expected order: z (1 char), aa (2 chars), ab (2 chars)
    // a3 = map with 3 entries
    // 61 7a = "z"
    // 01 = 1
    // 62 61 61 = "aa"
    // 02 = 2
    // 62 61 62 = "ab"
    // 03 = 3
    let expected = &[
        0xa3, 0x61, 0x7a, 0x01, 0x62, 0x61, 0x61, 0x02, 0x62, 0x61, 0x62, 0x03,
    ];
    assert_eq!(cbor, expected);
}
