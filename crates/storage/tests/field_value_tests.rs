//! Tests for FieldValue - Field value encoding and decoding
//!
//! These tests verify that field values encode/decode correctly
//! and maintain proper sort order for indexing.

use chrono::{TimeZone, Utc};
use document::NormalValue;
use schema::FieldKind;
use storage::field_value::{
    decode_field_value, encode_field_value, encode_indexed_field, IndexedField,
};

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
        .map(|v| encode_field_value(vec![], &NormalValue::String(v.to_string()), false).unwrap())
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
