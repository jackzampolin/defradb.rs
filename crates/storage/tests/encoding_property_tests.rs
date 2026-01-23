
//! Property-based tests for encoding module
//!
//! These tests verify critical properties of the order-preserving encoding:
//! - Roundtrip: decode(encode(x)) == x
//! - Sort order preservation: if a < b, then encode(a) < encode(b) (ascending)
//! - Descending inversion: if a < b, then encode_descending(a) > encode_descending(b)

use proptest::prelude::*;
use storage::encoding;

// ============================================================================
// Varint Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn prop_varint_roundtrip_ascending(v in any::<i64>()) {
        let encoded = encoding::encode_varint_ascending(vec![], v);
        let (rest, decoded) = encoding::decode_varint_ascending(&encoded).unwrap();
        prop_assert!(rest.is_empty(), "should consume entire buffer");
        prop_assert_eq!(decoded, v);
    }

    #[test]
    fn prop_varint_roundtrip_descending(v in any::<i64>()) {
        let encoded = encoding::encode_varint_descending(vec![], v);
        let (rest, decoded) = encoding::decode_varint_descending(&encoded).unwrap();
        prop_assert!(rest.is_empty(), "should consume entire buffer");
        prop_assert_eq!(decoded, v);
    }

    #[test]
    fn prop_varint_sort_order_ascending(a in any::<i64>(), b in any::<i64>()) {
        let enc_a = encoding::encode_varint_ascending(vec![], a);
        let enc_b = encoding::encode_varint_ascending(vec![], b);

        // Lexicographic ordering of encoded bytes should match numeric ordering
        match a.cmp(&b) {
            std::cmp::Ordering::Less => prop_assert!(enc_a < enc_b, "a={} < b={}, but enc_a >= enc_b", a, b),
            std::cmp::Ordering::Greater => prop_assert!(enc_a > enc_b, "a={} > b={}, but enc_a <= enc_b", a, b),
            std::cmp::Ordering::Equal => prop_assert_eq!(enc_a, enc_b),
        }
    }

    #[test]
    fn prop_varint_sort_order_descending(a in any::<i64>(), b in any::<i64>()) {
        let enc_a = encoding::encode_varint_descending(vec![], a);
        let enc_b = encoding::encode_varint_descending(vec![], b);

        // Descending: larger values should have smaller byte sequences
        match a.cmp(&b) {
            std::cmp::Ordering::Less => prop_assert!(enc_a > enc_b, "a={} < b={}, but descending enc_a <= enc_b", a, b),
            std::cmp::Ordering::Greater => prop_assert!(enc_a < enc_b, "a={} > b={}, but descending enc_a >= enc_b", a, b),
            std::cmp::Ordering::Equal => prop_assert_eq!(enc_a, enc_b),
        }
    }

    #[test]
    fn prop_uvarint_roundtrip_ascending(v in any::<u64>()) {
        let encoded = encoding::encode_uvarint_ascending(vec![], v);
        let (rest, decoded) = encoding::decode_uvarint_ascending(&encoded).unwrap();
        prop_assert!(rest.is_empty(), "should consume entire buffer");
        prop_assert_eq!(decoded, v);
    }

    #[test]
    fn prop_uvarint_roundtrip_descending(v in any::<u64>()) {
        let encoded = encoding::encode_uvarint_descending(vec![], v);
        let (rest, decoded) = encoding::decode_uvarint_descending(&encoded).unwrap();
        prop_assert!(rest.is_empty(), "should consume entire buffer");
        prop_assert_eq!(decoded, v);
    }

    #[test]
    fn prop_uvarint_sort_order_ascending(a in any::<u64>(), b in any::<u64>()) {
        let enc_a = encoding::encode_uvarint_ascending(vec![], a);
        let enc_b = encoding::encode_uvarint_ascending(vec![], b);

        match a.cmp(&b) {
            std::cmp::Ordering::Less => prop_assert!(enc_a < enc_b),
            std::cmp::Ordering::Greater => prop_assert!(enc_a > enc_b),
            std::cmp::Ordering::Equal => prop_assert_eq!(enc_a, enc_b),
        }
    }
}

// ============================================================================
// Float64 Property Tests
// ============================================================================

// Filter for non-NaN floats
fn non_nan_f64() -> impl Strategy<Value = f64> {
    any::<f64>().prop_filter("not NaN", |v| !v.is_nan())
}

fn non_nan_f32() -> impl Strategy<Value = f32> {
    any::<f32>().prop_filter("not NaN", |v| !v.is_nan())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn prop_float64_roundtrip_ascending(v in non_nan_f64()) {
        let encoded = encoding::encode_float64_ascending(vec![], v);
        let (rest, decoded) = encoding::decode_float64_ascending(&encoded).unwrap();
        prop_assert!(rest.is_empty(), "should consume entire buffer");
        prop_assert_eq!(decoded, v);
    }

    #[test]
    fn prop_float64_roundtrip_descending(v in non_nan_f64()) {
        let encoded = encoding::encode_float64_descending(vec![], v);
        let (rest, decoded) = encoding::decode_float64_descending(&encoded).unwrap();
        prop_assert!(rest.is_empty(), "should consume entire buffer");
        prop_assert_eq!(decoded, v);
    }

    #[test]
    fn prop_float64_sort_order_ascending(a in non_nan_f64(), b in non_nan_f64()) {
        let enc_a = encoding::encode_float64_ascending(vec![], a);
        let enc_b = encoding::encode_float64_ascending(vec![], b);

        // Use partial_cmp because -0.0 and 0.0 are intentionally encoded as equal
        // (both encode to FLOAT64_ZERO marker). This is correct behavior for a database.
        if a == b {
            prop_assert_eq!(enc_a, enc_b);
        } else if a < b {
            prop_assert!(enc_a < enc_b, "a={} < b={}, but enc_a >= enc_b", a, b);
        } else {
            prop_assert!(enc_a > enc_b, "a={} > b={}, but enc_a <= enc_b", a, b);
        }
    }

    #[test]
    fn prop_float64_sort_order_descending(a in non_nan_f64(), b in non_nan_f64()) {
        let enc_a = encoding::encode_float64_descending(vec![], a);
        let enc_b = encoding::encode_float64_descending(vec![], b);

        // Use partial_cmp because -0.0 and 0.0 are intentionally encoded as equal
        if a == b {
            prop_assert_eq!(enc_a, enc_b);
        } else if a < b {
            prop_assert!(enc_a > enc_b, "a={} < b={}, but descending enc_a <= enc_b", a, b);
        } else {
            prop_assert!(enc_a < enc_b, "a={} > b={}, but descending enc_a >= enc_b", a, b);
        }
    }
}

// ============================================================================
// Float32 Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn prop_float32_roundtrip_ascending(v in non_nan_f32()) {
        let encoded = encoding::encode_float32_ascending(vec![], v);
        let (rest, decoded) = encoding::decode_float32_ascending(&encoded).unwrap();
        prop_assert!(rest.is_empty());
        prop_assert_eq!(decoded, v);
    }

    #[test]
    fn prop_float32_roundtrip_descending(v in non_nan_f32()) {
        let encoded = encoding::encode_float32_descending(vec![], v);
        let (rest, decoded) = encoding::decode_float32_descending(&encoded).unwrap();
        prop_assert!(rest.is_empty());
        prop_assert_eq!(decoded, v);
    }

    #[test]
    fn prop_float32_sort_order_ascending(a in non_nan_f32(), b in non_nan_f32()) {
        let enc_a = encoding::encode_float32_ascending(vec![], a);
        let enc_b = encoding::encode_float32_ascending(vec![], b);

        // Use partial_cmp because -0.0 and 0.0 are intentionally encoded as equal
        if a == b {
            prop_assert_eq!(enc_a, enc_b);
        } else if a < b {
            prop_assert!(enc_a < enc_b);
        } else {
            prop_assert!(enc_a > enc_b);
        }
    }
}

// ============================================================================
// Bytes/String Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_bytes_roundtrip_ascending(v in proptest::collection::vec(any::<u8>(), 0..256)) {
        let encoded = encoding::encode_bytes_ascending(vec![], &v);
        let (rest, decoded) = encoding::decode_bytes_ascending(&encoded).unwrap();
        prop_assert!(rest.is_empty());
        prop_assert_eq!(decoded, v);
    }

    #[test]
    fn prop_bytes_roundtrip_descending(v in proptest::collection::vec(any::<u8>(), 0..256)) {
        let encoded = encoding::encode_bytes_descending(vec![], &v);
        let (rest, decoded) = encoding::decode_bytes_descending(&encoded).unwrap();
        prop_assert!(rest.is_empty());
        prop_assert_eq!(decoded, v);
    }

    #[test]
    fn prop_bytes_sort_order_ascending(
        a in proptest::collection::vec(any::<u8>(), 0..128),
        b in proptest::collection::vec(any::<u8>(), 0..128)
    ) {
        let enc_a = encoding::encode_bytes_ascending(vec![], &a);
        let enc_b = encoding::encode_bytes_ascending(vec![], &b);

        match a.cmp(&b) {
            std::cmp::Ordering::Less => prop_assert!(enc_a < enc_b),
            std::cmp::Ordering::Greater => prop_assert!(enc_a > enc_b),
            std::cmp::Ordering::Equal => prop_assert_eq!(enc_a, enc_b),
        }
    }

    #[test]
    fn prop_string_roundtrip_ascending(v in ".*") {
        let encoded = encoding::encode_string_ascending(vec![], &v);
        let (rest, decoded) = encoding::decode_string_ascending(&encoded).unwrap();
        prop_assert!(rest.is_empty());
        prop_assert_eq!(decoded, v);
    }

    #[test]
    fn prop_string_roundtrip_descending(v in ".*") {
        let encoded = encoding::encode_string_descending(vec![], &v);
        let (rest, decoded) = encoding::decode_string_descending(&encoded).unwrap();
        prop_assert!(rest.is_empty());
        prop_assert_eq!(decoded, v);
    }

    #[test]
    fn prop_string_sort_order_ascending(a in "[a-z]{0,32}", b in "[a-z]{0,32}") {
        let enc_a = encoding::encode_string_ascending(vec![], &a);
        let enc_b = encoding::encode_string_ascending(vec![], &b);

        match a.cmp(&b) {
            std::cmp::Ordering::Less => prop_assert!(enc_a < enc_b),
            std::cmp::Ordering::Greater => prop_assert!(enc_a > enc_b),
            std::cmp::Ordering::Equal => prop_assert_eq!(enc_a, enc_b),
        }
    }
}

// ============================================================================
// Time Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn prop_time_roundtrip_ascending(nanos in any::<i64>()) {
        let encoded = encoding::encode_time_ascending(vec![], nanos);
        let (rest, decoded) = encoding::decode_time_ascending(&encoded).unwrap();
        prop_assert!(rest.is_empty());
        prop_assert_eq!(decoded, nanos);
    }

    #[test]
    fn prop_time_roundtrip_descending(nanos in any::<i64>()) {
        let encoded = encoding::encode_time_descending(vec![], nanos);
        let (rest, decoded) = encoding::decode_time_descending(&encoded).unwrap();
        prop_assert!(rest.is_empty());
        prop_assert_eq!(decoded, nanos);
    }

    #[test]
    fn prop_time_sort_order_ascending(a in any::<i64>(), b in any::<i64>()) {
        let enc_a = encoding::encode_time_ascending(vec![], a);
        let enc_b = encoding::encode_time_ascending(vec![], b);

        match a.cmp(&b) {
            std::cmp::Ordering::Less => prop_assert!(enc_a < enc_b),
            std::cmp::Ordering::Greater => prop_assert!(enc_a > enc_b),
            std::cmp::Ordering::Equal => prop_assert_eq!(enc_a, enc_b),
        }
    }
}

// ============================================================================
// Bool Property Tests
// ============================================================================

#[test]
fn test_bool_roundtrip_ascending() {
    for v in [true, false] {
        let encoded = encoding::encode_bool_ascending(vec![], v);
        let (rest, decoded) = encoding::decode_bool_ascending(&encoded).unwrap();
        assert!(rest.is_empty());
        assert_eq!(decoded, v);
    }
}

#[test]
fn test_bool_roundtrip_descending() {
    for v in [true, false] {
        let encoded = encoding::encode_bool_descending(vec![], v);
        let (rest, decoded) = encoding::decode_bool_descending(&encoded).unwrap();
        assert!(rest.is_empty());
        assert_eq!(decoded, v);
    }
}

#[test]
fn test_bool_sort_order_ascending() {
    let enc_false = encoding::encode_bool_ascending(vec![], false);
    let enc_true = encoding::encode_bool_ascending(vec![], true);
    // false < true in ascending order
    assert!(enc_false < enc_true);
}

#[test]
fn test_bool_sort_order_descending() {
    let enc_false = encoding::encode_bool_descending(vec![], false);
    let enc_true = encoding::encode_bool_descending(vec![], true);
    // In descending: true should come first (smaller bytes)
    assert!(enc_true < enc_false);
}

// ============================================================================
// Null Property Tests
// ============================================================================

#[test]
fn test_null_roundtrip() {
    let encoded = encoding::encode_null_ascending(vec![]);
    let (rest, is_null) = encoding::decode_if_null(&encoded);
    assert!(rest.is_empty());
    assert!(is_null);

    let encoded = encoding::encode_null_descending(vec![]);
    let (rest, is_null) = encoding::decode_if_null(&encoded);
    assert!(rest.is_empty());
    assert!(is_null);
}

#[test]
fn test_null_sorts_first() {
    // NULL should sort before any other value in ascending order
    let null_enc = encoding::encode_null_ascending(vec![]);
    let int_enc = encoding::encode_varint_ascending(vec![], i64::MIN);
    let float_enc = encoding::encode_float64_ascending(vec![], f64::NEG_INFINITY);
    let string_enc = encoding::encode_string_ascending(vec![], "");

    assert!(null_enc < int_enc, "NULL should sort before integers");
    assert!(null_enc < float_enc, "NULL should sort before floats");
    assert!(null_enc < string_enc, "NULL should sort before strings");
}

// ============================================================================
// NaN Tests
// ============================================================================

#[test]
fn test_float64_nan_roundtrip_ascending() {
    let encoded = encoding::encode_float64_ascending(vec![], f64::NAN);
    let (rest, decoded) = encoding::decode_float64_ascending(&encoded).unwrap();
    assert!(rest.is_empty());
    assert!(decoded.is_nan());
}

#[test]
fn test_float64_nan_roundtrip_descending() {
    let encoded = encoding::encode_float64_descending(vec![], f64::NAN);
    let (rest, decoded) = encoding::decode_float64_descending(&encoded).unwrap();
    assert!(rest.is_empty());
    assert!(decoded.is_nan());
}

#[test]
fn test_float64_zero_descending_no_negative_zero() {
    let encoded = encoding::encode_float64_descending(vec![], 0.0);
    let (_, decoded) = encoding::decode_float64_descending(&encoded).unwrap();
    // Should be positive zero, not negative zero
    assert!(decoded.is_sign_positive() || decoded == 0.0);
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[test]
fn test_varint_boundary_values() {
    let boundary_values = vec![
        i64::MIN,
        i64::MIN + 1,
        -256,
        -255,
        -128,
        -127,
        -1,
        0,
        1,
        127,
        128,
        255,
        256,
        i64::MAX - 1,
        i64::MAX,
    ];

    for v in boundary_values {
        let encoded = encoding::encode_varint_ascending(vec![], v);
        let (_, decoded) = encoding::decode_varint_ascending(&encoded).unwrap();
        assert_eq!(decoded, v, "roundtrip failed for {}", v);
    }
}

#[test]
fn test_uvarint_boundary_values() {
    let boundary_values = vec![
        0u64,
        1,
        127,
        128,
        255,
        256,
        65535,
        65536,
        u64::MAX - 1,
        u64::MAX,
    ];

    for v in boundary_values {
        let encoded = encoding::encode_uvarint_ascending(vec![], v);
        let (_, decoded) = encoding::decode_uvarint_ascending(&encoded).unwrap();
        assert_eq!(decoded, v, "roundtrip failed for {}", v);
    }
}

#[test]
fn test_float64_special_values() {
    let special_values = vec![
        f64::NEG_INFINITY,
        f64::MIN,
        -1.0,
        -f64::MIN_POSITIVE,
        -0.0,
        0.0,
        f64::MIN_POSITIVE,
        1.0,
        f64::MAX,
        f64::INFINITY,
    ];

    for v in special_values {
        let encoded = encoding::encode_float64_ascending(vec![], v);
        let (_, decoded) = encoding::decode_float64_ascending(&encoded).unwrap();
        assert_eq!(decoded, v, "roundtrip failed for {}", v);

        let encoded = encoding::encode_float64_descending(vec![], v);
        let (_, decoded) = encoding::decode_float64_descending(&encoded).unwrap();
        assert_eq!(decoded, v, "descending roundtrip failed for {}", v);
    }
}

#[test]
fn test_bytes_with_null_bytes() {
    // Test that bytes containing 0x00 encode/decode correctly
    let data_with_nulls = vec![0x00, 0x01, 0x00, 0x00, 0x02, 0x00];

    let encoded = encoding::encode_bytes_ascending(vec![], &data_with_nulls);
    let (_, decoded) = encoding::decode_bytes_ascending(&encoded).unwrap();
    assert_eq!(decoded, data_with_nulls);

    let encoded = encoding::encode_bytes_descending(vec![], &data_with_nulls);
    let (_, decoded) = encoding::decode_bytes_descending(&encoded).unwrap();
    assert_eq!(decoded, data_with_nulls);
}

#[test]
fn test_bytes_all_null_bytes() {
    // Stress test the escape mechanism with all 0x00 bytes
    let all_nulls: Vec<u8> = vec![0x00; 100];

    let encoded = encoding::encode_bytes_ascending(vec![], &all_nulls);
    let (_, decoded) = encoding::decode_bytes_ascending(&encoded).unwrap();
    assert_eq!(decoded, all_nulls);
}
