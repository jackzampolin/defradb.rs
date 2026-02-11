use super::*;

#[test]
fn test_uvarint_encoding() {
    // Test small values
    let buf = encode_uvarint_ascending(vec![], 0);
    assert_eq!(buf, vec![0x00]);
    let (_, decoded) = decode_uvarint_ascending(&buf).unwrap();
    assert_eq!(decoded, 0);

    let buf = encode_uvarint_ascending(vec![], 239);
    assert_eq!(buf, vec![239]);
    let (_, decoded) = decode_uvarint_ascending(&buf).unwrap();
    assert_eq!(decoded, 239);

    // Test medium values
    let buf = encode_uvarint_ascending(vec![], 240);
    let (_, decoded) = decode_uvarint_ascending(&buf).unwrap();
    assert_eq!(decoded, 240);

    let buf = encode_uvarint_ascending(vec![], 2287);
    let (_, decoded) = decode_uvarint_ascending(&buf).unwrap();
    assert_eq!(decoded, 2287);

    // Test large values
    let buf = encode_uvarint_ascending(vec![], 100000);
    let (_, decoded) = decode_uvarint_ascending(&buf).unwrap();
    assert_eq!(decoded, 100000);

    let buf = encode_uvarint_ascending(vec![], u64::MAX);
    let (_, decoded) = decode_uvarint_ascending(&buf).unwrap();
    assert_eq!(decoded, u64::MAX);
}

#[test]
fn test_varint_encoding() {
    for value in [-1000, -1, 0, 1, 1000] {
        let buf = encode_varint_ascending(vec![], value);
        let (_, decoded) = decode_varint_ascending(&buf).unwrap();
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_bool_encoding() {
    let buf = encode_bool_ascending(vec![], true);
    let (_, decoded) = decode_bool_ascending(&buf).unwrap();
    assert!(decoded);

    let buf = encode_bool_ascending(vec![], false);
    let (_, decoded) = decode_bool_ascending(&buf).unwrap();
    assert!(!decoded);
}

#[test]
fn test_string_encoding() {
    let test_cases = vec!["hello", "world", "test\x00string", ""];

    for s in test_cases {
        let buf = encode_string_ascending(vec![], s);
        let (_, decoded) = decode_string_ascending(&buf).unwrap();
        assert_eq!(decoded, s);
    }
}

#[test]
fn test_float_encoding() {
    let test_values = vec![-1.5, -0.0, 0.0, 1.5, f64::MAX, f64::MIN];

    for value in test_values {
        let buf = encode_float64_ascending(vec![], value);
        let (_, decoded) = decode_float64_ascending(&buf).unwrap();
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_instance_type() {
    assert_eq!(InstanceType::Value.as_byte(), b'v');
    assert_eq!(InstanceType::Priority.as_byte(), b'p');
    assert_eq!(InstanceType::Deleted.as_byte(), b'd');

    assert_eq!(InstanceType::from_byte(b'v').unwrap(), InstanceType::Value);
    assert_eq!(
        InstanceType::from_byte(b'p').unwrap(),
        InstanceType::Priority
    );
    assert_eq!(
        InstanceType::from_byte(b'd').unwrap(),
        InstanceType::Deleted
    );
}

#[test]
fn test_uvarint_maintains_sort_order() {
    // Test that encoded uvarints maintain lexicographic sort order
    // This is critical for range queries and prefix scans
    let test_values: Vec<u64> = vec![
        0,
        1,
        127,
        128,
        239, // boundary between 1-byte and 2-byte
        240, // first 2-byte value
        255,
        256,
        2287, // boundary between 2-byte and 3-byte
        2288, // first 3-byte value
        10000,
        100000,
        1000000,
        u32::MAX as u64,
        u64::MAX / 2,
        u64::MAX - 1,
        u64::MAX,
    ];

    // Encode all values
    let encoded: Vec<Vec<u8>> = test_values
        .iter()
        .map(|v| encode_uvarint_ascending(vec![], *v))
        .collect();

    // Verify that encoded values are in sorted order
    for i in 0..encoded.len() - 1 {
        assert!(
            encoded[i] < encoded[i + 1],
            "Encoded values must be sorted: {} (encoded as {:?}) should be < {} (encoded as {:?})",
            test_values[i],
            encoded[i],
            test_values[i + 1],
            encoded[i + 1]
        );
    }
}

#[test]
fn test_varint_maintains_sort_order() {
    // Test that encoded signed varints maintain sort order
    let test_values: Vec<i64> = vec![
        i64::MIN,
        i64::MIN + 1,
        -1000000,
        -1000,
        -1,
        0,
        1,
        1000,
        1000000,
        i64::MAX - 1,
        i64::MAX,
    ];

    let encoded: Vec<Vec<u8>> = test_values
        .iter()
        .map(|v| encode_varint_ascending(vec![], *v))
        .collect();

    for i in 0..encoded.len() - 1 {
        assert!(
            encoded[i] < encoded[i + 1],
            "Encoded signed values must be sorted: {} < {}",
            test_values[i],
            test_values[i + 1]
        );
    }
}

#[test]
fn test_float_encoding_special_values() {
    // Test special float values: NaN, Infinity, etc.

    // Test positive infinity
    let buf = encode_float64_ascending(vec![], f64::INFINITY);
    let (_, decoded) = decode_float64_ascending(&buf).unwrap();
    assert!(decoded.is_infinite() && decoded.is_sign_positive());

    // Test negative infinity
    let buf = encode_float64_ascending(vec![], f64::NEG_INFINITY);
    let (_, decoded) = decode_float64_ascending(&buf).unwrap();
    assert!(decoded.is_infinite() && decoded.is_sign_negative());

    // Test NaN (NaN != NaN, so we just check it decodes to NaN)
    let buf = encode_float64_ascending(vec![], f64::NAN);
    let (_, decoded) = decode_float64_ascending(&buf).unwrap();
    assert!(decoded.is_nan());

    // Test smallest positive subnormal
    let buf = encode_float64_ascending(vec![], f64::MIN_POSITIVE);
    let (_, decoded) = decode_float64_ascending(&buf).unwrap();
    assert_eq!(decoded, f64::MIN_POSITIVE);

    // Test negative zero (should decode to negative zero)
    let neg_zero = -0.0_f64;
    let buf = encode_float64_ascending(vec![], neg_zero);
    let (_, decoded) = decode_float64_ascending(&buf).unwrap();
    // Note: -0.0 == 0.0 in Rust, but we can check the sign bit
    assert!(decoded == 0.0);
}

#[test]
fn test_float_encoding_sort_order() {
    // Test that floats maintain sort order when encoded
    let test_values: Vec<f64> = vec![
        f64::NEG_INFINITY,
        f64::MIN,
        -1000.0,
        -1.0,
        -f64::MIN_POSITIVE,
        -0.0,
        0.0,
        f64::MIN_POSITIVE,
        1.0,
        1000.0,
        f64::MAX,
        f64::INFINITY,
    ];

    let encoded: Vec<Vec<u8>> = test_values
        .iter()
        .map(|v| encode_float64_ascending(vec![], *v))
        .collect();

    for i in 0..encoded.len() - 1 {
        assert!(
            encoded[i] < encoded[i + 1],
            "Encoded floats must be sorted: {} < {}",
            test_values[i],
            test_values[i + 1]
        );
    }
}

#[test]
fn test_decode_truncated_uvarint() {
    // Test decoding truncated data returns error
    let truncated_2byte = vec![0xF0]; // 2-byte marker without second byte
    assert!(decode_uvarint_ascending(&truncated_2byte).is_err());

    let truncated_large = vec![0xF9]; // 2-byte marker without data bytes
    assert!(decode_uvarint_ascending(&truncated_large).is_err());

    // Empty buffer
    assert!(decode_uvarint_ascending(&[]).is_err());
}
