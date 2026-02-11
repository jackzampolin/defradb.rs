use super::super::{FLOAT32_POS, FLOAT64_NEG, FLOAT64_POS, FLOAT64_ZERO};
use super::*;

#[test]
fn test_float64_encoding() {
    let test_values: Vec<f64> = vec![
        f64::NEG_INFINITY,
        f64::MIN,
        -1000.0,
        -1.0,
        -f64::MIN_POSITIVE,
        0.0,
        f64::MIN_POSITIVE,
        1.0,
        1000.0,
        f64::MAX,
        f64::INFINITY,
    ];

    for v in &test_values {
        let buf = encode_float64_ascending(vec![], *v);
        let (_, decoded) = decode_float64_ascending(&buf).unwrap();
        assert_eq!(decoded, *v, "float64 roundtrip failed for {}", v);

        let buf = encode_float64_descending(vec![], *v);
        let (_, decoded) = decode_float64_descending(&buf).unwrap();
        assert_eq!(decoded, *v, "descending float64 roundtrip failed for {}", v);
    }

    // Test NaN
    let buf = encode_float64_ascending(vec![], f64::NAN);
    let (_, decoded) = decode_float64_ascending(&buf).unwrap();
    assert!(decoded.is_nan());

    // Verify sort order (NaN first, then negatives, zero, positives)
    let encoded: Vec<Vec<u8>> = test_values
        .iter()
        .map(|v| encode_float64_ascending(vec![], *v))
        .collect();
    for i in 0..encoded.len() - 1 {
        assert!(
            encoded[i] < encoded[i + 1],
            "float64 sort order failed: {} should be < {}",
            test_values[i],
            test_values[i + 1]
        );
    }
}

#[test]
fn test_float32_encoding() {
    let test_values: Vec<f32> = vec![-1.0, 0.0, 1.0];

    for v in &test_values {
        let buf = encode_float32_ascending(vec![], *v);
        let (_, decoded) = decode_float32_ascending(&buf).unwrap();
        assert_eq!(decoded, *v, "float32 roundtrip failed for {}", v);
    }
}

#[test]
fn test_float64_go_compatible_ascending() {
    let smallest_nonzero = f64::from_bits(1);
    let test_cases: Vec<(f64, Vec<u8>)> = vec![
        (0.0, vec![FLOAT64_ZERO]),
        (
            smallest_nonzero,
            vec![FLOAT64_POS, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
        ),
        (
            0.00123,
            vec![FLOAT64_POS, 0x3f, 0x54, 0x26, 0xfe, 0x71, 0x8a, 0x86, 0xd7],
        ),
        (
            1.0,
            vec![FLOAT64_POS, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ),
        (
            10.0,
            vec![FLOAT64_POS, 0x40, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ),
        (
            100.0,
            vec![FLOAT64_POS, 0x40, 0x59, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ),
        (
            -1.0,
            vec![FLOAT64_NEG, 0x40, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        ),
        (
            -100.0,
            vec![FLOAT64_NEG, 0x3f, 0xa6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        ),
    ];

    for (value, expected) in test_cases {
        let encoded = encode_float64_ascending(vec![], value);
        assert_eq!(
            encoded, expected,
            "float64 ascending mismatch for {}: got {:02x?}, expected {:02x?}",
            value, encoded, expected
        );

        let (_, decoded) = decode_float64_ascending(&encoded).unwrap();
        assert_eq!(decoded, value, "float64 roundtrip failed for {}", value);
    }
}

#[test]
fn test_float64_sort_order_comprehensive() {
    let test_values: Vec<f64> = vec![
        f64::NEG_INFINITY,
        -f64::MAX,
        -1e308,
        -10000.0,
        -100.0,
        -1.0,
        -0.00123,
        -f64::MIN_POSITIVE,
        0.0,
        f64::MIN_POSITIVE,
        0.00123,
        1.0,
        100.0,
        10000.0,
        1e308,
        f64::MAX,
        f64::INFINITY,
    ];

    // Test ascending order
    let encoded_asc: Vec<Vec<u8>> = test_values
        .iter()
        .map(|&v| encode_float64_ascending(vec![], v))
        .collect();

    for i in 0..encoded_asc.len() - 1 {
        assert!(
            encoded_asc[i] < encoded_asc[i + 1],
            "ascending sort order violated: {} should be < {}",
            test_values[i],
            test_values[i + 1]
        );

        let (_, decoded) = decode_float64_ascending(&encoded_asc[i]).unwrap();
        assert_eq!(
            decoded, test_values[i],
            "roundtrip failed for {}",
            test_values[i]
        );
    }

    // Test descending order
    let encoded_desc: Vec<Vec<u8>> = test_values
        .iter()
        .map(|&v| encode_float64_descending(vec![], v))
        .collect();

    for i in 0..encoded_desc.len() - 1 {
        assert!(
            encoded_desc[i] > encoded_desc[i + 1],
            "descending sort order violated: {} should be > {}",
            test_values[i],
            test_values[i + 1]
        );
    }
}

#[test]
fn test_float64_nan_sorts_first() {
    let nan_encoded = encode_float64_ascending(vec![], f64::NAN);
    let neg_inf_encoded = encode_float64_ascending(vec![], f64::NEG_INFINITY);
    let neg_encoded = encode_float64_ascending(vec![], -1000.0);
    let zero_encoded = encode_float64_ascending(vec![], 0.0);
    let pos_encoded = encode_float64_ascending(vec![], 1000.0);
    let pos_inf_encoded = encode_float64_ascending(vec![], f64::INFINITY);

    assert!(
        nan_encoded < neg_inf_encoded,
        "NaN should sort before -infinity"
    );
    assert!(
        nan_encoded < neg_encoded,
        "NaN should sort before negative numbers"
    );
    assert!(nan_encoded < zero_encoded, "NaN should sort before zero");
    assert!(
        nan_encoded < pos_encoded,
        "NaN should sort before positive numbers"
    );
    assert!(
        nan_encoded < pos_inf_encoded,
        "NaN should sort before +infinity"
    );

    let (_, decoded) = decode_float64_ascending(&nan_encoded).unwrap();
    assert!(decoded.is_nan());
}

#[test]
fn test_float32_comprehensive() {
    let test_values: Vec<f32> = vec![
        f32::NEG_INFINITY,
        -f32::MAX,
        -10000.0,
        -100.0,
        -1.0,
        -0.00123,
        -f32::MIN_POSITIVE,
        0.0,
        f32::MIN_POSITIVE,
        0.00123,
        1.0,
        100.0,
        10000.0,
        f32::MAX,
        f32::INFINITY,
    ];

    // Test ascending order
    let encoded_asc: Vec<Vec<u8>> = test_values
        .iter()
        .map(|&v| encode_float32_ascending(vec![], v))
        .collect();

    for i in 0..encoded_asc.len() - 1 {
        assert!(
            encoded_asc[i] < encoded_asc[i + 1],
            "ascending sort order violated: {} should be < {}",
            test_values[i],
            test_values[i + 1]
        );

        let (_, decoded) = decode_float32_ascending(&encoded_asc[i]).unwrap();
        assert_eq!(
            decoded, test_values[i],
            "roundtrip failed for {}",
            test_values[i]
        );
    }

    // Test descending order
    let encoded_desc: Vec<Vec<u8>> = test_values
        .iter()
        .map(|&v| encode_float32_descending(vec![], v))
        .collect();

    for i in 0..encoded_desc.len() - 1 {
        assert!(
            encoded_desc[i] > encoded_desc[i + 1],
            "descending sort order violated: {} should be > {}",
            test_values[i],
            test_values[i + 1]
        );

        let (_, decoded) = decode_float32_descending(&encoded_desc[i]).unwrap();
        assert_eq!(
            decoded, test_values[i],
            "roundtrip failed for {}",
            test_values[i]
        );
    }
}

#[test]
fn test_float32_nan_sorts_first() {
    let nan_encoded = encode_float32_ascending(vec![], f32::NAN);
    let neg_inf_encoded = encode_float32_ascending(vec![], f32::NEG_INFINITY);
    let zero_encoded = encode_float32_ascending(vec![], 0.0);
    let pos_inf_encoded = encode_float32_ascending(vec![], f32::INFINITY);

    assert!(
        nan_encoded < neg_inf_encoded,
        "NaN should sort before -infinity"
    );
    assert!(nan_encoded < zero_encoded, "NaN should sort before zero");
    assert!(
        nan_encoded < pos_inf_encoded,
        "NaN should sort before +infinity"
    );

    let (_, decoded) = decode_float32_ascending(&nan_encoded).unwrap();
    assert!(decoded.is_nan());
}

#[test]
fn test_decode_float64_truncated_buffer() {
    let buf = vec![FLOAT64_POS, 0x01, 0x02, 0x03];
    let result = decode_float64_ascending(&buf);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("insufficient bytes"));
}

#[test]
fn test_decode_float32_truncated_buffer() {
    let buf = vec![FLOAT32_POS, 0x01, 0x02];
    let result = decode_float32_ascending(&buf);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("insufficient bytes"));
}
