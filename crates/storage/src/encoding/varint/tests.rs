use super::*;

#[test]
fn test_varint_encoding() {
    let test_values: Vec<i64> = vec![
        i64::MIN,
        i64::MIN + 1,
        -1_000_000,
        -1000,
        -1,
        0,
        1,
        1000,
        1_000_000,
        i64::MAX - 1,
        i64::MAX,
    ];

    for v in &test_values {
        let buf = encode_varint_ascending(vec![], *v);
        let (_, decoded) = decode_varint_ascending(&buf).unwrap();
        assert_eq!(decoded, *v, "ascending varint roundtrip failed for {}", v);

        let buf = encode_varint_descending(vec![], *v);
        let (_, decoded) = decode_varint_descending(&buf).unwrap();
        assert_eq!(decoded, *v, "descending varint roundtrip failed for {}", v);
    }

    // Verify sort order in ascending
    let encoded: Vec<Vec<u8>> = test_values
        .iter()
        .map(|v| encode_varint_ascending(vec![], *v))
        .collect();
    for i in 0..encoded.len() - 1 {
        assert!(
            encoded[i] < encoded[i + 1],
            "ascending sort order failed: {} should be < {}",
            test_values[i],
            test_values[i + 1]
        );
    }
}

#[test]
fn test_uvarint_encoding() {
    let test_values: Vec<u64> = vec![
        0,
        1,
        109, // INT_SMALL
        110,
        255,
        256,
        65535,
        65536,
        u32::MAX as u64,
        u64::MAX,
    ];

    for v in &test_values {
        let buf = encode_uvarint_ascending(vec![], *v);
        let (_, decoded) = decode_uvarint_ascending(&buf).unwrap();
        assert_eq!(decoded, *v, "ascending uvarint roundtrip failed for {}", v);

        let buf = encode_uvarint_descending(vec![], *v);
        let (_, decoded) = decode_uvarint_descending(&buf).unwrap();
        assert_eq!(decoded, *v, "descending uvarint roundtrip failed for {}", v);
    }

    // Verify sort order
    let encoded: Vec<Vec<u8>> = test_values
        .iter()
        .map(|v| encode_uvarint_ascending(vec![], *v))
        .collect();
    for i in 0..encoded.len() - 1 {
        assert!(
            encoded[i] < encoded[i + 1],
            "ascending sort order failed: {} should be < {}",
            test_values[i],
            test_values[i + 1]
        );
    }
}

// =====================================================================
// Go-compatible encoding tests
// =====================================================================

#[test]
fn test_varint_go_compatible_ascending() {
    use super::super::{INT_MAX, INT_MIN};

    let test_cases: Vec<(i64, Vec<u8>)> = vec![
        (
            i64::MIN,
            vec![0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ),
        (
            i64::MIN + 1,
            vec![0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
        ),
        (-1 << 8, vec![0x86, 0xff, 0x00]),
        (-1, vec![0x87, 0xff]),
        (0, vec![0x88]),
        (1, vec![0x89]),
        (109, vec![0xf5]),
        (112, vec![0xf6, 0x70]),
        (1 << 8, vec![0xf7, 0x01, 0x00]),
        (
            i64::MAX,
            vec![0xfd, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        ),
    ];

    // Suppress unused warning for constants used in test
    let _ = (INT_MIN, INT_MAX);

    for (value, expected) in test_cases {
        let encoded = encode_varint_ascending(vec![], value);
        assert_eq!(
            encoded, expected,
            "varint ascending mismatch for {}: got {:02x?}, expected {:02x?}",
            value, encoded, expected
        );

        let (_, decoded) = decode_varint_ascending(&encoded).unwrap();
        assert_eq!(decoded, value, "varint roundtrip failed for {}", value);
    }
}

#[test]
fn test_varint_go_compatible_descending() {
    let test_cases: Vec<(i64, Vec<u8>)> = vec![
        (
            i64::MIN,
            vec![0xfd, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        ),
        (
            i64::MIN + 1,
            vec![0xfd, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe],
        ),
        (-1 << 8, vec![0xf6, 0xff]),
        (-110, vec![0xf5]),
        (-1, vec![0x88]),
        (0, vec![0x87, 0xff]),
        (1, vec![0x87, 0xfe]),
        (1 << 8, vec![0x86, 0xfe, 0xff]),
        (
            i64::MAX,
            vec![0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ),
    ];

    for (value, expected) in test_cases {
        let encoded = encode_varint_descending(vec![], value);
        assert_eq!(
            encoded, expected,
            "varint descending mismatch for {}: got {:02x?}, expected {:02x?}",
            value, encoded, expected
        );

        let (_, decoded) = decode_varint_descending(&encoded).unwrap();
        assert_eq!(
            decoded, value,
            "varint descending roundtrip failed for {}",
            value
        );
    }
}

#[test]
fn test_uvarint_go_compatible_ascending() {
    let test_cases: Vec<(u64, Vec<u8>)> = vec![
        (0, vec![0x88]),
        (1, vec![0x89]),
        (109, vec![0xf5]),
        (110, vec![0xf6, 0x6e]),
        (1 << 8, vec![0xf7, 0x01, 0x00]),
        (
            u64::MAX,
            vec![0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        ),
    ];

    for (value, expected) in test_cases {
        let encoded = encode_uvarint_ascending(vec![], value);
        assert_eq!(
            encoded, expected,
            "uvarint ascending mismatch for {}: got {:02x?}, expected {:02x?}",
            value, encoded, expected
        );

        let (_, decoded) = decode_uvarint_ascending(&encoded).unwrap();
        assert_eq!(decoded, value, "uvarint roundtrip failed for {}", value);
    }
}

#[test]
fn test_uvarint_go_compatible_descending() {
    let test_cases: Vec<(u64, Vec<u8>)> = vec![
        (0, vec![0x88]),
        (1, vec![0x87, 0xfe]),
        (1 << 8, vec![0x86, 0xfe, 0xff]),
        (
            u64::MAX - 1,
            vec![0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
        ),
        (
            u64::MAX,
            vec![0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ),
    ];

    for (value, expected) in test_cases {
        let encoded = encode_uvarint_descending(vec![], value);
        assert_eq!(
            encoded, expected,
            "uvarint descending mismatch for {}: got {:02x?}, expected {:02x?}",
            value, encoded, expected
        );

        let (_, decoded) = decode_uvarint_descending(&encoded).unwrap();
        assert_eq!(
            decoded, value,
            "uvarint descending roundtrip failed for {}",
            value
        );
    }
}

#[test]
fn test_varint_sort_order_comprehensive() {
    let test_values: Vec<i64> = vec![
        i64::MIN,
        i64::MIN + 1,
        (-1i64 << 56) - 1,
        -1i64 << 56,
        (-1i64 << 48) - 1,
        -1i64 << 48,
        (-1i64 << 40) - 1,
        -1i64 << 40,
        (-1i64 << 32) - 1,
        -1i64 << 32,
        (-1i64 << 24) - 1,
        -1i64 << 24,
        (-1i64 << 16) - 1,
        -1i64 << 16,
        (-1i64 << 8) - 1,
        -1i64 << 8,
        -1,
        0,
        1,
        (1i64 << 8) - 1,
        1i64 << 8,
        (1i64 << 16) - 1,
        1i64 << 16,
        (1i64 << 24) - 1,
        1i64 << 24,
        (1i64 << 32) - 1,
        1i64 << 32,
        (1i64 << 40) - 1,
        1i64 << 40,
        (1i64 << 48) - 1,
        1i64 << 48,
        (1i64 << 56) - 1,
        1i64 << 56,
        i64::MAX - 1,
        i64::MAX,
    ];

    // Test ascending order
    let encoded_asc: Vec<Vec<u8>> = test_values
        .iter()
        .map(|&v| encode_varint_ascending(vec![], v))
        .collect();

    for i in 0..encoded_asc.len() - 1 {
        assert!(
            encoded_asc[i] < encoded_asc[i + 1],
            "ascending sort order violated: {} ({:02x?}) should be < {} ({:02x?})",
            test_values[i],
            encoded_asc[i],
            test_values[i + 1],
            encoded_asc[i + 1]
        );

        let (_, decoded) = decode_varint_ascending(&encoded_asc[i]).unwrap();
        assert_eq!(decoded, test_values[i]);
    }

    // Test descending order
    let encoded_desc: Vec<Vec<u8>> = test_values
        .iter()
        .map(|&v| encode_varint_descending(vec![], v))
        .collect();

    for i in 0..encoded_desc.len() - 1 {
        assert!(
            encoded_desc[i] > encoded_desc[i + 1],
            "descending sort order violated: {} ({:02x?}) should be > {} ({:02x?})",
            test_values[i],
            encoded_desc[i],
            test_values[i + 1],
            encoded_desc[i + 1]
        );

        let (_, decoded) = decode_varint_descending(&encoded_desc[i]).unwrap();
        assert_eq!(decoded, test_values[i]);
    }
}

#[test]
fn test_uvarint_sort_order_comprehensive() {
    let test_values: Vec<u64> = vec![
        0,
        1,
        (1 << 8) - 1,
        1 << 8,
        (1 << 16) - 1,
        1 << 16,
        (1 << 24) - 1,
        1 << 24,
        (1 << 32) - 1,
        1 << 32,
        (1 << 40) - 1,
        1 << 40,
        (1 << 48) - 1,
        1 << 48,
        (1 << 56) - 1,
        1 << 56,
        u64::MAX - 1,
        u64::MAX,
    ];

    // Test ascending order
    let encoded_asc: Vec<Vec<u8>> = test_values
        .iter()
        .map(|&v| encode_uvarint_ascending(vec![], v))
        .collect();

    for i in 0..encoded_asc.len() - 1 {
        assert!(
            encoded_asc[i] < encoded_asc[i + 1],
            "ascending sort order violated: {} should be < {}",
            test_values[i],
            test_values[i + 1]
        );
    }

    // Test descending order
    let encoded_desc: Vec<Vec<u8>> = test_values
        .iter()
        .map(|&v| encode_uvarint_descending(vec![], v))
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
fn test_decode_varint_truncated_buffer() {
    // Empty buffer
    let result = decode_varint_ascending(&[]);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("insufficient bytes"));

    // Marker indicates multi-byte value but bytes are missing
    // INT_MIN + 4 = 132 indicates a 4-byte negative number
    let result = decode_varint_ascending(&[132, 0x01, 0x02]);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("insufficient bytes"));
}

#[test]
fn test_decode_uvarint_truncated_buffer() {
    // Empty buffer
    let result = decode_uvarint_ascending(&[]);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("insufficient bytes"));

    // Marker indicates multi-byte value but bytes are missing
    // INT_MAX - 4 = 249 indicates a 5-byte positive number
    let result = decode_uvarint_ascending(&[249, 0x01, 0x02]);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("insufficient bytes"));
}
