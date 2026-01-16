// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Variable-length integer encoding (varint/uvarint)

use crate::corekv::{Error, Result};

use super::{INT_MAX, INT_MIN, INT_SMALL, INT_ZERO};

/// Encode a signed 64-bit integer in ascending order (varint)
pub fn encode_varint_ascending(mut buf: Vec<u8>, v: i64) -> Vec<u8> {
    if v < 0 {
        match v {
            v if v >= -0xff => {
                buf.push(INT_MIN + 7);
                buf.push(v as u8);
            }
            v if v >= -0xffff => {
                buf.push(INT_MIN + 6);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
            v if v >= -0xffffff => {
                buf.push(INT_MIN + 5);
                buf.push((v >> 16) as u8);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
            v if v >= -0xffffffff => {
                buf.push(INT_MIN + 4);
                buf.push((v >> 24) as u8);
                buf.push((v >> 16) as u8);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
            v if v >= -0xffffffffff => {
                buf.push(INT_MIN + 3);
                buf.push((v >> 32) as u8);
                buf.push((v >> 24) as u8);
                buf.push((v >> 16) as u8);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
            v if v >= -0xffffffffffff => {
                buf.push(INT_MIN + 2);
                buf.push((v >> 40) as u8);
                buf.push((v >> 32) as u8);
                buf.push((v >> 24) as u8);
                buf.push((v >> 16) as u8);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
            v if v >= -0xffffffffffffff => {
                buf.push(INT_MIN + 1);
                buf.push((v >> 48) as u8);
                buf.push((v >> 40) as u8);
                buf.push((v >> 32) as u8);
                buf.push((v >> 24) as u8);
                buf.push((v >> 16) as u8);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
            _ => {
                buf.push(INT_MIN);
                buf.push((v >> 56) as u8);
                buf.push((v >> 48) as u8);
                buf.push((v >> 40) as u8);
                buf.push((v >> 32) as u8);
                buf.push((v >> 24) as u8);
                buf.push((v >> 16) as u8);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
        }
        buf
    } else {
        encode_uvarint_ascending(buf, v as u64)
    }
}

/// Encode a signed integer in descending order
pub fn encode_varint_descending(buf: Vec<u8>, v: i64) -> Vec<u8> {
    encode_varint_ascending(buf, !v)
}

/// Encode an unsigned 64-bit integer in ascending order (uvarint)
pub fn encode_uvarint_ascending(mut buf: Vec<u8>, v: u64) -> Vec<u8> {
    match v {
        v if v <= INT_SMALL as u64 => {
            buf.push(INT_ZERO + v as u8);
        }
        v if v <= 0xff => {
            buf.push(INT_MAX - 7);
            buf.push(v as u8);
        }
        v if v <= 0xffff => {
            buf.push(INT_MAX - 6);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffff => {
            buf.push(INT_MAX - 5);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffff => {
            buf.push(INT_MAX - 4);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffffff => {
            buf.push(INT_MAX - 3);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffffffff => {
            buf.push(INT_MAX - 2);
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffffffffff => {
            buf.push(INT_MAX - 1);
            buf.push((v >> 48) as u8);
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        _ => {
            buf.push(INT_MAX);
            buf.push((v >> 56) as u8);
            buf.push((v >> 48) as u8);
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
    }
    buf
}

/// Encode an unsigned integer in descending order
pub fn encode_uvarint_descending(mut buf: Vec<u8>, v: u64) -> Vec<u8> {
    match v {
        0 => {
            buf.push(INT_MIN + 8);
        }
        v if v <= 0xff => {
            let v = !v;
            buf.push(INT_MIN + 7);
            buf.push(v as u8);
        }
        v if v <= 0xffff => {
            let v = !v;
            buf.push(INT_MIN + 6);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffff => {
            let v = !v;
            buf.push(INT_MIN + 5);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffff => {
            let v = !v;
            buf.push(INT_MIN + 4);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffffff => {
            let v = !v;
            buf.push(INT_MIN + 3);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffffffff => {
            let v = !v;
            buf.push(INT_MIN + 2);
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffffffffff => {
            let v = !v;
            buf.push(INT_MIN + 1);
            buf.push((v >> 48) as u8);
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        _ => {
            let v = !v;
            buf.push(INT_MIN);
            buf.push((v >> 56) as u8);
            buf.push((v >> 48) as u8);
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
    }
    buf
}

/// Decode a signed varint from ascending encoding
pub fn decode_varint_ascending(buf: &[u8]) -> Result<(&[u8], i64)> {
    if buf.is_empty() {
        return Err(Error::Other(
            "insufficient bytes to decode varint".to_string(),
        ));
    }

    let length = buf[0] as i16 - INT_ZERO as i16;
    if length < 0 {
        let length = (-length) as usize;
        let rem = &buf[1..];
        if rem.len() < length {
            return Err(Error::Other(
                "insufficient bytes to decode varint".to_string(),
            ));
        }

        // Build up a positive number using ones-complement, then invert
        let mut v: i64 = 0;
        for &t in &rem[..length] {
            v = (v << 8) | (!t) as i64;
        }
        Ok((&rem[length..], !v))
    } else {
        let (rest, v) = decode_uvarint_ascending(buf)?;
        if v > i64::MAX as u64 {
            return Err(Error::Other(format!("varint overflow: {}", v)));
        }
        Ok((rest, v as i64))
    }
}

/// Decode a signed varint from descending encoding
pub fn decode_varint_descending(buf: &[u8]) -> Result<(&[u8], i64)> {
    let (rest, v) = decode_varint_ascending(buf)?;
    Ok((rest, !v))
}

/// Decode an unsigned varint from ascending encoding
pub fn decode_uvarint_ascending(buf: &[u8]) -> Result<(&[u8], u64)> {
    if buf.is_empty() {
        return Err(Error::Other(
            "insufficient bytes to decode uvarint".to_string(),
        ));
    }

    let length = buf[0] as i16 - INT_ZERO as i16;
    let rest = &buf[1..];

    if length <= INT_SMALL as i16 {
        return Ok((rest, length as u64));
    }

    let length = (length - INT_SMALL as i16) as usize;
    if length > 8 {
        return Err(Error::Other(format!("invalid uvarint length: {}", length)));
    }
    if rest.len() < length {
        return Err(Error::Other(
            "insufficient bytes to decode uvarint".to_string(),
        ));
    }

    let mut v: u64 = 0;
    for &t in &rest[..length] {
        v = (v << 8) | t as u64;
    }
    Ok((&rest[length..], v))
}

/// Decode an unsigned varint from descending encoding
pub fn decode_uvarint_descending(buf: &[u8]) -> Result<(&[u8], u64)> {
    if buf.is_empty() {
        return Err(Error::Other(
            "insufficient bytes to decode uvarint".to_string(),
        ));
    }

    let length = INT_ZERO as i16 - buf[0] as i16;
    let rest = &buf[1..];

    if !(0..=8).contains(&length) {
        return Err(Error::Other(format!("invalid uvarint length: {}", length)));
    }
    if rest.len() < length as usize {
        return Err(Error::Other(
            "insufficient bytes to decode uvarint".to_string(),
        ));
    }

    let mut x: u64 = 0;
    for &t in &rest[..length as usize] {
        x = (x << 8) | (!t) as u64;
    }
    Ok((&rest[length as usize..], x))
}

#[cfg(test)]
mod tests {
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
}
