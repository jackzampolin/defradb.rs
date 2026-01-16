// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Fixed-width integer encoding (uint32/uint64)

use crate::corekv::{Error, Result};

/// Encode uint32 in ascending order (big-endian, 4 bytes)
pub fn encode_uint32_ascending(mut buf: Vec<u8>, v: u32) -> Vec<u8> {
    buf.extend_from_slice(&v.to_be_bytes());
    buf
}

/// Encode uint32 in descending order
pub fn encode_uint32_descending(buf: Vec<u8>, v: u32) -> Vec<u8> {
    encode_uint32_ascending(buf, !v)
}

/// Decode uint32 from ascending encoding
pub fn decode_uint32_ascending(buf: &[u8]) -> Result<(&[u8], u32)> {
    if buf.len() < 4 {
        return Err(Error::Other(
            "insufficient bytes to decode uint32".to_string(),
        ));
    }
    let v = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    Ok((&buf[4..], v))
}

/// Decode uint32 from descending encoding
pub fn decode_uint32_descending(buf: &[u8]) -> Result<(&[u8], u32)> {
    let (rest, v) = decode_uint32_ascending(buf)?;
    Ok((rest, !v))
}

/// Encode uint64 in ascending order (big-endian, 8 bytes)
pub fn encode_uint64_ascending(mut buf: Vec<u8>, v: u64) -> Vec<u8> {
    buf.extend_from_slice(&v.to_be_bytes());
    buf
}

/// Encode uint64 in descending order
pub fn encode_uint64_descending(buf: Vec<u8>, v: u64) -> Vec<u8> {
    encode_uint64_ascending(buf, !v)
}

/// Decode uint64 from ascending encoding
pub fn decode_uint64_ascending(buf: &[u8]) -> Result<(&[u8], u64)> {
    if buf.len() < 8 {
        return Err(Error::Other(
            "insufficient bytes to decode uint64".to_string(),
        ));
    }
    let v = u64::from_be_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]);
    Ok((&buf[8..], v))
}

/// Decode uint64 from descending encoding
pub fn decode_uint64_descending(buf: &[u8]) -> Result<(&[u8], u64)> {
    let (rest, v) = decode_uint64_ascending(buf)?;
    Ok((rest, !v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uint32_fixed_encoding() {
        let test_values = vec![0u32, 1, 255, 256, u32::MAX / 2, u32::MAX];

        for v in &test_values {
            let buf = encode_uint32_ascending(vec![], *v);
            assert_eq!(buf.len(), 4);
            let (_, decoded) = decode_uint32_ascending(&buf).unwrap();
            assert_eq!(decoded, *v);

            let buf = encode_uint32_descending(vec![], *v);
            let (_, decoded) = decode_uint32_descending(&buf).unwrap();
            assert_eq!(decoded, *v);
        }
    }

    #[test]
    fn test_uint64_fixed_encoding() {
        let test_values = vec![0u64, 1, u32::MAX as u64, u64::MAX / 2, u64::MAX];

        for v in &test_values {
            let buf = encode_uint64_ascending(vec![], *v);
            assert_eq!(buf.len(), 8);
            let (_, decoded) = decode_uint64_ascending(&buf).unwrap();
            assert_eq!(decoded, *v);

            let buf = encode_uint64_descending(vec![], *v);
            let (_, decoded) = decode_uint64_descending(&buf).unwrap();
            assert_eq!(decoded, *v);
        }
    }
}
