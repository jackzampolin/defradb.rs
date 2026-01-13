//! Priority encoding and decoding utilities
//!
//! Uses unsigned varint encoding for storage efficiency.
//! Priority values are typically timestamps, which compress well with varint.

use defra_core::{Error, Result};

/// Encode a priority value as unsigned varint
///
/// # Arguments
/// * `priority` - The priority value to encode
///
/// # Returns
/// * Encoded bytes (1-10 bytes depending on value)
pub fn encode_priority(priority: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_varint(priority, &mut buf);
    buf
}

/// Decode a priority value from unsigned varint
///
/// # Arguments
/// * `data` - The encoded bytes
///
/// # Returns
/// * `Ok(priority)` if successful
/// * `Err(...)` if data is invalid or incomplete
pub fn decode_priority(data: &[u8]) -> Result<u64> {
    decode_varint(data)
        .map(|(value, _)| value)
        .ok_or_else(|| Error::MergeError("invalid priority encoding".into()))
}

/// Encode unsigned varint (LEB128)
fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode unsigned varint (LEB128)
///
/// Returns (value, bytes_consumed) or None if invalid
fn decode_varint(data: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        if shift >= 64 {
            return None; // Overflow
        }

        value |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            return Some((value, i + 1));
        }

        shift += 7;
    }

    None // Incomplete varint
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_small_value() {
        let priority = 127u64;
        let encoded = encode_priority(priority);
        assert_eq!(encoded.len(), 1);
        assert_eq!(decode_priority(&encoded).unwrap(), priority);
    }

    #[test]
    fn test_priority_large_value() {
        let priority = u64::MAX;
        let encoded = encode_priority(priority);
        assert!(encoded.len() <= 10);
        assert_eq!(decode_priority(&encoded).unwrap(), priority);
    }

    #[test]
    fn test_priority_typical_timestamp() {
        // Typical timestamp (nanoseconds since epoch)
        let priority = 1_700_000_000_000_000_000u64;
        let encoded = encode_priority(priority);
        assert!(encoded.len() < 10);
        assert_eq!(decode_priority(&encoded).unwrap(), priority);
    }

    #[test]
    fn test_invalid_priority() {
        let invalid = vec![
            0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01,
        ];
        assert!(decode_priority(&invalid).is_err());
    }

    #[test]
    fn test_incomplete_varint() {
        let incomplete = vec![0x80, 0x80];
        assert!(decode_priority(&incomplete).is_err());
    }

    #[test]
    fn test_priority_zero() {
        let priority = 0u64;
        let encoded = encode_priority(priority);
        assert_eq!(encoded.len(), 1);
        assert_eq!(encoded[0], 0);
        assert_eq!(decode_priority(&encoded).unwrap(), priority);
    }

    #[test]
    fn test_priority_one() {
        let priority = 1u64;
        let encoded = encode_priority(priority);
        assert_eq!(encoded.len(), 1);
        assert_eq!(decode_priority(&encoded).unwrap(), priority);
    }

    #[test]
    fn test_priority_boundary_128() {
        // 128 requires 2 bytes in varint encoding
        let priority = 128u64;
        let encoded = encode_priority(priority);
        assert_eq!(encoded.len(), 2);
        assert_eq!(decode_priority(&encoded).unwrap(), priority);
    }
}
