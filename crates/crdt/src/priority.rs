//! Priority encoding and decoding utilities
//!
//! Uses unsigned varint encoding for storage efficiency.
//! Priority values are typically timestamps, which compress well with varint.

use defra_core::{Error, Result};

/// Stack-allocated encoded priority (max 10 bytes for u64 varint)
pub struct EncodedPriority {
    buf: [u8; 10],
    len: usize,
}

impl AsRef<[u8]> for EncodedPriority {
    fn as_ref(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

impl std::ops::Deref for EncodedPriority {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

/// Encode a priority value as unsigned varint (stack-allocated)
///
/// # Arguments
/// * `priority` - The priority value to encode
///
/// # Returns
/// * `EncodedPriority` containing 1-10 bytes, dereferences to `&[u8]`
pub fn encode_priority(priority: u64) -> EncodedPriority {
    let mut ep = EncodedPriority {
        buf: [0u8; 10],
        len: 0,
    };
    encode_varint(priority, &mut ep);
    ep
}

/// Decode a priority value from unsigned varint
///
/// # Arguments
/// * `data` - The encoded bytes
///
/// # Returns
/// * `Ok(priority)` if successful
/// * `Err(...)` if data is invalid, incomplete, or has trailing bytes
pub fn decode_priority(data: &[u8]) -> Result<u64> {
    match decode_varint(data) {
        Some((value, consumed)) if consumed == data.len() => Ok(value),
        _ => Err(Error::MergeError("invalid priority encoding".into())),
    }
}

/// Encode unsigned varint (LEB128) into an `EncodedPriority` buffer
fn encode_varint(mut value: u64, ep: &mut EncodedPriority) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        ep.buf[ep.len] = byte;
        ep.len += 1;
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

    #[test]
    fn test_trailing_bytes_rejected() {
        let mut with_trailing = encode_priority(1u64).as_ref().to_vec();
        with_trailing.extend_from_slice(&[0x00, 0xFF]);
        assert!(decode_priority(&with_trailing).is_err());
    }

    #[test]
    fn test_empty_rejected() {
        assert!(decode_priority(&[]).is_err());
    }
}
