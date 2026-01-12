/// Encoding utilities for DefraDB keys
///
/// This module provides CockroachDB-style encoding functions that maintain
/// sort order and are compatible with the Go DefraDB implementation.

use crate::corekv::{Error, Result};

/// Key separator character
pub const SEPARATOR: u8 = b'/';

/// Instance type markers for DataStoreKey
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceType {
    /// Value instance ('v')
    Value,
    /// Priority instance ('p')
    Priority,
    /// Deleted instance ('d')
    Deleted,
}

impl InstanceType {
    /// Convert to byte character
    pub fn as_byte(&self) -> u8 {
        match self {
            InstanceType::Value => b'v',
            InstanceType::Priority => b'p',
            InstanceType::Deleted => b'd',
        }
    }

    /// Parse from byte character
    pub fn from_byte(b: u8) -> Result<Self> {
        match b {
            b'v' => Ok(InstanceType::Value),
            b'p' => Ok(InstanceType::Priority),
            b'd' => Ok(InstanceType::Deleted),
            _ => Err(Error::Other(format!("Invalid instance type: {}", b as char))),
        }
    }

    /// Convert to string
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceType::Value => "v",
            InstanceType::Priority => "p",
            InstanceType::Deleted => "d",
        }
    }
}

/// Encodes a uint64 as a variable-length unsigned integer in ascending order.
/// This matches CockroachDB's EncodeUvarintAscending encoding.
///
/// The encoding is:
/// - Values 0-239: single byte (0x00-0xEF)
/// - Values 240-2287: two bytes (0xF0-0xF7)
/// - Larger values: marker byte + big-endian bytes
pub fn encode_uvarint_ascending(mut buf: Vec<u8>, value: u64) -> Vec<u8> {
    if value < 240 {
        buf.push(value as u8);
    } else if value <= 2287 {
        let v = value - 240;
        buf.push(0xF0 + (v >> 8) as u8);
        buf.push((v & 0xFF) as u8);
    } else {
        // Determine number of bytes needed
        let bytes_needed = ((64 - value.leading_zeros() + 7) / 8) as usize;
        buf.push(0xF7 + bytes_needed as u8);

        // Encode big-endian
        for i in (0..bytes_needed).rev() {
            buf.push((value >> (i * 8)) as u8);
        }
    }
    buf
}

/// Decodes a uvarint from the beginning of the buffer.
/// Returns (remaining_bytes, decoded_value).
pub fn decode_uvarint_ascending(buf: &[u8]) -> Result<(&[u8], u64)> {
    if buf.is_empty() {
        return Err(Error::Other("Empty buffer for uvarint decode".into()));
    }

    let first = buf[0];
    if first < 240 {
        Ok((&buf[1..], first as u64))
    } else if first <= 0xF7 {
        if buf.len() < 2 {
            return Err(Error::Other("Buffer too short for 2-byte uvarint".into()));
        }
        let value = 240 + (((first - 0xF0) as u64) << 8) + buf[1] as u64;
        Ok((&buf[2..], value))
    } else {
        let bytes_needed = (first - 0xF7) as usize;
        if buf.len() < 1 + bytes_needed {
            return Err(Error::Other(format!(
                "Buffer too short for {}-byte uvarint",
                bytes_needed
            )));
        }

        let mut value: u64 = 0;
        for i in 0..bytes_needed {
            value = (value << 8) | buf[1 + i] as u64;
        }
        Ok((&buf[1 + bytes_needed..], value))
    }
}

/// Encodes a signed integer (varint) in ascending order
pub fn encode_varint_ascending(buf: Vec<u8>, value: i64) -> Vec<u8> {
    // Convert to unsigned using zigzag encoding for signed values
    let unsigned = if value >= 0 {
        (value as u64) << 1
    } else {
        (((-value - 1) as u64) << 1) | 1
    };
    encode_uvarint_ascending(buf, unsigned)
}

/// Decodes a signed varint from the buffer
pub fn decode_varint_ascending(buf: &[u8]) -> Result<(&[u8], i64)> {
    let (remaining, unsigned) = decode_uvarint_ascending(buf)?;
    let value = if unsigned & 1 == 0 {
        (unsigned >> 1) as i64
    } else {
        -((unsigned >> 1) as i64) - 1
    };
    Ok((remaining, value))
}

/// Encodes a boolean in ascending order
pub fn encode_bool_ascending(mut buf: Vec<u8>, value: bool) -> Vec<u8> {
    buf.push(if value { 0x21 } else { 0x20 });
    buf
}

/// Decodes a boolean from the buffer
pub fn decode_bool_ascending(buf: &[u8]) -> Result<(&[u8], bool)> {
    if buf.is_empty() {
        return Err(Error::Other("Empty buffer for bool decode".into()));
    }
    match buf[0] {
        0x20 => Ok((&buf[1..], false)),
        0x21 => Ok((&buf[1..], true)),
        _ => Err(Error::Other(format!("Invalid bool marker: {:#x}", buf[0]))),
    }
}

/// Encodes a string in ascending order with escape sequences
/// This maintains lexicographic sort order
pub fn encode_string_ascending(mut buf: Vec<u8>, s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    for &byte in bytes {
        if byte == 0x00 {
            // Escape null bytes as 0x00 0xFF
            buf.push(0x00);
            buf.push(0xFF);
        } else {
            buf.push(byte);
        }
    }
    // Terminator: 0x00 0x00
    buf.push(0x00);
    buf.push(0x00);
    buf
}

/// Decodes a string from the buffer
pub fn decode_string_ascending(buf: &[u8]) -> Result<(&[u8], String)> {
    let mut result = Vec::new();
    let mut i = 0;

    while i < buf.len() {
        if buf[i] == 0x00 {
            if i + 1 >= buf.len() {
                return Err(Error::Other("Unexpected end of buffer in string".into()));
            }
            if buf[i + 1] == 0x00 {
                // Terminator found
                let s = String::from_utf8(result)
                    .map_err(|e| Error::Other(format!("Invalid UTF-8: {}", e)))?;
                return Ok((&buf[i + 2..], s));
            } else if buf[i + 1] == 0xFF {
                // Escaped null byte
                result.push(0x00);
                i += 2;
            } else {
                return Err(Error::Other("Invalid escape sequence in string".into()));
            }
        } else {
            result.push(buf[i]);
            i += 1;
        }
    }

    Err(Error::Other("String not terminated".into()))
}

/// Encodes a null value in ascending order
pub fn encode_null_ascending(mut buf: Vec<u8>) -> Vec<u8> {
    buf.push(0x00);
    buf
}

/// Encodes f64 in ascending order
/// Uses IEEE 754 representation with bit flipping for sort order
pub fn encode_float64_ascending(mut buf: Vec<u8>, value: f64) -> Vec<u8> {
    let mut bits = value.to_bits();

    // Flip sign bit and all other bits if negative
    if (bits & (1 << 63)) != 0 {
        bits = !bits;
    } else {
        bits ^= 1 << 63;
    }

    // Encode as big-endian
    buf.extend_from_slice(&bits.to_be_bytes());
    buf
}

/// Decodes f64 from the buffer
pub fn decode_float64_ascending(buf: &[u8]) -> Result<(&[u8], f64)> {
    if buf.len() < 8 {
        return Err(Error::Other("Buffer too short for f64".into()));
    }

    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&buf[0..8]);
    let mut bits = u64::from_be_bytes(bytes);

    // Reverse bit flipping
    if (bits & (1 << 63)) != 0 {
        bits ^= 1 << 63;
    } else {
        bits = !bits;
    }

    Ok((&buf[8..], f64::from_bits(bits)))
}

/// Encodes f32 in ascending order
pub fn encode_float32_ascending(mut buf: Vec<u8>, value: f32) -> Vec<u8> {
    let mut bits = value.to_bits();

    if (bits & (1 << 31)) != 0 {
        bits = !bits;
    } else {
        bits ^= 1 << 31;
    }

    buf.extend_from_slice(&bits.to_be_bytes());
    buf
}

/// Decodes f32 from the buffer
pub fn decode_float32_ascending(buf: &[u8]) -> Result<(&[u8], f32)> {
    if buf.len() < 4 {
        return Err(Error::Other("Buffer too short for f32".into()));
    }

    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&buf[0..4]);
    let mut bits = u32::from_be_bytes(bytes);

    if (bits & (1 << 31)) != 0 {
        bits ^= 1 << 31;
    } else {
        bits = !bits;
    }

    Ok((&buf[4..], f32::from_bits(bits)))
}

/// Helper to convert u32 to decimal string (for keys that use decimal representation)
pub fn u32_to_decimal_string(value: u32) -> String {
    value.to_string()
}

/// Helper to parse decimal string to u32
pub fn decimal_string_to_u32(s: &str) -> Result<u32> {
    s.parse::<u32>()
        .map_err(|e| Error::Other(format!("Invalid decimal u32: {}", e)))
}

#[cfg(test)]
mod tests {
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
        assert_eq!(decoded, true);

        let buf = encode_bool_ascending(vec![], false);
        let (_, decoded) = decode_bool_ascending(&buf).unwrap();
        assert_eq!(decoded, false);
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
        assert_eq!(InstanceType::from_byte(b'p').unwrap(), InstanceType::Priority);
        assert_eq!(InstanceType::from_byte(b'd').unwrap(), InstanceType::Deleted);
    }
}
