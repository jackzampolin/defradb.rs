
//! Bytes and string encoding with escape-based termination

use crate::corekv::{Error, Result};

use super::{BYTES_DESC_MARKER, BYTES_MARKER, ESCAPE, ESCAPED_00, ESCAPED_TERM};

fn ones_complement(buf: &mut [u8]) {
    for b in buf.iter_mut() {
        *b = !*b;
    }
}

/// Encode bytes in ascending order with escape-based encoding
pub fn encode_bytes_ascending(mut buf: Vec<u8>, data: &[u8]) -> Vec<u8> {
    buf.push(BYTES_MARKER);

    let mut i = 0;
    while i < data.len() {
        let byte = data[i];
        if byte == ESCAPE {
            buf.push(ESCAPE);
            buf.push(ESCAPED_00);
        } else {
            buf.push(byte);
        }
        i += 1;
    }

    // Terminator: 0x00 0x01
    buf.push(ESCAPE);
    buf.push(ESCAPED_TERM);
    buf
}

/// Encode bytes in descending order
pub fn encode_bytes_descending(mut buf: Vec<u8>, data: &[u8]) -> Vec<u8> {
    let start = buf.len();
    buf = encode_bytes_ascending(buf, data);
    buf[start] = BYTES_DESC_MARKER;
    ones_complement(&mut buf[start + 1..]);
    buf
}

/// Encode a string in ascending order
pub fn encode_string_ascending(buf: Vec<u8>, s: &str) -> Vec<u8> {
    encode_bytes_ascending(buf, s.as_bytes())
}

/// Encode a string in descending order
pub fn encode_string_descending(buf: Vec<u8>, s: &str) -> Vec<u8> {
    encode_bytes_descending(buf, s.as_bytes())
}

/// Decode bytes from ascending encoding
pub fn decode_bytes_ascending(buf: &[u8]) -> Result<(&[u8], Vec<u8>)> {
    if buf.is_empty() || buf[0] != BYTES_MARKER {
        return Err(Error::Other(format!(
            "cannot decode bytes: marker not found in {:?}",
            buf.first()
        )));
    }
    decode_bytes_internal(&buf[1..], ESCAPE, ESCAPED_TERM, ESCAPED_00)
}

/// Decode bytes from descending encoding
pub fn decode_bytes_descending(buf: &[u8]) -> Result<(&[u8], Vec<u8>)> {
    if buf.is_empty() || buf[0] != BYTES_DESC_MARKER {
        return Err(Error::Other(format!(
            "cannot decode bytes: marker not found in {:?}",
            buf.first()
        )));
    }

    let (rest, mut r) = decode_bytes_internal(&buf[1..], !ESCAPE, !ESCAPED_TERM, !ESCAPED_00)?;
    ones_complement(&mut r);
    Ok((rest, r))
}

fn decode_bytes_internal(
    mut buf: &[u8],
    escape: u8,
    escaped_term: u8,
    escaped_00: u8,
) -> Result<(&[u8], Vec<u8>)> {
    let mut result = Vec::new();

    loop {
        let i = buf.iter().position(|&b| b == escape);
        let i = match i {
            Some(i) => i,
            None => return Err(Error::Other("terminator not found in bytes".to_string())),
        };

        if i + 1 >= buf.len() {
            return Err(Error::Other("malformed escape sequence".to_string()));
        }

        let v = buf[i + 1];
        if v == escaped_term {
            result.extend_from_slice(&buf[..i]);
            return Ok((&buf[i + 2..], result));
        }

        if v != escaped_00 {
            return Err(Error::Other(format!(
                "unknown escape sequence: {:02x} {:02x}",
                escape, v
            )));
        }

        result.extend_from_slice(&buf[..i]);
        result.push(if escaped_00 == ESCAPED_00 { 0x00 } else { 0xff });
        buf = &buf[i + 2..];
    }
}

/// Decode a string from ascending encoding
pub fn decode_string_ascending(buf: &[u8]) -> Result<(&[u8], String)> {
    let (rest, bytes) = decode_bytes_ascending(buf)?;
    let s = String::from_utf8(bytes).map_err(|e| Error::Other(format!("invalid utf-8: {}", e)))?;
    Ok((rest, s))
}

/// Decode a string from descending encoding
pub fn decode_string_descending(buf: &[u8]) -> Result<(&[u8], String)> {
    let (rest, bytes) = decode_bytes_descending(buf)?;
    let s = String::from_utf8(bytes).map_err(|e| Error::Other(format!("invalid utf-8: {}", e)))?;
    Ok((rest, s))
}

#[cfg(test)]
mod tests {
    use super::super::BYTES_MARKER;
    use super::*;

    #[test]
    fn test_bytes_encoding() {
        let test_cases: Vec<&[u8]> = vec![
            b"",
            b"hello",
            b"world",
            b"\x00",     // Null byte
            b"a\x00b",   // Embedded null
            b"\x00\x00", // Multiple nulls
            b"\x00\x01", // Looks like terminator
        ];

        for data in &test_cases {
            let buf = encode_bytes_ascending(vec![], data);
            let (_, decoded) = decode_bytes_ascending(&buf).unwrap();
            assert_eq!(&decoded, data, "bytes roundtrip failed");

            let buf = encode_bytes_descending(vec![], data);
            let (_, decoded) = decode_bytes_descending(&buf).unwrap();
            assert_eq!(&decoded, data, "descending bytes roundtrip failed");
        }

        // Verify sort order
        let test_sorted = vec![b"".as_slice(), b"a", b"aa", b"ab", b"b", b"ba"];
        let encoded: Vec<Vec<u8>> = test_sorted
            .iter()
            .map(|v| encode_bytes_ascending(vec![], v))
            .collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "bytes sort order failed: {:?} should be < {:?}",
                test_sorted[i],
                test_sorted[i + 1]
            );
        }
    }

    #[test]
    fn test_string_encoding() {
        let test_cases = vec!["", "hello", "world", "test\x00string"];

        for s in &test_cases {
            let buf = encode_string_ascending(vec![], s);
            let (_, decoded) = decode_string_ascending(&buf).unwrap();
            assert_eq!(&decoded, s, "string roundtrip failed");

            let buf = encode_string_descending(vec![], s);
            let (_, decoded) = decode_string_descending(&buf).unwrap();
            assert_eq!(&decoded, s, "descending string roundtrip failed");
        }
    }

    #[test]
    fn test_decode_bytes_missing_terminator() {
        let buf = vec![BYTES_MARKER, b'h', b'e', b'l', b'l', b'o'];
        let result = decode_bytes_ascending(&buf);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("terminator not found"));
    }

    #[test]
    fn test_decode_bytes_malformed_escape() {
        let buf = vec![BYTES_MARKER, b'h', b'i', ESCAPE];
        let result = decode_bytes_ascending(&buf);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("malformed escape"));
    }
}
