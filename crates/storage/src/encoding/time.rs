
//! Timestamp encoding

use crate::corekv::{Error, Result};

use super::{
    decode_varint_ascending, decode_varint_descending, encode_varint_ascending,
    encode_varint_descending, TIME_MARKER,
};

/// Encode a timestamp (as nanoseconds since Unix epoch) in ascending order
pub fn encode_time_ascending(mut buf: Vec<u8>, unix_nanos: i64) -> Vec<u8> {
    buf.push(TIME_MARKER);
    encode_varint_ascending(buf, unix_nanos)
}

/// Encode a timestamp in descending order
pub fn encode_time_descending(mut buf: Vec<u8>, unix_nanos: i64) -> Vec<u8> {
    buf.push(TIME_MARKER);
    encode_varint_descending(buf, unix_nanos)
}

/// Decode a timestamp from ascending encoding
pub fn decode_time_ascending(buf: &[u8]) -> Result<(&[u8], i64)> {
    if buf.is_empty() || buf[0] != TIME_MARKER {
        return Err(Error::Other(format!(
            "cannot decode time: marker not found in {:?}",
            buf.first()
        )));
    }
    decode_varint_ascending(&buf[1..])
}

/// Decode a timestamp from descending encoding
pub fn decode_time_descending(buf: &[u8]) -> Result<(&[u8], i64)> {
    if buf.is_empty() || buf[0] != TIME_MARKER {
        return Err(Error::Other(format!(
            "cannot decode time: marker not found in {:?}",
            buf.first()
        )));
    }
    decode_varint_descending(&buf[1..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_encoding() {
        let test_values: Vec<i64> = vec![
            i64::MIN,
            -1_000_000_000, // -1 second
            0,
            1_000_000_000, // +1 second
            i64::MAX,
        ];

        for v in &test_values {
            let buf = encode_time_ascending(vec![], *v);
            let (_, decoded) = decode_time_ascending(&buf).unwrap();
            assert_eq!(decoded, *v, "time roundtrip failed for {}", v);

            let buf = encode_time_descending(vec![], *v);
            let (_, decoded) = decode_time_descending(&buf).unwrap();
            assert_eq!(decoded, *v, "descending time roundtrip failed for {}", v);
        }
    }

    #[test]
    fn test_decode_time_missing_marker() {
        let buf = vec![0x50, 0x01, 0x02];
        let result = decode_time_ascending(&buf);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("marker not found"));
    }
}
