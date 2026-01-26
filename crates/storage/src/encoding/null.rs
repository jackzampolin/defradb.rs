//! Null value encoding

use super::{peek_type, EncodedType, ENCODED_NULL, ENCODED_NULL_DESC};

/// Encode a null value in ascending order
pub fn encode_null_ascending(mut buf: Vec<u8>) -> Vec<u8> {
    buf.push(ENCODED_NULL);
    buf
}

/// Encode a null value in descending order
pub fn encode_null_descending(mut buf: Vec<u8>) -> Vec<u8> {
    buf.push(ENCODED_NULL_DESC);
    buf
}

/// Decode and check if buffer starts with null
pub fn decode_if_null(buf: &[u8]) -> (&[u8], bool) {
    match peek_type(buf) {
        EncodedType::Null => (&buf[1..], true),
        _ => (buf, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_encoding() {
        let buf = encode_null_ascending(vec![]);
        assert_eq!(buf, vec![ENCODED_NULL]);
        let (rest, is_null) = decode_if_null(&buf);
        assert!(is_null);
        assert!(rest.is_empty());

        let buf = encode_null_descending(vec![]);
        assert_eq!(buf, vec![ENCODED_NULL_DESC]);
        let (_, is_null) = decode_if_null(&buf);
        assert!(is_null);
    }
}
