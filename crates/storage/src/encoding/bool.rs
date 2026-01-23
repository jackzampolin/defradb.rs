
//! Boolean value encoding

use crate::corekv::{Error, Result};

use super::{peek_type, EncodedType, FALSE_MARKER, TRUE_MARKER};

/// Encode a boolean in ascending order
pub fn encode_bool_ascending(mut buf: Vec<u8>, v: bool) -> Vec<u8> {
    buf.push(if v { TRUE_MARKER } else { FALSE_MARKER });
    buf
}

/// Encode a boolean in descending order
pub fn encode_bool_descending(buf: Vec<u8>, v: bool) -> Vec<u8> {
    encode_bool_ascending(buf, !v)
}

/// Decode a boolean encoded in ascending order
pub fn decode_bool_ascending(buf: &[u8]) -> Result<(&[u8], bool)> {
    if buf.is_empty() || peek_type(buf) != EncodedType::Bool {
        return Err(Error::Other(format!(
            "cannot decode bool: markers not found in {:?}",
            buf.first()
        )));
    }
    Ok((&buf[1..], buf[0] == TRUE_MARKER))
}

/// Decode a boolean encoded in descending order
pub fn decode_bool_descending(buf: &[u8]) -> Result<(&[u8], bool)> {
    let (rest, v) = decode_bool_ascending(buf)?;
    Ok((rest, !v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_encoding() {
        // Ascending
        let buf = encode_bool_ascending(vec![], true);
        let (_, v) = decode_bool_ascending(&buf).unwrap();
        assert!(v);

        let buf = encode_bool_ascending(vec![], false);
        let (_, v) = decode_bool_ascending(&buf).unwrap();
        assert!(!v);

        // Descending
        let buf = encode_bool_descending(vec![], true);
        let (_, v) = decode_bool_descending(&buf).unwrap();
        assert!(v);

        let buf = encode_bool_descending(vec![], false);
        let (_, v) = decode_bool_descending(&buf).unwrap();
        assert!(!v);

        // Sort order: false < true in ascending
        let buf_false = encode_bool_ascending(vec![], false);
        let buf_true = encode_bool_ascending(vec![], true);
        assert!(buf_false < buf_true);
    }

    #[test]
    fn test_decode_bool_invalid_marker() {
        // Not a valid bool marker
        let buf = vec![0x50];
        let result = decode_bool_ascending(&buf);
        assert!(result.is_err());
    }
}
