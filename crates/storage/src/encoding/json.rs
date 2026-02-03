//! JSON path+value encoding for indexing
//!
//! Encodes JSON leaf values with their paths for secondary index keys.
//! Format: [JSON_MARKER][path_parts][ESCAPED_TERM][PATH_END][typed_value]
//!
//! Path parts are encoded as:
//! - Property names: encoded as escaped bytes
//! - Array indices: encoded as uvarint (always 0 per Go behavior)

use document::{JsonLeafValue, JsonPath, JsonPathPart, JsonScalarValue};

use super::{
    decode_bool_ascending, decode_bool_descending, decode_bytes_ascending, decode_bytes_descending,
    decode_float64_ascending, decode_float64_descending, decode_if_null, decode_uvarint_ascending,
    decode_uvarint_descending, encode_bool_ascending, encode_bool_descending,
    encode_bytes_ascending, encode_bytes_descending, encode_float64_ascending,
    encode_float64_descending, encode_null_ascending, encode_null_descending,
    encode_uvarint_ascending, encode_uvarint_descending, peek_type, EncodedType, ESCAPED_TERM,
    INT_MAX, INT_MIN, JSON_MARKER,
};
use crate::corekv::{Error, Result};

/// Path terminator after all path parts, before value
pub const JSON_PATH_END: u8 = b'/'; // 0x2F - matches Go's jsonPathEnd

/// Encode a JSON leaf value with path in ascending order.
///
/// Format: [JSON_MARKER][path_parts...][ESCAPED_TERM][PATH_END][typed_value]
///
/// Path encoding:
/// - Properties: escaped bytes (using encode_bytes_ascending without marker)
/// - Array indices: uvarint (always 0 per Go)
///
/// Special sentinel values for range bounds:
/// - PathMin: Encodes path + [ESCAPED_TERM][PATH_END][NULL_MARKER] (comes before all values)
/// - PathMax: Encodes path + [ESCAPED_TERM][PATH_END + 1] (comes after all values)
pub fn encode_json_ascending(buf: Vec<u8>, leaf: &JsonLeafValue) -> Vec<u8> {
    let mut buf = buf;
    buf.push(JSON_MARKER);

    // Encode path parts
    buf = encode_path_ascending(buf, &leaf.path);

    // Handle sentinel values specially
    match &leaf.value {
        JsonScalarValue::PathMax => {
            // PathMax: use PATH_END + 1 to sort after all valid values
            buf.push(ESCAPED_TERM);
            buf.push(JSON_PATH_END + 1); // 0x30 comes after 0x2F
            buf
        }
        JsonScalarValue::PathMin => {
            // PathMin: encode null (lowest value type)
            buf.push(ESCAPED_TERM);
            buf.push(JSON_PATH_END);
            encode_null_ascending(buf)
        }
        _ => {
            // Normal value
            buf.push(ESCAPED_TERM);
            buf.push(JSON_PATH_END);
            encode_scalar_ascending(buf, &leaf.value)
        }
    }
}

/// Encode a JSON leaf value with path in descending order.
///
/// Special sentinel values for range bounds:
/// - PathMin: In descending order, this comes AFTER all values (inverted byte order)
/// - PathMax: In descending order, this comes BEFORE all values (inverted byte order)
pub fn encode_json_descending(buf: Vec<u8>, leaf: &JsonLeafValue) -> Vec<u8> {
    let mut buf = buf;
    buf.push(JSON_MARKER);

    // Encode path parts in descending order
    buf = encode_path_descending(buf, &leaf.path);

    // Handle sentinel values specially
    // Note: For descending, byte order is inverted, so PathMax becomes lower bound
    match &leaf.value {
        JsonScalarValue::PathMax => {
            // PathMax: in descending, use inverted (PATH_END + 1) to sort before all values
            buf.push(!ESCAPED_TERM);
            buf.push(!(JSON_PATH_END + 1));
            buf
        }
        JsonScalarValue::PathMin => {
            // PathMin: in descending, encode null (which becomes highest after inversion)
            buf.push(!ESCAPED_TERM);
            buf.push(!JSON_PATH_END);
            encode_null_descending(buf)
        }
        _ => {
            // Normal value
            buf.push(!ESCAPED_TERM);
            buf.push(!JSON_PATH_END);
            encode_scalar_descending(buf, &leaf.value)
        }
    }
}

/// Encode the JSON path parts.
fn encode_path_ascending(mut buf: Vec<u8>, path: &JsonPath) -> Vec<u8> {
    for part in path.iter() {
        match part {
            JsonPathPart::Property(name) => {
                // Encode property name as escaped bytes (without the BYTES_MARKER)
                buf = encode_path_string_ascending(buf, name);
            }
            JsonPathPart::Index => {
                // Array indices always encode as 0 per Go behavior
                buf = encode_uvarint_ascending(buf, 0);
            }
        }
    }
    buf
}

/// Encode the JSON path parts in descending order.
fn encode_path_descending(mut buf: Vec<u8>, path: &JsonPath) -> Vec<u8> {
    for part in path.iter() {
        match part {
            JsonPathPart::Property(name) => {
                buf = encode_path_string_descending(buf, name);
            }
            JsonPathPart::Index => {
                buf = encode_uvarint_descending(buf, 0);
            }
        }
    }
    buf
}

/// Encode a path property string with escaping (no marker prefix).
fn encode_path_string_ascending(mut buf: Vec<u8>, s: &str) -> Vec<u8> {
    for byte in s.as_bytes() {
        if *byte == 0x00 {
            buf.push(0x00);
            buf.push(0xff);
        } else {
            buf.push(*byte);
        }
    }
    // Terminator
    buf.push(0x00);
    buf.push(ESCAPED_TERM);
    buf
}

/// Encode a path property string with escaping in descending order.
fn encode_path_string_descending(mut buf: Vec<u8>, s: &str) -> Vec<u8> {
    for byte in s.as_bytes() {
        if *byte == 0x00 {
            buf.push(!0x00);
            buf.push(!0xff);
        } else {
            buf.push(!*byte);
        }
    }
    // Terminator (inverted)
    buf.push(!0x00);
    buf.push(!ESCAPED_TERM);
    buf
}

/// Encode a JSON scalar value in ascending order.
fn encode_scalar_ascending(buf: Vec<u8>, value: &JsonScalarValue) -> Vec<u8> {
    match value {
        JsonScalarValue::Null => encode_null_ascending(buf),
        JsonScalarValue::Bool(b) => encode_bool_ascending(buf, *b),
        JsonScalarValue::Number(n) => encode_float64_ascending(buf, *n),
        JsonScalarValue::String(s) => encode_bytes_ascending(buf, s.as_bytes()),
        // PathMin/PathMax are handled specially in encode_json_ascending
        JsonScalarValue::PathMin | JsonScalarValue::PathMax => buf,
    }
}

/// Encode a JSON scalar value in descending order.
fn encode_scalar_descending(buf: Vec<u8>, value: &JsonScalarValue) -> Vec<u8> {
    match value {
        JsonScalarValue::Null => encode_null_descending(buf),
        JsonScalarValue::Bool(b) => encode_bool_descending(buf, *b),
        JsonScalarValue::Number(n) => encode_float64_descending(buf, *n),
        JsonScalarValue::String(s) => encode_bytes_descending(buf, s.as_bytes()),
        // PathMin/PathMax are handled specially in encode_json_descending
        JsonScalarValue::PathMin | JsonScalarValue::PathMax => buf,
    }
}

/// Decode a JSON leaf value from bytes (ascending order).
///
/// Returns the remaining buffer and the decoded JsonLeafValue.
pub fn decode_json_ascending(buf: &[u8]) -> Result<(&[u8], JsonLeafValue)> {
    if buf.is_empty() || buf[0] != JSON_MARKER {
        return Err(Error::Other(format!(
            "expected JSON_MARKER (0x{:02x}), got 0x{:02x}",
            JSON_MARKER,
            buf.first().unwrap_or(&0)
        )));
    }

    let mut rest = &buf[1..];

    // Decode path parts until we hit the terminator
    let mut path = JsonPath::new();
    loop {
        if rest.len() < 2 {
            return Err(Error::Other("unexpected end of JSON path".into()));
        }

        // Check for path terminator: [ESCAPED_TERM][PATH_END]
        if rest[0] == ESCAPED_TERM && rest[1] == JSON_PATH_END {
            rest = &rest[2..];
            break;
        }

        // Check if this is an array index (uvarint) or property (string)
        // Array indices are encoded as uvarints which start with 0x80+ marker
        if rest[0] >= INT_MIN && rest[0] <= INT_MAX {
            // Array index - decode uvarint
            let (remaining, _idx) = decode_uvarint_ascending(rest)?;
            path = path.append_index();
            rest = remaining;
        } else {
            // Property name - decode as escaped string
            let (remaining, name) = decode_path_string_ascending(rest)?;
            path = path.append_property(&name);
            rest = remaining;
        }
    }

    // Decode the value
    let (rest, value) = decode_scalar_ascending(rest)?;

    Ok((rest, JsonLeafValue::new(path, value)))
}

/// Decode a JSON leaf value from bytes (descending order).
pub fn decode_json_descending(buf: &[u8]) -> Result<(&[u8], JsonLeafValue)> {
    if buf.is_empty() || buf[0] != JSON_MARKER {
        return Err(Error::Other(format!(
            "expected JSON_MARKER (0x{:02x}), got 0x{:02x}",
            JSON_MARKER,
            buf.first().unwrap_or(&0)
        )));
    }

    let mut rest = &buf[1..];

    // Decode path parts until we hit the terminator (inverted)
    let mut path = JsonPath::new();
    let term_inverted = !ESCAPED_TERM;
    let path_end_inverted = !JSON_PATH_END;

    loop {
        if rest.len() < 2 {
            return Err(Error::Other("unexpected end of JSON path".into()));
        }

        // Check for path terminator (inverted)
        if rest[0] == term_inverted && rest[1] == path_end_inverted {
            rest = &rest[2..];
            break;
        }

        // For descending, check if this looks like an inverted uvarint or string
        // Inverted bytes for INT_MIN..INT_MAX would be !INT_MAX..!INT_MIN
        let inverted_byte = !rest[0];
        if inverted_byte >= INT_MIN && inverted_byte <= INT_MAX {
            // Array index - decode descending uvarint
            let (remaining, _idx) = decode_uvarint_descending(rest)?;
            path = path.append_index();
            rest = remaining;
        } else {
            // Property name - decode as descending escaped string
            let (remaining, name) = decode_path_string_descending(rest)?;
            path = path.append_property(&name);
            rest = remaining;
        }
    }

    // Decode the value in descending order
    let (rest, value) = decode_scalar_descending(rest)?;

    Ok((rest, JsonLeafValue::new(path, value)))
}

/// Decode a path property string (ascending).
fn decode_path_string_ascending(buf: &[u8]) -> Result<(&[u8], String)> {
    let mut result = Vec::new();
    let mut i = 0;

    while i < buf.len() {
        if buf[i] == 0x00 {
            if i + 1 >= buf.len() {
                return Err(Error::Other("unexpected end of escaped string".into()));
            }
            match buf[i + 1] {
                ESCAPED_TERM => {
                    // End of string
                    let s = String::from_utf8(result)
                        .map_err(|e| Error::Other(format!("invalid utf-8 in path: {}", e)))?;
                    return Ok((&buf[i + 2..], s));
                }
                0xff => {
                    // Escaped null byte
                    result.push(0x00);
                    i += 2;
                }
                other => {
                    return Err(Error::Other(format!(
                        "invalid escape sequence in path: 0x00 0x{:02x}",
                        other
                    )));
                }
            }
        } else {
            result.push(buf[i]);
            i += 1;
        }
    }

    Err(Error::Other("unterminated path string".into()))
}

/// Decode a path property string (descending).
fn decode_path_string_descending(buf: &[u8]) -> Result<(&[u8], String)> {
    let mut result = Vec::new();
    let mut i = 0;

    let term_inverted = !0x00u8;
    let escaped_term_inverted = !ESCAPED_TERM;
    let escaped_00_inverted = !0xffu8;

    while i < buf.len() {
        if buf[i] == term_inverted {
            if i + 1 >= buf.len() {
                return Err(Error::Other("unexpected end of escaped string".into()));
            }
            match buf[i + 1] {
                b if b == escaped_term_inverted => {
                    // End of string
                    let s = String::from_utf8(result)
                        .map_err(|e| Error::Other(format!("invalid utf-8 in path: {}", e)))?;
                    return Ok((&buf[i + 2..], s));
                }
                b if b == escaped_00_inverted => {
                    // Escaped null byte
                    result.push(0x00);
                    i += 2;
                }
                other => {
                    return Err(Error::Other(format!(
                        "invalid escape sequence in descending path: 0x{:02x} 0x{:02x}",
                        buf[i], other
                    )));
                }
            }
        } else {
            // Invert the byte back
            result.push(!buf[i]);
            i += 1;
        }
    }

    Err(Error::Other("unterminated path string".into()))
}

/// Decode a JSON scalar value (ascending).
fn decode_scalar_ascending(buf: &[u8]) -> Result<(&[u8], JsonScalarValue)> {
    let typ = peek_type(buf);
    match typ {
        EncodedType::Null => {
            let (rest, _) = decode_if_null(buf);
            Ok((rest, JsonScalarValue::Null))
        }
        EncodedType::Bool => {
            let (rest, v) = decode_bool_ascending(buf)?;
            Ok((rest, JsonScalarValue::Bool(v)))
        }
        EncodedType::Float64 => {
            let (rest, v) = decode_float64_ascending(buf)?;
            Ok((rest, JsonScalarValue::Number(v)))
        }
        EncodedType::Int => {
            // JSON numbers are encoded as float64, but handle int just in case
            let (rest, v) = decode_uvarint_ascending(buf)?;
            Ok((rest, JsonScalarValue::Number(v as f64)))
        }
        EncodedType::Bytes | EncodedType::BytesDesc => {
            let (rest, v) = decode_bytes_ascending(buf)?;
            let s = String::from_utf8(v)
                .map_err(|e| Error::Other(format!("invalid utf-8 in JSON string: {}", e)))?;
            Ok((rest, JsonScalarValue::String(s)))
        }
        _ => Err(Error::Other(format!(
            "cannot decode JSON scalar: unknown type {:?} (marker 0x{:02x})",
            typ,
            buf.first().unwrap_or(&0)
        ))),
    }
}

/// Decode a JSON scalar value (descending).
fn decode_scalar_descending(buf: &[u8]) -> Result<(&[u8], JsonScalarValue)> {
    // For descending, we need to check inverted markers
    if buf.is_empty() {
        return Err(Error::Other("empty buffer for JSON scalar".into()));
    }

    let marker = buf[0];

    // Check for inverted null marker
    if marker == 0xff {
        // ENCODED_NULL_DESC
        return Ok((&buf[1..], JsonScalarValue::Null));
    }

    // Check for inverted bool markers
    if marker == !super::FALSE_MARKER {
        return Ok((&buf[1..], JsonScalarValue::Bool(false)));
    }
    if marker == !super::TRUE_MARKER {
        return Ok((&buf[1..], JsonScalarValue::Bool(true)));
    }

    // Check for float64 descending
    let inverted = !marker;
    if (super::FLOAT64_NAN..=super::FLOAT64_NAN_DESC).contains(&inverted) {
        let (rest, v) = decode_float64_descending(buf)?;
        return Ok((rest, JsonScalarValue::Number(v)));
    }

    // Check for bytes descending
    if marker == super::BYTES_DESC_MARKER {
        let (rest, v) = decode_bytes_descending(buf)?;
        let s = String::from_utf8(v)
            .map_err(|e| Error::Other(format!("invalid utf-8 in JSON string: {}", e)))?;
        return Ok((rest, JsonScalarValue::String(s)));
    }

    Err(Error::Other(format!(
        "cannot decode descending JSON scalar: marker 0x{:02x}",
        marker
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_json_simple_number() {
        let leaf = JsonLeafValue::new(JsonPath::new(), JsonScalarValue::Number(168.0));

        let encoded = encode_json_ascending(vec![], &leaf);

        // Should start with JSON_MARKER
        assert_eq!(encoded[0], JSON_MARKER);
        // Should have path terminator after marker (empty path)
        assert_eq!(encoded[1], ESCAPED_TERM);
        assert_eq!(encoded[2], JSON_PATH_END);
        // Value follows
        assert!(encoded.len() > 3);
    }

    #[test]
    fn test_encode_json_with_path() {
        let path = JsonPath::new()
            .append_property("custom")
            .append_property("height");
        let leaf = JsonLeafValue::new(path, JsonScalarValue::Number(168.0));

        let encoded = encode_json_ascending(vec![], &leaf);

        // Should start with JSON_MARKER
        assert_eq!(encoded[0], JSON_MARKER);
        // Path should contain "custom" and "height" somewhere before terminator
        assert!(encoded.len() > 20);
    }

    #[test]
    fn test_encode_json_with_array_path() {
        let path = JsonPath::new().append_property("tags").append_index();
        let leaf = JsonLeafValue::new(path, JsonScalarValue::String("value".to_string()));

        let encoded = encode_json_ascending(vec![], &leaf);

        assert_eq!(encoded[0], JSON_MARKER);
    }

    #[test]
    fn test_encode_json_string() {
        let leaf = JsonLeafValue::new(JsonPath::new(), JsonScalarValue::String("hello".to_string()));

        let encoded = encode_json_ascending(vec![], &leaf);
        assert_eq!(encoded[0], JSON_MARKER);
    }

    #[test]
    fn test_encode_json_bool() {
        let leaf_true = JsonLeafValue::new(JsonPath::new(), JsonScalarValue::Bool(true));
        let leaf_false = JsonLeafValue::new(JsonPath::new(), JsonScalarValue::Bool(false));

        let encoded_true = encode_json_ascending(vec![], &leaf_true);
        let encoded_false = encode_json_ascending(vec![], &leaf_false);

        // false < true in ascending order
        assert!(encoded_false < encoded_true);
    }

    #[test]
    fn test_encode_json_null() {
        let leaf = JsonLeafValue::new(JsonPath::new(), JsonScalarValue::Null);

        let encoded = encode_json_ascending(vec![], &leaf);
        assert_eq!(encoded[0], JSON_MARKER);
    }

    #[test]
    fn test_encode_json_sort_order_by_path() {
        // Same value, different paths - should sort by path
        let leaf_a = JsonLeafValue::new(
            JsonPath::new().append_property("aaa"),
            JsonScalarValue::Number(1.0),
        );
        let leaf_b = JsonLeafValue::new(
            JsonPath::new().append_property("bbb"),
            JsonScalarValue::Number(1.0),
        );

        let encoded_a = encode_json_ascending(vec![], &leaf_a);
        let encoded_b = encode_json_ascending(vec![], &leaf_b);

        assert!(encoded_a < encoded_b);
    }

    #[test]
    fn test_encode_json_sort_order_by_value() {
        // Same path, different values - should sort by value
        let path = JsonPath::new().append_property("num");
        let leaf_1 = JsonLeafValue::new(path.clone(), JsonScalarValue::Number(1.0));
        let leaf_2 = JsonLeafValue::new(path, JsonScalarValue::Number(2.0));

        let encoded_1 = encode_json_ascending(vec![], &leaf_1);
        let encoded_2 = encode_json_ascending(vec![], &leaf_2);

        assert!(encoded_1 < encoded_2);
    }

    #[test]
    fn test_encode_json_descending_reverses_order() {
        let leaf_1 = JsonLeafValue::new(JsonPath::new(), JsonScalarValue::Number(1.0));
        let leaf_2 = JsonLeafValue::new(JsonPath::new(), JsonScalarValue::Number(2.0));

        let asc_1 = encode_json_ascending(vec![], &leaf_1);
        let asc_2 = encode_json_ascending(vec![], &leaf_2);
        let desc_1 = encode_json_descending(vec![], &leaf_1);
        let desc_2 = encode_json_descending(vec![], &leaf_2);

        // Ascending: 1 < 2
        assert!(asc_1 < asc_2);
        // Descending: 1 > 2
        assert!(desc_1 > desc_2);
    }
}
