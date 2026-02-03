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
    encode_bool_ascending, encode_bool_descending, encode_bytes_ascending, encode_bytes_descending,
    encode_float64_ascending, encode_float64_descending, encode_null_ascending,
    encode_null_descending, encode_uvarint_ascending, encode_uvarint_descending, ESCAPED_TERM,
    JSON_MARKER,
};

/// Path terminator after all path parts, before value
pub const JSON_PATH_END: u8 = b'/'; // 0x2F - matches Go's jsonPathEnd

/// Encode a JSON leaf value with path in ascending order.
///
/// Format: [JSON_MARKER][path_parts...][ESCAPED_TERM][PATH_END][typed_value]
///
/// Path encoding:
/// - Properties: escaped bytes (using encode_bytes_ascending without marker)
/// - Array indices: uvarint (always 0 per Go)
pub fn encode_json_ascending(buf: Vec<u8>, leaf: &JsonLeafValue) -> Vec<u8> {
    let mut buf = buf;
    buf.push(JSON_MARKER);

    // Encode path parts
    buf = encode_path_ascending(buf, &leaf.path);

    // Path terminator
    buf.push(ESCAPED_TERM);
    buf.push(JSON_PATH_END);

    // Encode value based on type
    encode_scalar_ascending(buf, &leaf.value)
}

/// Encode a JSON leaf value with path in descending order.
pub fn encode_json_descending(buf: Vec<u8>, leaf: &JsonLeafValue) -> Vec<u8> {
    let mut buf = buf;
    buf.push(JSON_MARKER);

    // Encode path parts in descending order
    buf = encode_path_descending(buf, &leaf.path);

    // Path terminator (inverted for descending)
    buf.push(!ESCAPED_TERM);
    buf.push(!JSON_PATH_END);

    // Encode value in descending order
    encode_scalar_descending(buf, &leaf.value)
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
    }
}

/// Encode a JSON scalar value in descending order.
fn encode_scalar_descending(buf: Vec<u8>, value: &JsonScalarValue) -> Vec<u8> {
    match value {
        JsonScalarValue::Null => encode_null_descending(buf),
        JsonScalarValue::Bool(b) => encode_bool_descending(buf, *b),
        JsonScalarValue::Number(n) => encode_float64_descending(buf, *n),
        JsonScalarValue::String(s) => encode_bytes_descending(buf, s.as_bytes()),
    }
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
