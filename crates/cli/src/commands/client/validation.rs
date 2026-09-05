//! Input validation utilities for GraphQL query construction

use crate::error::{Error, Result};

/// Validate that a string is a valid GraphQL identifier (collection name, field name, etc.)
///
/// GraphQL identifiers must:
/// - Start with a letter or underscore
/// - Contain only letters, digits, and underscores
/// - Not be empty
pub fn validate_identifier(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidIdentifier(
            "identifier cannot be empty".to_string(),
        ));
    }

    let valid = name.chars().enumerate().all(|(i, c)| {
        if i == 0 {
            c.is_ascii_alphabetic() || c == '_'
        } else {
            c.is_ascii_alphanumeric() || c == '_'
        }
    });

    if !valid {
        return Err(Error::InvalidIdentifier(format!(
            "'{name}' is not a valid identifier (must match [A-Za-z_][A-Za-z0-9_]*)"
        )));
    }

    Ok(())
}

/// Escape special characters in a string for use in GraphQL string literals.
///
/// This prevents injection attacks when interpolating user input into GraphQL queries.
/// Control characters without a short escape are emitted as `\uXXXX`,
/// matching the REST layer (`query::rest`) and Go's `valueToGQL`.
pub fn escape_graphql_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => result.push_str("\\\\"),
            '"' => result.push_str("\\\""),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            '\u{0008}' => result.push_str("\\b"),
            '\u{000C}' => result.push_str("\\f"),
            c if (c as u32) < 0x20 || (c as u32) == 0x7F => {
                result.push_str(&format!("\\u{:04X}", c as u32));
            }
            _ => result.push(c),
        }
    }
    result
}

/// Convert a JSON value to GraphQL input literal syntax.
///
/// GraphQL input objects use unquoted keys: `{name: "Alice", age: 30}`
/// instead of JSON's `{"name": "Alice", "age": 30}`.
///
/// Object keys are written unquoted into the mutation, so a key that is
/// not a valid GraphQL name (e.g. containing `)`, `{`, whitespace) is
/// rejected here with `InvalidIdentifier` instead of emitting a
/// malformed query or a server-side parse error.
pub fn json_to_graphql_input(value: &serde_json::Value) -> Result<String> {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            let mut fields = Vec::with_capacity(map.len());
            for (k, v) in map {
                validate_identifier(k)?;
                fields.push(format!("{}: {}", k, json_to_graphql_input(v)?));
            }
            Ok(format!("{{{}}}", fields.join(", ")))
        }
        Value::Array(arr) => {
            let mut items = Vec::with_capacity(arr.len());
            for item in arr {
                items.push(json_to_graphql_input(item)?);
            }
            Ok(format!("[{}]", items.join(", ")))
        }
        Value::String(s) => Ok(format!("\"{}\"", escape_graphql_string(s))),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Null => Ok("null".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_identifier_valid() {
        assert!(validate_identifier("Users").is_ok());
        assert!(validate_identifier("_private").is_ok());
        assert!(validate_identifier("User123").is_ok());
        assert!(validate_identifier("_").is_ok());
        assert!(validate_identifier("A").is_ok());
    }

    #[test]
    fn test_validate_identifier_invalid() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("123Users").is_err());
        assert!(validate_identifier("User-Name").is_err());
        assert!(validate_identifier("User Name").is_err());
        assert!(validate_identifier("User.Name").is_err());
        assert!(validate_identifier("Users)").is_err());
        assert!(validate_identifier("Users{").is_err());
    }

    #[test]
    fn test_validate_identifier_injection_attempt() {
        // Attempt to inject additional GraphQL operations
        assert!(validate_identifier(
            "Users) { _docID } } mutation { delete_Users(docIDs: [\"all\"]"
        )
        .is_err());
    }

    #[test]
    fn test_escape_graphql_string_basic() {
        assert_eq!(escape_graphql_string("hello"), "hello");
        assert_eq!(escape_graphql_string("bae-123"), "bae-123");
    }

    #[test]
    fn test_escape_graphql_string_special_chars() {
        assert_eq!(escape_graphql_string(r#"test"value"#), r#"test\"value"#);
        assert_eq!(escape_graphql_string("test\\value"), "test\\\\value");
        assert_eq!(escape_graphql_string("test\nvalue"), "test\\nvalue");
        assert_eq!(escape_graphql_string("test\rvalue"), "test\\rvalue");
        assert_eq!(escape_graphql_string("test\tvalue"), "test\\tvalue");
    }

    #[test]
    fn test_escape_graphql_string_control_chars() {
        assert_eq!(escape_graphql_string("a\u{0008}b"), "a\\bb");
        assert_eq!(escape_graphql_string("a\u{000C}b"), "a\\fb");
        assert_eq!(escape_graphql_string("a\u{0000}b"), "a\\u0000b");
        assert_eq!(escape_graphql_string("a\u{0007}b"), "a\\u0007b");
    }

    #[test]
    fn test_escape_graphql_string_injection_attempt() {
        // Malicious input that tries to break out of a GraphQL string
        let malicious = r#"bae-123"}) { _docID }} mutation { delete_Users(docIDs: ["all"#;
        let escaped = escape_graphql_string(malicious);

        // The quotes should be escaped
        assert!(escaped.contains(r#"\""#));

        // The escaped string should NOT start with an unescaped quote that would end the string
        assert!(!escaped.starts_with('"'));

        // Original unescaped pattern should not appear as standalone
        // (the `"` in `"})` is now `\"` so it won't terminate the GraphQL string)
        assert_eq!(
            escaped,
            r#"bae-123\"}) { _docID }} mutation { delete_Users(docIDs: [\"all"#
        );
    }

    #[test]
    fn test_json_to_graphql_input_scalars() {
        use serde_json::json;
        assert_eq!(json_to_graphql_input(&json!(null)).unwrap(), "null");
        assert_eq!(json_to_graphql_input(&json!(true)).unwrap(), "true");
        assert_eq!(json_to_graphql_input(&json!(42)).unwrap(), "42");
        assert_eq!(
            json_to_graphql_input(&json!("a\u{0008}b")).unwrap(),
            "\"a\\bb\""
        );
    }

    #[test]
    fn test_json_to_graphql_input_rejects_invalid_keys() {
        use serde_json::json;
        assert!(json_to_graphql_input(&json!({"na)me": 1})).is_err());
        assert!(json_to_graphql_input(&json!({"na me": 1})).is_err());
        assert!(json_to_graphql_input(&json!({"user": {"na}me": 1}})).is_err());
        assert!(json_to_graphql_input(&json!([{"na)me": 1}])).is_err());
    }

    #[test]
    fn test_json_to_graphql_input_valid_object() {
        use serde_json::json;
        let result = json_to_graphql_input(&json!({"name": "Alice", "age": 30})).unwrap();
        assert!(result.contains("name: \"Alice\""));
        assert!(result.contains("age: 30"));
    }
}
