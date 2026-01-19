// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
pub fn escape_graphql_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => result.push_str("\\\\"),
            '"' => result.push_str("\\\""),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            _ => result.push(c),
        }
    }
    result
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
}
