//! Input validation utilities for HTTP handlers.

use crate::error::HttpError;

/// Validate that a string is a valid identifier (collection name, field name, etc.)
///
/// Identifiers must:
/// - Start with a letter or underscore
/// - Contain only letters, digits, and underscores
/// - Not be empty
pub fn validate_identifier(name: &str) -> Result<(), HttpError> {
    if name.is_empty() {
        return Err(HttpError::BadRequest(
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
        return Err(HttpError::BadRequest(format!(
            "'{}' is not a valid identifier (must match [A-Za-z_][A-Za-z0-9_]*)",
            name
        )));
    }

    Ok(())
}

/// Validate a collection name.
pub fn validate_collection_name(name: &str) -> Result<(), HttpError> {
    validate_identifier(name).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            name
        ))
    })
}

/// Validate a P2P multiaddr address format.
///
/// Basic validation: multiaddrs must start with '/'.
/// Full validation happens in the P2P layer.
pub fn validate_multiaddr(address: &str) -> Result<(), HttpError> {
    if address.trim().is_empty() {
        return Err(HttpError::BadRequest("address cannot be empty".to_string()));
    }

    if !address.starts_with('/') {
        return Err(HttpError::BadRequest(format!(
            "invalid multiaddr '{}': must start with '/' (e.g., /ip4/127.0.0.1/tcp/9000/p2p/...)",
            address
        )));
    }

    Ok(())
}

/// Validate a document ID format.
///
/// DefraDB document IDs have the format "bae-" followed by a UUID-like string.
/// This performs basic validation; full validation happens in the document layer.
pub fn validate_doc_id(doc_id: &str) -> Result<(), HttpError> {
    if doc_id.trim().is_empty() {
        return Err(HttpError::BadRequest("document ID cannot be empty".to_string()));
    }

    // DefraDB doc IDs start with "bae-"
    if !doc_id.starts_with("bae-") {
        return Err(HttpError::BadRequest(format!(
            "invalid document ID '{}': must start with 'bae-'",
            doc_id
        )));
    }

    // Basic format check: bae- followed by alphanumeric and dashes
    let suffix = &doc_id[4..];
    if suffix.is_empty() {
        return Err(HttpError::BadRequest(format!(
            "invalid document ID '{}': missing ID after 'bae-' prefix",
            doc_id
        )));
    }

    // Validate the suffix contains only valid characters (hex digits and dashes)
    let valid = suffix.chars().all(|c| c.is_ascii_hexdigit() || c == '-');
    if !valid {
        return Err(HttpError::BadRequest(format!(
            "invalid document ID '{}': ID suffix must contain only hex digits and dashes",
            doc_id
        )));
    }

    Ok(())
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
        assert!(validate_identifier("_1").is_ok());
    }

    #[test]
    fn test_validate_identifier_invalid() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("123Users").is_err());
        assert!(validate_identifier("User-Name").is_err());
        assert!(validate_identifier("User Name").is_err());
    }

    #[test]
    fn test_validate_identifier_unicode() {
        // Unicode characters should be rejected
        assert!(validate_identifier("Usuários").is_err());
        assert!(validate_identifier("用户").is_err());
        assert!(validate_identifier("Users日本").is_err());
        assert!(validate_identifier("Пользователи").is_err());
    }

    #[test]
    fn test_validate_identifier_control_chars() {
        // Control characters should be rejected
        assert!(validate_identifier("Users\0").is_err()); // null byte
        assert!(validate_identifier("Users\n").is_err()); // newline
        assert!(validate_identifier("Users\t").is_err()); // tab
        assert!(validate_identifier("Users\r").is_err()); // carriage return
    }

    #[test]
    fn test_validate_identifier_special_chars() {
        // Special characters should be rejected
        assert!(validate_identifier("Users!").is_err());
        assert!(validate_identifier("Users@").is_err());
        assert!(validate_identifier("Users#").is_err());
        assert!(validate_identifier("Users$").is_err());
        assert!(validate_identifier("Users%").is_err());
        assert!(validate_identifier("Users.field").is_err());
        assert!(validate_identifier("Users/path").is_err());
        assert!(validate_identifier("Users;DROP").is_err());
        assert!(validate_identifier("Users'--").is_err());
        assert!(validate_identifier("Users\"quote").is_err());
    }

    #[test]
    fn test_validate_identifier_long_name() {
        // Very long identifier should still be valid if it meets the pattern
        let long_name = "a".repeat(1000);
        assert!(validate_identifier(&long_name).is_ok());
    }

    #[test]
    fn test_validate_multiaddr_valid() {
        assert!(validate_multiaddr("/ip4/127.0.0.1/tcp/9000").is_ok());
        assert!(validate_multiaddr("/ip4/127.0.0.1/tcp/9000/p2p/12D3KooWtest").is_ok());
        assert!(validate_multiaddr("/dns/example.com/tcp/9000").is_ok());
        assert!(validate_multiaddr("/ip6/::1/tcp/9000").is_ok());
    }

    #[test]
    fn test_validate_multiaddr_invalid() {
        assert!(validate_multiaddr("").is_err());
        assert!(validate_multiaddr("192.168.1.1").is_err());
        assert!(validate_multiaddr("localhost:9000").is_err());
        assert!(validate_multiaddr("http://example.com").is_err());
    }

    #[test]
    fn test_validate_multiaddr_whitespace() {
        // Empty and whitespace-only should be rejected
        assert!(validate_multiaddr("   ").is_err());
        assert!(validate_multiaddr("\t").is_err());
        assert!(validate_multiaddr("\n").is_err());
    }

    #[test]
    fn test_validate_collection_name_valid() {
        assert!(validate_collection_name("Users").is_ok());
        assert!(validate_collection_name("_private").is_ok());
        assert!(validate_collection_name("Collection123").is_ok());
    }

    #[test]
    fn test_validate_collection_name_invalid() {
        assert!(validate_collection_name("").is_err());
        assert!(validate_collection_name("123Collection").is_err());
        assert!(validate_collection_name("User-Name").is_err());
    }

    #[test]
    fn test_validate_doc_id_valid() {
        assert!(validate_doc_id("bae-3fc941b7-505c-5ce2-91a0-b180930ec8a9").is_ok());
        assert!(validate_doc_id("bae-abcd1234").is_ok());
        assert!(validate_doc_id("bae-0123456789abcdef").is_ok());
    }

    #[test]
    fn test_validate_doc_id_invalid() {
        assert!(validate_doc_id("").is_err());
        assert!(validate_doc_id("   ").is_err());
        assert!(validate_doc_id("invalid-id").is_err());
        assert!(validate_doc_id("bae-").is_err());
        assert!(validate_doc_id("bae-GHIJK").is_err()); // G-Z are not hex
        assert!(validate_doc_id("BAE-abc").is_err()); // Must be lowercase bae-
    }
}
