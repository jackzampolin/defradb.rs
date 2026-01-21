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
        return Err(HttpError::BadRequest(
            "address cannot be empty".to_string(),
        ));
    }

    if !address.starts_with('/') {
        return Err(HttpError::BadRequest(format!(
            "invalid multiaddr '{}': must start with '/' (e.g., /ip4/127.0.0.1/tcp/9000/p2p/...)",
            address
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
    }

    #[test]
    fn test_validate_identifier_invalid() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("123Users").is_err());
        assert!(validate_identifier("User-Name").is_err());
        assert!(validate_identifier("User Name").is_err());
    }

    #[test]
    fn test_validate_multiaddr_valid() {
        assert!(validate_multiaddr("/ip4/127.0.0.1/tcp/9000").is_ok());
        assert!(validate_multiaddr("/ip4/127.0.0.1/tcp/9000/p2p/12D3KooWtest").is_ok());
    }

    #[test]
    fn test_validate_multiaddr_invalid() {
        assert!(validate_multiaddr("").is_err());
        assert!(validate_multiaddr("192.168.1.1").is_err());
        assert!(validate_multiaddr("localhost:9000").is_err());
    }
}
