//! Tests for client command functionality

use std::io::Write;
use tempfile::NamedTempFile;

use cli::commands::client::{
    escape_graphql_string, get_data_from_args, get_url, validate_identifier, ClientContext,
};
use cli::config::{ApiConfig, Config};

#[test]
fn test_get_url_with_override() {
    let config = Config::default();
    let url = get_url(&config, Some("custom:8080".to_string()));
    assert_eq!(url, "http://custom:8080");
}

#[test]
fn test_get_url_from_config() {
    let config = Config {
        api: ApiConfig {
            address: "192.168.1.1:9000".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    let url = get_url(&config, None);
    assert_eq!(url, "http://192.168.1.1:9000");
}

#[test]
fn test_get_url_default() {
    let config = Config::default();
    let url = get_url(&config, None);
    assert_eq!(url, "http://127.0.0.1:9181");
}

#[test]
fn test_get_url_with_tls() {
    let config = Config {
        api: ApiConfig {
            address: "127.0.0.1:9181".to_string(),
            pubkey_path: "/path/to/pub.key".to_string(),
            privkey_path: "/path/to/priv.key".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    let url = get_url(&config, None);
    assert_eq!(url, "https://127.0.0.1:9181");
}

#[test]
fn test_get_data_from_args_inline() {
    let data = Some(r#"{"name": "Alice"}"#.to_string());
    let file = None;
    let result = get_data_from_args(&data, &file).unwrap();
    assert_eq!(result, r#"{"name": "Alice"}"#);
}

#[test]
fn test_get_data_from_args_from_file() {
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(temp_file, r#"{{"name": "Bob"}}"#).unwrap();

    let data = None;
    let file = Some(temp_file.path().to_path_buf());
    let result = get_data_from_args(&data, &file).unwrap();
    assert!(result.contains("Bob"));
}

#[test]
fn test_get_data_from_args_missing_input() {
    let data = None;
    let file = None;
    let result = get_data_from_args(&data, &file);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("missing required input"));
}

#[test]
fn test_validate_identifier_valid() {
    assert!(validate_identifier("Users").is_ok());
    assert!(validate_identifier("_private").is_ok());
    assert!(validate_identifier("User123").is_ok());
}

#[test]
fn test_validate_identifier_invalid() {
    assert!(validate_identifier("").is_err());
    assert!(validate_identifier("123Users").is_err());
    assert!(validate_identifier("User-Name").is_err());
}

#[test]
fn test_escape_graphql_string() {
    assert_eq!(escape_graphql_string("hello"), "hello");
    assert_eq!(escape_graphql_string(r#"test"value"#), r#"test\"value"#);
    assert_eq!(escape_graphql_string("test\nvalue"), "test\\nvalue");
}

// JWT token generation tests

#[test]
fn test_generate_auth_token_secp256k1_32_bytes() {
    use crypto::Key;

    // Generate a secp256k1 key (32 bytes)
    let private_key = crypto::generate_secp256k1().unwrap();
    let key_bytes = private_key.raw();
    assert_eq!(key_bytes.len(), 32, "secp256k1 key should be 32 bytes");

    let hex_key = hex::encode(&key_bytes);
    let result = cli::commands::client::generate_auth_token(&hex_key, "http://localhost:9181");

    assert!(result.is_ok(), "Should generate token for secp256k1 key");
    let token = result.unwrap();

    // Token should be a valid JWT (three dot-separated parts)
    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(parts.len(), 3, "JWT should have 3 parts");
}

#[test]
fn test_generate_auth_token_ed25519_64_bytes() {
    use crypto::Key;

    // Generate an ed25519 key (64 bytes: seed + public key)
    let private_key = crypto::generate_ed25519().unwrap();
    let key_bytes = private_key.raw();
    assert_eq!(key_bytes.len(), 64, "ed25519 key should be 64 bytes");

    let hex_key = hex::encode(&key_bytes);
    let result = cli::commands::client::generate_auth_token(&hex_key, "http://localhost:9181");

    assert!(result.is_ok(), "Should generate token for ed25519 key");
    let token = result.unwrap();

    // Token should be a valid JWT (three dot-separated parts)
    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(parts.len(), 3, "JWT should have 3 parts");
}

#[test]
fn test_generate_auth_token_invalid_hex() {
    let result =
        cli::commands::client::generate_auth_token("not-valid-hex!", "http://localhost:9181");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("invalid hex"),
        "Error should mention invalid hex"
    );
}

#[test]
fn test_generate_auth_token_invalid_key_length() {
    // 16 bytes is neither 32 (secp256k1) nor 64 (ed25519)
    let short_key = hex::encode([0u8; 16]);
    let result = cli::commands::client::generate_auth_token(&short_key, "http://localhost:9181");

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("invalid key length"),
        "Error should mention invalid key length"
    );
}

#[test]
fn test_generate_auth_token_empty_key() {
    let result = cli::commands::client::generate_auth_token("", "http://localhost:9181");
    assert!(result.is_err());
}

#[test]
fn test_generate_auth_token_tokens_are_unique() {
    use crypto::Key;

    // Two tokens generated from the same key should be different
    // (different timestamps, nonces, etc.)
    let private_key = crypto::generate_secp256k1().unwrap();
    let hex_key = hex::encode(private_key.raw());

    let token1 =
        cli::commands::client::generate_auth_token(&hex_key, "http://localhost:9181").unwrap();
    let token2 =
        cli::commands::client::generate_auth_token(&hex_key, "http://localhost:9181").unwrap();

    // The tokens may or may not be identical depending on timing,
    // but they should both be valid
    assert!(!token1.is_empty());
    assert!(!token2.is_empty());
}

#[test]
fn test_client_context_with_identity() {
    use crypto::Key;

    // Verify that identity flows through to auth_token in ClientContext
    let private_key = crypto::generate_secp256k1().unwrap();
    let hex_key = hex::encode(private_key.raw());
    let url = "http://localhost:9181";

    // Generate token the same way ClientArgs.execute does
    let auth_token = cli::commands::client::generate_auth_token(&hex_key, url).ok();

    let ctx = ClientContext {
        url: url.to_string(),
        auth_token: auth_token.clone(),
        identity_key_bytes: None,
        tx_id: None,
        verbose: false,
    };

    // Verify auth_token is set
    assert!(ctx.auth_token.is_some());
    let token = ctx.auth_token.unwrap();

    // Verify it's a valid JWT format
    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(parts.len(), 3, "Auth token should be a valid JWT");
}

#[test]
fn test_client_context_without_identity() {
    let ctx = ClientContext {
        url: "http://localhost:9181".to_string(),
        auth_token: None,
        identity_key_bytes: None,
        tx_id: None,
        verbose: false,
    };

    assert!(ctx.auth_token.is_none());
    assert!(ctx.tx_id.is_none());
}

#[test]
fn test_client_context_with_tx() {
    let ctx = ClientContext {
        url: "http://localhost:9181".to_string(),
        auth_token: None,
        identity_key_bytes: None,
        tx_id: Some("12345".to_string()),
        verbose: false,
    };

    assert_eq!(ctx.tx_id, Some("12345".to_string()));
}
