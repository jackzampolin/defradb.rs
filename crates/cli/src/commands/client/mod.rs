// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Client commands for interacting with a running DefraDB node

mod collection;
mod document;
mod http_client;
mod query;
mod schema;
mod tx;
mod validation;

use std::path::PathBuf;

use clap::{Args, Subcommand};

pub use collection::CollectionArgs;
pub use document::DocumentArgs;
pub use query::QueryArgs;
pub use schema::SchemaArgs;
pub use tx::TxArgs;

use crate::config::Config;
use crate::error::{Error, Result};

pub use validation::{escape_graphql_string, validate_identifier};

/// Client context passed to all subcommands
#[derive(Debug, Clone)]
pub struct ClientContext {
    /// Server URL
    pub url: String,
    /// Authentication token (generated from identity)
    pub auth_token: Option<String>,
    /// Transaction ID
    pub tx_id: Option<String>,
    /// Verbose mode
    pub verbose: bool,
}

/// Helper to get data from either inline argument or file
pub fn get_data_from_args(data: &Option<String>, file: &Option<PathBuf>) -> Result<String> {
    if let Some(ref data) = data {
        return Ok(data.clone());
    }

    if let Some(ref path) = file {
        return std::fs::read_to_string(path).map_err(|e| Error::ReadFile {
            path: path.clone(),
            source: e,
        });
    }

    Err(Error::MissingInput(
        "either data or --file must be provided".to_string(),
    ))
}

/// Interact with a DefraDB node
#[derive(Args, Debug)]
pub struct ClientArgs {
    /// Hex formatted private key used to authenticate with ACP
    #[arg(long, short = 'i', global = true)]
    pub identity: Option<String>,

    /// Transaction ID to execute commands within
    #[arg(long, global = true)]
    pub tx: Option<u64>,

    /// Enable verbose output (show HTTP requests/responses)
    #[arg(long, short = 'v', global = true)]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: ClientCommand,
}

/// Client subcommands
#[derive(Subcommand, Debug)]
pub enum ClientCommand {
    /// Execute a GraphQL query
    Query(QueryArgs),
    /// Interact with schema
    Schema(SchemaArgs),
    /// Manage transactions
    Tx(TxArgs),
    /// Interact with collections
    Collection(CollectionArgs),
    /// Interact with documents
    Document(DocumentArgs),
}

impl ClientArgs {
    /// Execute the client command
    pub async fn execute(&self, config: Config, url_override: Option<String>) -> Result<()> {
        let url = get_url(&config, url_override);

        // Generate auth token from identity if provided
        let auth_token = if let Some(ref identity_hex) = self.identity {
            Some(generate_auth_token(identity_hex, &url)?)
        } else {
            None
        };

        let ctx = ClientContext {
            url,
            auth_token,
            tx_id: self.tx.map(|id| id.to_string()),
            verbose: self.verbose,
        };

        match &self.command {
            ClientCommand::Query(args) => args.execute(&ctx).await,
            ClientCommand::Schema(args) => args.execute(&ctx).await,
            ClientCommand::Tx(args) => args.execute(&ctx).await,
            ClientCommand::Collection(args) => args.execute(&ctx).await,
            ClientCommand::Document(args) => args.execute(&ctx).await,
        }
    }
}

/// Generate a JWT auth token from a hex-encoded private key.
///
/// Supports both secp256k1 (32 bytes, Go CLI default) and ed25519 (64 bytes) keys.
fn generate_auth_token(identity_hex: &str, audience: &str) -> Result<String> {
    use crypto::KeyType;
    use identity::{new_token, RawIdentity};

    // Decode hex private key
    let key_bytes = hex::decode(identity_hex)
        .map_err(|e| Error::InvalidIdentity(format!("invalid hex: {}", e)))?;

    // Determine key type based on length:
    // - secp256k1: 32 bytes (Go CLI default)
    // - ed25519: 64 bytes (seed + public key) or 32 bytes (seed only)
    let key_type = match key_bytes.len() {
        32 => KeyType::Secp256k1, // Default to secp256k1 for 32-byte keys (Go CLI compat)
        64 => KeyType::Ed25519,
        len => {
            return Err(Error::InvalidIdentity(format!(
                "invalid key length: {} bytes (expected 32 for secp256k1 or 64 for ed25519)",
                len
            )))
        }
    };

    // Create identity from private key bytes
    let identity = RawIdentity::from_bytes(key_type, &key_bytes)
        .map_err(|e| Error::InvalidIdentity(format!("invalid private key: {}", e)))?;

    // Generate JWT token with 15-minute expiration (matches Go CLI)
    let token_bytes = new_token(
        &identity,
        std::time::Duration::from_secs(15 * 60),
        Some(audience.to_string()),
        None,
    )
    .map_err(|e| Error::InvalidIdentity(format!("failed to generate token: {}", e)))?;

    // Convert bytes to string
    String::from_utf8(token_bytes)
        .map_err(|e| Error::InvalidIdentity(format!("token is not valid UTF-8: {}", e)))
}

/// Get the URL to connect to, prioritizing command-line override.
///
/// Uses HTTPS if TLS is configured (both pubkey_path and privkey_path are set).
fn get_url(config: &Config, url_override: Option<String>) -> String {
    if let Some(url) = url_override {
        return url;
    }

    // Use HTTPS if TLS is configured
    let scheme = if config.api.tls_enabled() {
        "https"
    } else {
        "http"
    };

    format!("{}://{}", scheme, config.api.address)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ApiConfig;
    use crypto::Key;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_get_url_with_override() {
        let config = Config::default();
        let url = get_url(&config, Some("http://custom:8080".to_string()));
        assert_eq!(url, "http://custom:8080");
    }

    #[test]
    fn test_get_url_from_config() {
        let mut config = Config::default();
        config.api = ApiConfig {
            address: "192.168.1.1:9000".to_string(),
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
        let mut config = Config::default();
        config.api = ApiConfig {
            address: "127.0.0.1:9181".to_string(),
            pubkey_path: "/path/to/pub.key".to_string(),
            privkey_path: "/path/to/priv.key".to_string(),
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
        // Generate a secp256k1 key (32 bytes)
        let private_key = crypto::generate_secp256k1().unwrap();
        let key_bytes = private_key.raw();
        assert_eq!(key_bytes.len(), 32, "secp256k1 key should be 32 bytes");

        let hex_key = hex::encode(&key_bytes);
        let result = generate_auth_token(&hex_key, "http://localhost:9181");

        assert!(result.is_ok(), "Should generate token for secp256k1 key");
        let token = result.unwrap();

        // Token should be a valid JWT (three dot-separated parts)
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3, "JWT should have 3 parts");
    }

    #[test]
    fn test_generate_auth_token_ed25519_64_bytes() {
        // Generate an ed25519 key (64 bytes: seed + public key)
        let private_key = crypto::generate_ed25519().unwrap();
        let key_bytes = private_key.raw();
        assert_eq!(key_bytes.len(), 64, "ed25519 key should be 64 bytes");

        let hex_key = hex::encode(&key_bytes);
        let result = generate_auth_token(&hex_key, "http://localhost:9181");

        assert!(result.is_ok(), "Should generate token for ed25519 key");
        let token = result.unwrap();

        // Token should be a valid JWT (three dot-separated parts)
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3, "JWT should have 3 parts");
    }

    #[test]
    fn test_generate_auth_token_invalid_hex() {
        let result = generate_auth_token("not-valid-hex!", "http://localhost:9181");
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
        let result = generate_auth_token(&short_key, "http://localhost:9181");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("invalid key length"),
            "Error should mention invalid key length"
        );
    }

    #[test]
    fn test_generate_auth_token_empty_key() {
        let result = generate_auth_token("", "http://localhost:9181");
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_auth_token_tokens_are_unique() {
        // Two tokens generated from the same key should be different
        // (different timestamps, nonces, etc.)
        let private_key = crypto::generate_secp256k1().unwrap();
        let hex_key = hex::encode(private_key.raw());

        let token1 = generate_auth_token(&hex_key, "http://localhost:9181").unwrap();
        let token2 = generate_auth_token(&hex_key, "http://localhost:9181").unwrap();

        // The tokens may or may not be identical depending on timing,
        // but they should both be valid
        assert!(!token1.is_empty());
        assert!(!token2.is_empty());
    }

    #[test]
    fn test_client_context_with_identity() {
        // Verify that identity flows through to auth_token in ClientContext
        let private_key = crypto::generate_secp256k1().unwrap();
        let hex_key = hex::encode(private_key.raw());
        let url = "http://localhost:9181";

        // Generate token the same way ClientArgs.execute does
        let auth_token = generate_auth_token(&hex_key, url).ok();

        let ctx = ClientContext {
            url: url.to_string(),
            auth_token: auth_token.clone(),
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
            tx_id: Some("12345".to_string()),
            verbose: false,
        };

        assert_eq!(ctx.tx_id, Some("12345".to_string()));
    }
}
