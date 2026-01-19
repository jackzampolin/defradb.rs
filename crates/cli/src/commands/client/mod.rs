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
        match &self.command {
            ClientCommand::Query(args) => args.execute(&url).await,
            ClientCommand::Schema(args) => args.execute(&url).await,
            ClientCommand::Tx(args) => args.execute(&url).await,
            ClientCommand::Collection(args) => args.execute(&url).await,
            ClientCommand::Document(args) => args.execute(&url).await,
        }
    }
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
}
