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

use clap::{Args, Subcommand};

pub use collection::CollectionArgs;
pub use document::DocumentArgs;
pub use query::QueryArgs;
pub use schema::SchemaArgs;
pub use tx::TxArgs;

use crate::config::Config;
use crate::error::Result;

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

/// Get the URL to connect to, prioritizing command-line override
fn get_url(config: &Config, url_override: Option<String>) -> String {
    if let Some(url) = url_override {
        return url;
    }

    format!("http://{}", config.api.address)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ApiConfig;

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
}
