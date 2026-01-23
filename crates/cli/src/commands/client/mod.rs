
//! Client commands for interacting with a running DefraDB node

mod acp;
mod backup;
mod collection;
mod document;
pub mod http_client;
mod index;
mod p2p;
mod query;
mod schema;
mod tx;
mod validation;

use std::path::PathBuf;

use clap::{Args, Subcommand};

pub use acp::AcpArgs;
pub use backup::BackupArgs;
pub use collection::CollectionArgs;
pub use document::DocumentArgs;
pub use index::IndexArgs;
pub use p2p::P2pArgs;
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
    /// Interact with Access Control Policies
    Acp(AcpArgs),
    /// Manage database backups
    Backup(BackupArgs),
    /// Interact with collections
    Collection(CollectionArgs),
    /// Interact with documents
    Document(DocumentArgs),
    /// Manage database indexes
    Index(IndexArgs),
    /// Manage P2P network
    P2p(P2pArgs),
    /// Execute a GraphQL query
    Query(QueryArgs),
    /// Interact with schema
    Schema(SchemaArgs),
    /// Manage transactions
    Tx(TxArgs),
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
            ClientCommand::Acp(args) => args.execute(&ctx).await,
            ClientCommand::Backup(args) => args.execute(&ctx).await,
            ClientCommand::Collection(args) => args.execute(&ctx).await,
            ClientCommand::Document(args) => args.execute(&ctx).await,
            ClientCommand::Index(args) => args.execute(&ctx).await,
            ClientCommand::P2p(args) => args.execute(&ctx).await,
            ClientCommand::Query(args) => args.execute(&ctx).await,
            ClientCommand::Schema(args) => args.execute(&ctx).await,
            ClientCommand::Tx(args) => args.execute(&ctx).await,
        }
    }
}

/// Generate a JWT auth token from a hex-encoded private key.
///
/// Supports both secp256k1 (32 bytes, Go CLI default) and ed25519 (64 bytes) keys.
pub fn generate_auth_token(identity_hex: &str, audience: &str) -> Result<String> {
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
pub fn get_url(config: &Config, url_override: Option<String>) -> String {
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
