//! Client commands for interacting with a running DefraDB node

mod acp;
mod backup;
mod block;
mod collection;
mod document;
mod dump;
mod encrypted_index;
pub mod http_client;
mod index;
mod lens;
mod node_identity;
mod p2p;
mod purge;
mod query;
mod schema;
mod tx;
mod validation;
mod view;

use std::path::PathBuf;

use clap::{Args, Subcommand};

pub use acp::AcpArgs;
pub use backup::BackupArgs;
pub use block::BlockArgs;
pub use collection::CollectionArgs;
pub use document::DocumentArgs;
pub use dump::DumpArgs;
pub use encrypted_index::EncryptedIndexArgs;
pub use index::IndexArgs;
pub use lens::LensArgs;
pub use node_identity::NodeIdentityArgs;
pub use p2p::P2pArgs;
pub use purge::PurgeArgs;
pub use query::QueryArgs;
pub use schema::SchemaArgs;
pub use tx::TxArgs;
pub use view::ViewArgs;

use crate::config::Config;
use crate::error::{Error, Result};

pub use validation::{escape_graphql_string, json_to_graphql_input, validate_identifier};

/// Client context passed to all subcommands
#[derive(Debug, Clone)]
pub struct ClientContext {
    /// Server URL
    pub url: String,
    /// Authentication token (generated from identity)
    pub auth_token: Option<String>,
    /// Raw identity private key bytes, when provided by the caller.
    pub identity_key_bytes: Option<Vec<u8>>,
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

    /// Load identity from keyring by name (alternative to --identity)
    #[arg(long, global = true)]
    pub identity_name: Option<String>,

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
    /// Interact with blocks
    Block(BlockArgs),
    /// Interact with collections
    Collection(CollectionArgs),
    /// Interact with documents
    Document(DocumentArgs),
    /// Dump the database contents
    Dump(DumpArgs),
    /// Manage encrypted indexes
    EncryptedIndex(EncryptedIndexArgs),
    /// Manage database indexes
    Index(IndexArgs),
    /// Manage lens schema migrations
    Lens(LensArgs),
    /// Get the node's identity
    NodeIdentity(NodeIdentityArgs),
    /// Manage P2P network
    P2p(P2pArgs),
    /// Purge all database data
    Purge(PurgeArgs),
    /// Execute a GraphQL query
    Query(QueryArgs),
    /// Interact with schema
    Schema(SchemaArgs),
    /// Manage transactions
    Tx(TxArgs),
    /// Manage views
    View(ViewArgs),
}

impl ClientArgs {
    /// Execute the client command
    pub async fn execute(&self, config: Config, url_override: Option<String>) -> Result<()> {
        let url = get_url(&config, url_override);

        let identity_key_bytes = if let Some(ref identity_hex) = self.identity {
            Some(decode_identity_hex(identity_hex)?)
        } else if let Some(ref name) = self.identity_name {
            Some(load_identity_bytes_from_keyring(&config, name)?)
        } else {
            None
        };

        let auth_token = if let Some(ref key_bytes) = identity_key_bytes {
            let identity_name = self.identity_name.as_deref().unwrap_or("inline identity");
            Some(generate_auth_token_from_key_bytes(
                identity_name,
                key_bytes,
                &url,
            )?)
        } else {
            None
        };

        let ctx = ClientContext {
            url,
            auth_token,
            identity_key_bytes,
            tx_id: self.tx.map(|id| id.to_string()),
            verbose: self.verbose,
        };

        match &self.command {
            ClientCommand::Acp(args) => args.execute(&ctx).await,
            ClientCommand::Backup(args) => args.execute(&ctx).await,
            ClientCommand::Block(args) => args.execute(&ctx).await,
            ClientCommand::Collection(args) => args.execute(&ctx).await,
            ClientCommand::Document(args) => args.execute(&ctx).await,
            ClientCommand::Dump(args) => args.execute(&ctx).await,
            ClientCommand::EncryptedIndex(args) => args.execute(&ctx).await,
            ClientCommand::Index(args) => args.execute(&ctx).await,
            ClientCommand::Lens(args) => args.execute(&ctx).await,
            ClientCommand::NodeIdentity(args) => args.execute(&ctx).await,
            ClientCommand::P2p(args) => args.execute(&ctx).await,
            ClientCommand::Purge(args) => args.execute(&ctx).await,
            ClientCommand::Query(args) => args.execute(&ctx).await,
            ClientCommand::Schema(args) => args.execute(&ctx).await,
            ClientCommand::Tx(args) => args.execute(&ctx).await,
            ClientCommand::View(args) => args.execute(&ctx).await,
        }
    }
}

/// Generate a JWT auth token from a hex-encoded private key.
///
/// Supports both secp256k1 (32 bytes, Go CLI default) and ed25519 (64 bytes) keys.
pub fn generate_auth_token(identity_hex: &str, audience: &str) -> Result<String> {
    let key_bytes = decode_identity_hex(identity_hex)?;
    generate_auth_token_from_key_bytes("inline identity", &key_bytes, audience)
}

fn load_identity_bytes_from_keyring(config: &Config, name: &str) -> Result<Vec<u8>> {
    let keyring = super::open_keyring(config)?;

    keyring
        .get(name)
        .map(|bytes| bytes.to_vec())
        .map_err(|e| Error::Keyring(e.to_string()))
}

fn decode_identity_hex(identity_hex: &str) -> Result<Vec<u8>> {
    hex::decode(identity_hex).map_err(|e| Error::InvalidIdentity(format!("invalid hex: {}", e)))
}

fn key_type_from_identity_bytes(name: &str, key_bytes: &[u8]) -> Result<crypto::KeyType> {
    match key_bytes.len() {
        32 => Ok(crypto::KeyType::Secp256k1),
        64 => Ok(crypto::KeyType::Ed25519),
        len => Err(Error::InvalidIdentity(format!(
            "invalid key length for '{}': {} bytes (expected 32 for secp256k1 or 64 for ed25519)",
            name, len
        ))),
    }
}

pub(crate) fn raw_identity_from_key_bytes(
    name: &str,
    key_bytes: &[u8],
) -> Result<identity::RawIdentity> {
    let key_type = key_type_from_identity_bytes(name, key_bytes)?;
    identity::RawIdentity::from_bytes(key_type, key_bytes)
        .map_err(|e| Error::InvalidIdentity(format!("invalid key '{}': {}", name, e)))
}

fn generate_auth_token_from_key_bytes(
    name: &str,
    key_bytes: &[u8],
    audience: &str,
) -> Result<String> {
    use identity::new_token;

    let identity = raw_identity_from_key_bytes(name, key_bytes)?;

    let audience_host = strip_url_scheme(audience);

    let token_bytes = new_token(
        &identity,
        std::time::Duration::from_secs(15 * 60),
        Some(audience_host.to_string()),
        None,
    )
    .map_err(|e| Error::InvalidIdentity(format!("failed to generate token: {}", e)))?;

    String::from_utf8(token_bytes)
        .map_err(|e| Error::InvalidIdentity(format!("token is not valid UTF-8: {}", e)))
}

/// Strip the URL scheme to get bare host:port for JWT audience.
///
/// The server uses `req.Host` (bare host:port) as the expected audience,
/// matching Go DefraDB behavior.
fn strip_url_scheme(url: &str) -> &str {
    url.strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url)
}

/// Get the URL to connect to, prioritizing command-line override.
///
/// Uses HTTPS if TLS is configured (both pubkey_path and privkey_path are set).
pub fn get_url(config: &Config, url_override: Option<String>) -> String {
    let address = url_override.unwrap_or_else(|| config.api.address.clone());
    let scheme = if config.api.tls_enabled() {
        "https"
    } else {
        "http"
    };
    format!("{}://{}", scheme, address)
}
