//! Client commands for interacting with a running DefraDB node

mod acp;
mod backup;
mod block;
mod collection;
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
    /// Interact with collections and documents
    Collection(CollectionArgs),
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

        // Generate auth token from identity (hex key or keyring name)
        let auth_token = if let Some(ref identity_hex) = self.identity {
            Some(generate_auth_token(identity_hex, &url)?)
        } else if let Some(ref name) = self.identity_name {
            Some(generate_auth_token_from_keyring(&config, name, &url)?)
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
            ClientCommand::Block(args) => args.execute(&ctx).await,
            ClientCommand::Collection(args) => args.execute(&ctx).await,
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

    // The audience must be bare host:port (matches Go's req.Host behavior)
    let audience_host = strip_url_scheme(audience);

    // Generate JWT token with 15-minute expiration (matches Go CLI)
    let token_bytes = new_token(
        &identity,
        std::time::Duration::from_secs(15 * 60),
        Some(audience_host.to_string()),
        None,
    )
    .map_err(|e| Error::InvalidIdentity(format!("failed to generate token: {}", e)))?;

    // Convert bytes to string
    String::from_utf8(token_bytes)
        .map_err(|e| Error::InvalidIdentity(format!("token is not valid UTF-8: {}", e)))
}

/// Generate a JWT auth token from a named key in the keyring.
fn generate_auth_token_from_keyring(config: &Config, name: &str, audience: &str) -> Result<String> {
    use crate::config::KeyringBackend;
    use crypto::KeyType;
    use identity::{new_token, RawIdentity};
    use std::path::PathBuf;

    if config.keyring.disabled {
        return Err(Error::Keyring("keyring is disabled".to_string()));
    }

    let keyring: Box<dyn keyring::Keyring> = match config.keyring.backend {
        KeyringBackend::File => {
            let path = {
                let p = PathBuf::from(&config.keyring.path);
                if p.is_absolute() {
                    p
                } else {
                    config.rootdir.join(p)
                }
            };
            let secret =
                keyring::load_secret_from_env().map_err(|e| Error::Keyring(e.to_string()))?;
            let kr = keyring::FileKeyring::open(&path, secret)
                .map_err(|e| Error::Keyring(e.to_string()))?;
            Box::new(kr)
        }
        KeyringBackend::System => {
            let kr = keyring::SystemKeyring::open(&config.keyring.namespace);
            Box::new(kr)
        }
    };

    let key_bytes = keyring
        .get(name)
        .map_err(|e| Error::Keyring(e.to_string()))?;

    let key_type = match key_bytes.len() {
        32 => KeyType::Secp256k1,
        64 => KeyType::Ed25519,
        len => {
            return Err(Error::InvalidIdentity(format!(
            "key '{}' has invalid length: {} bytes (expected 32 for secp256k1 or 64 for ed25519)",
            name, len
        )))
        }
    };

    let identity = RawIdentity::from_bytes(key_type, &key_bytes)
        .map_err(|e| Error::InvalidIdentity(format!("invalid key '{}': {}", name, e)))?;

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
