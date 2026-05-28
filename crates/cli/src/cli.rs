//! Root CLI definition with global flags

use clap::{Parser, Subcommand};

use crate::commands::{
    ClientArgs, IdentityArgs, KeyringArgs, SdlArgs, ServerDumpArgs, StartArgs, VersionArgs,
};
use crate::config::Config;
use crate::error::Result;

/// DefraDB Edge Database
///
/// DefraDB is the edge database to power the user-centric future.
/// Start a DefraDB node, interact with a local or remote node, and much more.
#[derive(Parser, Debug)]
#[command(name = "defradb")]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Directory for persistent data (default: $HOME/.defradb)
    #[arg(long, global = true, env = "DEFRA_ROOTDIR")]
    pub rootdir: Option<String>,

    /// Log level to use. Options are debug, info, error, fatal
    #[arg(long, global = true, env = "DEFRA_LOG_LEVEL")]
    pub log_level: Option<String>,

    /// Log output path. Options are stderr or stdout
    #[arg(long, global = true, env = "DEFRA_LOG_OUTPUT")]
    pub log_output: Option<String>,

    /// Log format to use. Options are text or json
    #[arg(long, global = true, env = "DEFRA_LOG_FORMAT")]
    pub log_format: Option<String>,

    /// Include stacktrace in error and fatal logs
    #[arg(long, global = true, env = "DEFRA_LOG_STACKTRACE", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub log_stacktrace: Option<bool>,

    /// Include source location in logs
    #[arg(long, global = true, env = "DEFRA_LOG_SOURCE", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub log_source: Option<bool>,

    /// Logger config overrides. Format <name>,<key>=<val>,...;<name>,...
    #[arg(long, global = true, env = "DEFRA_LOG_OVERRIDES")]
    pub log_overrides: Option<String>,

    /// Disable colored log output
    #[arg(long, global = true, env = "DEFRA_NO_LOG_COLOR", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub no_log_color: Option<bool>,

    /// URL of HTTP endpoint to listen on or connect to
    #[arg(long, global = true, env = "DEFRA_API_ADDRESS")]
    pub url: Option<String>,

    /// Service name to use when using the system backend
    #[arg(long, global = true, env = "DEFRA_KEYRING_NAMESPACE")]
    pub keyring_namespace: Option<String>,

    /// Keyring backend to use. Options are file or system
    #[arg(long, global = true, env = "DEFRA_KEYRING_BACKEND")]
    pub keyring_backend: Option<String>,

    /// Path to store encrypted keys when using the file backend
    #[arg(long, global = true, env = "DEFRA_KEYRING_PATH")]
    pub keyring_path: Option<String>,

    /// Disable the keyring and generate ephemeral keys
    #[arg(long, global = true, env = "DEFRA_NO_KEYRING", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub no_keyring: Option<bool>,

    /// The SourceHub address authorized by the client to make SourceHub transactions
    #[arg(long, global = true, env = "DEFRA_SOURCE_HUB_ADDRESS")]
    pub source_hub_address: Option<String>,

    /// SourceHub CometBFT RPC address for transaction broadcast
    #[arg(long, global = true, env = "DEFRA_SOURCE_HUB_COMET_ADDRESS")]
    pub source_hub_comet_address: Option<String>,

    /// SourceHub chain ID (e.g., "sourcehub-test")
    #[arg(long, global = true, env = "DEFRA_SOURCE_HUB_CHAIN_ID")]
    pub source_hub_chain_id: Option<String>,

    /// hub.rs JSON-RPC endpoint (e.g., "http://localhost:8545")
    #[arg(long, global = true, env = "DEFRA_HUB_RS_ADDRESS")]
    pub hub_rs_address: Option<String>,

    /// Path to the file containing secrets
    #[arg(long, global = true, env = "DEFRA_SECRET_FILE")]
    pub secret_file: Option<String>,

    /// Disable OpenTelemetry exporters (no-op unless the binary was built with `--features otel`)
    #[arg(long, global = true, env = "DEFRA_NO_TELEMETRY", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub no_telemetry: Option<bool>,

    /// Enables development mode features
    #[arg(long, global = true, env = "DEFRA_DEVELOPMENT", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub development: Option<bool>,

    /// Enable Node Access Control (NAC).
    ///
    /// When enabled, node operations require authentication and authorization
    /// based on the node's identity from the keyring.
    #[arg(long = "node-acp-enable", global = true, env = "DEFRA_ACP_NODE_ENABLE", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub acp_node_enable: Option<bool>,

    /// Document ACP type. Options are none, local, source-hub, or hub-rs
    #[arg(
        long = "document-acp-type",
        global = true,
        env = "DEFRA_ACP_DOCUMENT_TYPE"
    )]
    pub acp_document_type: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

/// Available subcommands
#[derive(Subcommand, Debug)]
#[allow(clippy::large_enum_variant)]
#[non_exhaustive]
pub enum Command {
    /// Start a DefraDB node
    Start(StartArgs),

    /// Display the version information of DefraDB and its components
    Version(VersionArgs),

    /// Manage keys in the keyring
    Keyring(KeyringArgs),

    /// Interact with a DefraDB node
    Client(ClientArgs),

    /// Manage identities
    Identity(IdentityArgs),

    /// Manage SDL (Schema Definition Language)
    Sdl(SdlArgs),

    /// Dump server-side data
    ServerDump(ServerDumpArgs),
}

impl Cli {
    /// Execute the CLI command
    pub async fn execute(self, config: Config) -> Result<()> {
        match self.command {
            Command::Start(args) => args.execute(config).await,
            Command::Version(args) => args.execute(),
            Command::Keyring(args) => args.execute(config),
            Command::Client(args) => args.execute(config, self.url).await,
            Command::Identity(args) => args.execute(config),
            Command::Sdl(args) => args.execute(),
            Command::ServerDump(args) => args.execute(&config).await,
        }
    }
}
