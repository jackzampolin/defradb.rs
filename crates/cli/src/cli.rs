// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Root CLI definition with global flags

use clap::{Parser, Subcommand};

use crate::commands::{StartArgs, VersionArgs};
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
    #[arg(long, global = true, env = "DEFRA_LOG_STACKTRACE")]
    pub log_stacktrace: Option<bool>,

    /// Include source location in logs
    #[arg(long, global = true, env = "DEFRA_LOG_SOURCE")]
    pub log_source: Option<bool>,

    /// Logger config overrides. Format <name>,<key>=<val>,...;<name>,...
    #[arg(long, global = true, env = "DEFRA_LOG_OVERRIDES")]
    pub log_overrides: Option<String>,

    /// Disable colored log output
    #[arg(long, global = true, env = "DEFRA_NO_LOG_COLOR")]
    pub no_log_color: Option<bool>,

    /// URL of HTTP endpoint to listen on or connect to
    #[arg(long, global = true, env = "DEFRA_URL")]
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
    #[arg(long, global = true, env = "DEFRA_NO_KEYRING")]
    pub no_keyring: Option<bool>,

    /// The SourceHub address authorized by the client to make SourceHub transactions
    #[arg(long, global = true, env = "DEFRA_SOURCE_HUB_ADDRESS")]
    pub source_hub_address: Option<String>,

    /// Path to the file containing secrets
    #[arg(long, global = true, env = "DEFRA_SECRET_FILE")]
    pub secret_file: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

/// Available subcommands
#[derive(Subcommand, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    /// Start a DefraDB node
    Start(StartArgs),

    /// Display the version information of DefraDB and its components
    Version(VersionArgs),
}

impl Cli {
    /// Execute the CLI command
    pub async fn execute(self, config: Config) -> Result<()> {
        match self.command {
            Command::Start(args) => args.execute(config).await,
            Command::Version(args) => args.execute(),
        }
    }
}
