//! DefraDB CLI - Command-line interface for DefraDB
//!
//! This binary provides the `defra` command for interacting with DefraDB nodes.
//! It supports starting nodes, managing schemas, querying data, and more.

use std::process::ExitCode;

use clap::Parser;
use tracing::error;

use cli::cli::Cli;
use cli::config::Config;
use cli::error::Result;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e}");
            eprintln!("Error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    // Load configuration (flags → env → config file → defaults)
    let config = Config::load(&cli)?;

    // Initialize logging based on config
    cli::logging::init(&config)?;

    // Execute the command
    cli.execute(config).await
}
