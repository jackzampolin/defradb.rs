//! DefraDB CLI - Command-line interface for DefraDB
//!
//! This binary provides the `defra` command for interacting with DefraDB nodes.
//! It supports starting nodes, managing schemas, querying data, and more.

use std::process::ExitCode;

use clap::Parser;
use tracing::error;

use cli::cli::{Cli, Command};
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

    // Initialize logging based on config. Telemetry only initializes for the
    // `start` command (matching Go, which configures it only in start.go) so
    // ephemeral commands like `version` / `client` don't spin up the exporter.
    let logging = cli::logging::init(&config, should_profile(&cli), enable_telemetry(&cli))?;

    // Execute the command
    let result = cli.execute(config).await;
    logging.finish();
    result
}

fn should_profile(cli: &Cli) -> bool {
    matches!(&cli.command, Command::Start(args) if args.profile)
}

fn enable_telemetry(cli: &Cli) -> bool {
    matches!(&cli.command, Command::Start(_))
}

#[cfg(test)]
#[path = "../tests/unit/main_tests.rs"]
mod tests;
