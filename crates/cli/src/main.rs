// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! DefraDB CLI - Command-line interface for DefraDB
//!
//! This binary provides the `defra` command for interacting with DefraDB nodes.
//! It supports starting nodes, managing schemas, querying data, and more.

mod cli;
mod commands;
mod config;
mod error;
mod logging;

use std::process::ExitCode;

use clap::Parser;
use tracing::error;

use crate::cli::Cli;
use crate::config::Config;
use crate::error::Result;

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
    logging::init(&config)?;

    // Execute the command
    cli.execute(config).await
}
