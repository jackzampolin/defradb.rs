// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Schema command implementation

use clap::{Args, Subcommand};

use super::http_client::HttpClient;
use crate::error::Result;

/// Interact with schema
#[derive(Args, Debug)]
pub struct SchemaArgs {
    #[command(subcommand)]
    pub command: SchemaCommand,
}

/// Schema subcommands
#[derive(Subcommand, Debug)]
pub enum SchemaCommand {
    /// Display the full GraphQL schema
    Describe(SchemaDescribeArgs),
}

/// Arguments for schema describe command
#[derive(Args, Debug)]
pub struct SchemaDescribeArgs {}

impl SchemaArgs {
    /// Execute the schema command
    pub async fn execute(&self, url: &str) -> Result<()> {
        match &self.command {
            SchemaCommand::Describe(args) => args.execute(url).await,
        }
    }
}

impl SchemaDescribeArgs {
    /// Execute the schema describe command
    pub async fn execute(&self, url: &str) -> Result<()> {
        let client = HttpClient::new(url)?;
        let schema = client.schema().await?;
        println!("{schema}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_describe_args() {
        let args = SchemaDescribeArgs {};
        // Just verify it can be created
        let _ = args;
    }
}
