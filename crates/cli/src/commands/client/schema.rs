//! Schema command implementation

use std::path::PathBuf;

use clap::{Args, Subcommand};

use super::http_client::HttpClient;
use super::ClientContext;
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
    /// Add a schema
    Add(SchemaAddArgs),
    /// Display the full GraphQL schema
    Describe(SchemaDescribeArgs),
}

/// Arguments for schema add command
#[derive(Args, Debug)]
pub struct SchemaAddArgs {
    /// The schema definition (SDL format)
    #[arg(value_name = "SCHEMA")]
    pub schema: Option<String>,

    /// Read schema from file(s)
    #[arg(long, short = 'f', value_name = "FILE")]
    pub file: Vec<PathBuf>,
}

/// Arguments for schema describe command
#[derive(Args, Debug)]
pub struct SchemaDescribeArgs {}

impl SchemaArgs {
    /// Execute the schema command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            SchemaCommand::Add(args) => args.execute(ctx).await,
            SchemaCommand::Describe(args) => args.execute(ctx).await,
        }
    }
}

impl SchemaAddArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let mut sdl_parts = Vec::new();

        if let Some(ref schema) = self.schema {
            sdl_parts.push(schema.clone());
        }

        for path in &self.file {
            let content =
                std::fs::read_to_string(path).map_err(|e| crate::error::Error::ReadFile {
                    path: path.clone(),
                    source: e,
                })?;
            sdl_parts.push(content);
        }

        if sdl_parts.is_empty() {
            return Err(crate::error::Error::MissingInput(
                "either SCHEMA argument or --file must be provided".to_string(),
            ));
        }

        let sdl = sdl_parts.join("\n");
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.schema_add(&sdl).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl SchemaDescribeArgs {
    /// Execute the schema describe command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);
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
        let _ = args;
    }
}
