//! Lens migration command implementation

use std::path::PathBuf;

use clap::{Args, Subcommand};

use super::http_client::HttpClient;
use super::{get_data_from_args, ClientContext};
use crate::error::Result;

/// Interact with Lens schema migrations
#[derive(Args, Debug)]
pub struct LensArgs {
    #[command(subcommand)]
    pub command: LensCommand,
}

/// Lens subcommands
#[derive(Subcommand, Debug)]
pub enum LensCommand {
    /// Add a lens migration
    Add(LensAddArgs),
    /// List lens migrations
    List(LensListArgs),
    /// Reload all lens modules
    Reload(LensReloadArgs),
    /// Set a migration between schema versions
    Set(LensSetArgs),
}

/// Arguments for lens add command
#[derive(Args, Debug)]
pub struct LensAddArgs {
    /// The lens configuration (JSON format)
    #[arg(value_name = "CONFIG")]
    pub config: Option<String>,

    /// Read lens configuration from file
    #[arg(long, short = 'f', value_name = "FILE")]
    pub file: Option<PathBuf>,
}

/// Arguments for lens list command
#[derive(Args, Debug)]
pub struct LensListArgs {}

/// Arguments for lens set command
#[derive(Args, Debug)]
pub struct LensSetArgs {
    /// Source schema version ID
    #[arg(value_name = "SRC")]
    pub src: Option<String>,

    /// Destination schema version ID
    #[arg(value_name = "DST")]
    pub dst: Option<String>,

    /// The lens configuration (JSON format)
    #[arg(value_name = "CONFIG")]
    pub config: Option<String>,

    /// Read lens configuration from file
    #[arg(long, short = 'f', value_name = "FILE")]
    pub file: Option<PathBuf>,
}

/// Arguments for lens reload command
#[derive(Args, Debug)]
pub struct LensReloadArgs {}

impl LensArgs {
    /// Execute the lens command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            LensCommand::Add(args) => args.execute(ctx).await,
            LensCommand::List(args) => args.execute(ctx).await,
            LensCommand::Reload(args) => args.execute(ctx).await,
            LensCommand::Set(args) => args.execute(ctx).await,
        }
    }
}

impl LensAddArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let config = get_data_from_args(&self.config, &self.file)?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.lens_add(&config).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl LensListArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.lens_list().await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl LensSetArgs {
    /// Execute the lens set command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let config = get_data_from_args(&self.config, &self.file)?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let response = client.lens_set_migration(&config).await?;
        println!("{}", serde_json::to_string_pretty(&response)?);
        Ok(())
    }
}

impl LensReloadArgs {
    /// Execute the lens reload command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.lens_reload().await?;
        println!("Lens modules reloaded successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lens_set_args_with_positional() {
        let args = LensSetArgs {
            src: Some("v1".to_string()),
            dst: Some("v2".to_string()),
            config: Some(r#"{"Lenses": []}"#.to_string()),
            file: None,
        };
        assert!(args.src.is_some());
        assert!(args.dst.is_some());
        assert!(args.config.is_some());
    }

    #[test]
    fn test_lens_set_args_with_file() {
        let args = LensSetArgs {
            src: None,
            dst: None,
            config: None,
            file: Some(PathBuf::from("migration.json")),
        };
        assert!(args.file.is_some());
    }

    #[test]
    fn test_lens_add_args_with_config() {
        let args = LensAddArgs {
            config: Some(r#"{"module": "test"}"#.to_string()),
            file: None,
        };
        assert!(args.config.is_some());
    }
}
