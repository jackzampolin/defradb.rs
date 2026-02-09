//! Encrypted index command implementation

use clap::{Args, Subcommand};

use super::ClientContext;
use crate::error::Result;

/// Manage encrypted indexes
#[derive(Args, Debug)]
pub struct EncryptedIndexArgs {
    #[command(subcommand)]
    pub command: EncryptedIndexCommand,
}

/// Encrypted index subcommands
#[derive(Subcommand, Debug)]
pub enum EncryptedIndexCommand {
    /// Create an encrypted index
    Create(EncryptedIndexCreateArgs),
    /// Delete an encrypted index
    Delete(EncryptedIndexDeleteArgs),
    /// List encrypted indexes
    List(EncryptedIndexListArgs),
}

/// Arguments for encrypted-index create command
#[derive(Args, Debug)]
pub struct EncryptedIndexCreateArgs {}

/// Arguments for encrypted-index delete command
#[derive(Args, Debug)]
pub struct EncryptedIndexDeleteArgs {}

/// Arguments for encrypted-index list command
#[derive(Args, Debug)]
pub struct EncryptedIndexListArgs {}

impl EncryptedIndexArgs {
    pub async fn execute(&self, _ctx: &ClientContext) -> Result<()> {
        match &self.command {
            EncryptedIndexCommand::Create(args) => args.execute().await,
            EncryptedIndexCommand::Delete(args) => args.execute().await,
            EncryptedIndexCommand::List(args) => args.execute().await,
        }
    }
}

impl EncryptedIndexCreateArgs {
    pub async fn execute(&self) -> Result<()> {
        eprintln!("not yet implemented");
        Ok(())
    }
}

impl EncryptedIndexDeleteArgs {
    pub async fn execute(&self) -> Result<()> {
        eprintln!("not yet implemented");
        Ok(())
    }
}

impl EncryptedIndexListArgs {
    pub async fn execute(&self) -> Result<()> {
        eprintln!("not yet implemented");
        Ok(())
    }
}
