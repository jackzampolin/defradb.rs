//! Encrypted index command implementation

use clap::{Args, Subcommand};

use super::http_client::HttpClient;
use super::{validate_identifier, ClientContext};
use crate::error::Result;

/// Manage encrypted indexes
#[derive(Args, Debug)]
pub struct EncryptedIndexArgs {
    #[command(subcommand)]
    pub command: EncryptedIndexCommand,
}

/// Encrypted index subcommands
#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum EncryptedIndexCommand {
    /// Make a new encrypted index on a collection field
    New(EncryptedIndexNewArgs),
    /// Delete an encrypted index from a collection field
    Delete(EncryptedIndexDeleteArgs),
    /// List encrypted indexes for a collection
    List(EncryptedIndexListArgs),
}

/// Arguments for encrypted-index new command
#[derive(Args, Debug)]
pub struct EncryptedIndexNewArgs {
    /// Collection name
    #[arg(long, short = 'c')]
    pub collection: String,

    /// Field name to make a new encrypted index on
    #[arg(long)]
    pub field: String,
}

/// Arguments for encrypted-index delete command
#[derive(Args, Debug)]
pub struct EncryptedIndexDeleteArgs {
    /// Collection name
    #[arg(long, short = 'c')]
    pub collection: String,

    /// Field name to delete encrypted index from
    #[arg(long)]
    pub field: String,
}

/// Arguments for encrypted-index list command
#[derive(Args, Debug)]
pub struct EncryptedIndexListArgs {
    /// Collection name
    #[arg(long, short = 'c')]
    pub collection: String,
}

impl EncryptedIndexArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            EncryptedIndexCommand::New(args) => args.execute(ctx).await,
            EncryptedIndexCommand::Delete(args) => args.execute(ctx).await,
            EncryptedIndexCommand::List(args) => args.execute(ctx).await,
        }
    }
}

impl EncryptedIndexNewArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        validate_identifier(&self.collection)?;
        validate_identifier(&self.field)?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let response = client
            .encrypted_index_add(&self.collection, &self.field)
            .await?;

        println!("{}", serde_json::to_string_pretty(&response)?);
        Ok(())
    }
}

impl EncryptedIndexDeleteArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        validate_identifier(&self.collection)?;
        validate_identifier(&self.field)?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client
            .encrypted_index_delete(&self.collection, &self.field)
            .await?;

        println!(
            "Encrypted index on field '{}' deleted from collection '{}'",
            self.field, self.collection
        );
        Ok(())
    }
}

impl EncryptedIndexListArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        validate_identifier(&self.collection)?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let indexes = client.encrypted_index_list(&self.collection).await?;
        println!("{}", serde_json::to_string_pretty(&indexes)?);
        Ok(())
    }
}
