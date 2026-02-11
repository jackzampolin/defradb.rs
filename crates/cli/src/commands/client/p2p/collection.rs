//! P2P collection command implementations

use clap::{Args, Subcommand};

use crate::commands::client::http_client::HttpClient;
use crate::commands::client::{validate_identifier, ClientContext};
use crate::error::Result;

/// P2P collection management subcommands
#[derive(Args, Debug)]
pub struct P2pCollectionArgs {
    #[command(subcommand)]
    pub command: P2pCollectionCommand,
}

/// P2P collection subcommands
#[derive(Subcommand, Debug)]
pub enum P2pCollectionCommand {
    /// Get all collections available for P2P sync
    GetAll(P2pCollectionGetAllArgs),
    /// Add a collection to P2P sync
    Add(P2pCollectionAddArgs),
    /// Remove a collection from P2P sync
    Remove(P2pCollectionRemoveArgs),
    /// Sync collection versions
    SyncVersions(P2pCollectionSyncVersionsArgs),
    /// Sync branchable collection
    SyncBranchable(P2pCollectionSyncBranchableArgs),
}

/// Arguments for collection getall command
#[derive(Args, Debug)]
pub struct P2pCollectionGetAllArgs {}

/// Arguments for collection add command
#[derive(Args, Debug)]
pub struct P2pCollectionAddArgs {
    /// Collection(s) to add (comma-separated or multiple --collection)
    #[arg(long, short = 'c', required = true, value_delimiter = ',')]
    pub collection: Vec<String>,
}

/// Arguments for collection remove command
#[derive(Args, Debug)]
pub struct P2pCollectionRemoveArgs {
    /// Collection(s) to remove (comma-separated or multiple --collection)
    #[arg(long, short = 'c', required = true, value_delimiter = ',')]
    pub collection: Vec<String>,
}

/// Arguments for collection sync-versions command
#[derive(Args, Debug)]
pub struct P2pCollectionSyncVersionsArgs {}

/// Arguments for collection sync-branchable command
#[derive(Args, Debug)]
pub struct P2pCollectionSyncBranchableArgs {}

impl P2pCollectionArgs {
    /// Execute the collection subcommand
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pCollectionCommand::GetAll(args) => args.execute(ctx).await,
            P2pCollectionCommand::Add(args) => args.execute(ctx).await,
            P2pCollectionCommand::Remove(args) => args.execute(ctx).await,
            P2pCollectionCommand::SyncVersions(args) => args.execute(ctx).await,
            P2pCollectionCommand::SyncBranchable(args) => args.execute(ctx).await,
        }
    }
}

impl P2pCollectionGetAllArgs {
    /// Execute the collection getall command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let collections = client.p2p_collection_list().await?;
        println!("{}", serde_json::to_string_pretty(&collections)?);
        Ok(())
    }
}

impl P2pCollectionAddArgs {
    /// Execute the collection add command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        for col in &self.collection {
            validate_identifier(col)?;
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.p2p_collection_add(&self.collection).await?;
        println!("Added collections to P2P: {}", self.collection.join(", "));
        Ok(())
    }
}

impl P2pCollectionRemoveArgs {
    /// Execute the collection remove command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        for col in &self.collection {
            validate_identifier(col)?;
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.p2p_collection_remove(&self.collection).await?;
        println!(
            "Removed collections from P2P: {}",
            self.collection.join(", ")
        );
        Ok(())
    }
}

impl P2pCollectionSyncVersionsArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.p2p_collection_sync().await?;
        println!("Collection sync initiated");
        Ok(())
    }
}

impl P2pCollectionSyncBranchableArgs {
    pub async fn execute(&self, _ctx: &ClientContext) -> Result<()> {
        Err(crate::error::Error::Server(
            "p2p collection sync-branchable requires branchable sync protocol (not yet implemented)".to_string(),
        ))
    }
}
