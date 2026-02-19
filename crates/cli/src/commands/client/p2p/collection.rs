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
    /// Add P2P collections to the synchronized pubsub topics
    Add(P2pCollectionAddArgs),
    /// Delete P2P collections from the followed pubsub topics
    Delete(P2pCollectionDeleteArgs),
    /// List P2P collections
    List(P2pCollectionListArgs),
    /// Sync collection versions
    SyncVersions(P2pCollectionSyncVersionsArgs),
    /// Sync branchable collection
    SyncBranchable(P2pCollectionSyncBranchableArgs),
}

/// Arguments for collection list command
#[derive(Args, Debug)]
pub struct P2pCollectionListArgs {}

/// Arguments for collection add command
#[derive(Args, Debug)]
pub struct P2pCollectionAddArgs {
    /// Collection names (comma-separated, e.g. User,Address)
    #[arg(value_name = "collectionNames")]
    pub collections: String,
}

/// Arguments for collection delete command
#[derive(Args, Debug)]
pub struct P2pCollectionDeleteArgs {
    /// Collection names (comma-separated, e.g. User,Address)
    #[arg(value_name = "collectionNames")]
    pub collections: String,
}

/// Arguments for collection sync-versions command
#[derive(Args, Debug)]
pub struct P2pCollectionSyncVersionsArgs {
    /// Version IDs to sync
    #[arg(value_name = "versionID")]
    pub version_ids: Vec<String>,
}

/// Arguments for collection sync-branchable command
#[derive(Args, Debug)]
pub struct P2pCollectionSyncBranchableArgs {
    /// Collection ID
    #[arg(value_name = "collection-id")]
    pub collection_id: String,
}

impl P2pCollectionArgs {
    /// Execute the collection subcommand
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pCollectionCommand::List(args) => args.execute(ctx).await,
            P2pCollectionCommand::Add(args) => args.execute(ctx).await,
            P2pCollectionCommand::Delete(args) => args.execute(ctx).await,
            P2pCollectionCommand::SyncVersions(args) => args.execute(ctx).await,
            P2pCollectionCommand::SyncBranchable(args) => args.execute(ctx).await,
        }
    }
}

impl P2pCollectionListArgs {
    /// Execute the collection list command
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
        let collections: Vec<String> = self
            .collections
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        for col in &collections {
            validate_identifier(col)?;
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.p2p_collection_add(&collections).await?;
        println!("Added collections to P2P: {}", collections.join(", "));
        Ok(())
    }
}

impl P2pCollectionDeleteArgs {
    /// Execute the collection delete command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let collections: Vec<String> = self
            .collections
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        for col in &collections {
            validate_identifier(col)?;
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.p2p_collection_remove(&collections).await?;
        println!("Removed collections from P2P: {}", collections.join(", "));
        Ok(())
    }
}

impl P2pCollectionSyncVersionsArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client
            .p2p_collection_sync_versions(&self.version_ids)
            .await?;
        println!("Collection sync-versions initiated");
        Ok(())
    }
}

impl P2pCollectionSyncBranchableArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client
            .p2p_collection_sync_branchable(&self.collection_id)
            .await?;
        println!("Collection sync-branchable initiated");
        Ok(())
    }
}
