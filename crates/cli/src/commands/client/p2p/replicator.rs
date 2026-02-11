//! P2P replicator command implementations

use clap::{Args, Subcommand};

use crate::commands::client::http_client::HttpClient;
use crate::commands::client::{validate_identifier, ClientContext};
use crate::error::Result;

/// Replicator management subcommands
#[derive(Args, Debug)]
pub struct P2pReplicatorArgs {
    #[command(subcommand)]
    pub command: P2pReplicatorCommand,
}

/// Replicator subcommands
#[derive(Subcommand, Debug)]
pub enum P2pReplicatorCommand {
    /// Get all replicators
    GetAll(P2pReplicatorGetAllArgs),
    /// Set a replicator for collections
    Set(P2pReplicatorSetArgs),
    /// Delete a replicator
    Delete(P2pReplicatorDeleteArgs),
}

/// Arguments for replicator getall command
#[derive(Args, Debug)]
pub struct P2pReplicatorGetAllArgs {}

/// Arguments for replicator set command
#[derive(Args, Debug)]
pub struct P2pReplicatorSetArgs {
    /// Collection(s) to replicate (comma-separated or multiple --collection)
    #[arg(long, short = 'c', required = true, value_delimiter = ',')]
    pub collection: Vec<String>,

    /// Peer address to replicate with
    #[arg(long, short = 'a')]
    pub address: Option<String>,
}

/// Arguments for replicator delete command
#[derive(Args, Debug)]
pub struct P2pReplicatorDeleteArgs {
    /// Collection(s) to stop replicating (comma-separated or multiple --collection)
    #[arg(long, short = 'c', required = true, value_delimiter = ',')]
    pub collection: Vec<String>,

    /// Peer address to stop replicating with
    #[arg(long, short = 'a')]
    pub address: Option<String>,
}

impl P2pReplicatorArgs {
    /// Execute the replicator subcommand
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pReplicatorCommand::GetAll(args) => args.execute(ctx).await,
            P2pReplicatorCommand::Set(args) => args.execute(ctx).await,
            P2pReplicatorCommand::Delete(args) => args.execute(ctx).await,
        }
    }
}

impl P2pReplicatorGetAllArgs {
    /// Execute the replicator getall command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let replicators = client.p2p_replicator_list().await?;
        println!("{}", serde_json::to_string_pretty(&replicators)?);
        Ok(())
    }
}

impl P2pReplicatorSetArgs {
    /// Execute the replicator set command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        for col in &self.collection {
            validate_identifier(col)?;
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client
            .p2p_replicator_add(&self.collection, self.address.as_deref())
            .await?;
        println!(
            "Set replicator for collections: {}",
            self.collection.join(", ")
        );
        Ok(())
    }
}

impl P2pReplicatorDeleteArgs {
    /// Execute the replicator delete command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        for col in &self.collection {
            validate_identifier(col)?;
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client
            .p2p_replicator_delete(&self.collection, self.address.as_deref())
            .await?;
        println!(
            "Removed replicator for collections: {}",
            self.collection.join(", ")
        );
        Ok(())
    }
}
