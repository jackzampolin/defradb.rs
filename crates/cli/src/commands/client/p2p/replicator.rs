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
    /// Create replicator(s) and start synchronization
    Create(P2pReplicatorCreateArgs),
    /// Delete replicator(s) and stop synchronization
    Delete(P2pReplicatorDeleteArgs),
    /// List all replicators
    List(P2pReplicatorListArgs),
}

/// Arguments for replicator list command
#[derive(Args, Debug)]
pub struct P2pReplicatorListArgs {}

/// Arguments for replicator create command
#[derive(Args, Debug)]
pub struct P2pReplicatorCreateArgs {
    /// Collection(s) to replicate (comma-separated or multiple --collection)
    #[arg(long, short = 'c', required = true, value_delimiter = ',')]
    pub collection: Vec<String>,

    /// Peer address(es) to replicate with
    #[arg(value_name = "addresses")]
    pub addresses: Vec<String>,
}

/// Arguments for replicator delete command
#[derive(Args, Debug)]
pub struct P2pReplicatorDeleteArgs {
    /// Collection(s) to stop replicating (comma-separated or multiple --collection)
    #[arg(long, short = 'c', required = true, value_delimiter = ',')]
    pub collection: Vec<String>,

    /// Peer ID to stop replicating with
    #[arg(value_name = "peerID")]
    pub peer_id: Option<String>,
}

impl P2pReplicatorArgs {
    /// Execute the replicator subcommand
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pReplicatorCommand::Create(args) => args.execute(ctx).await,
            P2pReplicatorCommand::Delete(args) => args.execute(ctx).await,
            P2pReplicatorCommand::List(args) => args.execute(ctx).await,
        }
    }
}

impl P2pReplicatorListArgs {
    /// Execute the replicator list command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let replicators = client.p2p_replicator_list().await?;
        println!("{}", serde_json::to_string_pretty(&replicators)?);
        Ok(())
    }
}

impl P2pReplicatorCreateArgs {
    /// Execute the replicator create command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        for col in &self.collection {
            validate_identifier(col)?;
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        // Use first address if provided (Go API sends all addresses)
        let address = self.addresses.first().map(|s| s.as_str());
        client.p2p_replicator_add(&self.collection, address).await?;
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
            .p2p_replicator_delete(&self.collection, self.peer_id.as_deref())
            .await?;
        println!(
            "Removed replicator for collections: {}",
            self.collection.join(", ")
        );
        Ok(())
    }
}
