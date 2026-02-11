//! P2P command implementation for peer-to-peer network management

mod collection;
mod document;
mod replicator;
#[cfg(test)]
mod tests;

use clap::{Args, Subcommand};

use super::http_client::HttpClient;
use super::ClientContext;
use crate::error::Result;

pub use collection::P2pCollectionArgs;
pub use document::P2pDocumentArgs;
pub use replicator::P2pReplicatorArgs;

/// Manage P2P network
#[derive(Args, Debug)]
pub struct P2pArgs {
    #[command(subcommand)]
    pub command: P2pCommand,
}

/// P2P subcommands
#[derive(Subcommand, Debug)]
pub enum P2pCommand {
    /// Show active peers
    ActivePeers(P2pActivePeersArgs),
    /// Manage P2P collections
    Collection(P2pCollectionArgs),
    /// Connect to a peer
    Connect(P2pConnectArgs),
    /// Manage document P2P sync
    Document(P2pDocumentArgs),
    /// Show P2P node information
    Info(P2pInfoArgs),
    /// Manage replicators
    Replicator(P2pReplicatorArgs),
}

/// Arguments for p2p active-peers command
#[derive(Args, Debug)]
pub struct P2pActivePeersArgs {}

/// Arguments for p2p connect command
#[derive(Args, Debug)]
pub struct P2pConnectArgs {
    /// Peer addresses to connect to
    #[arg(value_name = "ADDRESS")]
    pub addresses: Vec<String>,
}

/// Arguments for p2p info command
#[derive(Args, Debug)]
pub struct P2pInfoArgs {}

impl P2pArgs {
    /// Execute the p2p command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pCommand::ActivePeers(args) => args.execute(ctx).await,
            P2pCommand::Collection(args) => args.execute(ctx).await,
            P2pCommand::Connect(args) => args.execute(ctx).await,
            P2pCommand::Document(args) => args.execute(ctx).await,
            P2pCommand::Info(args) => args.execute(ctx).await,
            P2pCommand::Replicator(args) => args.execute(ctx).await,
        }
    }
}

impl P2pActivePeersArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.p2p_active_peers().await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl P2pConnectArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        if self.addresses.is_empty() {
            return Err(crate::error::Error::MissingInput(
                "at least one address is required".to_string(),
            ));
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.p2p_connect(&self.addresses).await?;
        println!("Connected to peer(s)");
        Ok(())
    }
}

impl P2pInfoArgs {
    /// Execute the p2p info command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let info = client.p2p_info().await?;
        println!("{}", serde_json::to_string_pretty(&info)?);
        Ok(())
    }
}
