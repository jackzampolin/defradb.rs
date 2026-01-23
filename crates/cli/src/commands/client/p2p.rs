
//! P2P command implementation for peer-to-peer network management

use clap::{Args, Subcommand};

use super::http_client::HttpClient;
use super::{validate_identifier, ClientContext};
use crate::error::Result;

/// Manage P2P network
#[derive(Args, Debug)]
pub struct P2pArgs {
    #[command(subcommand)]
    pub command: P2pCommand,
}

/// P2P subcommands
#[derive(Subcommand, Debug)]
pub enum P2pCommand {
    /// Show P2P node information
    Info(P2pInfoArgs),
    /// Manage peer connections
    Peers(P2pPeersArgs),
    /// Manage replicators
    Replicator(P2pReplicatorArgs),
    /// Manage P2P collections
    Collection(P2pCollectionArgs),
}

/// Arguments for p2p info command
#[derive(Args, Debug)]
pub struct P2pInfoArgs {}

/// Peer management subcommands
#[derive(Args, Debug)]
pub struct P2pPeersArgs {
    #[command(subcommand)]
    pub command: P2pPeersCommand,
}

/// Peer subcommands
#[derive(Subcommand, Debug)]
pub enum P2pPeersCommand {
    /// List connected peers
    List(P2pPeersListArgs),
    /// Connect to a peer
    Add(P2pPeersAddArgs),
}

/// Arguments for peers list command
#[derive(Args, Debug)]
pub struct P2pPeersListArgs {}

/// Arguments for peers add command
#[derive(Args, Debug)]
pub struct P2pPeersAddArgs {
    /// Multiaddr of the peer to connect to
    #[arg(value_name = "ADDRESS")]
    pub address: String,
}

/// Replicator management subcommands
#[derive(Args, Debug)]
pub struct P2pReplicatorArgs {
    #[command(subcommand)]
    pub command: P2pReplicatorCommand,
}

/// Replicator subcommands
#[derive(Subcommand, Debug)]
pub enum P2pReplicatorCommand {
    /// List replicators
    List(P2pReplicatorListArgs),
    /// Add a replicator for a collection
    Add(P2pReplicatorAddArgs),
    /// Remove a replicator
    Delete(P2pReplicatorDeleteArgs),
}

/// Arguments for replicator list command
#[derive(Args, Debug)]
pub struct P2pReplicatorListArgs {}

/// Arguments for replicator add command
#[derive(Args, Debug)]
pub struct P2pReplicatorAddArgs {
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

/// P2P collection management subcommands
#[derive(Args, Debug)]
pub struct P2pCollectionArgs {
    #[command(subcommand)]
    pub command: P2pCollectionCommand,
}

/// P2P collection subcommands
#[derive(Subcommand, Debug)]
pub enum P2pCollectionCommand {
    /// List collections available for P2P sync
    List(P2pCollectionListArgs),
    /// Add a collection to P2P sync
    Add(P2pCollectionAddArgs),
    /// Remove a collection from P2P sync
    Remove(P2pCollectionRemoveArgs),
}

/// Arguments for collection list command
#[derive(Args, Debug)]
pub struct P2pCollectionListArgs {}

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

impl P2pArgs {
    /// Execute the p2p command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pCommand::Info(args) => args.execute(ctx).await,
            P2pCommand::Peers(args) => args.execute(ctx).await,
            P2pCommand::Replicator(args) => args.execute(ctx).await,
            P2pCommand::Collection(args) => args.execute(ctx).await,
        }
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

impl P2pPeersArgs {
    /// Execute the peers subcommand
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pPeersCommand::List(args) => args.execute(ctx).await,
            P2pPeersCommand::Add(args) => args.execute(ctx).await,
        }
    }
}

impl P2pPeersListArgs {
    /// Execute the peers list command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let peers = client.p2p_peers_list().await?;
        println!("{}", serde_json::to_string_pretty(&peers)?);
        Ok(())
    }
}

impl P2pPeersAddArgs {
    /// Execute the peers add command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.p2p_peers_add(&self.address).await?;
        println!("Connected to peer: {}", self.address);
        Ok(())
    }
}

impl P2pReplicatorArgs {
    /// Execute the replicator subcommand
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pReplicatorCommand::List(args) => args.execute(ctx).await,
            P2pReplicatorCommand::Add(args) => args.execute(ctx).await,
            P2pReplicatorCommand::Delete(args) => args.execute(ctx).await,
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

impl P2pReplicatorAddArgs {
    /// Execute the replicator add command
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
            "Added replicator for collections: {}",
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

impl P2pCollectionArgs {
    /// Execute the collection subcommand
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pCollectionCommand::List(args) => args.execute(ctx).await,
            P2pCollectionCommand::Add(args) => args.execute(ctx).await,
            P2pCollectionCommand::Remove(args) => args.execute(ctx).await,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_p2p_peers_add_args() {
        let args = P2pPeersAddArgs {
            address: "/ip4/127.0.0.1/tcp/9000/p2p/12D3KooWtest".to_string(),
        };
        assert!(args.address.contains("12D3KooW"));
    }

    #[test]
    fn test_p2p_replicator_add_args() {
        let args = P2pReplicatorAddArgs {
            collection: vec!["Users".to_string(), "Posts".to_string()],
            address: Some("/ip4/127.0.0.1/tcp/9000".to_string()),
        };
        assert_eq!(args.collection.len(), 2);
        assert!(args.address.is_some());
    }

    #[test]
    fn test_p2p_replicator_add_args_no_address() {
        let args = P2pReplicatorAddArgs {
            collection: vec!["Users".to_string()],
            address: None,
        };
        assert!(args.address.is_none());
    }

    #[test]
    fn test_p2p_collection_add_args() {
        let args = P2pCollectionAddArgs {
            collection: vec!["Users".to_string()],
        };
        assert_eq!(args.collection.len(), 1);
    }
}
