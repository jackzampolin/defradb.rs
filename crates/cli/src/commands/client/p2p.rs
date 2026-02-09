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

/// P2P document management subcommands
#[derive(Args, Debug)]
pub struct P2pDocumentArgs {
    #[command(subcommand)]
    pub command: P2pDocumentCommand,
}

/// P2P document subcommands
#[derive(Subcommand, Debug)]
pub enum P2pDocumentCommand {
    /// Add a document to P2P sync
    Add(P2pDocumentAddArgs),
    /// Remove a document from P2P sync
    Remove(P2pDocumentRemoveArgs),
    /// Get all P2P synced documents
    GetAll(P2pDocumentGetAllArgs),
    /// Sync a document
    Sync(P2pDocumentSyncArgs),
}

/// Arguments for p2p document add command
#[derive(Args, Debug)]
pub struct P2pDocumentAddArgs {
    /// Document ID
    #[arg(long = "docID")]
    pub doc_id: Option<String>,

    /// Schema ID
    #[arg(long = "schemaID")]
    pub schema_id: Option<String>,
}

/// Arguments for p2p document remove command
#[derive(Args, Debug)]
pub struct P2pDocumentRemoveArgs {
    /// Document ID
    #[arg(long = "docID")]
    pub doc_id: Option<String>,

    /// Schema ID
    #[arg(long = "schemaID")]
    pub schema_id: Option<String>,
}

/// Arguments for p2p document get-all command
#[derive(Args, Debug)]
pub struct P2pDocumentGetAllArgs {}

/// Arguments for p2p document sync command
#[derive(Args, Debug)]
pub struct P2pDocumentSyncArgs {
    /// Document ID
    #[arg(long = "docID")]
    pub doc_id: Option<String>,

    /// Schema ID
    #[arg(long = "schemaID")]
    pub schema_id: Option<String>,
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
    pub async fn execute(&self, _ctx: &ClientContext) -> Result<()> {
        eprintln!("not yet implemented");
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

impl P2pDocumentArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pDocumentCommand::Add(args) => args.execute(ctx).await,
            P2pDocumentCommand::Remove(args) => args.execute(ctx).await,
            P2pDocumentCommand::GetAll(args) => args.execute(ctx).await,
            P2pDocumentCommand::Sync(args) => args.execute(ctx).await,
        }
    }
}

impl P2pDocumentAddArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let doc_ids: Vec<String> = self.doc_id.iter().cloned().collect();
        let schema_ids: Vec<String> = self.schema_id.iter().cloned().collect();

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.p2p_document_add(&doc_ids, &schema_ids).await?;
        println!("Added document(s) to P2P sync");
        Ok(())
    }
}

impl P2pDocumentRemoveArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let doc_ids: Vec<String> = self.doc_id.iter().cloned().collect();
        let schema_ids: Vec<String> = self.schema_id.iter().cloned().collect();

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.p2p_document_remove(&doc_ids, &schema_ids).await?;
        println!("Removed document(s) from P2P sync");
        Ok(())
    }
}

impl P2pDocumentGetAllArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.p2p_document_list().await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl P2pDocumentSyncArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client
            .p2p_document_sync(self.doc_id.as_deref(), self.schema_id.as_deref())
            .await?;
        println!("Document sync initiated");
        Ok(())
    }
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
        eprintln!("not yet implemented");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_p2p_replicator_set_args() {
        let args = P2pReplicatorSetArgs {
            collection: vec!["Users".to_string(), "Posts".to_string()],
            address: Some("/ip4/127.0.0.1/tcp/9000".to_string()),
        };
        assert_eq!(args.collection.len(), 2);
        assert!(args.address.is_some());
    }

    #[test]
    fn test_p2p_replicator_set_args_no_address() {
        let args = P2pReplicatorSetArgs {
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
