//! P2P document command implementations

use clap::{Args, Subcommand};

use crate::commands::client::http_client::HttpClient;
use crate::commands::client::ClientContext;
use crate::error::Result;

/// P2P document management subcommands
#[derive(Args, Debug)]
pub struct P2pDocumentArgs {
    #[command(subcommand)]
    pub command: P2pDocumentCommand,
}

/// P2P document subcommands
#[derive(Subcommand, Debug)]
pub enum P2pDocumentCommand {
    /// Add document(s) to P2P sync
    Create(P2pDocumentCreateArgs),
    /// Remove document(s) from P2P sync
    Delete(P2pDocumentDeleteArgs),
    /// List all P2P synced documents
    List(P2pDocumentListArgs),
    /// Sync a document
    Sync(P2pDocumentSyncArgs),
}

/// Arguments for p2p document create command
#[derive(Args, Debug)]
pub struct P2pDocumentCreateArgs {
    /// Document IDs to add
    #[arg(value_name = "docIDs")]
    pub doc_ids: Vec<String>,

    /// Schema ID
    #[arg(long = "schemaID")]
    pub schema_id: Option<String>,
}

/// Arguments for p2p document delete command
#[derive(Args, Debug)]
pub struct P2pDocumentDeleteArgs {
    /// Document IDs to remove
    #[arg(value_name = "docIDs")]
    pub doc_ids: Vec<String>,

    /// Schema ID
    #[arg(long = "schemaID")]
    pub schema_id: Option<String>,
}

/// Arguments for p2p document list command
#[derive(Args, Debug)]
pub struct P2pDocumentListArgs {}

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

impl P2pDocumentArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pDocumentCommand::Create(args) => args.execute(ctx).await,
            P2pDocumentCommand::Delete(args) => args.execute(ctx).await,
            P2pDocumentCommand::List(args) => args.execute(ctx).await,
            P2pDocumentCommand::Sync(args) => args.execute(ctx).await,
        }
    }
}

impl P2pDocumentCreateArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let schema_ids: Vec<String> = self.schema_id.iter().cloned().collect();

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.p2p_document_add(&self.doc_ids, &schema_ids).await?;
        println!("Added document(s) to P2P sync");
        Ok(())
    }
}

impl P2pDocumentDeleteArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let schema_ids: Vec<String> = self.schema_id.iter().cloned().collect();

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client
            .p2p_document_remove(&self.doc_ids, &schema_ids)
            .await?;
        println!("Removed document(s) from P2P sync");
        Ok(())
    }
}

impl P2pDocumentListArgs {
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
