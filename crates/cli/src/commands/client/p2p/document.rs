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
