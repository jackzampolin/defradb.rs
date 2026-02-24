//! Collection command implementation

mod document;
mod introspection;

use std::path::PathBuf;

use clap::{Args, Subcommand};

use super::ClientContext;
use crate::error::Result;

/// Interact with collections and documents
#[derive(Args, Debug)]
pub struct CollectionArgs {
    /// Collection name
    #[arg(long, global = true)]
    pub name: Option<String>,

    /// Collection ID
    #[arg(long, global = true)]
    pub collection_id: Option<String>,

    /// Schema version ID
    #[arg(long, global = true)]
    pub version_id: Option<String>,

    /// Get inactive collections
    #[arg(long, global = true)]
    pub get_inactive: bool,

    #[command(subcommand)]
    pub command: CollectionCommand,
}

/// Collection subcommands
#[derive(Subcommand, Debug)]
pub enum CollectionCommand {
    /// Create a new document
    Create(DocumentCreateArgs),
    /// Delete a document
    Delete(DocumentDeleteArgs),
    /// Describe a collection's schema
    Describe(CollectionDescribeArgs),
    /// Get document IDs
    #[command(name = "docIDs")]
    DocIds(DocIdsArgs),
    /// Get a document by ID
    Get(DocumentGetArgs),
    /// List all collections
    List(CollectionListArgs),
    /// Patch a collection schema
    Patch(CollectionPatchArgs),
    /// Set a collection as active
    SetActive(SetActiveArgs),
    /// Truncate a collection
    Truncate(TruncateArgs),
    /// Update a document
    Update(DocumentUpdateArgs),
}

/// Arguments for collection list command
#[derive(Args, Debug)]
pub struct CollectionListArgs {}

/// Arguments for collection describe command
#[derive(Args, Debug)]
pub struct CollectionDescribeArgs {}

/// Arguments for document create command
#[derive(Args, Debug)]
pub struct DocumentCreateArgs {
    /// The document data (JSON)
    #[arg(value_name = "DOCUMENT")]
    pub document: Option<String>,

    /// File containing document(s)
    #[arg(long, short = 'f')]
    pub file: Option<PathBuf>,

    /// Flag to enable encryption of the document
    #[arg(long, short = 'e')]
    pub encrypt: bool,

    /// Comma-separated list of fields to encrypt
    #[arg(long, value_delimiter = ',')]
    pub encrypt_fields: Vec<String>,
}

/// Arguments for document get command
#[derive(Args, Debug)]
pub struct DocumentGetArgs {
    /// The document ID
    #[arg(value_name = "DOC_ID")]
    pub doc_id: String,

    /// Show deleted documents
    #[arg(long)]
    pub show_deleted: bool,
}

/// Arguments for document update command
#[derive(Args, Debug)]
pub struct DocumentUpdateArgs {
    /// Document ID
    #[arg(long = "docID")]
    pub doc_id: Option<String>,

    /// Document filter
    #[arg(long)]
    pub filter: Option<String>,

    /// Document updater
    #[arg(long)]
    pub updater: Option<String>,
}

/// Arguments for document delete command
#[derive(Args, Debug)]
pub struct DocumentDeleteArgs {
    /// Document ID
    #[arg(long = "docID")]
    pub doc_id: Option<String>,

    /// Document filter
    #[arg(long)]
    pub filter: Option<String>,
}

/// Arguments for doc-ids command
#[derive(Args, Debug)]
pub struct DocIdsArgs {}

/// Arguments for collection patch command
#[derive(Args, Debug)]
pub struct CollectionPatchArgs {
    /// The patch data (JSON)
    #[arg(value_name = "PATCH")]
    pub patch: Option<String>,

    /// The migration configuration
    #[arg(value_name = "MIGRATION")]
    pub migration: Option<String>,

    /// File to load a patch from
    #[arg(long, short = 'p')]
    pub patch_file: Option<PathBuf>,

    /// File to load a lens config from
    #[arg(long, short = 't')]
    pub lens_file: Option<PathBuf>,
}

/// Arguments for set-active command
#[derive(Args, Debug)]
pub struct SetActiveArgs {
    /// Collection version ID to set as active
    #[arg(value_name = "VERSION_ID")]
    pub version_id: Option<String>,
}

/// Arguments for truncate command
#[derive(Args, Debug)]
pub struct TruncateArgs {}

impl CollectionArgs {
    /// Execute the collection command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            CollectionCommand::Create(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::Delete(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::Describe(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::DocIds(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::Get(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::List(args) => args.execute(ctx).await,
            CollectionCommand::Patch(args) => args.execute(ctx).await,
            CollectionCommand::SetActive(args) => args.execute(ctx).await,
            CollectionCommand::Truncate(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::Update(args) => args.execute(ctx, self.name.as_deref()).await,
        }
    }
}
