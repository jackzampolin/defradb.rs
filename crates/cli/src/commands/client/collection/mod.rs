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
    /// Add a new collection from a schema definition (SDL)
    Add(CollectionAddArgs),
    /// Create a new document
    Create(DocumentCreateArgs),
    /// Delete a document
    Delete(DocumentDeleteArgs),
    /// Describe a collection's schema
    Describe(CollectionDescribeArgs),
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

/// Arguments for collection add (schema definition) command
#[derive(Args, Debug)]
pub struct CollectionAddArgs {
    /// The schema definition (SDL format)
    #[arg(value_name = "SDL")]
    pub schema: Option<String>,

    /// Read schema from file(s)
    #[arg(long, short = 'f', value_name = "FILE")]
    pub file: Vec<PathBuf>,
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

impl CollectionAddArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let mut sdl_parts = Vec::new();

        if let Some(ref schema) = self.schema {
            sdl_parts.push(schema.clone());
        }

        for path in &self.file {
            let content =
                std::fs::read_to_string(path).map_err(|e| crate::error::Error::ReadFile {
                    path: path.clone(),
                    source: e,
                })?;
            sdl_parts.push(content);
        }

        if sdl_parts.is_empty() {
            return Err(crate::error::Error::MissingInput(
                "either SDL argument or --file must be provided".to_string(),
            ));
        }

        let sdl = sdl_parts.join("\n");
        let client = super::http_client::HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.schema_add(&sdl).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl CollectionArgs {
    /// Execute the collection command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            CollectionCommand::Add(args) => args.execute(ctx).await,
            CollectionCommand::Create(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::Delete(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::Describe(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::Get(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::List(args) => args.execute(ctx).await,
            CollectionCommand::Patch(args) => args.execute(ctx).await,
            CollectionCommand::SetActive(args) => args.execute(ctx).await,
            CollectionCommand::Truncate(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::Update(args) => args.execute(ctx, self.name.as_deref()).await,
        }
    }
}
