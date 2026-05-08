//! Collection command implementation

mod document;
pub(crate) mod introspection;

use std::path::PathBuf;

use clap::{Args, Subcommand};

use super::ClientContext;
use crate::error::Result;

/// Interact with collections
#[derive(Args, Debug)]
pub struct CollectionArgs {
    /// Collection name
    #[arg(long, global = true)]
    pub name: Option<String>,

    /// Collection ID
    #[arg(long, global = true)]
    pub collection_id: Option<String>,

    /// Collection version ID
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
#[non_exhaustive]
pub enum CollectionCommand {
    /// Add a new collection from an SDL definition
    Add(CollectionAddArgs),
    /// Describe a collection
    Describe(CollectionDescribeArgs),
    /// List all collections
    List(CollectionListArgs),
    /// Patch a collection
    Patch(CollectionPatchArgs),
    /// Set a collection as active
    SetActive(SetActiveArgs),
    /// Truncate a collection
    Truncate(TruncateArgs),
}

/// Arguments for collection add (schema definition) command
#[derive(Args, Debug)]
pub struct CollectionAddArgs {
    /// The collection definition (SDL format)
    #[arg(value_name = "SDL")]
    pub sdl: Option<String>,

    /// Read collection definition from file(s)
    #[arg(long, short = 'f', value_name = "FILE")]
    pub file: Vec<PathBuf>,
}

/// Arguments for collection list command
#[derive(Args, Debug)]
pub struct CollectionListArgs {}

/// Arguments for collection describe command
#[derive(Args, Debug)]
pub struct CollectionDescribeArgs {}

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

        if let Some(ref sdl) = self.sdl {
            sdl_parts.push(sdl.clone());
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

        let result = client.schema_add(&sdl, ctx.tx_id.as_deref()).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl CollectionArgs {
    /// Execute the collection command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            CollectionCommand::Add(args) => args.execute(ctx).await,
            CollectionCommand::Describe(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::List(args) => args.execute(ctx).await,
            CollectionCommand::Patch(args) => args.execute(ctx).await,
            CollectionCommand::SetActive(args) => args.execute(ctx).await,
            CollectionCommand::Truncate(args) => args.execute(ctx, self.name.as_deref()).await,
        }
    }
}
