use clap::{Args, Subcommand};

use super::http_client::{HttpClient, ViewRefreshSelectors};
use super::ClientContext;
use crate::error::Result;

/// Manage views
#[derive(Args, Debug)]
pub struct ViewArgs {
    #[command(subcommand)]
    pub command: ViewCommand,
}

/// View subcommands
#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum ViewCommand {
    /// Add a new view
    Add(ViewAddArgs),
    /// Refresh views
    Refresh(ViewRefreshArgs),
    /// Run explicit downsample history GC for views
    Gc(ViewGcArgs),
}

/// Arguments for view add command
#[derive(Args, Debug)]
pub struct ViewAddArgs {
    /// The GraphQL query for the view
    #[arg(long)]
    pub query: Option<String>,

    /// Path to a file containing the GraphQL query
    #[arg(long)]
    pub query_file: Option<String>,

    /// The SDL for the view
    #[arg(long)]
    pub sdl: Option<String>,

    /// Path to a file containing the SDL
    #[arg(long)]
    pub sdl_file: Option<String>,

    /// CID of the lens transform
    #[arg(long)]
    pub lens_cid: Option<String>,
}

/// Arguments for view refresh command.
///
/// The selectors mirror Go's `client view refresh` flags; with none of them
/// every view is refreshed.
#[derive(Args, Debug)]
pub struct ViewRefreshArgs {
    /// Collection name to refresh (refreshes all if not specified)
    #[arg(long = "collection-name")]
    pub name: Option<String>,

    /// Collection ID to refresh
    #[arg(long = "collection-id")]
    pub collection_id: Option<String>,

    /// Collection version ID to refresh
    #[arg(long = "version-id")]
    pub version_id: Option<String>,

    /// Include inactive collection versions
    #[arg(long = "get-inactive")]
    pub get_inactive: bool,
}

impl ViewRefreshArgs {
    pub fn selectors(&self) -> ViewRefreshSelectors {
        ViewRefreshSelectors {
            name: self.name.clone(),
            collection_id: self.collection_id.clone(),
            version_id: self.version_id.clone(),
            get_inactive: self.get_inactive,
        }
    }
}

/// Arguments for manual downsample GC command
#[derive(Args, Debug)]
pub struct ViewGcArgs {
    /// Collection name to GC (GCs all downsample views if not specified)
    #[arg(long)]
    pub name: Option<String>,
}

impl ViewArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            ViewCommand::Add(args) => args.execute(ctx).await,
            ViewCommand::Refresh(args) => args.execute(ctx).await,
            ViewCommand::Gc(args) => args.execute(ctx).await,
        }
    }
}

impl ViewAddArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let query = match (&self.query, &self.query_file) {
            (Some(q), _) => q.clone(),
            (None, Some(path)) => tokio::fs::read_to_string(path).await.map_err(|e| {
                crate::error::Error::Server(format!("failed to read query file: {}", e))
            })?,
            (None, None) => {
                return Err(crate::error::Error::Server(
                    "either --query or --query-file must be provided".into(),
                ));
            }
        };
        let sdl = match (&self.sdl, &self.sdl_file) {
            (Some(s), _) => s.clone(),
            (None, Some(path)) => tokio::fs::read_to_string(path).await.map_err(|e| {
                crate::error::Error::Server(format!("failed to read SDL file: {}", e))
            })?,
            (None, None) => {
                return Err(crate::error::Error::Server(
                    "either --sdl or --sdl-file must be provided".into(),
                ));
            }
        };

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client
            .view_add(&query, &sdl, self.lens_cid.as_deref())
            .await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl ViewRefreshArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.view_refresh(&self.selectors()).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl ViewGcArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let names = self.name.as_ref().map(|n| vec![n.clone()]);
        let result = client.view_gc(names).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}
