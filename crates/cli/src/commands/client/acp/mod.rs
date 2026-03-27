//! ACP (Access Control Policy) command implementation

mod document;
mod node;

use clap::{Args, Subcommand};

use super::ClientContext;
use crate::error::Result;

pub use document::AcpDocumentArgs;
pub use node::AcpNodeArgs;

/// Interact with Access Control Policies
#[derive(Args, Debug)]
pub struct AcpArgs {
    #[command(subcommand)]
    pub command: AcpCommand,
}

/// ACP subcommands
#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum AcpCommand {
    /// Manage document-level ACP
    Document(AcpDocumentArgs),
    /// Manage node-level ACP
    Node(AcpNodeArgs),
}

impl AcpArgs {
    /// Execute the ACP command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            AcpCommand::Document(args) => args.execute(ctx).await,
            AcpCommand::Node(args) => args.execute(ctx).await,
        }
    }
}
