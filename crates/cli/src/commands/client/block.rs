//! Block command implementation

use clap::{Args, Subcommand};

use super::ClientContext;
use crate::error::Result;

/// Interact with blocks
#[derive(Args, Debug)]
pub struct BlockArgs {
    #[command(subcommand)]
    pub command: BlockCommand,
}

/// Block subcommands
#[derive(Subcommand, Debug)]
pub enum BlockCommand {
    /// Verify the signature of a block
    VerifySignature(BlockVerifySignatureArgs),
}

/// Arguments for block verify-signature command
#[derive(Args, Debug)]
pub struct BlockVerifySignatureArgs {
    /// The public key
    #[arg(value_name = "PUBLIC_KEY")]
    pub public_key: String,

    /// The CID of the block
    #[arg(value_name = "CID")]
    pub cid: String,

    /// Key type
    #[arg(long, short = 't')]
    pub key_type: Option<String>,
}

impl BlockArgs {
    pub async fn execute(&self, _ctx: &ClientContext) -> Result<()> {
        match &self.command {
            BlockCommand::VerifySignature(args) => args.execute().await,
        }
    }
}

impl BlockVerifySignatureArgs {
    pub async fn execute(&self) -> Result<()> {
        Err(crate::error::Error::Server(
            "block verify-signature requires crypto verification infrastructure (not yet implemented)".to_string(),
        ))
    }
}
