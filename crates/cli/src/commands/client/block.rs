//! Block command implementation

use clap::{Args, Subcommand};

use super::http_client::HttpClient;
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
#[non_exhaustive]
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
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            BlockCommand::VerifySignature(args) => args.execute(ctx).await,
        }
    }
}

impl BlockVerifySignatureArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client
            .block_verify_signature(&self.cid, &self.public_key, self.key_type.as_deref())
            .await?;

        println!("Block's signature verified.");
        Ok(())
    }
}
