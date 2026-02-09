//! Node identity command implementation

use clap::Args;

use super::http_client::HttpClient;
use super::ClientContext;
use crate::error::Result;

/// Get the node's identity
#[derive(Args, Debug)]
pub struct NodeIdentityArgs {}

impl NodeIdentityArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.node_identity().await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}
