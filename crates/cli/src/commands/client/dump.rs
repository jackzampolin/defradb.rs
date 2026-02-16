use clap::Args;

use super::http_client::HttpClient;
use super::ClientContext;
use crate::error::Result;

/// Dump the database contents
#[derive(Args, Debug)]
pub struct DumpArgs {}

impl DumpArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.dump().await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}
