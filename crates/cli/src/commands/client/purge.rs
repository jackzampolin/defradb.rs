//! Purge command implementation

use clap::Args;

use super::http_client::HttpClient;
use super::ClientContext;
use crate::error::Result;

/// Purge all database data
#[derive(Args, Debug)]
pub struct PurgeArgs {
    /// Force purge without confirmation (required)
    #[arg(long, short = 'f', required = true)]
    pub force: bool,
}

impl PurgeArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        if !self.force {
            return Err(crate::error::Error::MissingInput(
                "--force is required to purge".to_string(),
            ));
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.purge().await?;
        println!("Database purged successfully");
        Ok(())
    }
}
