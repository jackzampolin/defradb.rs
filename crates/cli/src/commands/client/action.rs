use clap::{Args, Subcommand};

use super::http_client::HttpClient;
use super::ClientContext;
use crate::error::Result;

/// Inspect long-running database actions.
#[derive(Args, Debug)]
pub struct ActionArgs {
    #[command(subcommand)]
    pub command: ActionCommand,
}

#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum ActionCommand {
    /// List actions that are running or ended with an error.
    List,
}

impl ActionArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match self.command {
            ActionCommand::List => {
                let client = HttpClient::new(&ctx.url)?
                    .with_auth_token(ctx.auth_token.clone())
                    .with_verbose(ctx.verbose);
                println!(
                    "{}",
                    serde_json::to_string_pretty(&client.action_list().await?)?
                );
                Ok(())
            }
        }
    }
}
