//! Collection operation command implementations

use super::{CollectionPatchArgs, SetActiveArgs, TruncateArgs};
use crate::commands::client::http_client::HttpClient;
use crate::commands::client::ClientContext;
use crate::commands::client::{get_data_from_args, validate_identifier};
use crate::error::{Error, Result};

impl CollectionPatchArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let patch = get_data_from_args(&self.patch, &self.patch_file)?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.collection_patch(&patch).await?;
        Ok(())
    }
}

impl SetActiveArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client
            .collection_set_active(self.version_id.as_deref())
            .await?;
        Ok(())
    }
}

impl TruncateArgs {
    pub async fn execute(&self, ctx: &ClientContext, name: Option<&str>) -> Result<()> {
        let collection =
            name.ok_or_else(|| Error::MissingInput("--name is required for truncate".to_string()))?;
        validate_identifier(collection)?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.collection_truncate(collection).await?;
        println!("Truncated collection {}", collection);
        Ok(())
    }
}
