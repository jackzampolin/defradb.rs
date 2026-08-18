//! Collection operation command implementations

use super::{CollectionDeleteArgs, CollectionPatchArgs, SetActiveArgs, TruncateArgs};
use crate::commands::client::http_client::HttpClient;
use crate::commands::client::ClientContext;
use crate::commands::client::{get_data_from_args, validate_identifier};
use crate::error::{Error, Result};

impl CollectionPatchArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let patch = get_data_from_args(&self.patch, &self.patch_file)?;
        let migration = if self.migration.is_some() || self.lens_file.is_some() {
            Some(get_data_from_args(&self.migration, &self.lens_file)?)
        } else {
            None
        };

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client
            .collection_patch(&patch, migration.as_deref())
            .await?;
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

impl CollectionDeleteArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let names: Vec<String> = self
            .names
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if names.is_empty() {
            return Err(Error::MissingInput(
                "at least one collection name is required".to_string(),
            ));
        }

        for name in &names {
            validate_identifier(name)?;
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.collection_delete(&names, self.active_only).await?;

        if names.len() == 1 {
            println!("Deleted collection {}", names[0]);
        } else {
            println!("Deleted collections {}", names.join(", "));
        }
        Ok(())
    }
}
