//! Node-level ACP command implementations

use clap::{Args, Subcommand};

use crate::commands::client::http_client::HttpClient;
use crate::commands::client::ClientContext;
use crate::error::Result;

/// ACP node subcommands
#[derive(Args, Debug)]
pub struct AcpNodeArgs {
    #[command(subcommand)]
    pub command: AcpNodeCommand,
}

/// ACP node subcommands
#[derive(Subcommand, Debug)]
pub enum AcpNodeCommand {
    /// Manage node ACP relationships
    Relationship(AcpNodeRelationshipArgs),
    /// Show ACP status
    Status(AcpNodeStatusArgs),
    /// Disable node ACP
    Disable(AcpNodeDisableArgs),
    /// Re-enable node ACP
    ReEnable(AcpNodeReEnableArgs),
}

/// ACP node relationship subcommands
#[derive(Args, Debug)]
pub struct AcpNodeRelationshipArgs {
    #[command(subcommand)]
    pub command: AcpNodeRelationshipCommand,
}

/// ACP node relationship subcommands
#[derive(Subcommand, Debug)]
pub enum AcpNodeRelationshipCommand {
    /// Add a node ACP relationship
    Add(AcpNodeRelationshipAddArgs),
    /// Delete a node ACP relationship
    Delete(AcpNodeRelationshipDeleteArgs),
}

/// Arguments for acp node relationship add command
#[derive(Args, Debug)]
pub struct AcpNodeRelationshipAddArgs {
    /// Relation name
    #[arg(long, short = 'r')]
    pub relation: Option<String>,

    /// Actor (target identity)
    #[arg(long, short = 'a')]
    pub actor: Option<String>,
}

/// Arguments for acp node relationship delete command
#[derive(Args, Debug)]
pub struct AcpNodeRelationshipDeleteArgs {
    /// Relation name
    #[arg(long, short = 'r')]
    pub relation: Option<String>,

    /// Actor (target identity)
    #[arg(long, short = 'a')]
    pub actor: Option<String>,
}

/// Arguments for acp node status command
#[derive(Args, Debug)]
pub struct AcpNodeStatusArgs {}

/// Arguments for acp node disable command
#[derive(Args, Debug)]
pub struct AcpNodeDisableArgs {}

/// Arguments for acp node re-enable command
#[derive(Args, Debug)]
pub struct AcpNodeReEnableArgs {}

impl AcpNodeArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            AcpNodeCommand::Relationship(args) => args.execute(ctx).await,
            AcpNodeCommand::Status(args) => args.execute(ctx).await,
            AcpNodeCommand::Disable(args) => args.execute(ctx).await,
            AcpNodeCommand::ReEnable(args) => args.execute(ctx).await,
        }
    }
}

impl AcpNodeRelationshipArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            AcpNodeRelationshipCommand::Add(args) => args.execute(ctx).await,
            AcpNodeRelationshipCommand::Delete(args) => args.execute(ctx).await,
        }
    }
}

impl AcpNodeRelationshipAddArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let relation = self.relation.as_deref().ok_or_else(|| {
            crate::error::Error::MissingInput("--relation is required".to_string())
        })?;
        let actor = self
            .actor
            .as_deref()
            .ok_or_else(|| crate::error::Error::MissingInput("--actor is required".to_string()))?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.nac_add_relationship(relation, actor).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl AcpNodeRelationshipDeleteArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let relation = self.relation.as_deref().ok_or_else(|| {
            crate::error::Error::MissingInput("--relation is required".to_string())
        })?;
        let actor = self
            .actor
            .as_deref()
            .ok_or_else(|| crate::error::Error::MissingInput("--actor is required".to_string()))?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.nac_remove_relationship(relation, actor).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl AcpNodeStatusArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.nac_status().await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl AcpNodeDisableArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.nac_disable().await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl AcpNodeReEnableArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.nac_re_enable().await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::super::document::AcpPolicyAddArgs;

    #[test]
    fn test_acp_policy_add_args_with_data() {
        let args = AcpPolicyAddArgs {
            policy: Some("test policy".to_string()),
            file: None,
        };
        assert!(args.policy.is_some());
        assert!(args.file.is_none());
    }

    #[test]
    fn test_acp_policy_add_args_with_file() {
        let args = AcpPolicyAddArgs {
            policy: None,
            file: Some(PathBuf::from("policy.yaml")),
        };
        assert!(args.policy.is_none());
        assert!(args.file.is_some());
    }
}
