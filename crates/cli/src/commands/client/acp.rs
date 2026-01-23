
//! ACP (Access Control Policy) command implementation

use std::path::PathBuf;

use clap::{Args, Subcommand};

use super::http_client::HttpClient;
use super::{get_data_from_args, ClientContext};
use crate::error::{Error, Result};

/// Interact with Access Control Policies
#[derive(Args, Debug)]
pub struct AcpArgs {
    #[command(subcommand)]
    pub command: AcpCommand,
}

/// ACP subcommands
#[derive(Subcommand, Debug)]
pub enum AcpCommand {
    /// Add a new ACP policy
    Add(AcpAddArgs),
    /// List all ACP policies
    List(AcpListArgs),
    /// Show details of a specific policy
    Describe(AcpDescribeArgs),
}

/// Arguments for acp add command
#[derive(Args, Debug)]
pub struct AcpAddArgs {
    /// The policy definition (YAML or JSON format)
    #[arg(value_name = "POLICY")]
    pub policy: Option<String>,

    /// Read policy from file
    #[arg(long, short = 'f', value_name = "FILE")]
    pub file: Option<PathBuf>,
}

/// Arguments for acp list command
#[derive(Args, Debug)]
pub struct AcpListArgs {}

/// Arguments for acp describe command
#[derive(Args, Debug)]
pub struct AcpDescribeArgs {
    /// The policy ID
    #[arg(value_name = "ID")]
    pub id: String,
}

impl AcpArgs {
    /// Execute the ACP command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            AcpCommand::Add(args) => args.execute(ctx).await,
            AcpCommand::List(args) => args.execute(ctx).await,
            AcpCommand::Describe(args) => args.execute(ctx).await,
        }
    }
}

impl AcpAddArgs {
    /// Execute the acp add command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let policy = get_data_from_args(&self.policy, &self.file)?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let response = client.acp_add_policy(&policy).await?;
        println!("{}", serde_json::to_string_pretty(&response)?);
        Ok(())
    }
}

impl AcpListArgs {
    /// Execute the acp list command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let policies = client.acp_list_policies().await?;
        println!("{}", serde_json::to_string_pretty(&policies)?);
        Ok(())
    }
}

impl AcpDescribeArgs {
    /// Execute the acp describe command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        // Validate policy ID is not empty
        if self.id.trim().is_empty() {
            return Err(Error::InvalidIdentifier(
                "policy ID cannot be empty".to_string(),
            ));
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let policy = client.acp_get_policy(&self.id).await?;
        println!("{}", serde_json::to_string_pretty(&policy)?);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acp_add_args_with_data() {
        let args = AcpAddArgs {
            policy: Some("test policy".to_string()),
            file: None,
        };
        assert!(args.policy.is_some());
        assert!(args.file.is_none());
    }

    #[test]
    fn test_acp_add_args_with_file() {
        let args = AcpAddArgs {
            policy: None,
            file: Some(PathBuf::from("policy.yaml")),
        };
        assert!(args.policy.is_none());
        assert!(args.file.is_some());
    }

    #[test]
    fn test_acp_describe_args() {
        let args = AcpDescribeArgs {
            id: "policy123".to_string(),
        };
        assert_eq!(args.id, "policy123");
    }
}
