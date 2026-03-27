//! Document-level ACP command implementations

use std::path::PathBuf;

use clap::{Args, Subcommand};

use crate::commands::client::http_client::HttpClient;
use crate::commands::client::{get_data_from_args, ClientContext};
use crate::error::Result;

/// ACP document subcommands
#[derive(Args, Debug)]
pub struct AcpDocumentArgs {
    #[command(subcommand)]
    pub command: AcpDocumentCommand,
}

/// ACP document subcommands
#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum AcpDocumentCommand {
    /// Manage ACP policies
    Policy(AcpPolicyArgs),
    /// Manage ACP relationships
    Relationship(AcpDocumentRelationshipArgs),
}

/// ACP policy subcommands
#[derive(Args, Debug)]
pub struct AcpPolicyArgs {
    #[command(subcommand)]
    pub command: AcpPolicyCommand,
}

/// ACP policy subcommands
#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum AcpPolicyCommand {
    /// Add a new ACP policy
    Add(AcpPolicyAddArgs),
}

/// Arguments for acp policy add command
#[derive(Args, Debug)]
pub struct AcpPolicyAddArgs {
    /// The policy definition (YAML or JSON format)
    #[arg(value_name = "POLICY")]
    pub policy: Option<String>,

    /// Read policy from file
    #[arg(long, short = 'f', value_name = "FILE")]
    pub file: Option<PathBuf>,
}

/// ACP document relationship subcommands
#[derive(Args, Debug)]
pub struct AcpDocumentRelationshipArgs {
    #[command(subcommand)]
    pub command: AcpDocumentRelationshipCommand,
}

/// ACP document relationship subcommands
#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum AcpDocumentRelationshipCommand {
    /// Add a document ACP relationship
    Add(AcpDocumentRelationshipAddArgs),
    /// Delete a document ACP relationship
    Delete(AcpDocumentRelationshipDeleteArgs),
}

/// Arguments for acp document relationship add command
#[derive(Args, Debug)]
pub struct AcpDocumentRelationshipAddArgs {
    /// Collection name
    #[arg(long, short = 'c')]
    pub collection: Option<String>,

    /// Document ID
    #[arg(long = "docID")]
    pub doc_id: Option<String>,

    /// Relation name
    #[arg(long, short = 'r')]
    pub relation: Option<String>,

    /// Actor (target identity)
    #[arg(long, short = 'a')]
    pub actor: Option<String>,
}

/// Arguments for acp document relationship delete command
#[derive(Args, Debug)]
pub struct AcpDocumentRelationshipDeleteArgs {
    /// Collection name
    #[arg(long, short = 'c')]
    pub collection: Option<String>,

    /// Document ID
    #[arg(long = "docID")]
    pub doc_id: Option<String>,

    /// Relation name
    #[arg(long, short = 'r')]
    pub relation: Option<String>,

    /// Actor (target identity)
    #[arg(long, short = 'a')]
    pub actor: Option<String>,
}

impl AcpDocumentArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            AcpDocumentCommand::Policy(args) => args.execute(ctx).await,
            AcpDocumentCommand::Relationship(args) => args.execute(ctx).await,
        }
    }
}

impl AcpPolicyArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            AcpPolicyCommand::Add(args) => args.execute(ctx).await,
        }
    }
}

impl AcpPolicyAddArgs {
    /// Execute the acp policy add command
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

impl AcpDocumentRelationshipArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            AcpDocumentRelationshipCommand::Add(args) => args.execute(ctx).await,
            AcpDocumentRelationshipCommand::Delete(args) => args.execute(ctx).await,
        }
    }
}

impl AcpDocumentRelationshipAddArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let collection = self.collection.as_deref().ok_or_else(|| {
            crate::error::Error::MissingInput("--collection is required".to_string())
        })?;
        let doc_id = self
            .doc_id
            .as_deref()
            .ok_or_else(|| crate::error::Error::MissingInput("--docID is required".to_string()))?;
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

        let result = client
            .acp_doc_relationship_add(collection, doc_id, relation, actor)
            .await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl AcpDocumentRelationshipDeleteArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let collection = self.collection.as_deref().ok_or_else(|| {
            crate::error::Error::MissingInput("--collection is required".to_string())
        })?;
        let doc_id = self
            .doc_id
            .as_deref()
            .ok_or_else(|| crate::error::Error::MissingInput("--docID is required".to_string()))?;
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

        let result = client
            .acp_doc_relationship_delete(collection, doc_id, relation, actor)
            .await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}
