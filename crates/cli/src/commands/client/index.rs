//! Index command implementation for database index management

use clap::{Args, Subcommand};

use super::http_client::HttpClient;
use super::{validate_identifier, ClientContext};
use crate::error::Result;

/// Manage database indexes
#[derive(Args, Debug)]
pub struct IndexArgs {
    #[command(subcommand)]
    pub command: IndexCommand,
}

/// Index subcommands
#[derive(Subcommand, Debug)]
pub enum IndexCommand {
    /// Create a new index on a collection
    Create(IndexCreateArgs),
    /// List indexes (optionally filtered by collection)
    List(IndexListArgs),
    /// Delete an index by name
    Delete(IndexDeleteArgs),
}

/// Arguments for index create command
#[derive(Args, Debug)]
pub struct IndexCreateArgs {
    /// The collection to create the index on
    #[arg(long, short = 'c')]
    pub collection: String,

    /// Field(s) to index (comma-separated or multiple --fields)
    #[arg(long, short = 'f', required = true, value_delimiter = ',')]
    pub fields: Vec<String>,

    /// Optional name for the index (auto-generated if not provided)
    #[arg(long, short = 'n')]
    pub name: Option<String>,

    /// Create a unique index
    #[arg(long)]
    pub unique: bool,
}

/// Arguments for index list command
#[derive(Args, Debug)]
pub struct IndexListArgs {
    /// Optional collection to filter indexes
    #[arg(long, short = 'c')]
    pub collection: Option<String>,
}

/// Arguments for index delete command
#[derive(Args, Debug)]
pub struct IndexDeleteArgs {
    /// The collection containing the index
    #[arg(long, short = 'c')]
    pub collection: String,

    /// The name of the index to delete
    #[arg(long, short = 'n')]
    pub name: String,
}

impl IndexArgs {
    /// Execute the index command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            IndexCommand::Create(args) => args.execute(ctx).await,
            IndexCommand::List(args) => args.execute(ctx).await,
            IndexCommand::Delete(args) => args.execute(ctx).await,
        }
    }
}

impl IndexCreateArgs {
    /// Execute the index create command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        validate_identifier(&self.collection)?;
        for field in &self.fields {
            validate_identifier(field)?;
        }
        if let Some(ref name) = self.name {
            validate_identifier(name)?;
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let response = client
            .index_create(
                &self.collection,
                &self.fields,
                self.name.as_deref(),
                self.unique,
            )
            .await?;

        println!("{}", serde_json::to_string_pretty(&response)?);
        Ok(())
    }
}

impl IndexListArgs {
    /// Execute the index list command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        if let Some(ref col) = self.collection {
            validate_identifier(col)?;
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let indexes = client.index_list(self.collection.as_deref()).await?;
        println!("{}", serde_json::to_string_pretty(&indexes)?);
        Ok(())
    }
}

impl IndexDeleteArgs {
    /// Execute the index delete command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        validate_identifier(&self.collection)?;
        validate_identifier(&self.name)?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.index_delete(&self.collection, &self.name).await?;
        println!(
            "Index '{}' deleted from collection '{}'",
            self.name, self.collection
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_create_args() {
        let args = IndexCreateArgs {
            collection: "Users".to_string(),
            fields: vec!["name".to_string(), "email".to_string()],
            name: Some("idx_name_email".to_string()),
            unique: true,
        };
        assert_eq!(args.collection, "Users");
        assert_eq!(args.fields.len(), 2);
        assert!(args.unique);
    }

    #[test]
    fn test_index_create_args_minimal() {
        let args = IndexCreateArgs {
            collection: "Users".to_string(),
            fields: vec!["name".to_string()],
            name: None,
            unique: false,
        };
        assert!(args.name.is_none());
        assert!(!args.unique);
    }

    #[test]
    fn test_index_list_args_all() {
        let args = IndexListArgs { collection: None };
        assert!(args.collection.is_none());
    }

    #[test]
    fn test_index_list_args_filtered() {
        let args = IndexListArgs {
            collection: Some("Users".to_string()),
        };
        assert_eq!(args.collection.as_deref(), Some("Users"));
    }

    #[test]
    fn test_index_delete_args() {
        let args = IndexDeleteArgs {
            collection: "Users".to_string(),
            name: "idx_name".to_string(),
        };
        assert_eq!(args.collection, "Users");
        assert_eq!(args.name, "idx_name");
    }
}
