//! Index command implementation for database index management

use clap::{Args, Subcommand};

use super::http_client::HttpClient;
use super::{validate_identifier, ClientContext};
use crate::error::{Error, Result};

/// Manage database indexes
#[derive(Args, Debug)]
pub struct IndexArgs {
    #[command(subcommand)]
    pub command: IndexCommand,
}

/// Index subcommands
#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum IndexCommand {
    /// Make a new index on a collection
    New(IndexNewArgs),
    /// List indexes (optionally filtered by collection)
    List(IndexListArgs),
    /// Delete an index by name
    Delete(IndexDeleteArgs),
}

/// Arguments for index new command
#[derive(Args, Debug)]
pub struct IndexNewArgs {
    /// The collection to make the index on
    #[arg(long, short = 'c')]
    pub collection: String,

    /// Field(s) to index (comma-separated or multiple --fields)
    #[arg(long, short = 'f', required = true, value_delimiter = ',')]
    pub fields: Vec<String>,

    /// Optional name for the index (auto-generated if not provided)
    #[arg(long, short = 'n')]
    pub name: Option<String>,

    /// Make a unique index
    #[arg(long)]
    pub unique: bool,

    /// Make a vector index, searchable by SIMILARITY
    #[arg(long)]
    pub vector: bool,

    /// Vector length. Omit on an `@embedding` field, where the model fixes it.
    #[arg(long, requires = "vector")]
    pub vector_dimensions: Option<u32>,

    /// Vector index algorithm
    #[arg(
        long,
        requires = "vector",
        default_value = "HNSW",
        value_parser = algorithm_names()
    )]
    pub vector_algorithm: String,

    /// How the index ranks. SIMILARITY ranks by dot product, so only a DOT
    /// index can serve it; a COSINE index is built but never routed to.
    #[arg(
        long,
        requires = "vector",
        default_value = "COSINE",
        value_parser = metric_names()
    )]
    pub vector_metric: String,
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
            IndexCommand::New(args) => args.execute(ctx).await,
            IndexCommand::List(args) => args.execute(ctx).await,
            IndexCommand::Delete(args) => args.execute(ctx).await,
        }
    }
}

/// Accepted `--vector-algorithm` values, taken from the enum so a new engine
/// becomes selectable without touching the CLI.
fn algorithm_names() -> clap::builder::PossibleValuesParser {
    clap::builder::PossibleValuesParser::new(
        schema::VectorAlgorithm::ALL
            .iter()
            .map(|algorithm| algorithm.as_str())
            .collect::<Vec<_>>(),
    )
}

/// Accepted `--vector-metric` values, from the enum for the same reason.
fn metric_names() -> clap::builder::PossibleValuesParser {
    clap::builder::PossibleValuesParser::new(
        schema::DistanceMetric::ALL
            .iter()
            .map(|metric| metric.as_str())
            .collect::<Vec<_>>(),
    )
}

impl IndexNewArgs {
    /// The vector configuration this request carries, if any.
    pub fn vector_description(&self) -> Result<Option<schema::VectorIndexDescription>> {
        if !self.vector {
            return Ok(None);
        }

        let algorithm = schema::VectorAlgorithm::from_sdl_name(&self.vector_algorithm)
            .ok_or_else(|| Error::InvalidIdentifier(self.vector_algorithm.clone()))?;
        let metric = schema::DistanceMetric::from_sdl_name(&self.vector_metric)
            .ok_or_else(|| Error::InvalidIdentifier(self.vector_metric.clone()))?;

        Ok(Some(schema::VectorIndexDescription::with_defaults(
            algorithm,
            metric,
            self.vector_dimensions.unwrap_or(0),
        )))
    }

    /// Execute the index new command
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
                self.vector_description()?,
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
    fn test_index_new_args() {
        let args = IndexNewArgs {
            collection: "Users".to_string(),
            fields: vec!["name".to_string(), "email".to_string()],
            name: Some("idx_name_email".to_string()),
            unique: true,
            vector: false,
            vector_dimensions: None,
            vector_algorithm: "HNSW".to_string(),
            vector_metric: "COSINE".to_string(),
        };
        assert_eq!(args.collection, "Users");
        assert_eq!(args.fields.len(), 2);
        assert!(args.unique);
    }

    #[test]
    fn test_index_new_args_minimal() {
        let args = IndexNewArgs {
            collection: "Users".to_string(),
            fields: vec!["name".to_string()],
            name: None,
            unique: false,
            vector: false,
            vector_dimensions: None,
            vector_algorithm: "HNSW".to_string(),
            vector_metric: "COSINE".to_string(),
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
