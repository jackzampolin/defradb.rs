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

    /// Make a vector index (on a single field, never unique). The value is the
    /// index config as JSON, e.g. '{"Metric":"COSINE","Dimensions":3,"HNSW":{}}'.
    ///
    /// Dimensions is the vector length and must be greater than zero. Metric is
    /// the distance metric, one of COSINE, EUCLIDEAN or DOT, and cannot be
    /// changed later without dropping and recreating the index. The algorithm
    /// is chosen by which config block is given, or by "Algorithm"; HNSW is the
    /// default, and its tuning params (M, EfConstruction, EfSearch) default
    /// when omitted.
    #[arg(long, value_name = "JSON")]
    pub vector: Option<String>,
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

impl IndexNewArgs {
    /// The vector configuration this request carries, if any.
    ///
    /// Deserialized through the same `schema::VectorIndexDescription` serde the
    /// wire uses, so the flag cannot accept a shape the API would reject, nor
    /// drift from it. That is also why the JSON is the reference's spelling
    /// rather than a friendlier one of our own: a script written against either
    /// runtime runs on both.
    pub fn vector_description(&self) -> Result<Option<schema::VectorIndexDescription>> {
        let Some(json) = self.vector.as_deref() else {
            return Ok(None);
        };
        let value: serde_json::Value = serde_json::from_str(json)
            .map_err(|err| Error::InvalidVectorIndexConfig(err.to_string()))?;
        // Serde will happily read a struct from a JSON sequence, taking fields
        // positionally, so `[]` would otherwise become an all-default config
        // rather than an error. The reference unmarshals into a struct, which
        // rejects anything but an object, and this is the trust boundary where
        // that difference is visible.
        if !value.is_object() {
            return Err(Error::InvalidVectorIndexConfig(
                "expected a JSON object".to_string(),
            ));
        }
        serde_json::from_value(value)
            .map(Some)
            .map_err(|err| Error::InvalidVectorIndexConfig(err.to_string()))
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
