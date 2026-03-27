//! Document command implementation

use std::path::PathBuf;

use clap::{Args, Subcommand};
use serde_json::Value as JsonValue;

use super::http_client::HttpClient;
use super::{
    escape_graphql_string, get_data_from_args, json_to_graphql_input, validate_identifier,
    ClientContext,
};
use crate::commands::client::collection::introspection::get_collection_fields;
use crate::error::{Error, Result};

/// Interact with documents
#[derive(Args, Debug)]
pub struct DocumentArgs {
    /// Collection name
    #[arg(long = "collection-name", alias = "name", global = true)]
    pub collection_name: Option<String>,

    /// Collection ID
    #[arg(long, global = true)]
    pub collection_id: Option<String>,

    /// Schema version ID
    #[arg(long, global = true)]
    pub version_id: Option<String>,

    /// Get inactive collections
    #[arg(long, global = true)]
    pub get_inactive: bool,

    #[command(subcommand)]
    pub command: DocumentCommand,
}

/// Document subcommands
#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum DocumentCommand {
    /// Add a new document
    Add(DocumentAddArgs),
    /// Delete a document
    Delete(DocumentDeleteArgs),
    /// Get a document by ID
    Get(DocumentGetArgs),
    /// Update a document
    Update(DocumentUpdateArgs),
}

/// Arguments for document add command
#[derive(Args, Debug)]
pub struct DocumentAddArgs {
    /// The document data (JSON)
    #[arg(value_name = "DOCUMENT")]
    pub document: Option<String>,

    /// File containing document(s)
    #[arg(long, short = 'f')]
    pub file: Option<PathBuf>,

    /// Flag to enable encryption of the document
    #[arg(long, short = 'e')]
    pub encrypt: bool,

    /// Comma-separated list of fields to encrypt
    #[arg(long, value_delimiter = ',')]
    pub encrypt_fields: Vec<String>,
}

/// Arguments for document get command
#[derive(Args, Debug)]
pub struct DocumentGetArgs {
    /// The document ID
    #[arg(value_name = "DOC_ID")]
    pub doc_id: String,

    /// Show deleted documents
    #[arg(long)]
    pub show_deleted: bool,
}

/// Arguments for document update command
#[derive(Args, Debug)]
pub struct DocumentUpdateArgs {
    /// Document ID
    #[arg(long = "docID")]
    pub doc_id: Option<String>,

    /// Document filter
    #[arg(long)]
    pub filter: Option<String>,

    /// Document updater
    #[arg(long)]
    pub updater: Option<String>,
}

/// Arguments for document delete command
#[derive(Args, Debug)]
pub struct DocumentDeleteArgs {
    /// Document ID
    #[arg(long = "docID")]
    pub doc_id: Option<String>,

    /// Document filter
    #[arg(long)]
    pub filter: Option<String>,
}

impl DocumentArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            DocumentCommand::Add(args) => args.execute(ctx, self.collection_name.as_deref()).await,
            DocumentCommand::Delete(args) => {
                args.execute(ctx, self.collection_name.as_deref()).await
            }
            DocumentCommand::Get(args) => args.execute(ctx, self.collection_name.as_deref()).await,
            DocumentCommand::Update(args) => {
                args.execute(ctx, self.collection_name.as_deref()).await
            }
        }
    }
}

impl DocumentAddArgs {
    pub async fn execute(&self, ctx: &ClientContext, name: Option<&str>) -> Result<()> {
        let collection = name.ok_or_else(|| {
            Error::MissingInput("--collection-name is required for add".to_string())
        })?;
        validate_identifier(collection)?;

        let data = get_data_from_args(&self.document, &self.file)?;
        let parsed: JsonValue = serde_json::from_str(&data)?;

        let input_str = json_to_graphql_input(&parsed);
        let query = format!(
            r#"mutation {{ add_{collection}(input: {input}) {{ _docID }} }}"#,
            collection = collection,
            input = input_str
        );

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);
        let response = client.graphql(&query, None, ctx.tx_id.clone()).await?;

        if response.has_errors() {
            return Err(Error::Server(response.error_message()));
        }

        let data = response
            .data
            .ok_or_else(|| Error::Server("Server returned success but with no data".to_string()))?;

        let key = format!("add_{}", collection);
        let result = data.get(&key).ok_or_else(|| {
            Error::Server(format!(
                "Server response missing expected key '{}'. Response: {}",
                key,
                serde_json::to_string_pretty(&data).unwrap_or_else(|_| data.to_string())
            ))
        })?;
        let output = serde_json::to_string_pretty(result)?;
        println!("{output}");

        Ok(())
    }
}

impl DocumentGetArgs {
    pub async fn execute(&self, ctx: &ClientContext, name: Option<&str>) -> Result<()> {
        let collection = name.ok_or_else(|| {
            Error::MissingInput("--collection-name is required for get".to_string())
        })?;
        validate_identifier(collection)?;

        let fields = get_collection_fields(ctx, collection).await?;
        let field_selection = fields.join(" ");
        let escaped_doc_id = escape_graphql_string(&self.doc_id);

        let query = format!(
            r#"{{ {collection}(filter: {{_docID: {{_eq: "{doc_id}"}}}}) {{ {fields} }} }}"#,
            collection = collection,
            doc_id = escaped_doc_id,
            fields = field_selection
        );

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);
        let response = client.graphql(&query, None, ctx.tx_id.clone()).await?;

        if response.has_errors() {
            return Err(Error::Server(response.error_message()));
        }

        let data = response
            .data
            .ok_or_else(|| Error::Server("Server returned no data".to_string()))?;

        let results = data.get(collection).ok_or_else(|| {
            Error::Server(format!(
                "Server response missing collection key '{}'",
                collection
            ))
        })?;

        let arr = results.as_array().ok_or_else(|| {
            Error::Server(format!(
                "Expected array for collection '{}', got: {}",
                collection, results
            ))
        })?;

        if arr.is_empty() {
            println!("Document not found");
        } else if arr.len() == 1 {
            let output = serde_json::to_string_pretty(&arr[0])?;
            println!("{output}");
        } else {
            let output = serde_json::to_string_pretty(results)?;
            println!("{output}");
        }

        Ok(())
    }
}

impl DocumentUpdateArgs {
    pub async fn execute(&self, ctx: &ClientContext, name: Option<&str>) -> Result<()> {
        let collection = name.ok_or_else(|| {
            Error::MissingInput("--collection-name is required for update".to_string())
        })?;
        validate_identifier(collection)?;

        if let Some(ref doc_id) = self.doc_id {
            let updater = self.updater.as_deref().ok_or_else(|| {
                Error::MissingInput("--updater is required for update".to_string())
            })?;

            let client = HttpClient::new(&ctx.url)?
                .with_auth_token(ctx.auth_token.clone())
                .with_verbose(ctx.verbose);

            client
                .collection_update_doc(collection, doc_id, updater)
                .await?;
        } else {
            eprintln!("filter-based update not yet supported");
        }
        Ok(())
    }
}

impl DocumentDeleteArgs {
    pub async fn execute(&self, ctx: &ClientContext, name: Option<&str>) -> Result<()> {
        let collection = name.ok_or_else(|| {
            Error::MissingInput("--collection-name is required for delete".to_string())
        })?;
        validate_identifier(collection)?;

        if let Some(ref doc_id) = self.doc_id {
            let client = HttpClient::new(&ctx.url)?
                .with_auth_token(ctx.auth_token.clone())
                .with_verbose(ctx.verbose);

            client.collection_delete_doc(collection, doc_id).await?;
            println!("Deleted document {}", doc_id);
        } else {
            eprintln!("filter-based delete not yet supported");
        }
        Ok(())
    }
}
