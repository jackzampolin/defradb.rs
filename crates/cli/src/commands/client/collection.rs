//! Collection command implementation

use std::path::PathBuf;

use clap::{Args, Subcommand};
use serde_json::Value as JsonValue;

use super::http_client::HttpClient;
use super::{escape_graphql_string, get_data_from_args, validate_identifier, ClientContext};
use crate::error::{Error, Result};

/// Interact with collections and documents
#[derive(Args, Debug)]
pub struct CollectionArgs {
    /// Collection name
    #[arg(long, global = true)]
    pub name: Option<String>,

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
    pub command: CollectionCommand,
}

/// Collection subcommands
#[derive(Subcommand, Debug)]
pub enum CollectionCommand {
    /// Create a new document
    Create(DocumentCreateArgs),
    /// Delete a document
    Delete(DocumentDeleteArgs),
    /// Describe a collection's schema
    Describe(CollectionDescribeArgs),
    /// Get document IDs
    DocIds(DocIdsArgs),
    /// Get a document by ID
    Get(DocumentGetArgs),
    /// List all collections
    List(CollectionListArgs),
    /// Patch a collection schema
    Patch(CollectionPatchArgs),
    /// Set a collection as active
    SetActive(SetActiveArgs),
    /// Truncate a collection
    Truncate(TruncateArgs),
    /// Update a document
    Update(DocumentUpdateArgs),
}

/// Arguments for collection list command
#[derive(Args, Debug)]
pub struct CollectionListArgs {}

/// Arguments for collection describe command
#[derive(Args, Debug)]
pub struct CollectionDescribeArgs {}

/// Arguments for document create command
#[derive(Args, Debug)]
pub struct DocumentCreateArgs {
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

/// Arguments for doc-ids command
#[derive(Args, Debug)]
pub struct DocIdsArgs {}

/// Arguments for collection patch command
#[derive(Args, Debug)]
pub struct CollectionPatchArgs {
    /// The patch data (JSON)
    #[arg(value_name = "PATCH")]
    pub patch: Option<String>,

    /// The migration configuration
    #[arg(value_name = "MIGRATION")]
    pub migration: Option<String>,

    /// File to load a patch from
    #[arg(long, short = 'p')]
    pub patch_file: Option<PathBuf>,

    /// File to load a lens config from
    #[arg(long, short = 't')]
    pub lens_file: Option<PathBuf>,
}

/// Arguments for set-active command
#[derive(Args, Debug)]
pub struct SetActiveArgs {
    /// Collection version ID to set as active
    #[arg(value_name = "VERSION_ID")]
    pub version_id: Option<String>,
}

/// Arguments for truncate command
#[derive(Args, Debug)]
pub struct TruncateArgs {}

impl CollectionArgs {
    /// Execute the collection command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            CollectionCommand::Create(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::Delete(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::Describe(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::DocIds(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::Get(args) => args.execute(ctx, self.name.as_deref()).await,
            CollectionCommand::List(args) => args.execute(ctx).await,
            CollectionCommand::Patch(args) => args.execute(ctx).await,
            CollectionCommand::SetActive(args) => args.execute(ctx).await,
            CollectionCommand::Truncate(args) => args.execute(ctx).await,
            CollectionCommand::Update(args) => args.execute(ctx, self.name.as_deref()).await,
        }
    }
}

/// Introspection query to list collections
const INTROSPECTION_QUERY: &str = r#"
{
  __schema {
    queryType {
      fields {
        name
      }
    }
  }
}
"#;

/// Check if a field name is a built-in GraphQL or DefraDB field.
fn is_builtin_field(name: &str) -> bool {
    if name.starts_with("__") {
        return true;
    }

    let lower = name.to_lowercase();
    if lower == "commits" || lower.ends_with("commits") {
        return true;
    }

    false
}

impl CollectionListArgs {
    /// Execute the collection list command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);
        let response = client
            .graphql(INTROSPECTION_QUERY, None, ctx.tx_id.clone())
            .await?;

        if response.has_errors() {
            return Err(Error::Server(response.error_message()));
        }

        let collections = extract_collections(&response.data)?;
        for name in collections {
            println!("{name}");
        }

        Ok(())
    }
}

impl CollectionDescribeArgs {
    /// Execute the collection describe command
    pub async fn execute(&self, ctx: &ClientContext, name: Option<&str>) -> Result<()> {
        let collection_name =
            name.ok_or_else(|| Error::MissingInput("--name is required for describe".to_string()))?;
        validate_identifier(collection_name)?;

        let query = format!(
            r#"
{{
  __type(name: "{name}") {{
    name
    fields {{
      name
      type {{
        name
        kind
        ofType {{
          name
          kind
        }}
      }}
    }}
  }}
}}
"#,
            name = collection_name
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

        let type_info = data
            .get("__type")
            .ok_or_else(|| Error::CollectionNotFound(collection_name.to_string()))?;

        if type_info.is_null() {
            return Err(Error::CollectionNotFound(collection_name.to_string()));
        }

        let output = serde_json::to_string_pretty(type_info)?;
        println!("{output}");

        Ok(())
    }
}

impl DocumentCreateArgs {
    /// Execute the document create command
    pub async fn execute(&self, ctx: &ClientContext, name: Option<&str>) -> Result<()> {
        let collection =
            name.ok_or_else(|| Error::MissingInput("--name is required for create".to_string()))?;
        validate_identifier(collection)?;

        let data = get_data_from_args(&self.document, &self.file)?;
        let parsed: JsonValue = serde_json::from_str(&data)?;

        let input_str = serde_json::to_string(&parsed)?;
        let query = format!(
            r#"mutation {{ create_{collection}(input: {input}) {{ _docID }} }}"#,
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

        let key = format!("create_{}", collection);
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
    /// Execute the document get command.
    pub async fn execute(&self, ctx: &ClientContext, name: Option<&str>) -> Result<()> {
        let collection =
            name.ok_or_else(|| Error::MissingInput("--name is required for get".to_string()))?;
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
        let collection =
            name.ok_or_else(|| Error::MissingInput("--name is required for update".to_string()))?;
        validate_identifier(collection)?;

        if let Some(ref doc_id) = self.doc_id {
            let updater = self.updater.as_deref().ok_or_else(|| {
                Error::MissingInput("--updater is required for update".to_string())
            })?;

            let client = HttpClient::new(&ctx.url)?
                .with_auth_token(ctx.auth_token.clone())
                .with_verbose(ctx.verbose);

            let result = client
                .collection_update_doc(collection, doc_id, updater)
                .await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            eprintln!("filter-based update not yet supported");
        }
        Ok(())
    }
}

impl DocumentDeleteArgs {
    pub async fn execute(&self, ctx: &ClientContext, name: Option<&str>) -> Result<()> {
        let collection =
            name.ok_or_else(|| Error::MissingInput("--name is required for delete".to_string()))?;
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

impl DocIdsArgs {
    pub async fn execute(&self, ctx: &ClientContext, name: Option<&str>) -> Result<()> {
        let collection =
            name.ok_or_else(|| Error::MissingInput("--name is required for doc-ids".to_string()))?;
        validate_identifier(collection)?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let result = client.collection_doc_ids(collection).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

impl CollectionPatchArgs {
    pub async fn execute(&self, _ctx: &ClientContext) -> Result<()> {
        eprintln!("not yet implemented");
        Ok(())
    }
}

impl SetActiveArgs {
    pub async fn execute(&self, _ctx: &ClientContext) -> Result<()> {
        eprintln!("not yet implemented");
        Ok(())
    }
}

impl TruncateArgs {
    pub async fn execute(&self, _ctx: &ClientContext) -> Result<()> {
        eprintln!("not yet implemented");
        Ok(())
    }
}

/// Extract collection names from introspection response
fn extract_collections(data: &Option<JsonValue>) -> Result<Vec<String>> {
    let data = data
        .as_ref()
        .ok_or_else(|| Error::Server("No data in response".to_string()))?;

    let fields = data
        .get("__schema")
        .and_then(|s| s.get("queryType"))
        .and_then(|q| q.get("fields"))
        .and_then(|f| f.as_array())
        .ok_or_else(|| Error::Server("Invalid introspection response".to_string()))?;

    let mut collections: Vec<String> = fields
        .iter()
        .filter_map(|f| f.get("name").and_then(|n| n.as_str()))
        .filter(|name| !is_builtin_field(name))
        .map(|s| s.to_string())
        .collect();

    collections.sort();
    Ok(collections)
}

/// Get the field names for a collection (excluding relations for simplicity)
async fn get_collection_fields(ctx: &ClientContext, collection: &str) -> Result<Vec<String>> {
    let query = format!(
        r#"
{{
  __type(name: "{collection}") {{
    fields {{
      name
      type {{
        kind
        ofType {{
          kind
        }}
      }}
    }}
  }}
}}
"#
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
        .ok_or_else(|| Error::CollectionNotFound(collection.to_string()))?;

    let type_info = data
        .get("__type")
        .ok_or_else(|| Error::CollectionNotFound(collection.to_string()))?;

    if type_info.is_null() {
        return Err(Error::CollectionNotFound(collection.to_string()));
    }

    let fields = type_info
        .get("fields")
        .and_then(|f| f.as_array())
        .ok_or_else(|| Error::CollectionNotFound(collection.to_string()))?;

    let scalar_fields: Vec<String> = fields
        .iter()
        .filter_map(|f| {
            let name = f.get("name")?.as_str()?;
            let type_info = f.get("type")?;
            let kind = type_info.get("kind").and_then(|k| k.as_str())?;

            if kind == "SCALAR" {
                return Some(name.to_string());
            }

            if kind == "NON_NULL" {
                if let Some(of_type) = type_info.get("ofType") {
                    if let Some(inner_kind) = of_type.get("kind").and_then(|k| k.as_str()) {
                        if inner_kind == "SCALAR" {
                            return Some(name.to_string());
                        }
                    }
                }
            }

            None
        })
        .collect();

    if scalar_fields.is_empty() {
        eprintln!(
            "Warning: No queryable scalar fields found for collection '{}'. Only _docID will be returned.",
            collection
        );
        eprintln!("Use 'defra client query' for full control over field selection.");
        Ok(vec!["_docID".to_string()])
    } else {
        Ok(scalar_fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_is_builtin_field_introspection() {
        assert!(is_builtin_field("__schema"));
        assert!(is_builtin_field("__type"));
        assert!(is_builtin_field("__typename"));
    }

    #[test]
    fn test_is_builtin_field_commits() {
        assert!(is_builtin_field("commits"));
        assert!(is_builtin_field("latestCommits"));
        assert!(is_builtin_field("allCommits"));
        assert!(is_builtin_field("userCommits"));
    }

    #[test]
    fn test_is_builtin_field_user_collections() {
        assert!(!is_builtin_field("Users"));
        assert!(!is_builtin_field("Posts"));
        assert!(!is_builtin_field("Comments"));
    }

    #[test]
    fn test_extract_collections() {
        let data = Some(json!({
            "__schema": {
                "queryType": {
                    "fields": [
                        {"name": "__schema"},
                        {"name": "__type"},
                        {"name": "Users"},
                        {"name": "Posts"},
                        {"name": "commits"}
                    ]
                }
            }
        }));

        let collections = extract_collections(&data).unwrap();
        assert_eq!(collections, vec!["Posts", "Users"]);
    }

    #[test]
    fn test_extract_collections_filters_new_commit_variants() {
        let data = Some(json!({
            "__schema": {
                "queryType": {
                    "fields": [
                        {"name": "Users"},
                        {"name": "latestCommits"},
                        {"name": "allCommits"},
                        {"name": "documentCommits"}
                    ]
                }
            }
        }));

        let collections = extract_collections(&data).unwrap();
        assert_eq!(collections, vec!["Users"]);
    }

    #[test]
    fn test_extract_collections_empty() {
        let data = Some(json!({
            "__schema": {
                "queryType": {
                    "fields": []
                }
            }
        }));

        let collections = extract_collections(&data).unwrap();
        assert!(collections.is_empty());
    }

    #[test]
    fn test_extract_collections_no_data() {
        let data = None;
        let result = extract_collections(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_collection_name() {
        assert!(validate_identifier("Users").is_ok());
        assert!(validate_identifier("User_Posts").is_ok());
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("123Users").is_err());
    }
}
