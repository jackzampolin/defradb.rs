// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Document command implementation

use std::path::PathBuf;

use clap::{Args, Subcommand};
use serde_json::Value as JsonValue;

use super::http_client::HttpClient;
use super::{escape_graphql_string, get_data_from_args, validate_identifier, ClientContext};
use crate::error::{Error, Result};

/// Interact with documents
#[derive(Args, Debug)]
pub struct DocumentArgs {
    #[command(subcommand)]
    pub command: DocumentCommand,
}

/// Document subcommands
#[derive(Subcommand, Debug)]
pub enum DocumentCommand {
    /// Create a new document
    Create(DocumentCreateArgs),
    /// Get a document by ID
    Get(DocumentGetArgs),
    /// Update a document
    Update(DocumentUpdateArgs),
    /// Delete a document
    Delete(DocumentDeleteArgs),
}

/// Arguments for document create command
#[derive(Args, Debug)]
pub struct DocumentCreateArgs {
    /// The collection name
    #[arg(value_name = "COLLECTION")]
    pub collection: String,

    /// The document data (JSON)
    #[arg(value_name = "DATA")]
    pub data: Option<String>,

    /// Path to a file containing the document data
    #[arg(long, short = 'f', conflicts_with = "data")]
    pub file: Option<PathBuf>,
}

/// Arguments for document get command
#[derive(Args, Debug)]
pub struct DocumentGetArgs {
    /// The collection name
    #[arg(value_name = "COLLECTION")]
    pub collection: String,

    /// The document ID
    #[arg(value_name = "DOC_ID")]
    pub doc_id: String,

    /// Output in JSON format with consistent structure for programmatic use
    #[arg(long)]
    pub json: bool,
}

/// Arguments for document update command
#[derive(Args, Debug)]
pub struct DocumentUpdateArgs {
    /// The collection name
    #[arg(value_name = "COLLECTION")]
    pub collection: String,

    /// The document ID
    #[arg(value_name = "DOC_ID")]
    pub doc_id: String,

    /// The update data (JSON)
    #[arg(value_name = "DATA")]
    pub data: Option<String>,

    /// Path to a file containing the update data
    #[arg(long, short = 'f', conflicts_with = "data")]
    pub file: Option<PathBuf>,
}

/// Arguments for document delete command
#[derive(Args, Debug)]
pub struct DocumentDeleteArgs {
    /// The collection name
    #[arg(value_name = "COLLECTION")]
    pub collection: String,

    /// The document ID
    #[arg(value_name = "DOC_ID")]
    pub doc_id: String,
}

impl DocumentArgs {
    /// Execute the document command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            DocumentCommand::Create(args) => args.execute(ctx).await,
            DocumentCommand::Get(args) => args.execute(ctx).await,
            DocumentCommand::Update(args) => args.execute(ctx).await,
            DocumentCommand::Delete(args) => args.execute(ctx).await,
        }
    }
}

impl DocumentCreateArgs {
    /// Execute the document create command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        validate_identifier(&self.collection)?;

        let data = get_data_from_args(&self.data, &self.file)?;
        let parsed: JsonValue = serde_json::from_str(&data)?;

        let input_str = serde_json::to_string(&parsed)?;
        let query = format!(
            r#"mutation {{ create_{collection}(input: {input}) {{ _docID }} }}"#,
            collection = self.collection,
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

        let key = format!("create_{}", self.collection);
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
    ///
    /// Note: Only scalar fields are returned. Relations are excluded for simplicity.
    /// Use `defra client query` for full control over field selection.
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        validate_identifier(&self.collection)?;

        let fields = get_collection_fields(ctx, &self.collection).await?;
        let field_selection = fields.join(" ");
        let escaped_doc_id = escape_graphql_string(&self.doc_id);

        let query = format!(
            r#"{{ {collection}(filter: {{_docID: {{_eq: "{doc_id}"}}}}) {{ {fields} }} }}"#,
            collection = self.collection,
            doc_id = escaped_doc_id,
            fields = field_selection
        );

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);
        let response = client.graphql(&query, None, ctx.tx_id.clone()).await?;

        if response.has_errors() {
            if self.json {
                let output = serde_json::json!({
                    "success": false,
                    "error": response.error_message()
                });
                println!("{}", serde_json::to_string_pretty(&output)?);
            }
            // Always return error for proper exit code, even in JSON mode
            return Err(Error::Server(response.error_message()));
        }

        let data = response
            .data
            .ok_or_else(|| Error::Server("Server returned no data".to_string()))?;

        let results = data.get(&self.collection).ok_or_else(|| {
            Error::Server(format!(
                "Server response missing collection key '{}'",
                self.collection
            ))
        })?;

        let arr = results.as_array().ok_or_else(|| {
            Error::Server(format!(
                "Expected array for collection '{}', got: {}",
                self.collection, results
            ))
        })?;

        if arr.is_empty() {
            if self.json {
                let output = serde_json::json!({
                    "success": true,
                    "data": null
                });
                println!("{}", serde_json::to_string_pretty(&output)?);
            } else {
                println!("Document not found");
            }
        } else if arr.len() == 1 {
            if self.json {
                let output = serde_json::json!({
                    "success": true,
                    "data": arr[0]
                });
                println!("{}", serde_json::to_string_pretty(&output)?);
            } else {
                let output = serde_json::to_string_pretty(&arr[0])?;
                println!("{output}");
            }
        } else if self.json {
            let output = serde_json::json!({
                "success": true,
                "data": results
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        } else {
            let output = serde_json::to_string_pretty(results)?;
            println!("{output}");
        }

        Ok(())
    }
}

impl DocumentUpdateArgs {
    /// Execute the document update command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        validate_identifier(&self.collection)?;

        let data = get_data_from_args(&self.data, &self.file)?;
        let parsed: JsonValue = serde_json::from_str(&data)?;

        let input_str = serde_json::to_string(&parsed)?;
        let escaped_doc_id = escape_graphql_string(&self.doc_id);
        let query = format!(
            r#"mutation {{ update_{collection}(docIDs: ["{doc_id}"], input: {input}) {{ _docID }} }}"#,
            collection = self.collection,
            doc_id = escaped_doc_id,
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

        let key = format!("update_{}", self.collection);
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

impl DocumentDeleteArgs {
    /// Execute the document delete command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        validate_identifier(&self.collection)?;

        let escaped_doc_id = escape_graphql_string(&self.doc_id);
        let query = format!(
            r#"mutation {{ delete_{collection}(docIDs: ["{doc_id}"]) {{ _docID }} }}"#,
            collection = self.collection,
            doc_id = escaped_doc_id
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

        let key = format!("delete_{}", self.collection);
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

            // Include SCALAR fields and NON_NULL scalars
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
        // Fallback to _docID if collection has no scalar fields (e.g., only relations)
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

    #[test]
    fn test_get_data_from_args_inline() {
        let data = Some(r#"{"name": "Alice"}"#.to_string());
        let file = None;
        assert_eq!(
            get_data_from_args(&data, &file).unwrap(),
            r#"{"name": "Alice"}"#
        );
    }

    #[test]
    fn test_get_data_from_args_no_data() {
        let data = None;
        let file = None;
        assert!(get_data_from_args(&data, &file).is_err());
    }

    #[test]
    fn test_document_get_args() {
        let args = DocumentGetArgs {
            collection: "Users".to_string(),
            doc_id: "bae-123".to_string(),
            json: false,
        };
        assert_eq!(args.collection, "Users");
        assert_eq!(args.doc_id, "bae-123");
        assert!(!args.json);
    }

    #[test]
    fn test_document_delete_args() {
        let args = DocumentDeleteArgs {
            collection: "Users".to_string(),
            doc_id: "bae-456".to_string(),
        };
        assert_eq!(args.collection, "Users");
        assert_eq!(args.doc_id, "bae-456");
    }

    #[test]
    fn test_validate_collection_name_valid() {
        assert!(validate_identifier("Users").is_ok());
        assert!(validate_identifier("User_Posts").is_ok());
        assert!(validate_identifier("_private").is_ok());
    }

    #[test]
    fn test_validate_collection_name_invalid() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("123Users").is_err());
        assert!(validate_identifier("User-Posts").is_err());
    }

    #[test]
    fn test_escape_doc_id() {
        assert_eq!(escape_graphql_string("bae-123"), "bae-123");
        assert_eq!(
            escape_graphql_string(r#"bae-123"injection"#),
            r#"bae-123\"injection"#
        );
    }
}
