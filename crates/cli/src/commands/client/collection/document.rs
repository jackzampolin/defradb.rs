//! Document CRUD command implementations

use serde_json::Value as JsonValue;

use super::introspection::get_collection_fields;
use super::{
    CollectionPatchArgs, DocIdsArgs, DocumentCreateArgs, DocumentDeleteArgs, DocumentGetArgs,
    DocumentUpdateArgs, SetActiveArgs, TruncateArgs,
};
use crate::commands::client::http_client::HttpClient;
use crate::commands::client::ClientContext;
use crate::commands::client::{
    escape_graphql_string, get_data_from_args, json_to_graphql_input, validate_identifier,
};
use crate::error::{Error, Result};

impl DocumentCreateArgs {
    /// Execute the document create command
    pub async fn execute(&self, ctx: &ClientContext, name: Option<&str>) -> Result<()> {
        let collection =
            name.ok_or_else(|| Error::MissingInput("--name is required for create".to_string()))?;
        validate_identifier(collection)?;

        let data = get_data_from_args(&self.document, &self.file)?;
        let parsed: JsonValue = serde_json::from_str(&data)?;

        let input_str = json_to_graphql_input(&parsed);
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
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let patch = get_data_from_args(&self.patch, &self.patch_file)?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.collection_patch(&patch).await?;
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
