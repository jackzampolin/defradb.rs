//! Query command implementation

use std::path::PathBuf;

use clap::Args;
use serde_json::Value as JsonValue;

use super::http_client::HttpClient;
use super::ClientContext;
use crate::error::{Error, Result};

/// Execute a GraphQL query against a DefraDB node
#[derive(Args, Debug)]
pub struct QueryArgs {
    /// The GraphQL query to execute
    #[arg(value_name = "QUERY")]
    pub query: Option<String>,

    /// Path to a file containing the GraphQL query
    #[arg(long, short = 'f', conflicts_with = "query")]
    pub file: Option<PathBuf>,

    /// Variables to pass to the query (JSON format)
    #[arg(long)]
    pub variables: Option<String>,

    /// Transaction ID to execute the query within
    #[arg(long)]
    pub txn_id: Option<String>,
}

impl QueryArgs {
    /// Execute the query command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let query = self.get_query()?;
        let variables = self.parse_variables()?;

        // Use command-level txn_id if provided, otherwise use global context
        let txn_id = self.txn_id.clone().or_else(|| ctx.tx_id.clone());

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);
        let response = client.graphql(&query, variables, txn_id).await?;

        if response.has_errors() {
            // Just return the error - CLI framework will handle displaying it
            return Err(Error::Server(response.error_message()));
        }

        // Always output data, even if null (for consistent piping/scripting)
        let data = response.data.unwrap_or(serde_json::Value::Null);
        let output = serde_json::to_string_pretty(&data)?;
        println!("{output}");

        Ok(())
    }

    fn get_query(&self) -> Result<String> {
        if let Some(ref query) = self.query {
            return Ok(query.clone());
        }

        if let Some(ref path) = self.file {
            return std::fs::read_to_string(path).map_err(|e| Error::ReadFile {
                path: path.clone(),
                source: e,
            });
        }

        Err(Error::MissingInput(
            "either a query or --file must be provided".to_string(),
        ))
    }

    fn parse_variables(&self) -> Result<Option<JsonValue>> {
        match &self.variables {
            Some(vars) => {
                let parsed: JsonValue = serde_json::from_str(vars)?;
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_args_get_query_inline() {
        let args = QueryArgs {
            query: Some("{ Users { name } }".to_string()),
            file: None,
            variables: None,
            txn_id: None,
        };
        assert_eq!(args.get_query().unwrap(), "{ Users { name } }");
    }

    #[test]
    fn test_query_args_parse_variables() {
        let args = QueryArgs {
            query: Some("{ Users { name } }".to_string()),
            file: None,
            variables: Some(r#"{"limit": 10}"#.to_string()),
            txn_id: None,
        };
        let vars = args.parse_variables().unwrap().unwrap();
        assert_eq!(vars["limit"], 10);
    }

    #[test]
    fn test_query_args_no_query() {
        let args = QueryArgs {
            query: None,
            file: None,
            variables: None,
            txn_id: None,
        };
        assert!(args.get_query().is_err());
    }
}
