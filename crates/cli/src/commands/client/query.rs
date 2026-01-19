// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Query command implementation

use std::path::PathBuf;

use clap::Args;
use serde_json::Value as JsonValue;

use super::http_client::HttpClient;
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
    #[arg(long, short = 'v')]
    pub variables: Option<String>,

    /// Transaction ID to execute the query within
    #[arg(long)]
    pub txn_id: Option<String>,
}

impl QueryArgs {
    /// Execute the query command
    pub async fn execute(&self, url: &str) -> Result<()> {
        let query = self.get_query()?;
        let variables = self.parse_variables()?;

        let client = HttpClient::new(url);
        let response = client
            .graphql(&query, variables, self.txn_id.clone())
            .await?;

        if response.has_errors() {
            for error in &response.errors {
                eprintln!("Error: {}", error.message);
            }
            return Err(Error::Server(response.error_message()));
        }

        if let Some(data) = response.data {
            let output = serde_json::to_string_pretty(&data)?;
            println!("{output}");
        }

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

        Err(Error::Server(
            "Either a query or --file must be provided".to_string(),
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
