// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Collection command implementation

use clap::{Args, Subcommand};
use serde_json::Value as JsonValue;

use super::http_client::HttpClient;
use super::validate_identifier;
use crate::error::{Error, Result};

/// Interact with collections
#[derive(Args, Debug)]
pub struct CollectionArgs {
    #[command(subcommand)]
    pub command: CollectionCommand,
}

/// Collection subcommands
#[derive(Subcommand, Debug)]
pub enum CollectionCommand {
    /// List all collections
    List(CollectionListArgs),
    /// Describe a collection's schema
    Describe(CollectionDescribeArgs),
}

/// Arguments for collection list command
#[derive(Args, Debug)]
pub struct CollectionListArgs {}

/// Arguments for collection describe command
#[derive(Args, Debug)]
pub struct CollectionDescribeArgs {
    /// The collection name
    #[arg(value_name = "NAME")]
    pub name: String,
}

impl CollectionArgs {
    /// Execute the collection command
    pub async fn execute(&self, url: &str) -> Result<()> {
        match &self.command {
            CollectionCommand::List(args) => args.execute(url).await,
            CollectionCommand::Describe(args) => args.execute(url).await,
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
///
/// Uses pattern matching to be resilient to new built-in types.
fn is_builtin_field(name: &str) -> bool {
    // GraphQL introspection fields
    if name.starts_with("__") {
        return true;
    }

    // DefraDB commit-related fields (case-insensitive suffix matching)
    let lower = name.to_lowercase();
    if lower == "commits" || lower.ends_with("commits") {
        return true;
    }

    false
}

impl CollectionListArgs {
    /// Execute the collection list command
    pub async fn execute(&self, url: &str) -> Result<()> {
        let client = HttpClient::new(url)?;
        let response = client.graphql(INTROSPECTION_QUERY, None, None).await?;

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
    pub async fn execute(&self, url: &str) -> Result<()> {
        validate_identifier(&self.name)?;

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
            name = self.name
        );

        let client = HttpClient::new(url)?;
        let response = client.graphql(&query, None, None).await?;

        if response.has_errors() {
            return Err(Error::Server(response.error_message()));
        }

        if let Some(data) = response.data {
            if let Some(type_info) = data.get("__type") {
                if type_info.is_null() {
                    return Err(Error::CollectionNotFound(self.name.clone()));
                }
                let output = serde_json::to_string_pretty(type_info)?;
                println!("{output}");
            } else {
                return Err(Error::CollectionNotFound(self.name.clone()));
            }
        }

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
