//! Collection introspection via GraphQL schema queries

use serde_json::Value as JsonValue;

use super::{CollectionDescribeArgs, CollectionListArgs};
use crate::commands::client::http_client::HttpClient;
use crate::commands::client::validate_identifier;
use crate::commands::client::ClientContext;
use crate::error::{Error, Result};

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

/// Check if a field is a DefraDB aggregate field that requires arguments.
fn is_aggregate_field(name: &str) -> bool {
    matches!(
        name,
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP" | "SIMILARITY" | "BM25"
    ) || (name.starts_with('_') && name != "_docID")
}

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
pub(crate) async fn get_collection_fields(
    ctx: &ClientContext,
    collection: &str,
) -> Result<Vec<String>> {
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

            // Skip aggregate fields that require arguments
            if is_aggregate_field(name) {
                return None;
            }

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
    use crate::commands::client::validate_identifier;
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
