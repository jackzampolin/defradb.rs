//! Post-parse schema validation for GraphQL operations.
//!
//! Validates that all collection names referenced in a parsed operation
//! actually exist in the database, catching typos at parse time rather
//! than during execution.

use crate::error::{QueryError, Result};
use crate::fetcher::CollectionProvider;
use crate::mapper::{Mutation, Select};
use crate::query_parse::ParsedOperation;

/// Validate that all collections referenced in a parsed operation exist.
///
/// This runs after parsing but before execution, giving clear errors
/// like "Cannot query collection 'Articel': collection not found"
/// instead of opaque execution failures.
pub async fn validate_parsed_operation(
    op: &ParsedOperation,
    provider: &dyn CollectionProvider,
) -> Result<()> {
    match op {
        ParsedOperation::Query { selects, .. } => {
            for select in selects {
                validate_select_collection(select, provider).await?;
            }
        }
        ParsedOperation::Mutation { mutations, .. } => {
            for mutation in mutations {
                validate_mutation_collection(mutation, provider).await?;
            }
        }
        ParsedOperation::Subscription { select, .. } => {
            validate_select_collection(select, provider).await?;
        }
        ParsedOperation::Introspection { .. } => {}
    }
    Ok(())
}

/// Validate that the collection referenced by a Select exists.
///
/// Skips internal collections (`_commits`) and nested relation selects
/// (those are field names resolved later by the planner).
async fn validate_select_collection(
    select: &Select,
    provider: &dyn CollectionProvider,
) -> Result<()> {
    let name = &select.collection_name;

    if is_internal_collection(name) {
        return Ok(());
    }

    if provider.get_collection(name).await?.is_none() {
        let suggestion = suggest_collection(name, provider).await;
        let mut msg = format!("Cannot query collection '{}': collection not found", name);
        if let Some(suggested) = suggestion {
            msg.push_str(&format!(". Did you mean '{}'?", suggested));
        }
        return Err(QueryError::collection_not_found(msg));
    }

    Ok(())
}

/// Validate that the collection referenced by a Mutation exists.
async fn validate_mutation_collection(
    mutation: &Mutation,
    provider: &dyn CollectionProvider,
) -> Result<()> {
    let name = &mutation.collection_name;

    if provider.get_collection(name).await?.is_none() {
        let suggestion = suggest_collection(name, provider).await;
        let op = match mutation.mutation_type {
            crate::mapper::MutationType::Create => "create documents in",
            crate::mapper::MutationType::Update => "update documents in",
            crate::mapper::MutationType::Delete => "delete documents from",
            crate::mapper::MutationType::Upsert => "upsert documents in",
        };
        let mut msg = format!("Cannot {} collection '{}': collection not found", op, name);
        if let Some(suggested) = suggestion {
            msg.push_str(&format!(". Did you mean '{}'?", suggested));
        }
        return Err(QueryError::collection_not_found(msg));
    }

    Ok(())
}

fn is_internal_collection(name: &str) -> bool {
    name.starts_with('_')
}

/// Suggest a similar collection name using edit distance.
async fn suggest_collection(name: &str, provider: &dyn CollectionProvider) -> Option<String> {
    let collections = provider.list_collections().await.ok()?;
    let name_lower = name.to_lowercase();

    let mut best: Option<(String, usize)> = None;
    for candidate in &collections {
        let dist = edit_distance(&name_lower, &candidate.to_lowercase());
        let threshold = (name.len() / 3).max(2);
        if dist <= threshold && best.as_ref().is_none_or(|(_, d)| dist < *d) {
            best = Some((candidate.clone(), dist));
        }
    }

    best.map(|(name, _)| name)
}

/// Simple Levenshtein edit distance.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (m, n) = (a.len(), b.len());

    let mut prev = (0..=n).collect::<Vec<_>>();
    let mut curr = vec![0; n + 1];

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[n]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::DocumentMapping;
    use crate::fetcher::StaticCollectionProvider;
    use schema::CollectionVersion;

    fn make_provider(names: &[&str]) -> StaticCollectionProvider {
        let collections: Vec<CollectionVersion> = names
            .iter()
            .map(|name| serde_json::from_value(serde_json::json!({ "Name": name })).unwrap())
            .collect();
        StaticCollectionProvider::new(collections)
    }

    #[test]
    fn test_edit_distance() {
        assert_eq!(edit_distance("Article", "Articel"), 2);
        assert_eq!(edit_distance("User", "User"), 0);
        assert_eq!(edit_distance("User", "Users"), 1);
        assert_eq!(edit_distance("abc", "xyz"), 3);
    }

    #[tokio::test]
    async fn test_valid_query_passes() {
        let provider = make_provider(&["User", "Article"]);
        let op = ParsedOperation::Query {
            selects: vec![Select::new("User")],
            explain: None,
            exhaustive: false,
        };
        assert!(validate_parsed_operation(&op, &provider).await.is_ok());
    }

    #[tokio::test]
    async fn test_invalid_query_collection() {
        let provider = make_provider(&["User", "Article"]);
        let op = ParsedOperation::Query {
            selects: vec![Select::new("Uzr")],
            explain: None,
            exhaustive: false,
        };
        let err = validate_parsed_operation(&op, &provider).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Uzr"), "error should contain collection name");
        assert!(
            msg.contains("collection not found"),
            "error should mention not found"
        );
        assert!(
            msg.contains("Did you mean 'User'"),
            "error should suggest User, got: {}",
            msg
        );
    }

    #[tokio::test]
    async fn test_commits_skipped() {
        let provider = make_provider(&["User"]);
        let op = ParsedOperation::Query {
            selects: vec![Select::new("_commits")],
            explain: None,
            exhaustive: false,
        };
        assert!(validate_parsed_operation(&op, &provider).await.is_ok());
    }

    #[tokio::test]
    async fn test_invalid_mutation_collection() {
        let provider = make_provider(&["User"]);
        let mutation = Mutation {
            mutation_type: crate::mapper::MutationType::Create,
            collection_name: "Usr".to_string(),
            alias: None,
            create_input: vec![],
            update_input: Default::default(),
            doc_ids: None,
            filter: None,
            fields: vec![],
            document_mapping: DocumentMapping::new(),
            encrypt_doc: false,
            encrypt_fields: vec![],
        };
        let op = ParsedOperation::Mutation {
            mutations: vec![mutation],
            explain: None,
        };
        let err = validate_parsed_operation(&op, &provider).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("create documents in"));
        assert!(msg.contains("Usr"));
        assert!(msg.contains("Did you mean 'User'"));
    }

    #[tokio::test]
    async fn test_introspection_skipped() {
        let provider = make_provider(&[]);
        let op = ParsedOperation::Introspection {
            query: "{ __schema { types { name } } }".to_string(),
        };
        assert!(validate_parsed_operation(&op, &provider).await.is_ok());
    }

    #[tokio::test]
    async fn test_no_suggestion_for_distant_name() {
        let provider = make_provider(&["User"]);
        let op = ParsedOperation::Query {
            selects: vec![Select::new("CompletelyDifferent")],
            explain: None,
            exhaustive: false,
        };
        let err = validate_parsed_operation(&op, &provider).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("CompletelyDifferent"));
        assert!(!msg.contains("Did you mean"));
    }
}
