//! SE artifact storage and retrieval.
//!
//! Handles storing and querying SE artifacts in the datastore.
//! Artifacts are stored at: /se/<collectionID>/<indexID>/<searchTag>/<docID>
//!
//! Matches Go's internal/se/se.go storeArtifacts and fetchDocIDs.

use crypto::se::Artifact;
use storage::corekv::{IterOptions, Iterator, Key, Reader, Result, Writer};
use storage::keys::DatastoreSE;

/// A field query for SE artifact lookup.
#[derive(Debug, Clone)]
pub struct FieldQuery {
    /// Name of the field being queried.
    pub field_name: String,
    /// Index identifier (typically same as field name).
    pub index_id: String,
    /// The search tag computed from the query value.
    pub search_tag: Vec<u8>,
}

impl FieldQuery {
    /// Create a new field query.
    pub fn new(
        field_name: impl Into<String>,
        index_id: impl Into<String>,
        search_tag: Vec<u8>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            index_id: index_id.into(),
            search_tag,
        }
    }
}

/// Store SE artifacts in the datastore.
///
/// Artifacts are stored with empty values - the key itself contains all
/// the necessary information (collection, index, tag, docID).
///
/// # Arguments
///
/// * `store` - The datastore to write to
/// * `artifacts` - Artifacts to store
pub async fn store_artifacts<S: Writer>(store: &mut S, artifacts: &[Artifact]) -> Result<()> {
    for artifact in artifacts {
        let key = DatastoreSE::new(
            &artifact.collection_id,
            &artifact.index_id,
            artifact.search_tag.clone(),
            &artifact.doc_id,
        );

        // Value is empty - presence of key indicates match
        store.set(&key.bytes(), &[]).await?;
    }

    Ok(())
}

/// Query SE artifacts and return matching document IDs.
///
/// This performs an intersection across all queries - a document must match
/// ALL queries to be included in the results.
///
/// # Arguments
///
/// * `store` - The datastore to read from
/// * `collection_id` - Collection to search within
/// * `queries` - Field queries with search tags
///
/// # Returns
///
/// Document IDs that match all queries.
pub async fn fetch_doc_ids<S: Reader>(
    store: &S,
    collection_id: &str,
    queries: &[FieldQuery],
) -> Result<Vec<String>> {
    if queries.is_empty() {
        return Ok(Vec::new());
    }

    let mut doc_id_set: Option<std::collections::HashSet<String>> = None;

    for query in queries {
        // Build prefix key for this query
        let prefix_key = DatastoreSE::new(
            collection_id,
            &query.index_id,
            query.search_tag.clone(),
            "", // Empty doc_id for prefix scan
        );
        let prefix = prefix_key.bytes();

        // Iterate over matching keys
        let mut query_set = std::collections::HashSet::new();

        let opts = IterOptions::new().with_prefix(prefix);

        let mut iter = store.iterator(opts).await?;

        while let Some(kv) = iter.next().await? {
            // Parse the key to extract doc_id
            let key_str = kv.key_str();
            if let Some(doc_id) = extract_doc_id_from_key(&key_str) {
                if !doc_id.is_empty() {
                    query_set.insert(doc_id);
                }
            }
        }

        iter.close().await?;

        // Intersect with accumulated results
        match &mut doc_id_set {
            None => {
                doc_id_set = Some(query_set);
            }
            Some(accumulated) => {
                accumulated.retain(|id| query_set.contains(id));
            }
        }

        // Early exit if intersection is empty
        if doc_id_set.as_ref().map_or(false, |s| s.is_empty()) {
            break;
        }
    }

    Ok(doc_id_set
        .map(|s| s.into_iter().collect())
        .unwrap_or_default())
}

/// Extract doc_id from an SE key string.
///
/// Key format: /se/<collectionID>/<indexID>/<searchTagHex>/<docID>
fn extract_doc_id_from_key(key: &str) -> Option<String> {
    // Split by '/' and get the last segment
    let parts: Vec<&str> = key.split('/').collect();
    // Format: ["", "se", collectionID, indexID, searchTagHex, docID]
    if parts.len() >= 6 {
        Some(parts[5].to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_doc_id_from_key() {
        let key = "/se/col1/age/a1b2c3d4/bae123";
        let doc_id = extract_doc_id_from_key(key);
        assert_eq!(doc_id, Some("bae123".to_string()));
    }

    #[test]
    fn test_extract_doc_id_invalid_key() {
        let key = "/se/col1/age";
        let doc_id = extract_doc_id_from_key(key);
        assert_eq!(doc_id, None);
    }

    #[test]
    fn test_field_query_new() {
        let query = FieldQuery::new("age", "age", vec![1, 2, 3]);
        assert_eq!(query.field_name, "age");
        assert_eq!(query.index_id, "age");
        assert_eq!(query.search_tag, vec![1, 2, 3]);
    }
}
