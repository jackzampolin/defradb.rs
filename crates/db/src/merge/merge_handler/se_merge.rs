//! SE artifact generation during P2P merge.
//!
//! When documents are received via replication, the receiving node generates
//! SE artifacts if the collection has encrypted indexes and the node has an
//! SE encryption key configured. This ensures replicated documents are
//! searchable on the receiving node.

use std::collections::HashMap;

use document::NormalValue;
use schema::CollectionVersion;
use storage::corekv::{Result, Writer};

use crate::merge::se::{generate_doc_artifacts, store_artifacts};

/// Generate and store SE artifacts for a replicated document.
///
/// Called after a successful composite merge when the receiving node
/// has an SE encryption key configured. Generates search tags for
/// all encrypted-indexed fields and stores them in the datastore.
pub(crate) async fn generate_merge_artifacts<S: Writer>(
    store: &mut S,
    schema: &CollectionVersion,
    doc_id: &str,
    field_values: &HashMap<String, NormalValue>,
    enc_key: &[u8],
    identity_pubkey: Option<&[u8]>,
) -> Result<usize> {
    let encrypted_indexes = &schema.encrypted_indexes;
    if encrypted_indexes.is_empty() {
        return Ok(0);
    }

    let artifacts = generate_doc_artifacts(
        &schema.collection_id,
        doc_id,
        encrypted_indexes,
        &[], // all encrypted fields
        field_values,
        identity_pubkey,
        enc_key,
    )?;

    if artifacts.is_empty() {
        return Ok(0);
    }

    let count = artifacts.len();
    store_artifacts(store, &artifacts).await?;

    tracing::debug!(
        doc_id = %doc_id,
        collection_id = %schema.collection_id,
        artifact_count = count,
        "Generated SE artifacts for replicated document"
    );

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge::se::generate_doc_artifacts;
    use schema::EncryptedIndexDescription;

    fn test_schema(encrypted_fields: Vec<&str>) -> CollectionVersion {
        let mut col = CollectionVersion::new("test", "col_v1", "col_v1", vec![]);
        col.encrypted_indexes = encrypted_fields
            .into_iter()
            .map(EncryptedIndexDescription::new)
            .collect();
        col
    }

    #[test]
    fn test_no_encrypted_indexes_generates_nothing() {
        let schema = test_schema(vec![]);
        let fields = HashMap::new();
        let artifacts = generate_doc_artifacts(
            &schema.collection_id,
            "doc1",
            &schema.encrypted_indexes,
            &[],
            &fields,
            None,
            &[0u8; 32],
        )
        .unwrap();
        assert!(artifacts.is_empty());
    }

    #[test]
    fn test_no_matching_values_generates_nothing() {
        let schema = test_schema(vec!["age"]);
        let fields = HashMap::new(); // no "age" field value
        let artifacts = generate_doc_artifacts(
            &schema.collection_id,
            "doc1",
            &schema.encrypted_indexes,
            &[],
            &fields,
            None,
            &[0u8; 32],
        )
        .unwrap();
        assert!(artifacts.is_empty());
    }

    #[test]
    fn test_matching_encrypted_field_generates_artifact() {
        let schema = test_schema(vec!["age"]);
        let mut fields = HashMap::new();
        fields.insert("age".to_string(), NormalValue::Int(25));
        let artifacts = generate_doc_artifacts(
            &schema.collection_id,
            "doc1",
            &schema.encrypted_indexes,
            &[],
            &fields,
            None,
            &[1u8; 32],
        )
        .unwrap();
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].index_id, "age");
        assert_eq!(artifacts[0].doc_id, "doc1");
        assert_eq!(artifacts[0].search_tag.len(), 16);
    }

    #[test]
    fn test_multiple_encrypted_fields() {
        let schema = test_schema(vec!["age", "city"]);
        let mut fields = HashMap::new();
        fields.insert("age".to_string(), NormalValue::Int(30));
        fields.insert("city".to_string(), NormalValue::String("NYC".to_string()));
        let artifacts = generate_doc_artifacts(
            &schema.collection_id,
            "doc2",
            &schema.encrypted_indexes,
            &[],
            &fields,
            None,
            &[2u8; 32],
        )
        .unwrap();
        assert_eq!(artifacts.len(), 2);
    }
}
