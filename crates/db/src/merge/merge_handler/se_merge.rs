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
