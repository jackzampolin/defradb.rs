//! Resolve a collection from a document ID via the headstore.
//!
//! Ports Go's `internal/db/collection_retriever.go::RetrieveCollectionFromDocID`.
//! The headstore is keyed by doc_id (`HeadstorePriorityKey::document_prefix`),
//! so finding the doc's first head block is O(1) — no scan over all
//! collections. We then read `schema_version_id` off the block's delta and
//! resolve the collection via the systemstore.

use std::sync::Arc;

use cid::Cid;
use storage::corekv::Store;
use storage::keys::HeadstorePriorityKey;
use storage::IterOptions;

use crate::database::DB;
use crate::error::{Error, Result};
use crate::txn::DbTxn;

/// Subset of a collection's policy metadata sufficient for KMS access-control
/// checks. Returned by `resolve_collection_from_doc_id`.
#[derive(Debug, Clone)]
pub struct DocCollectionInfo {
    /// The collection id (for tracing; not used in the policy check itself).
    pub collection_id: String,
    /// Policy id under which to check the DAC permission.
    pub policy_id: String,
    /// Resource name within the policy.
    pub resource_name: String,
}

/// Resolve the collection metadata for a given document.
///
/// Returns `Ok(None)` when:
/// - The doc has no head blocks in the local headstore (unknown doc).
/// - The doc's first head block is not in the local blockstore.
/// - The block's delta has no schema_version_id (e.g. definition blocks).
/// - The schema_version_id does not resolve to a collection in the systemstore.
/// - The collection has no policy configured (`collection.policy == None`).
///
/// Mirrors Go's `RetrieveCollectionFromDocID` — same lookup chain via the
/// headstore index, no scan.
pub async fn resolve_collection_from_doc_id<S: Store>(
    db: &Arc<DB<S>>,
    doc_id: &str,
) -> Result<Option<DocCollectionInfo>> {
    let txn = db.new_txn(true).await?;
    let result = resolve_inner(&txn, doc_id).await;
    // Always discard the read-only transaction to release resources
    // (matches schema_loader.rs and Go's `defer txn.Discard()`).
    let _ = txn.discard();
    result
}

async fn resolve_inner<S: Store>(
    txn: &DbTxn<S>,
    doc_id: &str,
) -> Result<Option<DocCollectionInfo>> {
    let headstore = txn.headstore()?;

    let prefix = HeadstorePriorityKey::document_prefix(doc_id);
    let opts = IterOptions::new().with_prefix(prefix);
    let mut iter = headstore.iterator(opts).await.map_err(Error::Storage)?;
    let cid_offset = HeadstorePriorityKey::cid_offset(doc_id);

    let pair = match iter.next().await.map_err(Error::Storage)? {
        Some(p) => p,
        None => {
            iter.close().await.map_err(Error::Storage)?;
            return Ok(None);
        }
    };
    let cid_bytes = match pair.key.get(cid_offset..) {
        Some(b) => b.to_vec(),
        None => {
            iter.close().await.map_err(Error::Storage)?;
            return Ok(None);
        }
    };
    iter.close().await.map_err(Error::Storage)?;

    let cid = Cid::try_from(cid_bytes.as_slice())
        .map_err(|e| Error::Serialization(format!("decode head cid: {e}")))?;

    // Load the block to read its delta.
    let blockstore = txn.blockstore()?;
    let block_bytes = match blockstore
        .get(&cid.to_bytes())
        .await
        .map_err(Error::Storage)?
    {
        Some(b) => b,
        None => return Ok(None),
    };
    let block = defra_core::block::Block::from_dag_cbor(&block_bytes)
        .map_err(|e| Error::Serialization(format!("decode block: {e}")))?;
    let schema_version_id = match block.delta.schema_version_id() {
        Some(v) => v.to_string(),
        None => return Ok(None),
    };

    // Resolve collection via the systemstore.
    let systemstore = txn.systemstore()?;
    let collection =
        match crate::schema_loader::get_collection_by_version_id(&systemstore, &schema_version_id)
            .await?
        {
            Some(c) => c,
            None => return Ok(None),
        };
    let policy = match collection.policy.clone() {
        Some(p) => p,
        None => return Ok(None),
    };
    Ok(Some(DocCollectionInfo {
        collection_id: collection.collection_id.clone(),
        policy_id: policy.id,
        resource_name: policy.resource_name,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use document::Document;
    use query::mutator::DocMutator;
    use schema::{CollectionVersion, FieldDescription, FieldKind, PolicyDescription};
    use storage::backends::MemoryStore;

    use crate::doc_mutator::DbDocMutator;

    fn test_collection_with_policy() -> CollectionVersion {
        CollectionVersion::new(
            "TestDoc",
            "v1",
            "col-test-doc",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "x", FieldKind::int()),
            ],
        )
        .with_policy(PolicyDescription::new("policy-abc", "users"))
    }

    #[tokio::test]
    async fn resolves_collection_for_known_doc() {
        let db = Arc::new(DB::new(MemoryStore::new()).expect("create db"));
        db.create_collection(test_collection_with_policy())
            .await
            .expect("create collection");

        let txn = db.new_txn(false).await.expect("new_txn");
        let mutator = DbDocMutator::new(Arc::clone(&db), txn);
        let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
        let result = mutator.create("TestDoc", doc).await.expect("create");
        let txn = mutator.take_txn().await.expect("take txn");
        txn.commit().await.expect("commit");

        let doc_id = result.doc_id.to_string();
        let info = resolve_collection_from_doc_id(&db, &doc_id)
            .await
            .expect("resolve")
            .expect("doc has policy");

        assert_eq!(info.policy_id, "policy-abc");
        assert_eq!(info.resource_name, "users");
        // collection_id is the stable id passed to CollectionVersion::new.
        assert!(
            !info.collection_id.is_empty(),
            "collection_id should be populated"
        );
    }

    #[tokio::test]
    async fn returns_none_for_unknown_doc() {
        let db = Arc::new(DB::new(MemoryStore::new()).expect("create db"));
        let info = resolve_collection_from_doc_id(&db, "bafy-not-a-real-doc")
            .await
            .expect("resolve");
        assert!(info.is_none());
    }
}
