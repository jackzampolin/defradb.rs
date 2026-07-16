//! Block reading utilities that depend on the DB type.
//!
//! These functions are kept in the `db` crate (not `db-blocks`) because they
//! need access to `DB` and `DbTxn` which would create a circular dependency.

use db_blocks::{get_all_field_heads, BlockResult};
use document::{DocID, Document};

/// Read a committed document by ID from a collection, in a `Send`-safe way.
///
/// Used by the SE artifact retry path to regenerate search tags from a
/// document's current field values (mirrors Go's
/// `Coordinator.retrySEArtifacts`). The read transaction is owned and discarded
/// internally; no transaction reference crosses an `.await`, so the returned
/// future is `Send` and can run on the background retry task.
pub async fn read_document_for_se<S: storage::corekv::Store>(
    db: &crate::database::DB<S>,
    collection_id: &str,
    doc_id: &str,
) -> Result<Option<Document>, String> {
    let collection = match db
        .find_collection_by_id(collection_id)
        .map_err(|e| format!("failed to load collection: {e}"))?
    {
        Some(collection) => collection,
        None => return Ok(None),
    };

    let parsed_doc_id =
        DocID::from_string(doc_id).map_err(|e| format!("invalid doc id '{doc_id}': {e}"))?;

    let txn = db
        .new_txn(true)
        .await
        .map_err(|e| format!("failed to create read txn: {e}"))?;
    let datastore = txn
        .datastore()
        .map_err(|e| format!("failed to get datastore: {e}"))?;
    let systemstore = txn
        .systemstore()
        .map_err(|e| format!("failed to get systemstore: {e}"))?;

    let (doc_short_id, canonical_doc_id) = match collection
        .resolve_doc_identity(&systemstore, &parsed_doc_id)
        .await
        .map_err(|e| format!("failed to resolve doc short id: {e}"))?
    {
        Some(id) => id,
        None => {
            drop(datastore);
            drop(systemstore);
            let _ = txn.force_discard();
            return Ok(None);
        }
    };

    let doc_bytes = datastore
        .get(&collection.doc_key(doc_short_id))
        .await
        .map_err(|e| format!("failed to read document: {e}"))?;
    let version_bytes = datastore
        .get(&collection.version_key(doc_short_id))
        .await
        .map_err(|e| format!("failed to read document version: {e}"))?;
    drop(datastore);
    drop(systemstore);
    let _ = txn.force_discard();

    let Some(doc_bytes) = doc_bytes else {
        return Ok(None);
    };
    let mut document =
        Document::from_cbor(&doc_bytes).map_err(|e| format!("failed to decode document: {e}"))?;
    document.set_id(canonical_doc_id);
    if let Some(version_bytes) = version_bytes {
        if let Ok(version) = String::from_utf8(version_bytes) {
            document.set_schema_version_id(version);
        }
    }
    Ok(Some(document))
}

/// Read the latest composite block for a document from the committed store.
///
/// After a mutation (create/update/delete), the composite block and its CID
/// are written to the blockstore and headstore as part of the transaction.
/// This function reads the committed composite head CID from the headstore,
/// then fetches the corresponding block bytes from the blockstore.
///
/// Use this instead of `build_blocks_from_document` for P2P broadcast after
/// updates/deletes, since `build_blocks_from_document` recreates blocks from
/// scratch with wrong priority/heads.
pub async fn read_latest_composite_block<S: storage::corekv::Store>(
    db: &crate::database::DB<S>,
    doc_id: &str,
) -> Result<BlockResult, String> {
    let txn = db
        .new_txn(true)
        .await
        .map_err(|e| format!("Failed to create read txn: {}", e))?;

    let headstore = txn
        .headstore()
        .map_err(|e| format!("Failed to get headstore: {}", e))?;
    let systemstore = txn
        .systemstore()
        .map_err(|e| format!("Failed to get systemstore: {}", e))?;

    let doc_short_id = crate::doc_id_map::get_doc_ref(&systemstore, doc_id)
        .await
        .map_err(|e| format!("Failed to resolve doc ref: {}", e))?
        .map(|doc_ref| doc_ref.doc_short_id)
        .ok_or_else(|| format!("No doc-ID mapping found for doc {}", doc_id))?;

    let composite_heads = get_all_field_heads(&headstore, doc_short_id, "C").await?;

    let composite_cid = composite_heads
        .first()
        .map(|entry| entry.cid)
        .ok_or_else(|| format!("No composite head found for doc {}", doc_id))?;

    let blockstore = txn
        .blockstore()
        .map_err(|e| format!("Failed to get blockstore: {}", e))?;

    let block_bytes = blockstore
        .get(&composite_cid.to_bytes())
        .await
        .map_err(|e| format!("Failed to read composite block: {}", e))?
        .ok_or_else(|| format!("Composite block not found for CID {}", composite_cid))?;

    let _ = txn.force_discard();

    Ok(BlockResult {
        cid: composite_cid,
        block: block_bytes,
        doc_id: doc_id.to_string(),
        field_cids: vec![],
        encryption_cids: vec![],
    })
}
