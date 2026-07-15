//! Doc short-ID allocation and DocID mappings.
//!
//! Mirrors Go's `internal/db/id/document.go`: documents are stored under a
//! node-local `u64` short ID allocated from `/seq/doc`; the systemstore
//! holds the bidirectional mapping between short IDs and the public
//! genesis-CID-derived DocIDs, plus a block-CID -> DocID ownership index.

use datastore::NamespaceView;
use storage::corekv::Key;
use storage::keys::doc_id_index::{
    BlockCIDToDocIDKey, DocIDToDocRefKey, DocRef, DocShortIDSequenceKey, DocShortIDToDocIDAliasKey,
    DocShortIDToDocIDKey,
};

use crate::error::{Error, Result};

/// Allocate the next document short ID from the systemstore sequence.
pub async fn next_doc_short_id(systemstore: &NamespaceView) -> Result<u64> {
    let key = DocShortIDSequenceKey::new().bytes();
    let current: u64 = match systemstore.get(&key).await.map_err(Error::Storage)? {
        Some(bytes) if bytes.len() == 8 => u64::from_be_bytes(bytes.as_slice().try_into().unwrap()),
        _ => 0,
    };
    let next = current + 1;
    systemstore
        .set(&key, &next.to_be_bytes())
        .await
        .map_err(Error::Storage)?;
    Ok(next)
}

/// Persist the bidirectional short-ID <-> DocID mapping for a document.
pub async fn set_doc_id_mapping(
    systemstore: &NamespaceView,
    collection_short_id: u32,
    doc_short_id: u64,
    doc_id: &str,
) -> Result<()> {
    systemstore
        .set(
            &DocShortIDToDocIDKey::new(doc_short_id).bytes(),
            doc_id.as_bytes(),
        )
        .await
        .map_err(Error::Storage)?;

    if collection_short_id == 0 || doc_short_id == 0 || doc_id.is_empty() {
        return Ok(());
    }

    systemstore
        .set(
            &DocIDToDocRefKey::new(doc_id).bytes(),
            &DocRef::new(collection_short_id, doc_short_id).encode(),
        )
        .await
        .map_err(Error::Storage)?;

    systemstore
        .set(
            &DocShortIDToDocIDAliasKey::new(doc_short_id, doc_id).bytes(),
            doc_id.as_bytes(),
        )
        .await
        .map_err(Error::Storage)
}

/// Resolve a short ID back to its public DocID.
pub async fn get_doc_id(systemstore: &NamespaceView, doc_short_id: u64) -> Result<Option<String>> {
    match systemstore
        .get(&DocShortIDToDocIDKey::new(doc_short_id).bytes())
        .await
        .map_err(Error::Storage)?
    {
        Some(bytes) => Ok(Some(String::from_utf8(bytes).map_err(|e| {
            Error::InvalidDocument(format!("invalid utf-8 in doc-ID mapping: {e}"))
        })?)),
        None => Ok(None),
    }
}

/// Resolve doc short IDs to public DocIDs, preserving order and skipping
/// IDs with no mapping (index scan read path).
pub async fn resolve_doc_ids(
    systemstore: &NamespaceView,
    doc_short_ids: &[u64],
) -> Result<Vec<String>> {
    let mut doc_ids = Vec::with_capacity(doc_short_ids.len());
    for doc_short_id in doc_short_ids {
        if let Some(doc_id) = get_doc_id(systemstore, *doc_short_id).await? {
            doc_ids.push(doc_id);
        }
    }
    Ok(doc_ids)
}

/// Resolve short-ID-keyed scores to public-DocID-keyed scores, skipping
/// IDs with no mapping (full-text search read path).
pub async fn resolve_doc_id_scores(
    systemstore: &NamespaceView,
    scores: std::collections::HashMap<u64, f64>,
) -> Result<std::collections::HashMap<String, f64>> {
    let mut resolved = std::collections::HashMap::with_capacity(scores.len());
    for (doc_short_id, score) in scores {
        if let Some(doc_id) = get_doc_id(systemstore, doc_short_id).await? {
            resolved.insert(doc_id, score);
        }
    }
    Ok(resolved)
}

/// Resolve a public DocID to its DocRef (collection + doc short IDs).
pub async fn get_doc_ref(systemstore: &NamespaceView, doc_id: &str) -> Result<Option<DocRef>> {
    match systemstore
        .get(&DocIDToDocRefKey::new(doc_id).bytes())
        .await
        .map_err(Error::Storage)?
    {
        Some(bytes) => Ok(Some(DocRef::decode(&bytes).map_err(Error::Storage)?)),
        None => Ok(None),
    }
}

/// Resolve a public DocID to its short ID within a specific collection.
///
/// Returns `None` when the document is unknown or belongs to a different
/// collection (Go parity: `GetDocShortID`).
pub async fn get_doc_short_id(
    systemstore: &NamespaceView,
    collection_short_id: u32,
    doc_id: &str,
) -> Result<Option<u64>> {
    match get_doc_ref(systemstore, doc_id).await? {
        Some(doc_ref) if doc_ref.collection_short_id == collection_short_id => {
            Ok(Some(doc_ref.doc_short_id))
        }
        _ => Ok(None),
    }
}

/// Resolve a short ID for a DocID, allocating and persisting a new mapping
/// if none exists (merge/ingest path, mirrors Go's
/// `resolveOrAllocateDocShortID`).
pub async fn resolve_or_allocate_doc_short_id(
    systemstore: &NamespaceView,
    collection_short_id: u32,
    doc_id: &str,
) -> Result<u64> {
    if let Some(short_id) = get_doc_short_id(systemstore, collection_short_id, doc_id).await? {
        return Ok(short_id);
    }
    let short_id = next_doc_short_id(systemstore).await?;
    set_doc_id_mapping(systemstore, collection_short_id, short_id, doc_id).await?;
    Ok(short_id)
}

/// Register an additional public DocID for an existing document (backup
/// import aliasing): the alias resolves through `/d/p` and is enumerable
/// via `/d/r`, without replacing the document's primary `/d/s` entry.
pub async fn set_doc_id_alias(
    systemstore: &NamespaceView,
    collection_short_id: u32,
    doc_short_id: u64,
    alias_doc_id: &str,
) -> Result<()> {
    if collection_short_id == 0 || doc_short_id == 0 || alias_doc_id.is_empty() {
        return Ok(());
    }

    let new_ref = DocRef::new(collection_short_id, doc_short_id);
    if let Some(existing_ref) = get_doc_ref(systemstore, alias_doc_id).await? {
        if existing_ref != new_ref {
            return Err(Error::InvalidDocument(format!(
                "document ID '{alias_doc_id}' already belongs to another document"
            )));
        }
    }

    systemstore
        .set(
            &DocIDToDocRefKey::new(alias_doc_id).bytes(),
            &new_ref.encode(),
        )
        .await
        .map_err(Error::Storage)?;

    systemstore
        .set(
            &DocShortIDToDocIDAliasKey::new(doc_short_id, alias_doc_id).bytes(),
            alias_doc_id.as_bytes(),
        )
        .await
        .map_err(Error::Storage)
}

/// Record that a block belongs to a document.
///
/// Field blocks can be byte-identical across documents, so ownership is a
/// set keyed by (block CID, DocID).
pub async fn set_block_doc_id_mapping(
    systemstore: &NamespaceView,
    block_cid: &str,
    doc_id: &str,
) -> Result<()> {
    if block_cid.is_empty() || doc_id.is_empty() {
        return Ok(());
    }
    systemstore
        .set(&BlockCIDToDocIDKey::new(block_cid, doc_id).bytes(), &[])
        .await
        .map_err(Error::Storage)
}

/// Return every DocID that owns `block_cid`.
pub async fn get_doc_ids_for_block(
    systemstore: &NamespaceView,
    block_cid: &str,
) -> Result<Vec<String>> {
    use storage::corekv::IterOptions;

    if block_cid.is_empty() {
        return Ok(Vec::new());
    }

    let prefix = BlockCIDToDocIDKey::block_prefix(block_cid);
    let prefix_len = prefix.len();
    let mut iter = systemstore
        .iterator(IterOptions::new().with_prefix(prefix))
        .await
        .map_err(Error::Storage)?;

    let mut doc_ids = Vec::new();
    while let Some(kv) = iter.next().await.map_err(Error::Storage)? {
        if kv.key.len() > prefix_len {
            if let Ok(doc_id) = String::from_utf8(kv.key[prefix_len..].to_vec()) {
                doc_ids.push(doc_id);
            }
        }
    }
    Ok(doc_ids)
}

/// Resolve the document owners used to authorize a block.
///
/// `Some([])` identifies collection-level/non-document blocks, `Some(owners)`
/// identifies document data, and `None` means a document block has no
/// trustworthy owner yet and must not be served.
pub async fn resolve_block_doc_ids(
    systemstore: &NamespaceView,
    cid: &cid::Cid,
    block: &defra_core::Block,
) -> Result<Option<Vec<String>>> {
    let is_doc_delta = matches!(
        block.delta,
        defra_core::CrdtDelta::Lww(_)
            | defra_core::CrdtDelta::Counter(_)
            | defra_core::CrdtDelta::Composite(_)
    );
    if !is_doc_delta {
        return Ok(Some(Vec::new()));
    }

    let mut doc_ids = get_doc_ids_for_block(systemstore, &cid.to_string()).await?;
    if !doc_ids.is_empty() {
        doc_ids.sort();
        doc_ids.dedup();
        return Ok(Some(doc_ids));
    }

    let is_genesis = block.heads.as_ref().is_none_or(Vec::is_empty)
        && block.delta.priority() == 1
        && matches!(block.delta, defra_core::CrdtDelta::Composite(_));
    if is_genesis {
        return Ok(Some(vec![document::DocID::new_v0(*cid).to_string()]));
    }

    Ok(None)
}

/// Delete the block ownership record for (block CID, DocID).
pub async fn delete_block_doc_id_mapping(
    systemstore: &NamespaceView,
    block_cid: &str,
    doc_id: &str,
) -> Result<()> {
    if block_cid.is_empty() || doc_id.is_empty() {
        return Ok(());
    }
    systemstore
        .delete(&BlockCIDToDocIDKey::new(block_cid, doc_id).bytes())
        .await
        .map_err(Error::Storage)
}

/// Delete every mapping owned by a short ID (purge path): the
/// short-ID -> DocID entry, all alias entries, and their DocID -> DocRef
/// counterparts.
pub async fn delete_doc_id_mappings(systemstore: &NamespaceView, doc_short_id: u64) -> Result<()> {
    use storage::corekv::IterOptions;

    if doc_short_id == 0 {
        return Ok(());
    }

    systemstore
        .delete(&DocShortIDToDocIDKey::new(doc_short_id).bytes())
        .await
        .map_err(Error::Storage)?;

    let prefix = DocShortIDToDocIDAliasKey::short_id_prefix(doc_short_id);
    let prefix_len = prefix.len();
    let mut iter = systemstore
        .iterator(IterOptions::new().with_prefix(prefix))
        .await
        .map_err(Error::Storage)?;

    // Recover each alias DocID from the KEY suffix, not the value: the key
    // is authoritative, so a malformed value can never orphan the reverse
    // (DocID -> DocRef) mapping.
    let mut mappings: Vec<(Vec<u8>, String)> = Vec::new();
    while let Some(kv) = iter.next().await.map_err(Error::Storage)? {
        let doc_id = if kv.key.len() > prefix_len {
            String::from_utf8(kv.key[prefix_len..].to_vec()).map_err(|e| {
                Error::InvalidDocument(format!("invalid utf-8 in doc-ID alias key: {e}"))
            })?
        } else {
            String::new()
        };
        mappings.push((kv.key.clone(), doc_id));
    }
    drop(iter);

    for (key, doc_id) in mappings {
        if !doc_id.is_empty() {
            systemstore
                .delete(&DocIDToDocRefKey::new(&doc_id).bytes())
                .await
                .map_err(Error::Storage)?;
        }
        systemstore.delete(&key).await.map_err(Error::Storage)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastore::SharedTxn;
    use storage::backends::MemoryStore;
    use storage::corekv::Store;
    use storage::namespace::Namespace;

    async fn systemstore() -> NamespaceView {
        let store = MemoryStore::new();
        let txn = store.new_txn(false).await.unwrap();
        NamespaceView::new(SharedTxn::new(txn), Namespace::Systemstore)
    }

    #[tokio::test]
    async fn short_ids_allocate_from_one() {
        let store = systemstore().await;
        assert_eq!(next_doc_short_id(&store).await.unwrap(), 1);
        assert_eq!(next_doc_short_id(&store).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn mapping_roundtrip() {
        let store = systemstore().await;
        let short_id = next_doc_short_id(&store).await.unwrap();
        set_doc_id_mapping(&store, 7, short_id, "bae-x")
            .await
            .unwrap();

        assert_eq!(
            get_doc_id(&store, short_id).await.unwrap().as_deref(),
            Some("bae-x")
        );
        assert_eq!(
            get_doc_short_id(&store, 7, "bae-x").await.unwrap(),
            Some(short_id)
        );
        // Wrong collection: not visible.
        assert_eq!(get_doc_short_id(&store, 8, "bae-x").await.unwrap(), None);
    }

    #[tokio::test]
    async fn resolve_or_allocate_is_idempotent() {
        let store = systemstore().await;
        let a = resolve_or_allocate_doc_short_id(&store, 3, "bae-y")
            .await
            .unwrap();
        let b = resolve_or_allocate_doc_short_id(&store, 3, "bae-y")
            .await
            .unwrap();
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn alias_cannot_replace_an_existing_document() {
        let store = systemstore().await;
        set_doc_id_mapping(&store, 3, 1, "bae-a").await.unwrap();
        set_doc_id_mapping(&store, 3, 2, "bae-b").await.unwrap();

        let error = set_doc_id_alias(&store, 3, 2, "bae-a").await.unwrap_err();

        assert!(error.to_string().contains("already belongs"));
        assert_eq!(get_doc_short_id(&store, 3, "bae-a").await.unwrap(), Some(1));
    }

    #[tokio::test]
    async fn setting_the_same_alias_is_idempotent() {
        let store = systemstore().await;
        set_doc_id_mapping(&store, 3, 1, "bae-a").await.unwrap();
        store
            .delete(&DocShortIDToDocIDAliasKey::new(1, "bae-a").bytes())
            .await
            .unwrap();

        set_doc_id_alias(&store, 3, 1, "bae-a").await.unwrap();

        assert_eq!(get_doc_short_id(&store, 3, "bae-a").await.unwrap(), Some(1));
        assert!(store
            .has(&DocShortIDToDocIDAliasKey::new(1, "bae-a").bytes())
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn ownerless_document_blocks_are_not_authorizable() {
        let store = systemstore().await;
        let field_block = defra_core::Block::new(
            defra_core::CrdtDelta::Lww(defra_core::LwwDeltaPayload {
                field_name: "name".to_string(),
                schema_version_id: "v1".to_string(),
                priority: 1,
                data: vec![1],
            }),
            vec![],
            vec![],
        );
        let field_cid = field_block.generate_cid().unwrap();
        assert_eq!(
            resolve_block_doc_ids(&store, &field_cid, &field_block)
                .await
                .unwrap(),
            None
        );

        let genesis = defra_core::Block::new(
            defra_core::CrdtDelta::Composite(defra_core::CompositeDeltaPayload {
                schema_version_id: "v1".to_string(),
                priority: 1,
                status: 1,
            }),
            vec![],
            vec![],
        );
        let genesis_cid = genesis.generate_cid().unwrap();
        assert_eq!(
            resolve_block_doc_ids(&store, &genesis_cid, &genesis)
                .await
                .unwrap(),
            Some(vec![document::DocID::new_v0(genesis_cid).to_string()])
        );
    }

    #[tokio::test]
    async fn block_ownership_is_a_set() {
        let store = systemstore().await;
        set_block_doc_id_mapping(&store, "bafy1", "bae-a")
            .await
            .unwrap();
        set_block_doc_id_mapping(&store, "bafy1", "bae-b")
            .await
            .unwrap();

        let mut owners = get_doc_ids_for_block(&store, "bafy1").await.unwrap();
        owners.sort();
        assert_eq!(owners, vec!["bae-a".to_string(), "bae-b".to_string()]);

        delete_block_doc_id_mapping(&store, "bafy1", "bae-a")
            .await
            .unwrap();
        assert_eq!(
            get_doc_ids_for_block(&store, "bafy1").await.unwrap(),
            vec!["bae-b".to_string()]
        );
    }

    #[tokio::test]
    async fn delete_doc_id_mappings_removes_all() {
        let store = systemstore().await;
        let short_id = next_doc_short_id(&store).await.unwrap();
        set_doc_id_mapping(&store, 2, short_id, "bae-z")
            .await
            .unwrap();

        delete_doc_id_mappings(&store, short_id).await.unwrap();

        assert_eq!(get_doc_id(&store, short_id).await.unwrap(), None);
        assert_eq!(get_doc_short_id(&store, 2, "bae-z").await.unwrap(), None);
    }
}
