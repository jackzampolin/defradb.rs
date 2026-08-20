//! Doc short-ID allocation and DocID mappings.
//!
//! Mirrors Go's `internal/db/id/document.go`: documents are stored under a
//! node-local `u64` short ID allocated from `/seq/doc`; the systemstore
//! holds the bidirectional mapping between short IDs and the public
//! genesis-CID-derived DocIDs, plus a block-CID -> DocID ownership index.

use async_lock::Mutex;
use datastore::NamespaceView;
use std::sync::Arc;
use storage::corekv::{Key, Store};
use storage::keys::doc_id_index::{
    BlockCIDToDocIDKey, DocIDToDocRefKey, DocRef, DocShortIDSequenceKey, DocShortIDToDocIDAliasKey,
    DocShortIDToDocIDKey,
};
use storage::stores::Systemstore;

use crate::error::{Error, Result};

const DOC_SHORT_ID_RESERVATION_SIZE: u64 = 1024;
const MAX_RESERVATION_CONFLICT_RETRIES: u32 = 16;

#[derive(Default)]
struct ReservedRange {
    next: u64,
    remaining: u64,
}

/// Allocates document short IDs from persisted ranges.
///
/// The sequence stores the end of the latest reserved range. A crash can leave
/// gaps, but every returned ID remains unique across restarts and DB instances.
pub struct DocShortIdAllocator<S: Store> {
    systemstore: Systemstore<S>,
    range: Mutex<ReservedRange>,
    reservation_size: u64,
}

impl<S: Store> DocShortIdAllocator<S> {
    pub(crate) fn new(store: Arc<S>) -> Self {
        Self::with_reservation_size(store, DOC_SHORT_ID_RESERVATION_SIZE)
    }

    pub fn with_reservation_size(store: Arc<S>, reservation_size: u64) -> Self {
        assert!(reservation_size > 0, "reservation size must be non-zero");
        Self {
            systemstore: Systemstore::new(store),
            range: Mutex::new(ReservedRange::default()),
            reservation_size,
        }
    }

    pub async fn next(&self) -> Result<u64> {
        let mut range = self.range.lock().await;
        if range.remaining == 0 {
            range.next = self.reserve_range().await?;
            range.remaining = self.reservation_size;
        }

        let id = range.next;
        range.remaining -= 1;
        if range.remaining > 0 {
            range.next = id
                .checked_add(1)
                .ok_or_else(|| Error::Other("document short-ID sequence exhausted".into()))?;
        }
        Ok(id)
    }

    async fn reserve_range(&self) -> Result<u64> {
        let key = DocShortIDSequenceKey::new().bytes();
        let mut conflicts = 0;
        loop {
            let mut txn = self
                .systemstore
                .new_txn(false)
                .await
                .map_err(Error::Storage)?;
            let current = decode_sequence(txn.get(&key).await.map_err(Error::Storage)?)?;
            let start = current
                .checked_add(1)
                .ok_or_else(|| Error::Other("document short-ID sequence exhausted".into()))?;
            let end = current
                .checked_add(self.reservation_size)
                .ok_or_else(|| Error::Other("document short-ID sequence exhausted".into()))?;
            txn.set(&key, &end.to_be_bytes())
                .await
                .map_err(Error::Storage)?;

            match txn.commit().await {
                Ok(()) => return Ok(start),
                Err(error)
                    if error.is_txn_conflict() && conflicts < MAX_RESERVATION_CONFLICT_RETRIES =>
                {
                    conflicts += 1;
                }
                Err(error) => return Err(Error::Storage(error)),
            }
        }
    }
}

pub fn decode_sequence(value: Option<Vec<u8>>) -> Result<u64> {
    match value {
        None => Ok(0),
        Some(bytes) => {
            let bytes: [u8; 8] = bytes
                .try_into()
                .map_err(|_| Error::Other("invalid persisted document short-ID sequence".into()))?;
            Ok(u64::from_be_bytes(bytes))
        }
    }
}

/// Allocate the next document short ID within a migration transaction.
///
/// Runtime writes must use [`crate::DB::next_doc_short_id`] so the global
/// sequence is not part of the caller's conflict set.
pub async fn next_doc_short_id(systemstore: &NamespaceView) -> Result<u64> {
    let key = DocShortIDSequenceKey::new().bytes();
    let current = decode_sequence(systemstore.get(&key).await.map_err(Error::Storage)?)?;
    let next = current + 1;
    systemstore
        .set(&key, &next.to_be_bytes())
        .await
        .map_err(Error::Storage)?;
    Ok(next)
}

/// Advance the document short-ID sequence to at least `minimum`.
///
/// Store migrations can encounter an already-created mapping after an
/// interrupted or mixed-version upgrade. Keeping the allocator above every
/// reused ID prevents a later document from overwriting that mapping.
pub async fn ensure_doc_short_id_sequence_at_least(
    systemstore: &NamespaceView,
    minimum: u64,
) -> Result<()> {
    let key = DocShortIDSequenceKey::new().bytes();
    let current = decode_sequence(systemstore.get(&key).await.map_err(Error::Storage)?)?;
    if current < minimum {
        systemstore
            .set(&key, &minimum.to_be_bytes())
            .await
            .map_err(Error::Storage)?;
    }
    Ok(())
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
