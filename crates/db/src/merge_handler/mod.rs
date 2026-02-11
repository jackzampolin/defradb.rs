//! Database merge handler for processing incoming P2P blocks.
//!
//! This module implements the `MergeHandler` trait from the P2P layer,
//! bridging incoming blocks to the CRDT system for document merging.

mod collection;
mod composite;
mod counter;
mod definition;
mod lww;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use cid::Cid;
use crdt::traits::{Context, ReplicatedData, ValueReader};
use crdt::{Counter, CounterDelta, Lww, LwwDelta, NumericKind};
use datastore::NamespaceView;
use defra_core::block::{
    Block, CollectionDefinitionDeltaPayload, CrdtDelta, Encryption, FieldDefinitionDeltaPayload,
};
use defra_core::types::DocId;
use document::{DocID, Document, NormalValue};
use events::{MergeCompleteData, Message, Update};
use p2p::sync::{BlockMetadata, MergeHandler, MergeOutcome};
use schema::{
    self, CType, CollectionSource, CollectionVersion, FieldDescription, FieldKind, QuerySource,
    ScalarKind,
};
use storage::corekv::{Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionVersionKey};

use crate::collection::collection_short_id;
use crate::database::DB;
use crate::error::Error;
use crate::index_manager::IndexManager;

/// Encode a priority value as a varint (matches Go's binary.PutUvarint).
pub(crate) fn encode_priority_varint(priority: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10);
    let mut n = priority;
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
    buf
}

/// Error type for database merge operations.
#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    /// Failed to decode block from DAG-CBOR.
    #[error("block decode failed: {0}")]
    BlockDecode(String),

    /// Unsupported CRDT delta type.
    #[error("unsupported delta type: {0}")]
    UnsupportedDelta(String),

    /// Missing metadata during non-recovery operation.
    #[error("missing metadata: {0}")]
    MissingMetadata(String),

    /// CRDT merge failed.
    #[error("merge failed: {0}")]
    MergeFailed(String),

    /// Database error.
    #[error("database error: {0}")]
    Database(#[from] Error),

    /// Storage error.
    #[error("storage error: {0}")]
    Storage(String),
}

/// Result of processing an LWW delta, including whether it was applied
/// and the value to use for document reconstruction.
pub(crate) struct LwwMergeResult {
    /// Whether the merge was applied (vs rejected/skipped)
    pub(crate) applied: bool,
    /// The winning value for document reconstruction (if applied, use incoming; else read from store)
    pub(crate) value: Option<NormalValue>,
}

/// Result of processing a Counter delta, including whether it was applied
/// and the accumulated value for document reconstruction.
pub(crate) struct CounterMergeResult {
    /// Whether the merge was applied (vs skipped due to nonce)
    pub(crate) applied: bool,
    /// The accumulated counter value after merge
    pub(crate) value: Option<NormalValue>,
}

/// Database merge handler that processes incoming P2P blocks.
///
/// This handler decodes IPLD blocks, extracts CRDT deltas, and applies
/// them to the database using the appropriate CRDT type.
pub struct DbMergeHandler<S: Store, B: blockstore::Blockstore> {
    /// Reference to the database for creating transactions.
    db: Arc<DB<S>>,
    /// Reference to the blockstore for loading linked blocks.
    blockstore: Arc<B>,
    /// Optional DocumentACP for checking access on encrypted+ACP-protected documents.
    /// Uses OnceLock so it can be set after construction (p2p.rs creates the merge
    /// handler before document_acp is available).
    document_acp: std::sync::OnceLock<Arc<dyn acp::DocumentACP>>,
    /// Tracks composite CIDs that have already been merged, preventing
    /// duplicate processing from concurrent dual-broadcast paths (doc topic
    /// + collection topic). Matches Go's `loadComposites` dedup guard.
    merged_composites: std::sync::Mutex<HashSet<Cid>>,
}

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    /// Create a new database merge handler.
    pub fn new(db: Arc<DB<S>>, blockstore: Arc<B>) -> Self {
        Self {
            db,
            blockstore,
            document_acp: std::sync::OnceLock::new(),
            merged_composites: std::sync::Mutex::new(HashSet::new()),
        }
    }

    /// Set the DocumentACP for access control checks during merge (builder pattern).
    pub fn with_document_acp(self, acp: Arc<dyn acp::DocumentACP>) -> Self {
        let _ = self.document_acp.set(acp);
        self
    }

    /// Set the DocumentACP after construction (when merge handler is already in Arc).
    pub fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>) {
        let _ = self.document_acp.set(acp);
    }

    /// Get reference to blockstore.
    pub fn blockstore(&self) -> &Arc<B> {
        &self.blockstore
    }

    /// Decrypt block delta data using the encryption metadata block.
    ///
    /// If `encryption_cid` is Some, loads the Encryption block from blockstore,
    /// extracts the AES key, and decrypts the data. Returns data unchanged if
    /// no encryption CID is present.
    pub(crate) async fn decrypt_block_data(
        &self,
        data: &[u8],
        encryption_cid: Option<&Cid>,
    ) -> std::result::Result<Vec<u8>, MergeError> {
        let enc_cid = match encryption_cid {
            Some(cid) => cid,
            None => return Ok(data.to_vec()),
        };

        // Load the Encryption block from blockstore
        let enc_data = self
            .blockstore
            .get(enc_cid)
            .await
            .map_err(|e| MergeError::Storage(e.to_string()))?
            .ok_or_else(|| {
                MergeError::Storage(format!("Encryption block {} not found", enc_cid))
            })?;

        let enc_block = Encryption::from_dag_cbor(&enc_data).map_err(|e| {
            MergeError::BlockDecode(format!("Failed to decode encryption block: {}", e))
        })?;

        // Decrypt using AES-256-GCM (nonce is prepended to ciphertext)
        match crypto::encryption::aes::decrypt_aes(None, data, &enc_block.key, &[]) {
            Ok(decrypted) => {
                tracing::debug!(
                    encryption_cid = %enc_cid,
                    plaintext_len = decrypted.len(),
                    "Decrypted block data"
                );
                Ok(decrypted)
            }
            Err(e) => {
                tracing::warn!(
                    encryption_cid = %enc_cid,
                    error = %e,
                    "Failed to decrypt block data"
                );
                Err(MergeError::MergeFailed(format!("Decryption failed: {}", e)))
            }
        }
    }

    /// Look up a previous CollectionVersion from block heads.
    ///
    /// For patched collection versions, the block's heads point to previous version CIDs.
    /// First checks systemstore, then falls back to decoding the head block directly
    /// from blockstore (for the UnknownCollection case where the initial version
    /// hasn't been processed yet).
    async fn resolve_previous_collection_version(
        &self,
        block: &Block,
    ) -> Result<Option<CollectionVersion>, MergeError> {
        let heads = match &block.heads {
            Some(heads) if !heads.is_empty() => heads,
            _ => return Ok(None),
        };

        for head_cid in heads {
            // Fast path: check systemstore (KnownCollection case)
            let head_key = CollectionKey::new(head_cid.to_string());
            let txn = self.db.new_txn(true).await.map_err(MergeError::Database)?;
            let systemstore = txn.systemstore().map_err(MergeError::Database)?;

            if let Ok(Some(data)) = systemstore.get(&head_key.bytes()).await {
                if let Ok(prev) = serde_json::from_slice::<CollectionVersion>(&data) {
                    tracing::debug!(
                        head_cid = %head_cid,
                        name = %prev.name,
                        collection_id = %prev.collection_id,
                        "Resolved previous collection version from systemstore"
                    );
                    return Ok(Some(prev));
                }
            }

            // Slow path: decode head block directly from blockstore.
            // Build a CollectionVersion from the raw block data without going
            // through the full merge handler (avoids async recursion).
            if let Ok(Some(head_block_data)) = self.blockstore.get(head_cid).await {
                let head_block = Block::from_dag_cbor(&head_block_data).map_err(|e| {
                    MergeError::BlockDecode(format!("Failed to decode head block: {}", e))
                })?;

                if let CrdtDelta::CollectionDefinition(head_payload) = &head_block.delta {
                    if let Some(name) = &head_payload.name {
                        tracing::debug!(
                            head_cid = %head_cid,
                            name = %name,
                            "Resolved previous collection version from blockstore"
                        );

                        // Decode field blocks from the head block's links
                        let mut prev_fields = Vec::new();
                        if let Some(links) = &head_block.links {
                            for link in links.iter() {
                                let field_cid = &link.link;
                                if let Ok(Some(field_bytes)) = self.blockstore.get(field_cid).await
                                {
                                    if let Ok(field_block) = Block::from_dag_cbor(&field_bytes) {
                                        if let CrdtDelta::FieldDefinition(fp) = &field_block.delta {
                                            if let Ok(fd) = self.field_definition_to_description(
                                                fp,
                                                &field_cid.to_string(),
                                            ) {
                                                prev_fields.push(fd);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        let head_version_id = head_cid.to_string();
                        let mut prev = CollectionVersion::new(
                            name,
                            &head_version_id,
                            &head_version_id,
                            prev_fields,
                        );
                        prev.is_active = false;
                        prev.is_materialized = true;

                        // Ensure _docID is first in fields
                        if let Some(pos) = prev.fields.iter().position(|f| f.name == "_docID") {
                            if pos > 0 {
                                let f = prev.fields.remove(pos);
                                prev.fields.insert(0, f);
                            }
                        }

                        // Store in systemstore so GetCollections can find it
                        let txn2 = self.db.new_txn(false).await.map_err(MergeError::Database)?;
                        {
                            let ss = txn2.systemstore().map_err(MergeError::Database)?;
                            let key = CollectionKey::new(&head_version_id);
                            let data = serde_json::to_vec(&prev).map_err(|e| {
                                MergeError::Storage(format!(
                                    "Failed to serialize prev collection: {}",
                                    e
                                ))
                            })?;
                            ss.set(&key.bytes(), &data).await.map_err(|e| {
                                MergeError::Storage(format!(
                                    "Failed to store prev collection: {}",
                                    e
                                ))
                            })?;
                            let vkey =
                                CollectionVersionKey::new(&head_version_id, &head_version_id);
                            ss.set(&vkey.bytes(), b"1").await.map_err(|e| {
                                MergeError::Storage(format!(
                                    "Failed to store prev version index: {}",
                                    e
                                ))
                            })?;
                        }
                        txn2.commit().await.map_err(MergeError::Database)?;
                        self.db
                            .add_collection_to_cache(prev.clone())
                            .map_err(MergeError::Database)?;

                        return Ok(Some(prev));
                    }
                }
            }
        }

        Ok(None)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static, B: blockstore::Blockstore + Send + Sync + 'static> MergeHandler
    for DbMergeHandler<S, B>
{
    type Error = MergeError;

    async fn handle_block(
        &self,
        cid: &Cid,
        block_data: &[u8],
        metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        tracing::debug!(
            cid = %cid,
            block_size = block_data.len(),
            is_recovery = metadata.is_recovery,
            "Handling block for merge"
        );

        // Decode the block from DAG-CBOR
        let block =
            Block::from_dag_cbor(block_data).map_err(|e| MergeError::BlockDecode(e.to_string()))?;

        tracing::debug!(
            cid = %cid,
            delta_type = ?std::mem::discriminant(&block.delta),
            heads_count = block.heads.as_ref().map(|h| h.len()).unwrap_or(0),
            links_count = block.links.as_ref().map(|l| l.len()).unwrap_or(0),
            "Block decoded successfully"
        );

        // Decrypt delta data if the block has encryption
        let decrypted_block;
        let effective_block = if block.encryption.is_some() {
            match &block.delta {
                CrdtDelta::Lww(payload) => {
                    match self
                        .decrypt_block_data(&payload.data, block.encryption.as_ref())
                        .await
                    {
                        Ok(decrypted_data) => {
                            let mut new_payload = payload.clone();
                            new_payload.data = decrypted_data;
                            decrypted_block = Block {
                                delta: CrdtDelta::Lww(new_payload),
                                heads: block.heads.clone(),
                                links: block.links.clone(),
                                encryption: block.encryption,
                                signature: block.signature,
                            };
                            &decrypted_block
                        }
                        Err(_) => &block, // Decryption failed (no key) — use encrypted data
                    }
                }
                CrdtDelta::Counter(payload) => {
                    match self
                        .decrypt_block_data(&payload.data, block.encryption.as_ref())
                        .await
                    {
                        Ok(decrypted_data) => {
                            let mut new_payload = payload.clone();
                            new_payload.data = decrypted_data;
                            decrypted_block = Block {
                                delta: CrdtDelta::Counter(new_payload),
                                heads: block.heads.clone(),
                                links: block.links.clone(),
                                encryption: block.encryption,
                                signature: block.signature,
                            };
                            &decrypted_block
                        }
                        Err(_) => &block,
                    }
                }
                _ => &block,
            }
        } else {
            &block
        };

        // Process based on delta type
        match &effective_block.delta {
            CrdtDelta::Lww(payload) => self.process_lww_delta(cid, payload, &metadata).await,
            CrdtDelta::Counter(payload) => {
                self.process_counter_delta(cid, payload, &metadata).await
            }
            CrdtDelta::Composite(payload) => {
                self.process_composite_delta(cid, &block, payload, &metadata, false)
                    .await
            }
            CrdtDelta::Collection(payload) => {
                self.process_collection_delta(cid, &block, payload, &metadata)
                    .await
            }
            CrdtDelta::FieldDefinition(_) => {
                // Field definitions are processed as part of CollectionDefinition
                tracing::debug!(cid = %cid, "FieldDefinition delta - skipping (processed with collection)");
                Ok(MergeOutcome::skipped(
                    "field definition processed with collection",
                ))
            }
            CrdtDelta::CollectionDefinition(payload) => {
                self.process_collection_definition_delta(cid, &block, payload, &metadata)
                    .await
            }
            CrdtDelta::CollectionSet(_) => {
                tracing::debug!(cid = %cid, "CollectionSet delta - skipping");
                Ok(MergeOutcome::skipped("collection set delta"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockstore::DefraBlockstore;
    use storage::backends::MemoryStore;

    #[tokio::test]
    async fn test_merge_handler_creation() {
        let store = MemoryStore::new();
        let store_arc = Arc::new(store);
        let db = Arc::new(DB::from_arc(store_arc.clone()).unwrap());
        let blockstore = Arc::new(DefraBlockstore::new(store_arc, false));
        let _handler = DbMergeHandler::new(db, blockstore);
    }
}
