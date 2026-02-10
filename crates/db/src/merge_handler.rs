//! Database merge handler for processing incoming P2P blocks.
//!
//! This module implements the `MergeHandler` trait from the P2P layer,
//! bridging incoming blocks to the CRDT system for document merging.

use std::collections::HashMap;
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
    self, CType, CollectionVersion, FieldDescription, FieldKind, QuerySource, ScalarKind,
};
use storage::corekv::{Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionVersionKey};

use crate::collection::collection_short_id;
use crate::database::DB;
use crate::error::Error;
use crate::index_manager::IndexManager;

/// Check whether the merge handler should skip decryption for an encrypted block.
///
/// Returns `true` if the block is encrypted AND the collection has an ACP policy
/// AND the document is NOT registered in the local ACP.
///
/// Go stores encryption keys in a separate Encstore that is only populated via KMS
/// (Key Management Service). Without KMS authorization, the keys never reach the
/// remote node's Encstore, so Go's merge handler cannot decrypt and sets canRead=false.
///
/// Rust doesn't have KMS — encryption blocks are in the main blockstore and synced
/// via Bitswap. This helper replicates Go's behavior: if we don't have local ACP
/// registration for the document, we treat it as if we don't have the key.
async fn should_skip_encrypted_merge(
    document_acp: Option<&Arc<dyn acp::DocumentACP>>,
    collection: Option<&CollectionVersion>,
    doc_id: &str,
) -> bool {
    let acp = match document_acp {
        Some(a) => a,
        None => return false,
    };
    let col = match collection {
        Some(c) => c,
        None => return false,
    };
    let policy = match &col.policy {
        Some(p) => p,
        None => return false,
    };

    // If the document IS registered in our local ACP, we created it (or have explicit access).
    // Allow decryption.
    match acp
        .is_doc_registered(&policy.id, &policy.resource_name, doc_id)
        .await
    {
        Ok(true) => false, // Registered → allow decryption
        Ok(false) => true, // Not registered → skip (replicated doc, no local access)
        Err(_) => true,    // Error checking → fail-closed, skip
    }
}

/// Marker byte indicating a document is deleted (matches Go's DeletedObjectMarker).
const DELETED_MARKER: u8 = 0x01;

/// Build the deletion marker key: /del/{collection_id}/{doc_id}
fn build_deleted_key(collection_id: &str, doc_id: &str) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(b"/del/");
    key.extend_from_slice(collection_id.as_bytes());
    key.push(b'/');
    key.extend_from_slice(doc_id.as_bytes());
    key
}

/// Encode a priority value as a varint (matches Go's binary.PutUvarint).
fn encode_priority_varint(priority: u64) -> Vec<u8> {
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
struct LwwMergeResult {
    /// Whether the merge was applied (vs rejected/skipped)
    applied: bool,
    /// The winning value for document reconstruction (if applied, use incoming; else read from store)
    value: Option<NormalValue>,
}

/// Result of processing a Counter delta, including whether it was applied
/// and the accumulated value for document reconstruction.
struct CounterMergeResult {
    /// Whether the merge was applied (vs skipped due to nonce)
    applied: bool,
    /// The accumulated counter value after merge
    value: Option<NormalValue>,
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
}

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    /// Create a new database merge handler.
    pub fn new(db: Arc<DB<S>>, blockstore: Arc<B>) -> Self {
        Self {
            db,
            blockstore,
            document_acp: std::sync::OnceLock::new(),
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
    async fn decrypt_block_data(
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

    /// Process an LWW delta from a block (standalone, with its own transaction).
    async fn process_lww_delta(
        &self,
        cid: &Cid,
        payload: &defra_core::block::LwwDeltaPayload,
        _metadata: &BlockMetadata<'_>,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        tracing::debug!(
            cid = %cid,
            field_name = %payload.field_name,
            priority = payload.priority,
            "Processing LWW delta"
        );

        // Create a new transaction for this merge
        let txn = self.db.new_txn(false).await?;

        // Create the LWW CRDT for this field
        let lww = Lww::new(
            payload.schema_version_id.clone(),
            &payload.doc_id,
            payload.field_name.clone(),
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Create the delta
        let delta = LwwDelta::new(
            payload.doc_id.clone(),
            payload.field_name.clone(),
            payload.priority,
            payload.schema_version_id.clone(),
            payload.data.clone(),
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Create the context
        let doc_id_str = String::from_utf8_lossy(&payload.doc_id).to_string();
        let ctx = Context {
            doc_id: DocId::new(&doc_id_str),
            schema_version: payload.schema_version_id.clone(),
        };

        // Perform the merge in a scoped block to ensure datastore reference is dropped
        // before we try to commit/discard the transaction.
        let result = {
            let mut datastore = txn.datastore()?;
            lww.merge(&mut datastore, &ctx, &delta).await
        };

        match result {
            Ok(merge_result) => {
                if merge_result.was_applied() {
                    // Commit the transaction
                    txn.force_commit().await?;
                    tracing::info!(
                        cid = %cid,
                        field_name = %payload.field_name,
                        doc_id = %doc_id_str,
                        "LWW delta merged successfully"
                    );
                    Ok(MergeOutcome::Merged)
                } else if merge_result.was_rejected() {
                    // Discard the transaction - nothing to commit
                    if let Err(e) = txn.force_discard() {
                        tracing::error!(
                            cid = %cid,
                            error = %e,
                            "Failed to discard transaction after CRDT rejection - potential resource leak"
                        );
                    }
                    tracing::debug!(
                        cid = %cid,
                        field_name = %payload.field_name,
                        "LWW delta rejected by CRDT (lower priority or tie-break)"
                    );
                    Ok(MergeOutcome::skipped(
                        "rejected by CRDT conflict resolution",
                    ))
                } else {
                    // Skipped (already applied)
                    if let Err(e) = txn.force_discard() {
                        tracing::error!(
                            cid = %cid,
                            error = %e,
                            "Failed to discard transaction after skip - potential resource leak"
                        );
                    }
                    tracing::debug!(
                        cid = %cid,
                        field_name = %payload.field_name,
                        "LWW delta skipped (already applied)"
                    );
                    Ok(MergeOutcome::skipped("already applied"))
                }
            }
            Err(e) => {
                if let Err(discard_err) = txn.force_discard() {
                    tracing::error!(
                        cid = %cid,
                        discard_error = %discard_err,
                        merge_error = %e,
                        "Failed to discard transaction after merge error - potential resource leak"
                    );
                }
                Err(MergeError::MergeFailed(e.to_string()))
            }
        }
    }

    /// Process an LWW delta within an existing transaction, returning the merge result
    /// and the winning value for document reconstruction.
    async fn process_lww_delta_in_txn(
        &self,
        datastore: &mut NamespaceView,
        cid: &Cid,
        payload: &defra_core::block::LwwDeltaPayload,
    ) -> std::result::Result<LwwMergeResult, MergeError> {
        tracing::debug!(
            cid = %cid,
            field_name = %payload.field_name,
            priority = payload.priority,
            "Processing LWW delta in transaction"
        );

        // Create the LWW CRDT for this field
        let lww = Lww::new(
            payload.schema_version_id.clone(),
            &payload.doc_id,
            payload.field_name.clone(),
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Create the delta
        let delta = LwwDelta::new(
            payload.doc_id.clone(),
            payload.field_name.clone(),
            payload.priority,
            payload.schema_version_id.clone(),
            payload.data.clone(),
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Create the context
        let doc_id_str = String::from_utf8_lossy(&payload.doc_id).to_string();
        let ctx = Context {
            doc_id: DocId::new(&doc_id_str),
            schema_version: payload.schema_version_id.clone(),
        };

        // Perform the merge
        let merge_result = lww
            .merge(datastore, &ctx, &delta)
            .await
            .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Determine the winning value for document reconstruction
        let (applied, value) = if merge_result.was_applied() {
            // Incoming value won - use it
            tracing::debug!(
                cid = %cid,
                field_name = %payload.field_name,
                "LWW delta applied - using incoming value"
            );
            let value = if !payload.data.is_empty() {
                match ciborium::from_reader::<NormalValue, _>(&payload.data[..]) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        tracing::error!(
                            field_name = %payload.field_name,
                            error = %e,
                            "Failed to decode applied field value from CBOR"
                        );
                        return Err(MergeError::BlockDecode(format!(
                            "Failed to decode field '{}': {}",
                            payload.field_name, e
                        )));
                    }
                }
            } else {
                None // Tombstone
            };
            (true, value)
        } else {
            // Incoming value was rejected - read the winning value from CRDT storage
            tracing::debug!(
                cid = %cid,
                field_name = %payload.field_name,
                result = ?merge_result,
                "LWW delta rejected - reading winning value from storage"
            );

            // Read the current (winning) value from storage
            let value = match crdt::traits::ValueReader::value(&lww, datastore).await {
                Ok(data) => {
                    if data.is_empty() {
                        None // Tombstone/deleted
                    } else {
                        match ciborium::from_reader::<NormalValue, _>(&data[..]) {
                            Ok(v) => Some(v),
                            Err(e) => {
                                tracing::warn!(
                                    field_name = %payload.field_name,
                                    error = %e,
                                    "Failed to decode existing field value from CBOR - skipping field"
                                );
                                None
                            }
                        }
                    }
                }
                Err(e) => {
                    // Field may not exist yet - this is not an error
                    tracing::debug!(
                        field_name = %payload.field_name,
                        error = %e,
                        "Could not read existing field value - field may not exist"
                    );
                    None
                }
            };
            (false, value)
        };

        Ok(LwwMergeResult { applied, value })
    }

    /// Process a Composite delta from a block.
    ///
    /// Composite deltas contain links to the actual field LWW/Counter blocks.
    /// This method processes all linked blocks within a SINGLE transaction to ensure
    /// atomicity between CRDT field merges and document storage.
    ///
    /// When `from_collection` is true, this composite is being processed as part of
    /// a collection-level sync (BranchableSync). The caller (`process_collection_delta`)
    /// handles collection headstore updates, so we skip creating local collection blocks
    /// to avoid race conditions with _commits queries.
    async fn process_composite_delta(
        &self,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CompositeDeltaPayload,
        metadata: &BlockMetadata<'_>,
        from_collection: bool,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        let doc_id_str = String::from_utf8_lossy(&payload.doc_id).to_string();

        tracing::info!(
            cid = %cid,
            doc_id = %doc_id_str,
            priority = payload.priority,
            status = payload.status,
            links = ?block.links,
            heads = ?block.heads,
            "Processing Composite delta (document-level)"
        );

        // Recursively merge parent composites referenced in `heads` before
        // processing this block.  This matches Go's processLog which walks
        // the DAG backwards and merges from oldest to newest, ensuring all
        // prior CRDT deltas are applied before the current one.
        if let Some(heads) = &block.heads {
            for head_cid in heads {
                // Load the parent block from blockstore
                let head_data = match self.blockstore.get(head_cid).await {
                    Ok(Some(data)) => data,
                    Ok(None) => {
                        tracing::debug!(
                            parent_cid = %head_cid,
                            child_cid = %cid,
                            "Parent composite not in blockstore, skipping"
                        );
                        continue;
                    }
                    Err(e) => {
                        tracing::debug!(
                            parent_cid = %head_cid,
                            error = %e,
                            "Failed to load parent composite, skipping"
                        );
                        continue;
                    }
                };

                let head_block = match Block::from_dag_cbor(&head_data) {
                    Ok(b) => b,
                    Err(_) => continue,
                };

                if let CrdtDelta::Composite(head_payload) = &head_block.delta {
                    tracing::info!(
                        parent_cid = %head_cid,
                        child_cid = %cid,
                        "Recursively merging parent composite before current"
                    );
                    // Recursive call — the parent will in turn merge its own parents.
                    // Each composite opens its own transaction so ordering is safe.
                    // Box::pin is required because recursive async fns are unsized.
                    let _ = Box::pin(self.process_composite_delta(
                        head_cid,
                        &head_block,
                        head_payload,
                        metadata,
                        from_collection,
                    ))
                    .await;
                }
            }
        }

        // Create a SINGLE transaction for all field merges AND document storage
        let txn = self.db.new_txn(false).await?;

        // Pre-lookup the collection for ACP checks on encrypted blocks.
        // Go uses a separate Encstore + KMS for encryption key distribution;
        // without KMS authorization the remote node never gets the key, so
        // Go's merge handler sets canRead=false and skips the field merge.
        // We replicate that by checking local ACP registration.
        let collection_for_acp = self
            .db
            .find_collection_by_id(&payload.schema_version_id)
            .ok()
            .flatten()
            .or_else(|| {
                metadata
                    .collection_id
                    .and_then(|cid| self.db.find_collection_by_id(cid).ok().flatten())
            });

        let skip_encrypted = should_skip_encrypted_merge(
            self.document_acp.get(),
            collection_for_acp.as_ref().map(|c| c.schema()),
            &doc_id_str,
        )
        .await;

        if skip_encrypted {
            tracing::info!(
                doc_id = %doc_id_str,
                "Skipping encrypted field merges: doc not registered in local ACP"
            );
        }

        // Collect winning field values for document reconstruction
        // These are the values that WON conflict resolution, not just the incoming values
        let mut field_values: HashMap<String, NormalValue> = HashMap::new();
        let mut any_field_applied = false;
        let mut process_error: Option<MergeError> = None;
        let mut is_branchable = false;
        // Collect field block heads for proper headstore merging during concurrent updates
        let mut field_block_heads: HashMap<String, Vec<Cid>> = HashMap::new();

        // Process linked blocks within the transaction scope
        // Use a scoped block to ensure datastore is dropped before commit/discard
        {
            let mut datastore = match txn.datastore() {
                Ok(ds) => ds,
                Err(e) => {
                    let _ = txn.force_discard();
                    return Err(MergeError::Database(e));
                }
            };

            if let Some(links) = &block.links {
                tracing::info!(
                    cid = %cid,
                    links_count = links.len(),
                    "Processing linked blocks from Composite delta"
                );

                for dag_link in links {
                    let link_name = &dag_link.name;
                    let link_cid = &dag_link.link;

                    tracing::debug!(
                        parent_cid = %cid,
                        link_cid = %link_cid,
                        link_name = %link_name,
                        "Processing linked block"
                    );

                    // Load the linked block from storage
                    let linked_block_data = match self.blockstore.get(link_cid).await {
                        Ok(Some(data)) => data,
                        Ok(None) => {
                            tracing::error!(
                                parent_cid = %cid,
                                link_cid = %link_cid,
                                "Linked block not found in blockstore"
                            );
                            process_error = Some(MergeError::Storage(format!(
                                "Linked block {} not found in blockstore",
                                link_cid
                            )));
                            break;
                        }
                        Err(e) => {
                            tracing::error!(
                                parent_cid = %cid,
                                link_cid = %link_cid,
                                error = %e,
                                "Failed to load linked block from blockstore"
                            );
                            process_error = Some(MergeError::Storage(e.to_string()));
                            break;
                        }
                    };

                    // Decode and process the linked block
                    let linked_block = match Block::from_dag_cbor(&linked_block_data) {
                        Ok(b) => b,
                        Err(e) => {
                            process_error = Some(MergeError::BlockDecode(e.to_string()));
                            break;
                        }
                    };

                    // Collect field block heads for proper headstore merging.
                    // During concurrent updates, we must only delete the heads that
                    // this field block explicitly supersedes, not ALL heads for the field.
                    if let Some(heads) = &linked_block.heads {
                        field_block_heads.insert(link_name.clone(), heads.clone());
                    }

                    // Skip encrypted field merge if ACP denies access (Go compat:
                    // Go's merge handler sets canRead=false when encryption key is
                    // unavailable via KMS for ACP-protected collections).
                    if skip_encrypted && linked_block.encryption.is_some() {
                        tracing::debug!(
                            link_cid = %link_cid,
                            link_name = %link_name,
                            "Skipping encrypted linked block (no local ACP registration)"
                        );
                        continue;
                    }

                    // Decrypt linked block data if it has encryption
                    let effective_linked_delta = match &linked_block.delta {
                        CrdtDelta::Lww(p) if linked_block.encryption.is_some() => {
                            match self
                                .decrypt_block_data(&p.data, linked_block.encryption.as_ref())
                                .await
                            {
                                Ok(decrypted) => {
                                    let mut dp = p.clone();
                                    dp.data = decrypted;
                                    CrdtDelta::Lww(dp)
                                }
                                Err(_) => linked_block.delta.clone(),
                            }
                        }
                        CrdtDelta::Counter(p) if linked_block.encryption.is_some() => {
                            match self
                                .decrypt_block_data(&p.data, linked_block.encryption.as_ref())
                                .await
                            {
                                Ok(decrypted) => {
                                    let mut dp = p.clone();
                                    dp.data = decrypted;
                                    CrdtDelta::Counter(dp)
                                }
                                Err(_) => linked_block.delta.clone(),
                            }
                        }
                        other => other.clone(),
                    };

                    match &effective_linked_delta {
                        CrdtDelta::Lww(lww_payload) => {
                            // Process the LWW delta within our transaction
                            match self
                                .process_lww_delta_in_txn(&mut datastore, link_cid, lww_payload)
                                .await
                            {
                                Ok(result) => {
                                    if result.applied {
                                        any_field_applied = true;
                                    }
                                    // Collect the WINNING value for document reconstruction
                                    if let Some(value) = result.value {
                                        field_values.insert(lww_payload.field_name.clone(), value);
                                    }
                                }
                                Err(e) => {
                                    process_error = Some(e);
                                    break;
                                }
                            }
                        }
                        CrdtDelta::Counter(counter_payload) => {
                            // Process the Counter delta within our transaction
                            match self
                                .process_counter_delta_in_txn(
                                    &mut datastore,
                                    link_cid,
                                    counter_payload,
                                    metadata.collection_id,
                                )
                                .await
                            {
                                Ok(result) => {
                                    if result.applied {
                                        any_field_applied = true;
                                    }
                                    // Collect the accumulated value for document reconstruction
                                    if let Some(value) = result.value {
                                        field_values
                                            .insert(counter_payload.field_name.clone(), value);
                                    }
                                }
                                Err(e) => {
                                    process_error = Some(e);
                                    break;
                                }
                            }
                        }
                        other => {
                            tracing::error!(
                                parent_cid = %cid,
                                link_cid = %link_cid,
                                delta_type = ?std::mem::discriminant(other),
                                "Unexpected delta type in linked block - expected LWW or Counter"
                            );
                            process_error = Some(MergeError::UnsupportedDelta(format!(
                                "Unexpected delta type in linked block: {:?}",
                                std::mem::discriminant(other)
                            )));
                            break;
                        }
                    }
                }
            }

            // Find the collection by schema version ID, with fallback to
            // the P2P metadata's collection_id (handles cross-version sync
            // where the incoming block's schema version differs from local)
            if process_error.is_none() {
                let collection_lookup = self
                    .db
                    .find_collection_by_id(&payload.schema_version_id)
                    .ok()
                    .flatten()
                    .or_else(|| {
                        metadata
                            .collection_id
                            .and_then(|cid| self.db.find_collection_by_id(cid).ok().flatten())
                    });

                let is_delete = payload.status == 2;

                match collection_lookup {
                    Some(collection) => {
                        is_branchable = collection.schema().is_branchable;
                        if is_delete {
                            // Handle delete: remove index entries, then write deletion marker.
                            // Must load the old document first so we know which index
                            // entries to remove (Go's syncIndexedDoc does the same).
                            if let Ok(doc_id) = DocID::from_string(&doc_id_str) {
                                if let Ok(Some(old_doc)) =
                                    collection.get_with_datastore(&datastore, &doc_id).await
                                {
                                    let short_id = collection_short_id(collection.collection_id());
                                    if let Ok(index_manager) =
                                        IndexManager::from_collection(short_id, collection.schema())
                                    {
                                        if let Err(e) = index_manager
                                            .on_document_delete(
                                                &datastore,
                                                &old_doc,
                                                collection.schema(),
                                            )
                                            .await
                                        {
                                            tracing::warn!(
                                                doc_id = %doc_id_str,
                                                error = %e,
                                                "Failed to delete indexes after merge"
                                            );
                                        }
                                    }
                                }
                            }

                            let deleted_key =
                                build_deleted_key(collection.collection_id(), &doc_id_str);
                            if let Err(e) = datastore.set(&deleted_key, &[DELETED_MARKER]).await {
                                process_error =
                                    Some(MergeError::Database(crate::error::Error::Storage(e)));
                            } else {
                                tracing::info!(
                                    doc_id = %doc_id_str,
                                    collection = %collection.name(),
                                    "Deletion marker set after P2P merge"
                                );
                            }
                        } else if !field_values.is_empty() {
                            // Store the reconstructed document within the same transaction.
                            // Load the existing document first so unmodified fields (e.g.
                            // foreign keys like _AuthorID) are preserved across partial
                            // updates that only touch a subset of fields.
                            match DocID::from_string(&doc_id_str) {
                                Ok(doc_id) => {
                                    let (mut doc, old_doc) = match collection
                                        .get_with_datastore(&datastore, &doc_id)
                                        .await
                                    {
                                        Ok(Some(existing)) => {
                                            let old = existing.clone();
                                            (existing, Some(old))
                                        }
                                        _ => {
                                            let mut new_doc = Document::new();
                                            new_doc.set_id(doc_id.clone());
                                            (new_doc, None)
                                        }
                                    };

                                    // Set the schema version from the incoming block so the
                                    // lensed fetcher can detect version mismatches and apply
                                    // migrations at query time (matches Go's composite merge).
                                    doc.set_schema_version_id(&payload.schema_version_id);

                                    // Overlay new/winning field values on top of existing fields
                                    for (field_name, value) in &field_values {
                                        doc.set(field_name, value.clone());
                                    }

                                    // Only store fields that the local collection knows about,
                                    // so cross-version syncs don't leak unknown fields into
                                    // query results.
                                    let known_fields: std::collections::HashSet<&str> = collection
                                        .schema()
                                        .fields
                                        .iter()
                                        .map(|f| f.name.as_str())
                                        .collect();
                                    let all_field_names: Vec<String> =
                                        doc.field_names().map(|s| s.to_string()).collect();
                                    for fname in &all_field_names {
                                        if !known_fields.contains(fname.as_str()) {
                                            doc.remove(fname);
                                        }
                                    }

                                    if let Err(e) =
                                        collection.save_with_datastore(&datastore, &doc).await
                                    {
                                        process_error = Some(MergeError::Database(e));
                                    } else {
                                        // Update indexes for the merged document
                                        let short_id =
                                            collection_short_id(collection.collection_id());
                                        if let Ok(index_manager) = IndexManager::from_collection(
                                            short_id,
                                            collection.schema(),
                                        ) {
                                            let index_result = match &old_doc {
                                                Some(old) => {
                                                    index_manager
                                                        .on_document_update(
                                                            &datastore,
                                                            old,
                                                            &doc,
                                                            collection.schema(),
                                                        )
                                                        .await
                                                }
                                                None => {
                                                    index_manager
                                                        .on_document_create(
                                                            &datastore,
                                                            &doc,
                                                            collection.schema(),
                                                        )
                                                        .await
                                                }
                                            };
                                            if let Err(e) = index_result {
                                                tracing::warn!(
                                                    doc_id = %doc_id_str,
                                                    error = %e,
                                                    "Failed to update indexes after merge"
                                                );
                                            }
                                        }

                                        tracing::info!(
                                            doc_id = %doc_id_str,
                                            collection = %collection.name(),
                                            fields_count = field_values.len(),
                                            any_applied = any_field_applied,
                                            "Document stored for queries"
                                        );
                                    }
                                }
                                Err(e) => {
                                    process_error = Some(MergeError::MergeFailed(format!(
                                        "Invalid doc_id: {}",
                                        e
                                    )));
                                }
                            }
                        }
                    }
                    None => {
                        process_error = Some(MergeError::MissingMetadata(format!(
                            "Collection not found for schema_version_id: {}",
                            payload.schema_version_id
                        )));
                    }
                }
            }
        } // datastore dropped here

        // Write headstore entries so _version / _commits queries work on
        // the receiving node.  The headstore tracks the latest CID for each
        // field and for the composite ("C"), keyed by doc_id.
        //
        // IMPORTANT: Use proper head merging — only delete heads that this block
        // explicitly supersedes (listed in block.heads / field block heads).
        // This preserves concurrent branches during concurrent P2P updates.
        if process_error.is_none() && !skip_encrypted {
            if let Ok(headstore) = txn.headstore() {
                let priority_bytes = encode_priority_varint(payload.priority);

                // Composite head: only delete heads listed in block.heads
                if let Some(heads) = &block.heads {
                    for parent_cid in heads {
                        let parent_key = storage::keys::headstore::HeadstoreDocKey::new(
                            &doc_id_str,
                            "C",
                            *parent_cid,
                        );
                        let _ = headstore
                            .delete(
                                &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(&parent_key),
                            )
                            .await;
                    }
                }
                // Add new composite head
                let composite_head_key =
                    storage::keys::headstore::HeadstoreDocKey::new(&doc_id_str, "C", *cid);
                if let Err(e) = headstore
                    .set(
                        &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(
                            &composite_head_key,
                        ),
                        &priority_bytes,
                    )
                    .await
                {
                    tracing::warn!(error = %e, "Failed to write composite head to headstore");
                }

                // Field heads: only delete heads that each field block supersedes
                if let Some(links) = &block.links {
                    for dag_link in links {
                        // Delete only the parent field heads (from the field block's heads)
                        if let Some(parent_cids) = field_block_heads.get(&dag_link.name) {
                            for parent_cid in parent_cids {
                                let parent_key = storage::keys::headstore::HeadstoreDocKey::new(
                                    &doc_id_str,
                                    &dag_link.name,
                                    *parent_cid,
                                );
                                let _ = headstore
                                    .delete(
                                        &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(&parent_key),
                                    )
                                    .await;
                            }
                        }
                        // Add new field head
                        let field_head_key = storage::keys::headstore::HeadstoreDocKey::new(
                            &doc_id_str,
                            &dag_link.name,
                            dag_link.link,
                        );
                        if let Err(e) = headstore
                            .set(
                                &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(&field_head_key),
                                &priority_bytes,
                            )
                            .await
                        {
                            tracing::warn!(
                                field = %dag_link.name,
                                error = %e,
                                "Failed to write field head to headstore"
                            );
                        }
                    }
                }
            }
        }

        // For branchable collections, the sender broadcasts the collection block
        // separately (dual broadcast), so we don't create local collection blocks here.
        // The sender's collection block arrives via handle_block → process_collection_delta
        // which preserves the exact collection CID for cross-node consistency.

        // Handle transaction commit/discard based on result
        match process_error {
            None => {
                // Commit the entire transaction (all field merges + document storage + headstore)
                txn.force_commit().await?;
                tracing::info!(
                    cid = %cid,
                    doc_id = %doc_id_str,
                    fields_merged = field_values.len(),
                    "Composite delta processed and committed successfully"
                );

                // Emit update event for subscriptions (P2P relay)
                if let Some(bus) = self.db.event_bus() {
                    let update = Update::new(
                        doc_id_str.clone(),
                        *cid,
                        payload.schema_version_id.clone(),
                        vec![], // Block data not needed for subscription re-query
                        false,  // is_retry
                        true,   // is_relay (P2P update)
                    );
                    bus.publish(Message::update(update));

                    // For branchable collections, emit a collection-level merge_complete
                    // event. Uses composite CID to match the sender's Update event CID.
                    if is_branchable {
                        let by_peer = metadata.creator.unwrap_or("").to_string();
                        let mc = MergeCompleteData {
                            doc_id: String::new(), // empty → keyed by collection_id
                            cid: *cid,
                            collection_id: metadata
                                .collection_id
                                .unwrap_or(&payload.schema_version_id)
                                .to_string(),
                            by_peer,
                        };
                        bus.publish(Message::merge_complete(mc));
                    }
                }

                Ok(MergeOutcome::Merged)
            }
            Some(e) => {
                // Discard the transaction - rollback all changes
                if let Err(discard_err) = txn.force_discard() {
                    tracing::error!(
                        cid = %cid,
                        discard_error = %discard_err,
                        merge_error = %e,
                        "Failed to discard transaction after composite merge error - potential resource leak"
                    );
                }
                Err(e)
            }
        }
    }

    /// Process a Counter delta from a block (standalone, with its own transaction).
    async fn process_counter_delta(
        &self,
        cid: &Cid,
        payload: &defra_core::block::CounterDeltaPayload,
        metadata: &BlockMetadata<'_>,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        let doc_id_str = String::from_utf8_lossy(&payload.doc_id).to_string();

        tracing::debug!(
            cid = %cid,
            field_name = %payload.field_name,
            doc_id = %doc_id_str,
            priority = payload.priority,
            nonce = payload.nonce,
            "Processing Counter delta"
        );

        // Look up the collection to determine field kind and counter type,
        // with fallback to metadata's collection_id for cross-version sync
        let collection = self
            .db
            .find_collection_by_id(&payload.schema_version_id)?
            .or(metadata
                .collection_id
                .and_then(|cid| self.db.find_collection_by_id(cid).ok().flatten()))
            .ok_or_else(|| {
                MergeError::MissingMetadata(format!(
                    "Collection not found for schema_version_id: {}",
                    payload.schema_version_id
                ))
            })?;

        // Get field definition to determine numeric kind and allow_decrement
        let field = collection
            .schema()
            .field_by_name(&payload.field_name)
            .ok_or_else(|| {
                MergeError::MissingMetadata(format!(
                    "Field '{}' not found in collection",
                    payload.field_name
                ))
            })?;

        // Determine numeric kind from field type
        let numeric_kind = self.get_numeric_kind_from_field(field)?;

        // Determine if decrement is allowed (PnCounter allows, PCounter doesn't)
        let allow_decrement = field.crdt_type.allows_decrement();

        // Create a new transaction for this merge
        let txn = self.db.new_txn(false).await?;

        // Create the Counter CRDT
        let counter = Counter::new(
            payload.schema_version_id.clone(),
            &payload.doc_id,
            payload.field_name.clone(),
            allow_decrement,
            numeric_kind,
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Create the CounterDelta from payload
        let delta = self.create_counter_delta(payload, numeric_kind)?;

        // Create the context
        let ctx = Context {
            doc_id: DocId::new(&doc_id_str),
            schema_version: payload.schema_version_id.clone(),
        };

        // Perform the merge in a scoped block
        let result = {
            let mut datastore = txn.datastore()?;
            counter.merge(&mut datastore, &ctx, &delta).await
        };

        match result {
            Ok(merge_result) => {
                if merge_result.was_applied() {
                    txn.force_commit().await?;
                    tracing::info!(
                        cid = %cid,
                        field_name = %payload.field_name,
                        doc_id = %doc_id_str,
                        "Counter delta merged successfully"
                    );
                    Ok(MergeOutcome::Merged)
                } else {
                    // Skipped (nonce already applied)
                    if let Err(e) = txn.force_discard() {
                        tracing::error!(
                            cid = %cid,
                            error = %e,
                            "Failed to discard transaction after skip"
                        );
                    }
                    tracing::debug!(
                        cid = %cid,
                        field_name = %payload.field_name,
                        "Counter delta skipped (nonce already applied)"
                    );
                    Ok(MergeOutcome::skipped("nonce already applied"))
                }
            }
            Err(e) => {
                if let Err(discard_err) = txn.force_discard() {
                    tracing::error!(
                        cid = %cid,
                        discard_error = %discard_err,
                        merge_error = %e,
                        "Failed to discard transaction after merge error"
                    );
                }
                Err(MergeError::MergeFailed(e.to_string()))
            }
        }
    }

    /// Process a Counter delta within an existing transaction, returning the merge result
    /// and the accumulated value for document reconstruction.
    async fn process_counter_delta_in_txn(
        &self,
        datastore: &mut NamespaceView,
        cid: &Cid,
        payload: &defra_core::block::CounterDeltaPayload,
        fallback_collection_id: Option<&str>,
    ) -> std::result::Result<CounterMergeResult, MergeError> {
        tracing::debug!(
            cid = %cid,
            field_name = %payload.field_name,
            priority = payload.priority,
            nonce = payload.nonce,
            "Processing Counter delta in transaction"
        );

        // Look up the collection to determine field kind and counter type,
        // with fallback to metadata's collection_id for cross-version sync
        let collection = self
            .db
            .find_collection_by_id(&payload.schema_version_id)?
            .or(fallback_collection_id
                .and_then(|cid| self.db.find_collection_by_id(cid).ok().flatten()))
            .ok_or_else(|| {
                MergeError::MissingMetadata(format!(
                    "Collection not found for schema_version_id: {}",
                    payload.schema_version_id
                ))
            })?;

        // Get field definition
        let field = collection
            .schema()
            .field_by_name(&payload.field_name)
            .ok_or_else(|| {
                MergeError::MissingMetadata(format!(
                    "Field '{}' not found in collection",
                    payload.field_name
                ))
            })?;

        // Determine numeric kind and allow_decrement
        let numeric_kind = self.get_numeric_kind_from_field(field)?;
        let allow_decrement = field.crdt_type.allows_decrement();

        // Create the Counter CRDT
        let counter = Counter::new(
            payload.schema_version_id.clone(),
            &payload.doc_id,
            payload.field_name.clone(),
            allow_decrement,
            numeric_kind,
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Seed counter CRDT from existing document if CRDT storage isn't initialized.
        // Local document creation stores counter values in the document layer but not
        // in CRDT accumulation storage, so we must seed before merging remote deltas.
        let doc_id_str = String::from_utf8_lossy(&payload.doc_id).to_string();
        if let Ok(doc_id) = DocID::from_string(&doc_id_str) {
            if let Ok(Some(existing_doc)) = collection.get_with_datastore(datastore, &doc_id).await
            {
                if let Some(field_value) = existing_doc.get(&payload.field_name) {
                    match (numeric_kind, field_value) {
                        (NumericKind::Int64, NormalValue::Int(v)) => {
                            let _ = counter.seed_if_uninitialized_int64(datastore, *v).await;
                        }
                        (NumericKind::Float64, NormalValue::Float64(v)) => {
                            let _ = counter.seed_if_uninitialized_float64(datastore, *v).await;
                        }
                        _ => {}
                    }
                }
            }
        }

        // Create the CounterDelta from payload
        let delta = self.create_counter_delta(payload, numeric_kind)?;
        let ctx = Context {
            doc_id: DocId::new(&doc_id_str),
            schema_version: payload.schema_version_id.clone(),
        };

        // Perform the merge
        let merge_result = counter
            .merge(datastore, &ctx, &delta)
            .await
            .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Read the accumulated value (counters always accumulate, so we always read current)
        let value = match ValueReader::value(&counter, datastore).await {
            Ok(bytes) => {
                if bytes.is_empty() {
                    None
                } else {
                    // Convert raw bytes to NormalValue based on kind
                    match numeric_kind {
                        NumericKind::Int64 => {
                            if bytes.len() == 8 {
                                let arr: [u8; 8] = bytes[..8].try_into().unwrap();
                                Some(NormalValue::Int(i64::from_be_bytes(arr)))
                            } else {
                                tracing::warn!(
                                    field_name = %payload.field_name,
                                    "Invalid counter value length for Int64"
                                );
                                None
                            }
                        }
                        NumericKind::Float64 => {
                            if bytes.len() == 8 {
                                let arr: [u8; 8] = bytes[..8].try_into().unwrap();
                                Some(NormalValue::Float64(f64::from_be_bytes(arr)))
                            } else {
                                tracing::warn!(
                                    field_name = %payload.field_name,
                                    "Invalid counter value length for Float64"
                                );
                                None
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::debug!(
                    field_name = %payload.field_name,
                    error = %e,
                    "Could not read counter value"
                );
                None
            }
        };

        Ok(CounterMergeResult {
            applied: merge_result.was_applied(),
            value,
        })
    }

    /// Determine numeric kind from field definition
    fn get_numeric_kind_from_field(
        &self,
        field: &schema::FieldDescription,
    ) -> std::result::Result<NumericKind, MergeError> {
        use schema::FieldKind;

        match &field.kind {
            FieldKind::Scalar(scalar_kind) => {
                use schema::ScalarKind;
                match scalar_kind {
                    ScalarKind::Int => Ok(NumericKind::Int64),
                    ScalarKind::Float64 | ScalarKind::Float32 => Ok(NumericKind::Float64),
                    other => Err(MergeError::UnsupportedDelta(format!(
                        "Counter field '{}' has unsupported scalar kind: {:?}",
                        field.name, other
                    ))),
                }
            }
            other => Err(MergeError::UnsupportedDelta(format!(
                "Counter field '{}' has unsupported kind: {:?}",
                field.name, other
            ))),
        }
    }

    /// Process a Collection delta from a block.
    ///
    /// Collection blocks are metadata containers that link to document composite
    /// blocks. The collection CRDT merge itself is a no-op (matching Go behavior).
    /// The real work is:
    /// 1. Recursively process parent collection blocks from `heads`
    /// 2. Process each linked document composite via `process_composite_delta`
    /// 3. Update the collection headstore with the new head CID
    async fn process_collection_delta(
        &self,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CollectionDeltaPayload,
        metadata: &BlockMetadata<'_>,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        tracing::debug!(
            cid = %cid,
            schema_version = %payload.schema_version_id,
            priority = payload.priority,
            links_count = block.links.as_ref().map(|l| l.len()).unwrap_or(0),
            heads_count = block.heads.as_ref().map(|h| h.len()).unwrap_or(0),
            "Processing Collection delta"
        );

        // Recursively process parent collection blocks from `heads` before
        // this block, ensuring older documents are merged first.
        if let Some(heads) = &block.heads {
            for head_cid in heads {
                let head_data = match self.blockstore.get(head_cid).await {
                    Ok(Some(data)) => data,
                    Ok(None) => {
                        tracing::debug!(
                            parent_cid = %head_cid,
                            child_cid = %cid,
                            "Parent collection block not in blockstore, skipping"
                        );
                        continue;
                    }
                    Err(e) => {
                        tracing::debug!(
                            parent_cid = %head_cid,
                            error = %e,
                            "Failed to load parent collection block, skipping"
                        );
                        continue;
                    }
                };

                let head_block = match Block::from_dag_cbor(&head_data) {
                    Ok(b) => b,
                    Err(_) => continue,
                };

                if let CrdtDelta::Collection(head_payload) = &head_block.delta {
                    tracing::info!(
                        parent_cid = %head_cid,
                        child_cid = %cid,
                        "Recursively merging parent collection block"
                    );
                    let _ = Box::pin(self.process_collection_delta(
                        head_cid,
                        &head_block,
                        head_payload,
                        metadata,
                    ))
                    .await;
                }
            }
        }

        // Process linked document composites
        let mut any_merged = false;
        if let Some(links) = &block.links {
            for dag_link in links {
                let link_cid = &dag_link.link;

                tracing::debug!(
                    collection_cid = %cid,
                    link_cid = %link_cid,
                    link_name = %dag_link.name,
                    "Processing linked block from Collection delta"
                );

                let linked_data = match self.blockstore.get(link_cid).await {
                    Ok(Some(data)) => data,
                    Ok(None) => {
                        tracing::warn!(
                            link_cid = %link_cid,
                            "Linked block not found in blockstore"
                        );
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(
                            link_cid = %link_cid,
                            error = %e,
                            "Failed to load linked block"
                        );
                        continue;
                    }
                };

                let linked_block = match Block::from_dag_cbor(&linked_data) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(
                            link_cid = %link_cid,
                            error = %e,
                            "Failed to decode linked block"
                        );
                        continue;
                    }
                };

                tracing::debug!(
                    link_cid = %link_cid,
                    delta_type = ?std::mem::discriminant(&linked_block.delta),
                    "Processing linked block from Collection"
                );

                match &linked_block.delta {
                    CrdtDelta::Composite(composite_payload) => {
                        let doc_id_str =
                            String::from_utf8_lossy(&composite_payload.doc_id).to_string();
                        tracing::debug!(
                            link_cid = %link_cid,
                            doc_id = %doc_id_str,
                            "Processing linked composite from Collection"
                        );
                        match self
                            .process_composite_delta(
                                link_cid,
                                &linked_block,
                                composite_payload,
                                metadata,
                                true, // from_collection: skip local collection block creation
                            )
                            .await
                        {
                            Ok(MergeOutcome::Merged) => {
                                tracing::debug!(link_cid = %link_cid, "Composite merged successfully");
                                any_merged = true;

                                // Publish per-document MergeComplete so the Go test
                                // framework's WaitForSync can track each document.
                                if let Some(bus) = self.db.event_bus() {
                                    let col_id = metadata
                                        .collection_id
                                        .unwrap_or(&payload.schema_version_id)
                                        .to_string();
                                    bus.publish(Message::merge_complete(MergeCompleteData {
                                        doc_id: doc_id_str,
                                        cid: *link_cid,
                                        collection_id: col_id,
                                        by_peer: String::new(),
                                    }));
                                }
                            }
                            Ok(outcome) => {
                                tracing::debug!(
                                    link_cid = %link_cid,
                                    outcome = ?outcome,
                                    "Composite skipped"
                                );
                            }
                            Err(e) => {
                                tracing::debug!(link_cid = %link_cid, error = %e, "Composite merge failed");
                            }
                        }
                    }
                    other => {
                        tracing::debug!(
                            link_cid = %link_cid,
                            delta_type = ?std::mem::discriminant(other),
                            "Skipping non-composite link"
                        );
                    }
                }
            }
        }

        // Update collection headstore using proper head merging.
        // Only remove heads that this block explicitly supersedes (listed in block.heads),
        // preserving concurrent branches for later merge via write_collection_block.
        let collection_id = metadata.collection_id.unwrap_or(&payload.schema_version_id);
        let short_id = collection_short_id(collection_id);

        let txn = self.db.new_txn(false).await?;
        if let Ok(headstore) = txn.headstore() {
            // Remove only the heads that this block supersedes (its parents).
            // This preserves concurrent branches in the headstore.
            if let Some(heads) = &block.heads {
                for parent_cid in heads {
                    let parent_key =
                        storage::keys::headstore::HeadstoreColKey::new(short_id, *parent_cid);
                    let _ = headstore
                        .delete(
                            &<storage::keys::headstore::HeadstoreColKey as storage::corekv::Key>::bytes(
                                &parent_key,
                            ),
                        )
                        .await;
                }
            }

            // Add the new collection head (idempotent if already exists)
            let col_key = storage::keys::headstore::HeadstoreColKey::new(short_id, *cid);
            let priority_bytes = encode_priority_varint(payload.priority);
            if let Err(e) = headstore
                .set(
                    &<storage::keys::headstore::HeadstoreColKey as storage::corekv::Key>::bytes(
                        &col_key,
                    ),
                    &priority_bytes,
                )
                .await
            {
                tracing::warn!(
                    error = %e,
                    collection_id = %collection_id,
                    "Failed to write collection head to headstore"
                );
            }
        }
        txn.force_commit().await?;

        tracing::info!(
            cid = %cid,
            collection_id = %collection_id,
            short_id = short_id,
            any_merged = any_merged,
            "Collection delta processed"
        );

        if any_merged {
            Ok(MergeOutcome::Merged)
        } else {
            Ok(MergeOutcome::skipped("no linked composites needed merging"))
        }
    }

    /// Create a CounterDelta from the block payload
    fn create_counter_delta(
        &self,
        payload: &defra_core::block::CounterDeltaPayload,
        kind: NumericKind,
    ) -> std::result::Result<CounterDelta, MergeError> {
        // Go encodes counter data as CBOR. We need to decode it first.
        // The payload.data contains CBOR-encoded i64 or f64
        match kind {
            NumericKind::Int64 => {
                let increment: i64 = ciborium::from_reader(&payload.data[..]).map_err(|e| {
                    MergeError::BlockDecode(format!(
                        "Failed to decode Counter Int64 increment: {}",
                        e
                    ))
                })?;
                CounterDelta::new_int64(
                    payload.doc_id.clone(),
                    payload.field_name.clone(),
                    payload.priority,
                    payload.nonce,
                    payload.schema_version_id.clone(),
                    increment,
                )
                .map_err(|e| MergeError::MergeFailed(e.to_string()))
            }
            NumericKind::Float64 => {
                let increment: f64 = ciborium::from_reader(&payload.data[..]).map_err(|e| {
                    MergeError::BlockDecode(format!(
                        "Failed to decode Counter Float64 increment: {}",
                        e
                    ))
                })?;
                CounterDelta::new_float64(
                    payload.doc_id.clone(),
                    payload.field_name.clone(),
                    payload.priority,
                    payload.nonce,
                    payload.schema_version_id.clone(),
                    increment,
                )
                .map_err(|e| MergeError::MergeFailed(e.to_string()))
            }
        }
    }

    /// Process a CollectionDefinition delta - register synced collection schema in systemstore.
    ///
    /// When a peer receives collection definition blocks via Bitswap sync, this method
    /// reconstructs the `CollectionVersion` from the definition deltas and stores it
    /// in systemstore so `set_active_collection_version` can find and activate it.
    async fn process_collection_definition_delta(
        &self,
        cid: &Cid,
        block: &Block,
        payload: &CollectionDefinitionDeltaPayload,
        _metadata: &BlockMetadata<'_>,
    ) -> Result<MergeOutcome, MergeError> {
        let collection_name = match &payload.name {
            Some(name) => name.clone(),
            None => {
                tracing::debug!(cid = %cid, "CollectionDefinition has no name - skipping");
                return Ok(MergeOutcome::skipped("collection definition has no name"));
            }
        };

        // The version_id is the CID of this collection definition block
        let version_id = cid.to_string();

        tracing::info!(
            cid = %cid,
            collection_name = %collection_name,
            version_id = %version_id,
            "Processing collection definition delta"
        );

        // Load and decode all linked field definition blocks
        // Note: _docID is already included in the linked blocks from the source node,
        // so we don't need to add it implicitly.
        let mut fields = Vec::new();
        if let Some(links) = &block.links {
            for link in links.iter() {
                let field_cid = &link.link;
                let field_bytes = self
                    .blockstore
                    .get(field_cid)
                    .await
                    .map_err(|e| MergeError::Storage(format!("Failed to load field block: {}", e)))?
                    .ok_or_else(|| {
                        MergeError::Storage(format!("Field block not found: {}", field_cid))
                    })?;

                let field_block = Block::from_dag_cbor(&field_bytes).map_err(|e| {
                    MergeError::BlockDecode(format!("Failed to decode field block: {}", e))
                })?;

                if let CrdtDelta::FieldDefinition(field_payload) = &field_block.delta {
                    // Use the field block CID as the field ID (matches Go's behavior)
                    let field_desc = self
                        .field_definition_to_description(field_payload, &field_cid.to_string())?;
                    fields.push(field_desc);
                } else {
                    tracing::warn!(
                        field_cid = %field_cid,
                        "Linked block is not a FieldDefinition - skipping"
                    );
                }
            }
        }

        // Ensure _docID is first in the fields list (Go expects this ordering)
        if let Some(docid_pos) = fields.iter().position(|f| f.name == "_docID") {
            if docid_pos > 0 {
                let docid_field = fields.remove(docid_pos);
                fields.insert(0, docid_field);
            }
        }

        // For initial collection creation, collection_id equals version_id (the CID)
        // For patched versions, we'd need to look up the existing collection
        let collection_id = version_id.clone();

        // Build the CollectionVersion
        // Synced collections come in as inactive (user must activate manually via SetActiveCollectionVersion)
        // and materialized (matching Go's behavior)
        let mut schema =
            CollectionVersion::new(&collection_name, &version_id, &collection_id, fields);
        schema.is_active = false;

        // Views (collections with a query_select) are non-materialized and carry query metadata.
        // Regular collections are materialized.
        if let Some(ref query_bytes) = payload.query_select {
            schema.is_materialized = false;
            if let Ok(query_value) = serde_cbor::from_slice::<serde_json::Value>(query_bytes) {
                let mut source = QuerySource::new(query_value);
                if let Some(ref transform_cid) = payload.query_transform {
                    source.transform = Some(transform_cid.to_string());
                }
                schema.query = Some(source);
            } else {
                tracing::warn!(
                    cid = %cid,
                    "Failed to decode query_select CBOR bytes for view collection"
                );
            }
        } else {
            schema.is_materialized = true;
        }

        // Store in systemstore
        let txn = self.db.new_txn(false).await.map_err(MergeError::Database)?;
        {
            let systemstore = txn.systemstore().map_err(MergeError::Database)?;

            // 1. Store full schema at /collection/id/{version_id}
            let collection_key = CollectionKey::new(&version_id);
            let data = serde_json::to_vec(&schema).map_err(|e| {
                MergeError::Storage(format!("Failed to serialize collection schema: {}", e))
            })?;
            systemstore
                .set(&collection_key.bytes(), &data)
                .await
                .map_err(|e| MergeError::Storage(format!("Failed to store collection: {}", e)))?;

            // 2. Store version index at /collection/version/{collection_id}/{version_id}
            let version_key = CollectionVersionKey::new(&collection_id, &version_id);
            systemstore
                .set(&version_key.bytes(), b"1")
                .await
                .map_err(|e| {
                    MergeError::Storage(format!("Failed to store version index: {}", e))
                })?;
        }
        txn.commit().await.map_err(MergeError::Database)?;

        // Add to runtime cache so it's visible via list_collections/get_collection.
        // Synced collections are inactive but still need to be in the cache for
        // GetCollections with IncludeInactive=true to find them.
        self.db
            .add_collection_to_cache(schema.clone())
            .map_err(MergeError::Database)?;

        tracing::debug!(
            collection_name = %collection_name,
            version_id = %version_id,
            is_active = schema.is_active,
            is_materialized = schema.is_materialized,
            "Stored synced collection schema in cache"
        );

        tracing::info!(
            collection_name = %collection_name,
            version_id = %version_id,
            field_count = schema.fields.len(),
            "Registered synced collection schema in systemstore and cache (inactive, requires manual activation)"
        );

        Ok(MergeOutcome::Merged)
    }

    /// Convert a FieldDefinitionDeltaPayload to a FieldDescription.
    fn field_definition_to_description(
        &self,
        payload: &FieldDefinitionDeltaPayload,
        field_id: &str,
    ) -> Result<FieldDescription, MergeError> {
        let name = payload
            .name
            .clone()
            .unwrap_or_else(|| format!("field_{}", field_id));

        // Determine the FieldKind from the payload
        let kind = if let Some(collection_id) = &payload.collection_id {
            // Relation field
            FieldKind::Relation {
                collection_id: collection_id.clone(),
                is_array: false, // Default; actual value would need additional info
            }
        } else if let Some(relative_id) = payload.relative_id {
            // Self-referencing field
            FieldKind::SelfRef {
                relative_id: relative_id.to_string(),
                is_array: false,
            }
        } else if let Some(scalar_kind_u8) = payload.scalar_kind {
            // Scalar field - convert u8 to ScalarKind
            let scalar_kind = match scalar_kind_u8 {
                0 => ScalarKind::None,
                1 => ScalarKind::DocID,
                2 => ScalarKind::Bool,
                4 => ScalarKind::Int,
                6 => ScalarKind::Float64,
                8 => ScalarKind::Float32,
                10 => ScalarKind::DateTime,
                11 => ScalarKind::String,
                13 => ScalarKind::Blob,
                14 => ScalarKind::Json,
                _ => ScalarKind::None,
            };
            FieldKind::Scalar(scalar_kind)
        } else {
            // Default to None scalar
            FieldKind::Scalar(ScalarKind::None)
        };

        // Determine CRDT type
        let crdt_type = payload.crdt.map(CType::from_u8).unwrap_or_default();

        Ok(FieldDescription::new(field_id.to_string(), name, kind).with_crdt_type(crdt_type))
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
