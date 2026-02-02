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
use defra_core::block::{Block, CrdtDelta};
use defra_core::types::DocId;
use document::{DocID, Document, NormalValue};
use events::{Message, Update};
use p2p::sync::{BlockMetadata, MergeHandler, MergeOutcome};
use schema;
use storage::corekv::Store;

use crate::database::DB;
use crate::error::Error;

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
}

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    /// Create a new database merge handler.
    pub fn new(db: Arc<DB<S>>, blockstore: Arc<B>) -> Self {
        Self { db, blockstore }
    }

    /// Get reference to blockstore.
    pub fn blockstore(&self) -> &Arc<B> {
        &self.blockstore
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
    async fn process_composite_delta(
        &self,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CompositeDeltaPayload,
        metadata: &BlockMetadata<'_>,
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
                    let _ = Box::pin(
                        self.process_composite_delta(head_cid, &head_block, head_payload, metadata),
                    )
                    .await;
                }
            }
        }

        // Create a SINGLE transaction for all field merges AND document storage
        let txn = self.db.new_txn(false).await?;

        // Collect winning field values for document reconstruction
        // These are the values that WON conflict resolution, not just the incoming values
        let mut field_values: HashMap<String, NormalValue> = HashMap::new();
        let mut any_field_applied = false;
        let mut process_error: Option<MergeError> = None;

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

                    match &linked_block.delta {
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

            // Store the reconstructed document within the same transaction
            if process_error.is_none() && !field_values.is_empty() {
                // Find the collection by schema version ID, with fallback to
                // the P2P metadata's collection_id (handles cross-version sync
                // where the incoming block's schema version differs from local)
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
                match collection_lookup {
                    Some(collection) => {
                        // Build the document with WINNING field values
                        let mut doc = Document::new();
                        for (field_name, value) in &field_values {
                            doc.set(field_name, value.clone());
                        }

                        // Set the document ID
                        match DocID::from_string(&doc_id_str) {
                            Ok(doc_id) => {
                                doc.set_id(doc_id.clone());

                                // Store the document (upsert)
                                if let Err(e) =
                                    collection.save_with_datastore(&datastore, &doc).await
                                {
                                    process_error = Some(MergeError::Database(e));
                                } else {
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
                                process_error =
                                    Some(MergeError::MergeFailed(format!("Invalid doc_id: {}", e)));
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

        // Handle transaction commit/discard based on result
        match process_error {
            None => {
                // Commit the entire transaction (all field merges + document storage)
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
            if let Ok(Some(existing_doc)) =
                collection.get_with_datastore(datastore, &doc_id).await
            {
                if let Some(field_value) = existing_doc.get(&payload.field_name) {
                    match (numeric_kind, field_value) {
                        (NumericKind::Int64, NormalValue::Int(v)) => {
                            let _ = counter
                                .seed_if_uninitialized_int64(datastore, *v)
                                .await;
                        }
                        (NumericKind::Float64, NormalValue::Float64(v)) => {
                            let _ = counter
                                .seed_if_uninitialized_float64(datastore, *v)
                                .await;
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

        // Process based on delta type
        match &block.delta {
            CrdtDelta::Lww(payload) => self.process_lww_delta(cid, payload, &metadata).await,
            CrdtDelta::Counter(payload) => {
                self.process_counter_delta(cid, payload, &metadata).await
            }
            CrdtDelta::Composite(payload) => {
                self.process_composite_delta(cid, &block, payload, &metadata)
                    .await
            }
            CrdtDelta::Collection(_) => {
                tracing::debug!(cid = %cid, "Collection delta - skipping (handled at schema level)");
                Ok(MergeOutcome::skipped(
                    "collection deltas handled at schema level",
                ))
            }
            CrdtDelta::FieldDefinition(_) | CrdtDelta::CollectionDefinition(_) => {
                tracing::debug!(cid = %cid, "Schema definition delta - skipping");
                Ok(MergeOutcome::skipped("schema definition deltas not merged"))
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
