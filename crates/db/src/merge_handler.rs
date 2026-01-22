// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Database merge handler for processing incoming P2P blocks.
//!
//! This module implements the `MergeHandler` trait from the P2P layer,
//! bridging incoming blocks to the CRDT system for document merging.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use cid::Cid;
use crdt::traits::{Context, ReplicatedData};
use crdt::{Lww, LwwDelta};
use defra_core::block::{Block, CrdtDelta};
use defra_core::types::DocId;
use document::{DocID, Document, NormalValue};
use p2p::sync::{BlockMetadata, MergeHandler, MergeOutcome};
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

    /// Process an LWW delta from a block.
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
                    let _ = txn.force_discard();
                    tracing::debug!(
                        cid = %cid,
                        field_name = %payload.field_name,
                        "LWW delta rejected by CRDT (lower priority or tie-break)"
                    );
                    Ok(MergeOutcome::skipped("rejected by CRDT conflict resolution"))
                } else {
                    // Skipped (already applied)
                    let _ = txn.force_discard();
                    tracing::debug!(
                        cid = %cid,
                        field_name = %payload.field_name,
                        "LWW delta skipped (already applied)"
                    );
                    Ok(MergeOutcome::skipped("already applied"))
                }
            }
            Err(e) => {
                let _ = txn.force_discard();
                Err(MergeError::MergeFailed(e.to_string()))
            }
        }
    }

    /// Process a Composite delta from a block.
    ///
    /// Composite deltas contain links to the actual field LWW/Counter blocks.
    /// We need to process all linked blocks to merge the document properly,
    /// then reconstruct and store the document for queries.
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
            "Processing Composite delta (document-level)"
        );

        // Collect field values for document reconstruction
        let mut field_values: HashMap<String, NormalValue> = HashMap::new();

        // Process linked blocks (LWW/Counter deltas for each field)
        if let Some(links) = &block.links {
            tracing::info!(
                cid = %cid,
                links_count = links.len(),
                "Processing linked blocks from Composite delta"
            );

            for dag_link in links {
                let link_name = &dag_link.name;
                let link_cid = &dag_link.link;

                tracing::info!(
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
                        return Err(MergeError::Storage(format!(
                            "Linked block {} not found in blockstore",
                            link_cid
                        )));
                    }
                    Err(e) => {
                        tracing::error!(
                            parent_cid = %cid,
                            link_cid = %link_cid,
                            error = %e,
                            "Failed to load linked block from blockstore"
                        );
                        return Err(MergeError::Storage(e.to_string()));
                    }
                };

                // Decode and process the linked block
                let linked_block = Block::from_dag_cbor(&linked_block_data)
                    .map_err(|e| MergeError::BlockDecode(e.to_string()))?;

                match &linked_block.delta {
                    CrdtDelta::Lww(lww_payload) => {
                        // Process the LWW delta (stores in CRDT layer)
                        self.process_lww_delta(link_cid, lww_payload, metadata)
                            .await?;

                        // Collect field value for document reconstruction
                        if !lww_payload.data.is_empty() {
                            match ciborium::from_reader::<NormalValue, _>(&lww_payload.data[..]) {
                                Ok(value) => {
                                    field_values.insert(lww_payload.field_name.clone(), value);
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        field_name = %lww_payload.field_name,
                                        error = %e,
                                        "Failed to decode field value from CBOR - skipping for document reconstruction"
                                    );
                                }
                            }
                        }
                    }
                    CrdtDelta::Counter(counter_payload) => {
                        self.process_counter_delta(link_cid, counter_payload, metadata)
                            .await?;
                        // Counter values would need special handling for document reconstruction
                    }
                    other => {
                        tracing::warn!(
                            parent_cid = %cid,
                            link_cid = %link_cid,
                            delta_type = ?std::mem::discriminant(other),
                            "Unexpected delta type in linked block - expected LWW or Counter"
                        );
                    }
                }
            }
        }

        // Reconstruct and store the document for queries
        if !field_values.is_empty() {
            self.store_document(&doc_id_str, &payload.schema_version_id, field_values)
                .await?;
        }

        tracing::info!(
            cid = %cid,
            doc_id = %doc_id_str,
            "Composite delta processed successfully"
        );

        Ok(MergeOutcome::Merged)
    }

    /// Store a reconstructed document in the collection's document storage.
    ///
    /// This bridges the gap between CRDT field-level storage and the document
    /// storage that queries read from.
    async fn store_document(
        &self,
        doc_id_str: &str,
        schema_version_id: &str,
        field_values: HashMap<String, NormalValue>,
    ) -> std::result::Result<(), MergeError> {
        // Find the collection by schema version ID
        let collection = self
            .db
            .find_collection_by_id(schema_version_id)
            .map_err(|e| MergeError::Database(e))?
            .ok_or_else(|| {
                MergeError::MissingMetadata(format!(
                    "Collection not found for schema_version_id: {}",
                    schema_version_id
                ))
            })?;

        // Build the document
        let mut doc = Document::new();
        for (field_name, value) in field_values {
            doc.set(&field_name, value);
        }

        // Set the document ID
        let doc_id = DocID::from_string(doc_id_str)
            .map_err(|e| MergeError::MergeFailed(format!("Invalid doc_id: {}", e)))?;
        doc.set_id(doc_id.clone());

        // Store the document using collection's save method (upsert)
        let txn = self.db.new_txn(false).await?;
        {
            let datastore = txn.datastore()?;
            collection
                .save_with_datastore(&datastore, &doc)
                .await
                .map_err(|e| MergeError::Database(e))?;
        }
        txn.force_commit().await?;

        tracing::info!(
            doc_id = %doc_id_str,
            collection = %collection.name(),
            fields_count = doc.values().len(),
            "Document stored for queries"
        );

        Ok(())
    }

    /// Process a Counter delta from a block.
    async fn process_counter_delta(
        &self,
        cid: &Cid,
        payload: &defra_core::block::CounterDeltaPayload,
        _metadata: &BlockMetadata<'_>,
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

        // Counter CRDT support would be similar to LWW but using Counter type
        // For now, acknowledge but log that it's not fully implemented
        tracing::warn!(
            cid = %cid,
            field_name = %payload.field_name,
            "Counter delta merge not yet implemented - skipping"
        );

        Ok(MergeOutcome::skipped("counter merge not yet implemented"))
    }
}

#[async_trait]
impl<S: Store + 'static, B: blockstore::Blockstore + Send + Sync + 'static> MergeHandler for DbMergeHandler<S, B> {
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
        let block = Block::from_dag_cbor(block_data)
            .map_err(|e| MergeError::BlockDecode(e.to_string()))?;

        tracing::debug!(
            cid = %cid,
            delta_type = ?std::mem::discriminant(&block.delta),
            heads_count = block.heads.as_ref().map(|h| h.len()).unwrap_or(0),
            links_count = block.links.as_ref().map(|l| l.len()).unwrap_or(0),
            "Block decoded successfully"
        );

        // Process based on delta type
        match &block.delta {
            CrdtDelta::Lww(payload) => {
                self.process_lww_delta(cid, payload, &metadata).await
            }
            CrdtDelta::Counter(payload) => {
                self.process_counter_delta(cid, payload, &metadata).await
            }
            CrdtDelta::Composite(payload) => {
                self.process_composite_delta(cid, &block, payload, &metadata).await
            }
            CrdtDelta::Collection(_) => {
                tracing::debug!(cid = %cid, "Collection delta - skipping (handled at schema level)");
                Ok(MergeOutcome::skipped("collection deltas handled at schema level"))
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
    use storage::backends::MemoryStore;

    #[tokio::test]
    async fn test_merge_handler_creation() {
        let store = MemoryStore::new();
        let db = Arc::new(DB::new(store));
        let _handler = DbMergeHandler::new(db);
    }
}
