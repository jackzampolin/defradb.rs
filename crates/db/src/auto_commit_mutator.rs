//! Auto-committing document mutator for non-transactional mutations.
//!
//! This mutator wraps a database and automatically creates and commits
//! a write transaction for each mutation operation. This enables mutations
//! without explicit transaction management while still providing proper
//! transactional semantics per operation.

use async_trait::async_trait;
use cid::Cid;
use document::{DocID, Document};
use events::{Message, Update};
use query::mutator::{CreateResult, DeleteResult, DocMutator, UpdateResult};
use std::sync::Arc;
use storage::corekv::Store;
use tracing::warn;

use crate::block_builder::{write_collection_block, write_delete_block, write_document_blocks};
use crate::collection::collection_short_id;
use crate::database::DB;
use crate::index_manager::IndexManager;
use defra_core::encryption::{get_doc_encryption, get_encryption_config, store_doc_encryption};
use defra_core::signing::get_signing_config;

/// Document mutator that auto-commits transactions for each operation.
///
/// This is useful for mutations that don't need explicit transaction control.
/// Each operation creates a new write transaction, performs the mutation,
/// and commits (or discards on error).
///
/// # Transaction Semantics
///
/// Each mutation is atomic: it either succeeds entirely or fails without
/// partial changes. However, multiple mutations are NOT atomic with respect
/// to each other - if you need multiple operations to be atomic, use
/// `DbDocMutator` with explicit transaction management instead.
pub struct AutoCommitMutator<S: Store> {
    db: Arc<DB<S>>,
}

impl<S: Store> AutoCommitMutator<S> {
    /// Create a new auto-committing mutator wrapping the given database.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self { db }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocMutator for AutoCommitMutator<S> {
    async fn create(
        &self,
        collection_name: &str,
        mut doc: Document,
    ) -> query::error::Result<CreateResult> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a write transaction
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Generate document ID if not present
        if doc.id().is_none() {
            doc.generate_and_set_doc_id().map_err(|e| {
                query::error::QueryError::execution(format!("failed to generate DocID: {}", e))
            })?;
        }

        let doc_id = doc.id().cloned().ok_or_else(|| {
            query::error::QueryError::execution("document should have ID after generation")
        })?;

        // Execute the mutation in a block to drop datastore before commit
        let result = {
            let datastore = txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?;

            // Create an IndexManager for unique constraint enforcement
            let short_id = collection_short_id(collection.collection_id());
            let index_manager = IndexManager::from_collection(short_id, collection.schema())
                .map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to create index manager for collection '{}': {}",
                        collection_name, e
                    ))
                })?;

            // Use create_with_indexes to enforce unique constraints and maintain indexes
            collection
                .create_with_indexes(&datastore, &doc, &index_manager)
                .await
                .map_err(|e| {
                    let msg = e.to_string();
                    // If this is a unique constraint violation, return the core message without wrapping
                    if msg.contains("can not index a doc's field(s) that violates unique index") {
                        query::error::QueryError::execution(
                            "can not index a doc's field(s) that violates unique index."
                                .to_string(),
                        )
                    } else {
                        query::error::QueryError::execution(format!("create error: {}", e))
                    }
                })
        };

        match result {
            Ok(_returned_doc_id) => {
                // Build blocks and write to blockstore/headstore in a scoped block
                // This enables _commits queries to find the document's version history
                // The stores must be dropped before commit, so scope them
                // (composite_cid, composite_bytes, optional (collection_cid, collection_bytes))
                let commit_result: Option<(Cid, Vec<u8>, Option<(Cid, Vec<u8>)>)> = {
                    let blockstore = txn.blockstore().map_err(|e| {
                        query::error::QueryError::execution(format!(
                            "failed to get blockstore: {}",
                            e
                        ))
                    })?;
                    let headstore = txn.headstore().map_err(|e| {
                        query::error::QueryError::execution(format!(
                            "failed to get headstore: {}",
                            e
                        ))
                    })?;

                    // Use version_id for collectionVersionID (matches Go's VersionID())
                    let schema_version_id = collection.version_id();

                    // Get encryption config from thread-local (set by plan nodes)
                    let enc_config = get_encryption_config();
                    // Get signing config from thread-local (set by FFI exec_request)
                    let sign_config = get_signing_config();
                    tracing::debug!(
                        has_signing_config = sign_config.is_some(),
                        has_encryption_config = enc_config.is_some(),
                        "Auto-commit create mutation configs"
                    );

                    // For create operations, all fields are new - pass None for modified_fields
                    match write_document_blocks(
                        &blockstore,
                        &headstore,
                        &doc,
                        schema_version_id,
                        None,
                        enc_config.as_ref(),
                        sign_config.as_ref(),
                    )
                    .await
                    {
                        Ok(block_result) => {
                            // Store encryption config per-document so updates re-apply it
                            if let Some(ref config) = enc_config {
                                store_doc_encryption(&doc_id.to_string(), config.clone());
                            }

                            // For branchable collections, create a collection-level block
                            let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
                            if collection.schema().is_branchable {
                                let short_id = collection_short_id(collection.collection_id());
                                match write_collection_block(
                                    &blockstore,
                                    &headstore,
                                    short_id,
                                    schema_version_id,
                                    block_result.cid,
                                    sign_config.as_ref(),
                                )
                                .await
                                {
                                    Ok((col_cid, col_bytes)) => {
                                        col_block_data = Some((col_cid, col_bytes));
                                    }
                                    Err(e) => {
                                        warn!(
                                            collection = %collection_name,
                                            error = %e,
                                            "Failed to write collection block for branchable create"
                                        );
                                    }
                                }
                            }

                            Some((block_result.cid, block_result.block, col_block_data))
                        }
                        Err(e) => {
                            warn!(
                                collection = %collection_name,
                                error = %e,
                                "Failed to write document blocks - commits queries may not work"
                            );
                            // Don't fail the mutation, just log the warning
                            // The document was stored successfully, blocks are for commit history
                            None
                        }
                    }
                }; // blockstore and headstore dropped here

                // Commit the transaction (all store references now dropped)
                if let Err(e) = txn.commit().await {
                    warn!(
                        collection = %collection_name,
                        error = %e,
                        "Failed to commit transaction after create"
                    );
                    return Err(query::error::QueryError::execution(format!(
                        "commit error: {}",
                        e
                    )));
                }

                // Emit update event for subscriptions
                if let Some(bus) = self.db.event_bus() {
                    let (cid, block) = commit_result
                        .as_ref()
                        .map(|(c, b, _)| (*c, b.clone()))
                        .unwrap_or_default();
                    let update = Update::new(
                        doc_id.to_string(),
                        cid,
                        collection.collection_id().to_string(),
                        block.clone(),
                        false, // is_retry
                        false, // is_relay (local mutation)
                    );
                    bus.publish(Message::update(update));

                    // For branchable collections, emit a second Update event for the
                    // collection-level DAG. The test framework uses this to track
                    // collection heads separately from document heads.
                    if collection.schema().is_branchable {
                        let col_update = Update::new(
                            String::new(), // empty doc_id → keyed by collection_id
                            cid,
                            collection.collection_id().to_string(),
                            block,
                            false, // is_retry
                            false, // is_relay
                        );
                        bus.publish(Message::update(col_update));
                    }
                }

                // Return result with commit CID and block if available
                match commit_result {
                    Some((cid, block, col_data)) => {
                        let mut result = CreateResult::with_commit(doc_id, doc, cid, block);
                        if let Some((col_cid, col_bytes)) = col_data {
                            result.broadcast_cid = Some(col_cid);
                            result.broadcast_block = Some(col_bytes);
                        }
                        Ok(result)
                    }
                    None => Ok(CreateResult::new(doc_id, doc)),
                }
            }
            Err(e) => {
                // Discard the transaction on error
                if let Err(discard_err) = txn.discard() {
                    warn!(
                        collection = %collection_name,
                        error = %discard_err,
                        "Failed to discard transaction after create error"
                    );
                }
                Err(e)
            }
        }
    }

    async fn update(
        &self,
        collection_name: &str,
        doc: Document,
        modified_fields: std::collections::HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a write transaction
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Execute the mutation in a block to drop datastore before commit
        let result = {
            let datastore = txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?;

            // Create an IndexManager for index maintenance
            let short_id = collection_short_id(collection.collection_id());
            let index_manager = IndexManager::from_collection(short_id, collection.schema())
                .map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to create index manager for collection '{}': {}",
                        collection_name, e
                    ))
                })?;

            // Use update_with_indexes to maintain index consistency
            collection
                .update_with_indexes(&datastore, &doc, &index_manager)
                .await
                .map_err(|e| match e {
                    crate::error::Error::DocumentNotFound(id) => {
                        query::error::QueryError::document_not_found(id)
                    }
                    other => {
                        let msg = other.to_string();
                        // If this is a unique constraint violation, return the core message without wrapping
                        if msg.contains("can not index a doc's field(s) that violates unique index")
                        {
                            query::error::QueryError::execution(
                                "can not index a doc's field(s) that violates unique index."
                                    .to_string(),
                            )
                        } else {
                            query::error::QueryError::execution(format!("update error: {}", other))
                        }
                    }
                })
        };

        match result {
            Ok(()) => {
                // Build blocks and write to blockstore/headstore in a scoped block
                // This enables _commits queries to find the document's version history
                // (composite_cid, composite_bytes, optional (collection_cid, collection_bytes))
                let commit_result: Option<(Cid, Vec<u8>, Option<(Cid, Vec<u8>)>)> = {
                    let blockstore = txn.blockstore().map_err(|e| {
                        query::error::QueryError::execution(format!(
                            "failed to get blockstore: {}",
                            e
                        ))
                    })?;
                    let headstore = txn.headstore().map_err(|e| {
                        query::error::QueryError::execution(format!(
                            "failed to get headstore: {}",
                            e
                        ))
                    })?;

                    // Use version_id for collectionVersionID (matches Go's VersionID())
                    let schema_version_id = collection.version_id();

                    // Get encryption config: first try thread-local (explicit in mutation),
                    // then fall back to per-document stored config (from create with encryption).
                    // This matches Go's behavior where encryption propagates through the DAG.
                    let enc_config = get_encryption_config()
                        .or_else(|| doc.id().and_then(|id| get_doc_encryption(&id.to_string())));
                    // Get signing config from thread-local (set by FFI exec_request)
                    let sign_config = get_signing_config();

                    // For update operations, pass the modified fields to only create blocks
                    // for the fields that actually changed
                    match write_document_blocks(
                        &blockstore,
                        &headstore,
                        &doc,
                        schema_version_id,
                        Some(&modified_fields),
                        enc_config.as_ref(),
                        sign_config.as_ref(),
                    )
                    .await
                    {
                        Ok(block_result) => {
                            // For branchable collections, create a collection-level block
                            let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
                            if collection.schema().is_branchable {
                                let short_id = collection_short_id(collection.collection_id());
                                match write_collection_block(
                                    &blockstore,
                                    &headstore,
                                    short_id,
                                    schema_version_id,
                                    block_result.cid,
                                    sign_config.as_ref(),
                                )
                                .await
                                {
                                    Ok((col_cid, col_bytes)) => {
                                        col_block_data = Some((col_cid, col_bytes));
                                    }
                                    Err(e) => {
                                        warn!(
                                            collection = %collection_name,
                                            error = %e,
                                            "Failed to write collection block for branchable update"
                                        );
                                    }
                                }
                            }
                            Some((block_result.cid, block_result.block, col_block_data))
                        }
                        Err(e) => {
                            warn!(
                                collection = %collection_name,
                                error = %e,
                                "Failed to write document blocks - commits queries may not work"
                            );
                            // Don't fail the mutation, just log the warning
                            None
                        }
                    }
                }; // blockstore and headstore dropped here

                // Commit the transaction (all store references now dropped)
                if let Err(e) = txn.commit().await {
                    warn!(
                        collection = %collection_name,
                        error = %e,
                        "Failed to commit transaction after update"
                    );
                    return Err(query::error::QueryError::execution(format!(
                        "commit error: {}",
                        e
                    )));
                }

                // Emit update event for subscriptions
                if let Some(bus) = self.db.event_bus() {
                    if let Some(doc_id) = doc.id() {
                        let (cid, block) = commit_result
                            .as_ref()
                            .map(|(c, b, _)| (*c, b.clone()))
                            .unwrap_or_default();
                        let update = Update::new(
                            doc_id.to_string(),
                            cid,
                            collection.collection_id().to_string(),
                            block.clone(),
                            false, // is_retry
                            false, // is_relay (local mutation)
                        );
                        bus.publish(Message::update(update));

                        // For branchable collections, emit a second Update event for the
                        // collection-level DAG.
                        if collection.schema().is_branchable {
                            let col_update = Update::new(
                                String::new(), // empty doc_id → keyed by collection_id
                                cid,
                                collection.collection_id().to_string(),
                                block,
                                false,
                                false,
                            );
                            bus.publish(Message::update(col_update));
                        }
                    }
                }

                // Count modified fields
                let fields_modified = doc.values().len();
                match commit_result {
                    Some((cid, block, col_data)) => {
                        let mut result =
                            UpdateResult::with_commit(doc, fields_modified, cid, block);
                        if let Some((col_cid, col_bytes)) = col_data {
                            result.broadcast_cid = Some(col_cid);
                            result.broadcast_block = Some(col_bytes);
                        }
                        Ok(result)
                    }
                    None => Ok(UpdateResult::new(doc, fields_modified)),
                }
            }
            Err(e) => {
                // Discard the transaction on error
                if let Err(discard_err) = txn.discard() {
                    warn!(
                        collection = %collection_name,
                        error = %discard_err,
                        "Failed to discard transaction after update error"
                    );
                }
                Err(e)
            }
        }
    }

    async fn delete(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a write transaction
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Execute the mutation in a block to drop datastore before commit
        let result = {
            let datastore = txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?;

            // Create an IndexManager for index maintenance
            let short_id = collection_short_id(collection.collection_id());
            let index_manager = IndexManager::from_collection(short_id, collection.schema())
                .map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to create index manager for collection '{}': {}",
                        collection_name, e
                    ))
                })?;

            // Use delete_with_indexes to maintain index consistency
            collection
                .delete_with_indexes(&datastore, doc_id, &index_manager)
                .await
                .map_err(|e| query::error::QueryError::execution(format!("delete error: {}", e)))
        };

        match result {
            Ok(existed) => {
                // Build delete block (composite with status=2) in a scoped block
                let commit_result: Option<(Cid, Vec<u8>)> = {
                    let blockstore = txn.blockstore().map_err(|e| {
                        query::error::QueryError::execution(format!(
                            "failed to get blockstore: {}",
                            e
                        ))
                    })?;
                    let headstore = txn.headstore().map_err(|e| {
                        query::error::QueryError::execution(format!(
                            "failed to get headstore: {}",
                            e
                        ))
                    })?;

                    let schema_version_id = collection.version_id();
                    let doc_id_str = doc_id.to_string();
                    // Get signing config from thread-local (set by FFI exec_request)
                    let sign_config = get_signing_config();

                    match write_delete_block(
                        &blockstore,
                        &headstore,
                        &doc_id_str,
                        schema_version_id,
                        sign_config.as_ref(),
                    )
                    .await
                    {
                        Ok(block_result) => {
                            let composite_cid = block_result.cid;

                            // For branchable collections, also create a collection-level block
                            if collection.schema().is_branchable {
                                let short_id = collection_short_id(collection.collection_id());
                                if let Err(e) = write_collection_block(
                                    &blockstore,
                                    &headstore,
                                    short_id,
                                    schema_version_id,
                                    composite_cid,
                                    sign_config.as_ref(),
                                )
                                .await
                                .map(|_| ())
                                {
                                    warn!(
                                        collection = %collection_name,
                                        error = %e,
                                        "Failed to write collection block for branchable delete"
                                    );
                                }
                            }

                            Some((composite_cid, block_result.block))
                        }
                        Err(e) => {
                            warn!(
                                collection = %collection_name,
                                error = %e,
                                "Failed to write delete block - commits queries may not work"
                            );
                            None
                        }
                    }
                }; // blockstore and headstore dropped here

                // Commit the transaction (datastore reference is now dropped)
                if let Err(e) = txn.commit().await {
                    warn!(
                        collection = %collection_name,
                        error = %e,
                        "Failed to commit transaction after delete"
                    );
                    return Err(query::error::QueryError::execution(format!(
                        "commit error: {}",
                        e
                    )));
                }

                // Emit update event for subscriptions (deletes are also "updates")
                if let Some(bus) = self.db.event_bus() {
                    let (cid, block) = commit_result
                        .as_ref()
                        .map(|(c, b)| (*c, b.clone()))
                        .unwrap_or_default();
                    let update = Update::new(
                        doc_id.to_string(),
                        cid,
                        collection.collection_id().to_string(),
                        block.clone(),
                        false, // is_retry
                        false, // is_relay (local mutation)
                    );
                    bus.publish(Message::update(update));

                    // For branchable collections, emit a second Update event for the
                    // collection-level DAG.
                    if collection.schema().is_branchable {
                        let col_update = Update::new(
                            String::new(), // empty doc_id → keyed by collection_id
                            cid,
                            collection.collection_id().to_string(),
                            block,
                            false,
                            false,
                        );
                        bus.publish(Message::update(col_update));
                    }
                }

                Ok(DeleteResult::new(doc_id.clone(), existed))
            }
            Err(e) => {
                // Discard the transaction on error
                if let Err(discard_err) = txn.discard() {
                    warn!(
                        collection = %collection_name,
                        error = %discard_err,
                        "Failed to discard transaction after delete error"
                    );
                }
                Err(e)
            }
        }
    }

    async fn exists(&self, collection_name: &str, doc_id: &DocID) -> query::error::Result<bool> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a read-only transaction (exists is read-only)
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Get the datastore
        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;

        // Execute the check
        let result = collection
            .exists_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("exists error: {}", e)));

        // Discard the read-only transaction
        if let Err(e) = txn.discard() {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to discard read-only transaction after exists"
            );
        }

        result
    }

    async fn get_for_update(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<Option<Document>> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a read-only transaction (get_for_update is read-only)
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Get the datastore
        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;

        // Execute the fetch
        let result = collection
            .get_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("get_for_update error: {}", e))
            });

        // Discard the read-only transaction
        if let Err(e) = txn.discard() {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to discard read-only transaction after get_for_update"
            );
        }

        result
    }
}
