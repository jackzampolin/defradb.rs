use super::helpers::{
    ensure_collection_is_active, register_block_doc_id_mappings, write_local_update,
};
use super::*;

use db_blocks::DocStorageIdentity;

#[allow(clippy::type_complexity)]
impl<S: Store + 'static> AutoCommitMutator<S> {
    pub(super) async fn update_impl(
        &self,
        collection_name: &str,
        doc: Document,
        modified_fields: std::collections::HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        self.db
            .check_node_access(None, acp::nac::NodePermission::DocumentUpdate)
            .await
            .map_err(|e| query::error::QueryError::permission_denied(e.to_string()))?;

        let collection = self.get_collection_or_err(collection_name)?;
        ensure_collection_is_active(&self.db, collection_name, &collection)?;

        // Generate embeddings if source fields were modified
        let mut doc = doc;
        let mut modified_fields = modified_fields;
        let embedding_config = self.db.options().embedding_config();

        let generated = db_search::set_embedding(
            &collection.schema().vector_embeddings,
            &mut doc,
            false,
            Some(&modified_fields),
            &embedding_config,
        )
        .await
        .map_err(|e| query::error::QueryError::execution(format!("embedding error: {}", e)))?;

        for field in generated {
            modified_fields.insert(field);
        }

        let input_doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| query::error::QueryError::execution("update requires a document ID"))?;
        let canonical_lock_id = {
            let identity_txn = self.db.new_txn(true).await.map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to create identity transaction: {e}"
                ))
            })?;
            let canonical = {
                let systemstore = identity_txn.systemstore().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to get systemstore: {e}"))
                })?;
                collection
                    .require_doc_identity(&systemstore, &input_doc_id)
                    .await
                    .map_err(|e| match e {
                        crate::error::Error::DocumentNotFound(id) => {
                            query::error::QueryError::document_not_found(id)
                        }
                        other => query::error::QueryError::execution(other.to_string()),
                    })?
                    .1
            };
            identity_txn.discard().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to discard identity transaction: {e}"
                ))
            })?;
            canonical
        };
        doc.set_id(canonical_lock_id.clone());

        // Serialize this write against concurrent merges (and other local writes)
        // touching the same document. Local counter increments and P2P merges both
        // read-modify-write the CRDT accumulation store; without this per-doc lock
        // their txns can race in a way the store's optimistic-conflict detection
        // does not always catch, dropping increments (#1021). The guard is held
        // across the whole write + commit.
        let _doc_guard = self
            .db
            .doc_write_queue()
            .acquire(&canonical_lock_id.to_string())
            .await;

        // Create a write transaction
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Acquire store views up front (dropped before commit); the mutation
        // itself runs in an async block so errors fall through to the discard.
        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        let result: query::error::Result<u64> = async {
            // Create an IndexManager for index maintenance
            let short_id = collection.resolved_root_id();
            let index_manager = IndexManager::from_collection(short_id, collection.schema())
                .map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to create index manager for collection '{}': {}",
                        collection_name, e
                    ))
                })?;

            self.db
                .validate_downsample_write(
                    &datastore,
                    &systemstore,
                    collection.schema(),
                    &doc,
                    Some(&modified_fields),
                )
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

            let (doc_short_id, canonical_doc_id) = collection
                .require_doc_identity(&systemstore, &input_doc_id)
                .await
                .map_err(|e| match e {
                    crate::error::Error::DocumentNotFound(id) => {
                        query::error::QueryError::document_not_found(id)
                    }
                    other => query::error::QueryError::execution(other.to_string()),
                })?;
            doc.set_id(canonical_doc_id);

            // Bundle the counter RMW (#1021) with the doc blob + index write so
            // the authoritative CRDT accumulation store always advances before the
            // blob is persisted — enforced by construction in `write_local_update`.
            write_local_update(
                &datastore,
                &collection,
                &mut doc,
                doc_short_id,
                &index_manager,
            )
            .await?;
            Ok(doc_short_id)
        }
        .await;

        drop(datastore);
        drop(systemstore);

        match result {
            Ok(doc_short_id) => {
                // Build blocks and write to blockstore/headstore in a scoped block
                // This enables _commits queries to find the document's version history
                // (composite_cid, composite_bytes, optional (collection_cid, collection_bytes))
                // Block-ownership registration is correctness-critical (P2P
                // serve, KMS, and ACP resolution all read the index), so unlike
                // the best-effort commits-query block writes below its failure
                // is fatal: surfaced here and routed through the discard path.
                let mut ownership_error: Option<String> = None;
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

                    let identity =
                        DocStorageIdentity::new(collection.resolved_root_id(), doc_short_id);

                    // For update operations, pass the modified fields to only create blocks
                    // for the fields that actually changed
                    match write_document_blocks(
                        &blockstore,
                        &headstore,
                        &doc,
                        schema_version_id,
                        identity,
                        Some(&modified_fields),
                        enc_config.as_ref(),
                        sign_config.as_ref(),
                        None,
                    )
                    .await
                    {
                        Ok(block_result) => {
                            if let Some(doc_id) = doc.id() {
                                match txn.systemstore() {
                                    Ok(systemstore) => {
                                        if let Err(e) = register_block_doc_id_mappings(
                                            &systemstore,
                                            &block_result,
                                            &doc_id.to_string(),
                                        )
                                        .await
                                        {
                                            ownership_error = Some(e.to_string());
                                        }
                                    }
                                    Err(e) => ownership_error = Some(e.to_string()),
                                }
                            }
                            // For branchable collections, create a collection-level block
                            let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
                            if collection.schema().is_branchable {
                                let short_id = collection.resolved_root_id();
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

                // A failed ownership registration must not commit a partial
                // index; drop the txn (rolls back) and surface the error.
                if let Some(e) = ownership_error {
                    return Err(query::error::QueryError::execution(format!(
                        "failed to record block ownership mappings for update: {e}"
                    )));
                }

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

                // Emit update event for subscriptions when blocks were written.
                if let (Some(doc_id), Some((cid, block, col_data))) =
                    (doc.id(), commit_result.as_ref())
                {
                    self.emit_update_events(
                        &collection,
                        &doc_id.to_string(),
                        *cid,
                        block.clone(),
                        col_data.clone(),
                    );
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
}
