use super::helpers::{ensure_collection_is_active, write_branchable_collection_block};
use super::*;

impl<S: Store + 'static> AutoCommitMutator<S> {
    pub(super) async fn delete_impl(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        self.db
            .check_node_access(None, acp::nac::NodePermission::DocumentDelete)
            .await
            .map_err(|e| query::error::QueryError::permission_denied(e.to_string()))?;

        let collection = self.get_collection_or_err(collection_name)?;
        let _collection_guard = self
            .db
            .collection_read_guard(collection.collection_id())
            .await
            .map_err(|error| query::error::QueryError::execution(error.to_string()))?;
        ensure_collection_is_active(&self.db, collection_name, &collection)?;

        // Create a write transaction
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Acquire store views up front (dropped before commit); the mutation
        // itself runs in an async block so errors fall through to the discard.
        // Some(short_id) means the doc existed and was deleted.
        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        let result: query::error::Result<Option<(u64, DocID)>> = async {
            let Some((doc_short_id, canonical_doc_id)) = collection
                .resolve_doc_identity(&systemstore, doc_id)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            else {
                return Ok(None);
            };

            // Create an IndexManager for index maintenance
            let short_id = collection.resolved_root_id();
            let index_manager = IndexManager::from_indexes(
                short_id,
                collection.schema(),
                collection.write_indexes(),
            )
            .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to create index manager for collection '{}': {}",
                    collection_name, e
                ))
            })?;

            // Use delete_with_indexes to maintain index consistency
            let existed = collection
                .delete_with_indexes(&datastore, &canonical_doc_id, doc_short_id, &index_manager)
                .await
                .map_err(|e| query::error::QueryError::execution(format!("delete error: {}", e)))?;
            Ok(existed.then_some((doc_short_id, canonical_doc_id)))
        }
        .await;

        drop(datastore);
        drop(systemstore);

        match result {
            Ok(deleted) => {
                // DeleteNode treats existed==false as a no-op; don't write a
                // tombstone block or emit an event for a missing doc.
                // Propagate any commit error so callers see the same failure
                // surface they get on the normal commit path (Go returns the
                // commit error on a no-op delete too).
                let Some((deleted_short_id, canonical_doc_id)) = deleted else {
                    if let Err(e) = txn.commit().await {
                        warn!(
                            collection = %collection_name,
                            error = %e,
                            "Failed to commit transaction after no-op delete"
                        );
                        return Err(crate::error::commit_query_error(e));
                    }
                    return Ok(DeleteResult::new(doc_id.clone(), false));
                };
                let existed = true;

                // Build delete block (composite with status=2) in a scoped block
                let commit_result: CommitArtifacts = {
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
                    let doc_id_str = canonical_doc_id.to_string();
                    // Get signing config from thread-local (set by FFI exec_request)
                    let sign_config = get_signing_config();

                    let block_result = write_delete_block(
                        &blockstore,
                        &headstore,
                        &doc_id_str,
                        deleted_short_id,
                        schema_version_id,
                        sign_config.as_ref(),
                    )
                    .await
                    .map_err(|e| {
                        query::error::QueryError::execution(format!(
                            "failed to write delete block for collection {}: {}",
                            collection_name, e
                        ))
                    })?;
                    let composite_cid = block_result.cid;

                    let systemstore = txn.systemstore().map_err(|e| {
                        query::error::QueryError::execution(format!(
                            "failed to get systemstore: {}",
                            e
                        ))
                    })?;
                    crate::doc_id_map::set_block_doc_id_mapping(
                        &systemstore,
                        &composite_cid.to_string(),
                        &doc_id_str,
                    )
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

                    let col_block_data = write_branchable_collection_block(
                        collection_name,
                        &collection,
                        &blockstore,
                        &headstore,
                        composite_cid,
                        sign_config.as_ref(),
                    )
                    .await?;

                    (composite_cid, block_result.block, col_block_data)
                }; // blockstore and headstore dropped here

                // Commit the transaction (datastore reference is now dropped)
                if let Err(e) = txn.commit().await {
                    warn!(
                        collection = %collection_name,
                        error = %e,
                        "Failed to commit transaction after delete"
                    );
                    return Err(crate::error::commit_query_error(e));
                }

                let (cid, block, col_data) = commit_result;
                self.emit_update_events(
                    &collection,
                    &canonical_doc_id.to_string(),
                    cid,
                    block.clone(),
                    col_data.clone(),
                );

                let mut result = DeleteResult::with_commit(canonical_doc_id, existed, cid, block);
                if let Some((col_cid, col_bytes)) = col_data {
                    result.broadcast_cid = Some(col_cid);
                    result.broadcast_block = Some(col_bytes);
                }
                Ok(result)
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
}
