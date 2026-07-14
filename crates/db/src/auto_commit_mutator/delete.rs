use super::helpers::ensure_collection_is_active;
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

        let result: query::error::Result<Option<u64>> = async {
            let Some(doc_short_id) = collection
                .resolve_doc_short_id(&systemstore, doc_id)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            else {
                return Ok(None);
            };

            // Create an IndexManager for index maintenance
            let short_id = collection.resolved_root_id();
            let index_manager = IndexManager::from_collection(short_id, collection.schema())
                .map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to create index manager for collection '{}': {}",
                        collection_name, e
                    ))
                })?;

            // Use delete_with_indexes to maintain index consistency
            let existed = collection
                .delete_with_indexes(&datastore, doc_id, doc_short_id, &index_manager)
                .await
                .map_err(|e| query::error::QueryError::execution(format!("delete error: {}", e)))?;
            Ok(existed.then_some(doc_short_id))
        }
        .await;

        drop(datastore);
        drop(systemstore);

        match result {
            Ok(deleted_short_id) => {
                let existed = deleted_short_id.is_some();
                // DeleteNode treats existed==false as a no-op; don't write a
                // tombstone block or emit an event for a missing doc.
                // Propagate any commit error so callers see the same failure
                // surface they get on the normal commit path (Go returns the
                // commit error on a no-op delete too).
                if !existed {
                    if let Err(e) = txn.commit().await {
                        warn!(
                            collection = %collection_name,
                            error = %e,
                            "Failed to commit transaction after no-op delete"
                        );
                        return Err(query::error::QueryError::execution(format!(
                            "commit error: {}",
                            e
                        )));
                    }
                    return Ok(DeleteResult::new(doc_id.clone(), existed));
                }

                // Build delete block (composite with status=2) in a scoped block
                let commit_result: Option<CommitArtifacts> = {
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
                        deleted_short_id.expect("existed implies short id"),
                        schema_version_id,
                        sign_config.as_ref(),
                    )
                    .await
                    {
                        Ok(block_result) => {
                            let composite_cid = block_result.cid;

                            if let Ok(systemstore) = txn.systemstore() {
                                if let Err(e) = crate::doc_id_map::set_block_doc_id_mapping(
                                    &systemstore,
                                    &composite_cid.to_string(),
                                    &doc_id_str,
                                )
                                .await
                                {
                                    warn!(
                                        collection = %collection_name,
                                        error = %e,
                                        "Failed to record block ownership mapping for delete"
                                    );
                                }
                            }

                            let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
                            if collection.schema().is_branchable {
                                let short_id = collection.resolved_root_id();
                                match write_collection_block(
                                    &blockstore,
                                    &headstore,
                                    short_id,
                                    schema_version_id,
                                    composite_cid,
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
                                            "Failed to write collection block for branchable delete"
                                        );
                                    }
                                }
                            }

                            Some((composite_cid, block_result.block, col_block_data))
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

                // Emit update event for subscriptions when blocks were written.
                if let Some((cid, block, col_data)) = commit_result.as_ref() {
                    self.emit_update_events(
                        &collection,
                        &doc_id.to_string(),
                        *cid,
                        block.clone(),
                        col_data.clone(),
                    );
                }

                match commit_result {
                    Some((cid, block, col_data)) => {
                        let mut result =
                            DeleteResult::with_commit(doc_id.clone(), existed, cid, block);
                        if let Some((col_cid, col_bytes)) = col_data {
                            result.broadcast_cid = Some(col_cid);
                            result.broadcast_block = Some(col_bytes);
                        }
                        Ok(result)
                    }
                    None => Ok(DeleteResult::new(doc_id.clone(), existed)),
                }
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
