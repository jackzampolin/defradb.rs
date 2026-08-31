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

        let txn = self.new_mutation_txn().await?;

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
        let blockstore = txn.blockstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get blockstore: {}", e))
        })?;
        let headstore = txn.headstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get headstore: {}", e))
        })?;

        let result: query::error::Result<Option<(DocID, CommitArtifacts)>> = async {
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
            if !existed {
                return Ok(None);
            }

            let schema_version_id = collection.version_id();
            let doc_id_str = canonical_doc_id.to_string();
            // Get signing config from thread-local (set by FFI exec_request)
            let sign_config = get_signing_config();

            let block_result = write_delete_block(
                &blockstore,
                &headstore,
                &doc_id_str,
                doc_short_id,
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

            crate::docid::map::set_block_doc_id_mapping(
                &systemstore,
                &composite_cid.to_string(),
                &doc_id_str,
            )
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

            let col_block_data = write_branchable_collection_block(
                &self.db,
                collection_name,
                &collection,
                &blockstore,
                &headstore,
                composite_cid,
                sign_config.as_ref(),
            )
            .await?;

            Ok(Some((
                canonical_doc_id,
                (composite_cid, block_result.block, col_block_data),
            )))
        }
        .await;

        drop(datastore);
        drop(systemstore);
        drop(blockstore);
        drop(headstore);

        let operation = if matches!(&result, Ok(None)) {
            "no-op delete"
        } else {
            "delete"
        };
        let deleted = self
            .finish_mutation(txn, result, collection_name, operation)
            .await?;

        let Some((canonical_doc_id, commit_result)) = deleted else {
            return Ok(DeleteResult::new(doc_id.clone(), false));
        };

        let (cid, block, col_data) = commit_result;
        self.emit_update_events(
            &collection,
            &canonical_doc_id.to_string(),
            cid,
            block.clone(),
            col_data.clone(),
        );

        let mut result = DeleteResult::with_commit(canonical_doc_id, true, cid, block);
        if let Some((col_cid, col_bytes)) = col_data {
            result.broadcast_cid = Some(col_cid);
            result.broadcast_block = Some(col_bytes);
        }
        Ok(result)
    }

    /// Delete multiple documents in a single transaction.
    ///
    /// Mirrors `create_many_impl`: one transaction for all deletes, one commit.
    /// This avoids creating N separate MVCC snapshots and N COW epochs when
    /// purging large batches (e.g. 686 documents caused a 20GB memory spike
    /// with per-document transactions).
    pub async fn delete_many_impl(
        &self,
        collection_name: &str,
        doc_ids: &[DocID],
    ) -> query::error::Result<Vec<DeleteResult>> {
        if doc_ids.is_empty() {
            return Ok(Vec::new());
        }

        if doc_ids.len() == 1 {
            return self
                .delete_impl(collection_name, &doc_ids[0])
                .await
                .map(|r| vec![r]);
        }

        let collection = self.get_collection_or_err(collection_name)?;
        let _collection_guard = self
            .db
            .collection_read_guard(collection.collection_id())
            .await
            .map_err(|error| query::error::QueryError::execution(error.to_string()))?;
        ensure_collection_is_active(&self.db, collection_name, &collection)?;
        let short_id = collection.resolved_root_id();
        let schema_version_id = collection.version_id().to_string();
        let sign_config = get_signing_config();

        let index_manager =
            IndexManager::from_indexes(short_id, collection.schema(), collection.write_indexes())
                .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to create index manager for collection '{}': {}",
                    collection_name, e
                ))
            })?;

        // Single transaction for all deletes
        let txn = self.new_mutation_txn().await?;

        let mut results: Vec<(DocID, bool, Option<CommitArtifacts>)> =
            Vec::with_capacity(doc_ids.len());

        for doc_id in doc_ids {
            // Delete from datastore + indexes
            let (existed, doc_short_id, canonical_doc_id) = {
                let datastore = txn.datastore().map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to get datastore for collection '{}': {}",
                        collection_name, e
                    ))
                })?;
                let systemstore = txn.systemstore().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
                })?;

                let (doc_short_id, canonical_doc_id) =
                    match collection.resolve_doc_identity(&systemstore, doc_id).await {
                        Ok(Some(identity)) => identity,
                        Ok(None) => {
                            results.push((doc_id.clone(), false, None));
                            continue;
                        }
                        Err(e) => {
                            return Err(query::error::QueryError::execution(format!(
                                "failed to resolve document identity for '{doc_id}': {e}"
                            )));
                        }
                    };

                match collection
                    .delete_with_indexes(
                        &datastore,
                        &canonical_doc_id,
                        doc_short_id,
                        &index_manager,
                    )
                    .await
                {
                    Ok(existed) => (existed, doc_short_id, canonical_doc_id),
                    Err(e) => {
                        warn!(
                            collection = %collection_name,
                            doc_id = %doc_id,
                            error = %e,
                            "Failed to delete document in batch"
                        );
                        results.push((doc_id.clone(), false, None));
                        continue;
                    }
                }
            };

            // Skip block-write and event emit for missing docs; DeleteNode
            // treats existed==false as a no-op.
            if !existed {
                results.push((canonical_doc_id, existed, None));
                continue;
            }

            // Write delete block (composite with status=2)
            let commit_result: Option<CommitArtifacts> = {
                let blockstore = txn.blockstore().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to get blockstore: {}", e))
                })?;
                let headstore = txn.headstore().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to get headstore: {}", e))
                })?;

                let block_result = write_delete_block(
                    &blockstore,
                    &headstore,
                    &canonical_doc_id.to_string(),
                    doc_short_id,
                    &schema_version_id,
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
                    query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
                })?;
                crate::docid::map::set_block_doc_id_mapping(
                    &systemstore,
                    &composite_cid.to_string(),
                    &canonical_doc_id.to_string(),
                )
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

                let col_block_data = write_branchable_collection_block(
                    &self.db,
                    collection_name,
                    &collection,
                    &blockstore,
                    &headstore,
                    composite_cid,
                    sign_config.as_ref(),
                )
                .await?;

                Some((composite_cid, block_result.block, col_block_data))
            };

            results.push((canonical_doc_id, existed, commit_result));
        }

        // Single commit for entire batch
        if let Err(e) = txn.commit().await {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to commit batch delete transaction"
            );
            return Err(crate::error::commit_query_error(e));
        }

        // Emit events after commit, only when blocks were written.
        let mut delete_results = Vec::with_capacity(results.len());
        for (doc_id, existed, commit_result) in results {
            if let Some((cid, block, col_data)) = commit_result {
                self.emit_update_events(&collection, &doc_id.to_string(), cid, block, col_data);
            }
            delete_results.push(DeleteResult::new(doc_id, existed));
        }

        Ok(delete_results)
    }
}
