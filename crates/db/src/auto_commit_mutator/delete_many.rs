use super::helpers::ensure_collection_is_active;
use super::*;

impl<S: Store + 'static> AutoCommitMutator<S> {
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
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Block-ownership registration is correctness-critical; a failure must
        // abort the whole batch rather than commit a tombstone with no owner
        // (which would break P2P serve, KMS, and ACP resolution).
        let mut ownership_error: Option<String> = None;
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

                match write_delete_block(
                    &blockstore,
                    &headstore,
                    &canonical_doc_id.to_string(),
                    doc_short_id,
                    &schema_version_id,
                    sign_config.as_ref(),
                )
                .await
                {
                    Ok(block_result) => {
                        let composite_cid = block_result.cid;

                        match txn.systemstore() {
                            Ok(systemstore) => {
                                if let Err(e) = crate::doc_id_map::set_block_doc_id_mapping(
                                    &systemstore,
                                    &composite_cid.to_string(),
                                    &canonical_doc_id.to_string(),
                                )
                                .await
                                {
                                    ownership_error = Some(e.to_string());
                                }
                            }
                            Err(e) => ownership_error = Some(e.to_string()),
                        }

                        let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
                        if collection.schema().is_branchable {
                            match write_collection_block(
                                &blockstore,
                                &headstore,
                                short_id,
                                &schema_version_id,
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
                            doc_id = %canonical_doc_id,
                            error = %e,
                            "Failed to write delete block in batch"
                        );
                        None
                    }
                }
            };

            results.push((canonical_doc_id, existed, commit_result));
        }

        // A failed ownership registration must not commit a partial index for
        // the batch; drop the txn (rolls back) and surface the error.
        if let Some(e) = ownership_error {
            return Err(query::error::QueryError::execution(format!(
                "failed to record block ownership mapping for delete: {e}"
            )));
        }

        // Single commit for entire batch
        if let Err(e) = txn.commit().await {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to commit batch delete transaction"
            );
            return Err(query::error::QueryError::execution(format!(
                "commit error: {}",
                e
            )));
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
