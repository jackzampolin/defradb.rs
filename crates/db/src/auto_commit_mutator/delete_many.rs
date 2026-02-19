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
        let short_id = collection_short_id(collection.collection_id());
        let schema_version_id = collection.version_id().to_string();
        let sign_config = get_signing_config();

        let index_manager =
            IndexManager::from_collection(short_id, collection.schema()).map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to create index manager for collection '{}': {}",
                    collection_name, e
                ))
            })?;

        // Single transaction for all deletes
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let mut results: Vec<(DocID, bool, Option<Cid>)> = Vec::with_capacity(doc_ids.len());

        for doc_id in doc_ids {
            // Delete from datastore + indexes
            let existed = {
                let datastore = txn.datastore().map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to get datastore for collection '{}': {}",
                        collection_name, e
                    ))
                })?;

                match collection
                    .delete_with_indexes(&datastore, doc_id, &index_manager)
                    .await
                {
                    Ok(existed) => existed,
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

            // Write delete block (composite with status=2)
            let commit_cid = {
                let blockstore = txn.blockstore().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to get blockstore: {}", e))
                })?;
                let headstore = txn.headstore().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to get headstore: {}", e))
                })?;

                match write_delete_block(
                    &blockstore,
                    &headstore,
                    &doc_id.to_string(),
                    &schema_version_id,
                    sign_config.as_ref(),
                )
                .await
                {
                    Ok(block_result) => {
                        let composite_cid = block_result.cid;

                        if collection.schema().is_branchable {
                            if let Err(e) = write_collection_block(
                                &blockstore,
                                &headstore,
                                short_id,
                                &schema_version_id,
                                composite_cid,
                                sign_config.as_ref(),
                            )
                            .await
                            {
                                warn!(
                                    collection = %collection_name,
                                    error = %e,
                                    "Failed to write collection block for branchable delete"
                                );
                            }
                        }

                        Some(composite_cid)
                    }
                    Err(e) => {
                        warn!(
                            collection = %collection_name,
                            doc_id = %doc_id,
                            error = %e,
                            "Failed to write delete block in batch"
                        );
                        None
                    }
                }
            };

            results.push((doc_id.clone(), existed, commit_cid));
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

        // Emit events after commit
        let mut delete_results = Vec::with_capacity(results.len());
        for (doc_id, existed, commit_cid) in results {
            let cid = commit_cid.unwrap_or_default();
            self.emit_update_events(&collection, &doc_id.to_string(), cid);
            delete_results.push(DeleteResult::new(doc_id, existed));
        }

        Ok(delete_results)
    }
}
