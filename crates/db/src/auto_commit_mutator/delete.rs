use super::*;

impl<S: Store + 'static> AutoCommitMutator<S> {
    pub(super) async fn delete_impl(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        let collection = self.get_collection_or_err(collection_name)?;

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
                let (cid, block) = commit_result
                    .as_ref()
                    .map(|(c, b)| (*c, b.clone()))
                    .unwrap_or_default();
                self.emit_update_events(&collection, &doc_id.to_string(), cid, block);

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
}
