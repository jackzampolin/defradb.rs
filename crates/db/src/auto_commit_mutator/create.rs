use super::helpers::ensure_collection_is_active;
use super::*;

#[allow(clippy::type_complexity)]
impl<S: Store + 'static> AutoCommitMutator<S> {
    pub(super) async fn create_impl(
        &self,
        collection_name: &str,
        mut doc: Document,
    ) -> query::error::Result<CreateResult> {
        let collection = self.get_collection_or_err(collection_name)?;
        ensure_collection_is_active(&self.db, collection_name, &collection)?;

        // Create a write transaction
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Generate embeddings before doc ID (embedding values affect content hash)
        let embedding_config = self.db.options().embedding_config();
        db_search::set_embedding(
            &collection.schema().vector_embeddings,
            &mut doc,
            true,
            None,
            &embedding_config,
        )
        .await
        .map_err(|e| query::error::QueryError::execution(format!("embedding error: {}", e)))?;

        // Generate document ID if not present.
        // Track whether ID was just generated — if so, it's content-addressed and
        // guaranteed unique, so we can skip the existence check (blind create).
        let id_was_generated = doc.id().is_none();
        if id_was_generated {
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
            let short_id = collection.resolved_root_id();
            let index_manager = IndexManager::from_collection(short_id, collection.schema())
                .map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to create index manager for collection '{}': {}",
                        collection_name, e
                    ))
                })?;

            self.db
                .validate_downsample_write(&datastore, collection.schema(), &doc, None)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

            // Use create_with_indexes to enforce unique constraints and maintain indexes.
            // Blind create skips existence check for content-addressed (generated) IDs.
            collection
                .create_with_indexes(&datastore, &doc, &index_manager, id_was_generated)
                .await
                .map_err(|e| crate::error::index_write_query_error("create", e))
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

                // Emit update event for subscriptions when blocks were written.
                // Skipping the default-cid emit avoids publishing a misleading
                // Update on the block-write failure path.
                if let Some((cid, block, col_data)) = commit_result.as_ref() {
                    self.emit_update_events(
                        &collection,
                        &doc_id.to_string(),
                        *cid,
                        block.clone(),
                        col_data.clone(),
                    );
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
}
