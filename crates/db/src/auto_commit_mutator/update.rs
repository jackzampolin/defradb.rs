use super::*;

#[allow(clippy::type_complexity)]
impl<S: Store + 'static> AutoCommitMutator<S> {
    pub(super) async fn update_impl(
        &self,
        collection_name: &str,
        doc: Document,
        modified_fields: std::collections::HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        let collection = self.get_collection_or_err(collection_name)?;

        // Generate embeddings if source fields were modified
        let mut doc = doc;
        let mut modified_fields = modified_fields;

        let generated = crate::embedding::set_embedding(
            &collection.schema().vector_embeddings,
            &mut doc,
            false,
            Some(&modified_fields),
        )
        .await
        .map_err(|e| query::error::QueryError::execution(format!("embedding error: {}", e)))?;

        for field in generated {
            modified_fields.insert(field);
        }

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
                if let Some(doc_id) = doc.id() {
                    let cid = commit_result
                        .as_ref()
                        .map(|(c, _, _)| *c)
                        .unwrap_or_default();
                    self.emit_update_events(&collection, &doc_id.to_string(), cid);
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
