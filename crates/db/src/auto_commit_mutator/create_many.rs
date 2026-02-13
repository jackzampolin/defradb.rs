use super::*;

#[allow(clippy::type_complexity)]
impl<S: Store + 'static> AutoCommitMutator<S> {
    pub(super) async fn create_many_impl(
        &self,
        collection_name: &str,
        docs: Vec<Document>,
    ) -> query::error::Result<Vec<CreateResult>> {
        if docs.is_empty() {
            return Ok(Vec::new());
        }

        // Single-doc fast path: reuse existing create() to avoid overhead
        if docs.len() == 1 {
            let doc = docs.into_iter().next().unwrap();
            return self
                .create_impl(collection_name, doc)
                .await
                .map(|r| vec![r]);
        }

        let collection = self.get_collection_or_err(collection_name)?;

        // Create ONE write transaction for the entire batch
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let short_id = collection_short_id(collection.collection_id());
        let schema_version_id = collection.version_id();
        let sign_config = get_signing_config();

        // Build IndexManager once for the entire batch (schema is identical for all docs)
        let index_manager =
            IndexManager::from_collection(short_id, collection.schema()).map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to create index manager for collection '{}': {}",
                    collection_name, e
                ))
            })?;

        let mut results: Vec<(
            DocID,
            Document,
            Option<(Cid, Vec<u8>, Option<(Cid, Vec<u8>)>)>,
        )> = Vec::with_capacity(docs.len());

        // Process each document within the SAME transaction
        for mut doc in docs {
            // Generate embeddings
            crate::embedding::set_embedding(
                &collection.schema().vector_embeddings,
                &mut doc,
                true,
                None,
            )
            .await
            .map_err(|e| query::error::QueryError::execution(format!("embedding error: {}", e)))?;

            // Generate document ID.
            // Track whether ID was just generated for blind create optimization.
            let id_was_generated = doc.id().is_none();
            if id_was_generated {
                doc.generate_and_set_doc_id().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to generate DocID: {}", e))
                })?;
            }

            let doc_id = doc.id().cloned().ok_or_else(|| {
                query::error::QueryError::execution("document should have ID after generation")
            })?;

            // Create with indexes (scoped borrow of datastore)
            {
                let datastore = txn.datastore().map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to get datastore for collection '{}': {}",
                        collection_name, e
                    ))
                })?;

                collection
                    .create_with_indexes(&datastore, &doc, &index_manager, id_was_generated)
                    .await
                    .map_err(|e| {
                        let msg = e.to_string();
                        if msg.contains("can not index a doc's field(s) that violates unique index")
                        {
                            query::error::QueryError::execution(
                                "can not index a doc's field(s) that violates unique index."
                                    .to_string(),
                            )
                        } else {
                            query::error::QueryError::execution(format!("create error: {}", e))
                        }
                    })?;
            } // datastore dropped

            // Write blocks (scoped borrow of blockstore + headstore)
            let commit_result: Option<(Cid, Vec<u8>, Option<(Cid, Vec<u8>)>)> = {
                let blockstore = txn.blockstore().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to get blockstore: {}", e))
                })?;
                let headstore = txn.headstore().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to get headstore: {}", e))
                })?;

                let enc_config = get_encryption_config();

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
                        if let Some(ref config) = enc_config {
                            store_doc_encryption(&doc_id.to_string(), config.clone());
                        }

                        let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
                        if collection.schema().is_branchable {
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
                        None
                    }
                }
            }; // blockstore and headstore dropped

            results.push((doc_id, doc, commit_result));
        }

        // Commit ONCE for the entire batch
        if let Err(e) = txn.commit().await {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to commit batch transaction"
            );
            return Err(query::error::QueryError::execution(format!(
                "commit error: {}",
                e
            )));
        }

        // Emit events and build results
        let mut create_results = Vec::with_capacity(results.len());
        for (doc_id, doc, commit_result) in results {
            let cid = commit_result
                .as_ref()
                .map(|(c, _, _)| *c)
                .unwrap_or_default();
            self.emit_update_events(&collection, &doc_id.to_string(), cid);

            match commit_result {
                Some((cid, block, col_data)) => {
                    let mut result = CreateResult::with_commit(doc_id, doc, cid, block);
                    if let Some((col_cid, col_bytes)) = col_data {
                        result.broadcast_cid = Some(col_cid);
                        result.broadcast_block = Some(col_bytes);
                    }
                    create_results.push(result);
                }
                None => {
                    create_results.push(CreateResult::new(doc_id, doc));
                }
            }
        }

        Ok(create_results)
    }
}
