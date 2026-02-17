use super::*;

use crate::block_builder::{compute_document_blocks, insert_computed_blocks, ComputedBlocks};

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

        let short_id = collection_short_id(collection.collection_id());
        let schema_version_id = collection.version_id().to_string();
        let sign_config = get_signing_config();
        let enc_config = get_encryption_config();

        // Build IndexManager once for the entire batch (schema is identical for all docs)
        let index_manager =
            IndexManager::from_collection(short_id, collection.schema()).map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to create index manager for collection '{}': {}",
                    collection_name, e
                ))
            })?;

        // === Phase 1: Prepare documents (sequential — embeddings are async/external) ===
        let mut prepared_docs: Vec<(Document, bool)> = Vec::with_capacity(docs.len());
        for mut doc in docs {
            crate::embedding::set_embedding(
                &collection.schema().vector_embeddings,
                &mut doc,
                true,
                None,
            )
            .await
            .map_err(|e| query::error::QueryError::execution(format!("embedding error: {}", e)))?;

            let id_was_generated = doc.id().is_none();
            if id_was_generated {
                doc.generate_and_set_doc_id().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to generate DocID: {}", e))
                })?;
            }

            prepared_docs.push((doc, id_was_generated));
        }

        // === Phase 2: Compute blocks (parallel on native, sequential on WASM) ===
        #[cfg(feature = "native")]
        let computed_blocks: Vec<Option<ComputedBlocks>> = {
            let block_futures: Vec<_> = prepared_docs
                .iter()
                .map(|(doc, _)| {
                    let doc_clone = doc.clone();
                    let schema = schema_version_id.clone();
                    let enc = enc_config.clone();
                    let sign = sign_config.clone();
                    tokio::task::spawn_blocking(move || {
                        compute_document_blocks(&doc_clone, &schema, enc.as_ref(), sign.as_ref())
                    })
                })
                .collect();

            let computed_results = futures::future::join_all(block_futures).await;

            computed_results
                .into_iter()
                .map(|join_result| match join_result {
                    Ok(Ok(blocks)) => Some(blocks),
                    Ok(Err(e)) => {
                        warn!(error = %e, "Failed to compute document blocks");
                        None
                    }
                    Err(e) => {
                        warn!(error = %e, "Block computation task panicked");
                        None
                    }
                })
                .collect()
        };

        #[cfg(not(feature = "native"))]
        let computed_blocks: Vec<Option<ComputedBlocks>> = prepared_docs
            .iter()
            .map(|(doc, _)| {
                match compute_document_blocks(
                    doc,
                    &schema_version_id,
                    enc_config.as_ref(),
                    sign_config.as_ref(),
                ) {
                    Ok(blocks) => Some(blocks),
                    Err(e) => {
                        warn!(error = %e, "Failed to compute document blocks");
                        None
                    }
                }
            })
            .collect();

        // === Phase 3: Transaction — sequential writes ===
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let mut results: Vec<(
            DocID,
            Document,
            Option<(Cid, Vec<u8>, Option<(Cid, Vec<u8>)>)>,
        )> = Vec::with_capacity(prepared_docs.len());

        for ((doc, id_was_generated), blocks) in
            prepared_docs.into_iter().zip(computed_blocks.into_iter())
        {
            let doc_id = doc.id().cloned().ok_or_else(|| {
                query::error::QueryError::execution("document should have ID after generation")
            })?;

            // Datastore + index writes
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

            // Insert pre-computed blocks + collection blocks
            let commit_result: Option<(Cid, Vec<u8>, Option<(Cid, Vec<u8>)>)> = match blocks {
                Some(computed) => {
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

                    match insert_computed_blocks(&blockstore, &headstore, &computed).await {
                        Ok(()) => {
                            if let Some(ref config) = enc_config {
                                store_doc_encryption(&doc_id.to_string(), config.clone());
                            }

                            let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
                            if collection.schema().is_branchable {
                                match write_collection_block(
                                    &blockstore,
                                    &headstore,
                                    short_id,
                                    &schema_version_id,
                                    computed.block_result.cid,
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

                            Some((
                                computed.block_result.cid,
                                computed.block_result.block,
                                col_block_data,
                            ))
                        }
                        Err(e) => {
                            warn!(
                                collection = %collection_name,
                                error = %e,
                                "Failed to insert pre-computed blocks"
                            );
                            None
                        }
                    }
                }
                None => None,
            };

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
