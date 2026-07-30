use super::helpers::{ensure_collection_is_active, register_created_doc, write_local_create};
use super::*;

use crate::block_builder::{compute_document_blocks, insert_computed_blocks, ComputedBlocks};
use db_blocks::DocStorageIdentity;

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
            let doc = docs.into_iter().next().expect("length checked above");
            return self
                .create_impl(collection_name, doc)
                .await
                .map(|r| vec![r]);
        }

        self.db
            .check_node_access(None, acp::nac::NodePermission::DocumentUpdate)
            .await
            .map_err(|e| query::error::QueryError::permission_denied(e.to_string()))?;

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
        let enc_config = get_encryption_config();

        // Build IndexManager once for the entire batch (schema is identical for all docs)
        let index_manager =
            IndexManager::from_indexes(short_id, collection.schema(), collection.write_indexes())
                .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to create index manager for collection '{}': {}",
                    collection_name, e
                ))
            })?;

        // === Phase 1: Prepare documents (sequential — embeddings are async/external) ===
        let mut prepared_docs: Vec<Document> = Vec::with_capacity(docs.len());
        let embedding_config = self.db.options().embedding_config();
        for mut doc in docs {
            db_search::set_embedding(
                &collection.schema().vector_embeddings,
                &mut doc,
                true,
                None,
                &embedding_config,
            )
            .await
            .map_err(|e| query::error::QueryError::execution(format!("embedding error: {}", e)))?;

            prepared_docs.push(doc);
        }

        // No per-doc write guards for creates: identities are derived inside
        // the txn; the DocID-mapping duplicate check is the gate.

        // === Phase 2: Transaction — allocate identities, then compute blocks ===
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let identities: Vec<DocStorageIdentity> = {
            let systemstore = txn.systemstore().map_err(|e| {
                query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
            })?;
            let mut identities = Vec::with_capacity(prepared_docs.len());
            for _ in &prepared_docs {
                let doc_short_id = crate::doc_id_map::next_doc_short_id(&systemstore)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
                identities.push(DocStorageIdentity::new(short_id, doc_short_id));
            }
            identities
        };

        // Compute blocks (parallel on native, sequential on WASM). A failure
        // aborts the batch: without blocks there is no derived DocID.
        #[cfg(feature = "native")]
        let computed_blocks: Vec<ComputedBlocks> = {
            let block_futures: Vec<_> = prepared_docs
                .iter()
                .zip(identities.iter())
                .map(|(doc, identity)| {
                    let doc_clone = doc.clone();
                    let schema = schema_version_id.clone();
                    let identity = *identity;
                    let enc = enc_config.clone();
                    let sign = sign_config.clone();
                    tokio::task::spawn_blocking(move || {
                        compute_document_blocks(
                            &doc_clone,
                            &schema,
                            identity,
                            enc.as_ref(),
                            sign.as_ref(),
                        )
                    })
                })
                .collect();

            let computed_results = futures::future::join_all(block_futures).await;

            let mut computed = Vec::with_capacity(computed_results.len());
            for join_result in computed_results {
                let blocks = match join_result {
                    Ok(Ok(blocks)) => blocks,
                    Ok(Err(e)) => {
                        return Err(query::error::QueryError::execution(format!(
                            "failed to compute document blocks: {}",
                            e
                        )))
                    }
                    Err(e) => {
                        return Err(query::error::QueryError::execution(format!(
                            "block computation task panicked: {}",
                            e
                        )))
                    }
                };
                computed.push(blocks);
            }
            computed
        };

        #[cfg(not(feature = "native"))]
        let computed_blocks: Vec<ComputedBlocks> = {
            let mut computed = Vec::with_capacity(prepared_docs.len());
            for (doc, identity) in prepared_docs.iter().zip(identities.iter()) {
                let blocks = compute_document_blocks(
                    doc,
                    &schema_version_id,
                    *identity,
                    enc_config.as_ref(),
                    sign_config.as_ref(),
                )
                .map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to compute document blocks: {}",
                        e
                    ))
                })?;
                computed.push(blocks);
            }
            computed
        };

        // === Phase 3: Sequential writes ===
        let mut results: Vec<(DocID, Document, Cid, Vec<u8>, Option<(Cid, Vec<u8>)>)> =
            Vec::with_capacity(prepared_docs.len());

        for ((mut doc, identity), computed) in prepared_docs
            .into_iter()
            .zip(identities)
            .zip(computed_blocks)
        {
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

            self.db
                .validate_downsample_write(
                    &datastore,
                    &systemstore,
                    collection.schema(),
                    &doc,
                    None,
                )
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

            insert_computed_blocks(&blockstore, &headstore, &computed)
                .await
                .map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to insert pre-computed blocks: {}",
                        e
                    ))
                })?;

            let doc_id = register_created_doc(
                &systemstore,
                &datastore,
                &collection,
                identity.doc_short_id,
                &computed.block_result,
            )
            .await?;
            doc.set_id(doc_id.clone());

            if let Some(ref config) = enc_config {
                store_doc_encryption(&doc_id.to_string(), config.clone());
            }

            write_local_create(
                &datastore,
                &collection,
                &doc,
                identity.doc_short_id,
                &index_manager,
            )
            .await?;

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

            results.push((
                doc_id,
                doc,
                computed.block_result.cid,
                computed.block_result.block,
                col_block_data,
            ));
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

        for (doc_id, ..) in &results {
            self.register_created_doc_with_acp(&collection, &doc_id.to_string())
                .await?;
        }

        // Emit events and build results
        let mut create_results = Vec::with_capacity(results.len());
        for (doc_id, doc, cid, block, col_data) in results {
            self.emit_update_events(
                &collection,
                &doc_id.to_string(),
                cid,
                block.clone(),
                col_data.clone(),
            );

            let mut result = CreateResult::with_commit(doc_id, doc, cid, block);
            if let Some((col_cid, col_bytes)) = col_data {
                result.broadcast_cid = Some(col_cid);
                result.broadcast_block = Some(col_bytes);
            }
            create_results.push(result);
        }

        Ok(create_results)
    }
}
