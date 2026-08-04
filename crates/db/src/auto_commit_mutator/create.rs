use super::helpers::{ensure_collection_is_active, register_created_doc, write_local_create};
use super::*;

use db_blocks::DocStorageIdentity;

#[allow(clippy::type_complexity)]
impl<S: Store + 'static> AutoCommitMutator<S> {
    pub(super) async fn create_impl(
        &self,
        collection_name: &str,
        mut doc: Document,
    ) -> query::error::Result<CreateResult> {
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

        // Generate embeddings before blocks (embedding values affect the genesis CID)
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

        // No per-doc write guard for creates: the DocID is derived from the
        // genesis block inside the txn, so no identity exists to guard yet.
        // The DocID-mapping duplicate check is the gate.
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

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

        let result: query::error::Result<(DocID, Cid, Vec<u8>, Option<(Cid, Vec<u8>)>)> = async {
            // Create an IndexManager for unique constraint enforcement
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

            let doc_short_id = crate::doc_id_map::next_doc_short_id(&systemstore)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            let identity = DocStorageIdentity::new(short_id, doc_short_id);

            // Use version_id for collectionVersionID (matches Go's VersionID())
            let schema_version_id = collection.version_id();
            let enc_config = get_encryption_config();
            let sign_config = get_signing_config();
            tracing::debug!(
                has_signing_config = sign_config.is_some(),
                has_encryption_config = enc_config.is_some(),
                "Auto-commit create mutation configs"
            );

            // Blocks are written FIRST: the public DocID is derived from the
            // genesis composite block CID (Go #4838).
            let block_result = write_document_blocks(
                &blockstore,
                &headstore,
                &doc,
                schema_version_id,
                identity,
                None,
                enc_config.as_ref(),
                sign_config.as_ref(),
                None,
            )
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to write document blocks for create on collection {}: {}",
                    collection_name, e
                ))
            })?;

            let doc_id = register_created_doc(
                &systemstore,
                &datastore,
                &collection,
                doc_short_id,
                &block_result,
            )
            .await?;
            doc.set_id(doc_id.clone());

            // Store encryption config per-document so updates re-apply it
            if let Some(ref config) = enc_config {
                store_doc_encryption(&doc_id.to_string(), config.clone());
            }

            // Bundle the doc blob + index write with the counter-store seeding so
            // the authoritative CRDT accumulation store is always seeded on create
            // (#1021 single-store invariant) — enforced by construction in
            // `write_local_create`.
            write_local_create(&datastore, &collection, &doc, doc_short_id, &index_manager).await?;

            // For branchable collections, create a collection-level block
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

            Ok((doc_id, block_result.cid, block_result.block, col_block_data))
        }
        .await;

        drop(datastore);
        drop(systemstore);
        drop(blockstore);
        drop(headstore);

        match result {
            Ok((doc_id, cid, block, col_data)) => {
                // Commit the transaction (all store references now dropped)
                if let Err(e) = txn.commit().await {
                    warn!(
                        collection = %collection_name,
                        error = %e,
                        "Failed to commit transaction after create"
                    );
                    return Err(crate::error::commit_query_error(e));
                }

                self.register_created_doc_with_acp(&collection, &doc_id.to_string())
                    .await?;

                // Emit update event for subscriptions
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
                Ok(result)
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
