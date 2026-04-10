use super::helpers::{
    blockstore_for_txn, commit_txn, create_result_from_commit, discard_txn, headstore_for_txn,
    map_create_error, write_document_commit_result,
};
use super::*;

#[allow(clippy::type_complexity)]
impl<S: Store + 'static> AutoCommitMutator<S> {
    pub(super) async fn create_impl(
        &self,
        collection_name: &str,
        mut doc: Document,
    ) -> query::error::Result<CreateResult> {
        let collection = self.get_collection_or_err(collection_name)?;

        // Create a write transaction
        let txn = self.new_write_txn().await?;

        // Generate embeddings before doc ID (embedding values affect content hash)
        let embedding_config = self.db.options().embedding_config();
        crate::embedding::set_embedding(
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
            let datastore = self.datastore_for_collection(&txn, collection_name)?;

            // Create an IndexManager for unique constraint enforcement
            let index_manager = self.index_manager_for_collection(&collection, collection_name)?;

            self.db
                .validate_downsample_write(&datastore, collection.schema(), &doc, None)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

            // Use create_with_indexes to enforce unique constraints and maintain indexes.
            // Blind create skips existence check for content-addressed (generated) IDs.
            collection
                .create_with_indexes(&datastore, &doc, &index_manager, id_was_generated)
                .await
                .map_err(map_create_error)
        };

        match result {
            Ok(_returned_doc_id) => {
                // Build blocks and write to blockstore/headstore in a scoped block
                // This enables _commits queries to find the document's version history
                // The stores must be dropped before commit, so scope them
                // (composite_cid, composite_bytes, optional (collection_cid, collection_bytes))
                let commit_result: Option<(Cid, Vec<u8>, Option<(Cid, Vec<u8>)>)> = {
                    let blockstore = blockstore_for_txn(&txn)?;
                    let headstore = headstore_for_txn(&txn)?;

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
                    write_document_commit_result(
                        collection_name,
                        "create",
                        &collection,
                        &blockstore,
                        &headstore,
                        &doc,
                        None,
                        enc_config.as_ref(),
                        sign_config.as_ref(),
                        true,
                    )
                    .await
                }; // blockstore and headstore dropped here

                // Commit the transaction (all store references now dropped)
                commit_txn(txn, collection_name, "create").await?;

                // Emit update event for subscriptions
                let cid = commit_result
                    .as_ref()
                    .map(|(c, _, _)| *c)
                    .unwrap_or_default();
                self.emit_update_events(&collection, &doc_id.to_string(), cid);

                // Return result with commit CID and block if available
                Ok(create_result_from_commit(doc_id, doc, commit_result))
            }
            Err(e) => {
                // Discard the transaction on error
                discard_txn(txn, collection_name, "create");
                Err(e)
            }
        }
    }
}
