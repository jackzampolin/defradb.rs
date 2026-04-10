use super::helpers::{
    blockstore_for_txn, commit_txn, discard_txn, headstore_for_txn, map_update_error,
    update_result_from_commit, write_document_commit_result,
};
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
        let embedding_config = self.db.options().embedding_config();

        let generated = crate::embedding::set_embedding(
            &collection.schema().vector_embeddings,
            &mut doc,
            false,
            Some(&modified_fields),
            &embedding_config,
        )
        .await
        .map_err(|e| query::error::QueryError::execution(format!("embedding error: {}", e)))?;

        for field in generated {
            modified_fields.insert(field);
        }

        // Create a write transaction
        let txn = self.new_write_txn().await?;

        // Execute the mutation in a block to drop datastore before commit
        let result = {
            let datastore = self.datastore_for_collection(&txn, collection_name)?;

            // Create an IndexManager for index maintenance
            let index_manager = self.index_manager_for_collection(&collection, collection_name)?;

            self.db
                .validate_downsample_write(
                    &datastore,
                    collection.schema(),
                    &doc,
                    Some(&modified_fields),
                )
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

            // Use update_with_indexes to maintain index consistency
            collection
                .update_with_indexes(&datastore, &doc, &index_manager)
                .await
                .map_err(map_update_error)
        };

        match result {
            Ok(()) => {
                // Build blocks and write to blockstore/headstore in a scoped block
                // This enables _commits queries to find the document's version history
                // (composite_cid, composite_bytes, optional (collection_cid, collection_bytes))
                let commit_result: Option<(Cid, Vec<u8>, Option<(Cid, Vec<u8>)>)> = {
                    let blockstore = blockstore_for_txn(&txn)?;
                    let headstore = headstore_for_txn(&txn)?;

                    // Get encryption config: first try thread-local (explicit in mutation),
                    // then fall back to per-document stored config (from create with encryption).
                    // This matches Go's behavior where encryption propagates through the DAG.
                    let enc_config = get_encryption_config()
                        .or_else(|| doc.id().and_then(|id| get_doc_encryption(&id.to_string())));
                    // Get signing config from thread-local (set by FFI exec_request)
                    let sign_config = get_signing_config();

                    // For update operations, pass the modified fields to only create blocks
                    // for the fields that actually changed
                    write_document_commit_result(
                        collection_name,
                        "update",
                        &collection,
                        &blockstore,
                        &headstore,
                        &doc,
                        Some(&modified_fields),
                        enc_config.as_ref(),
                        sign_config.as_ref(),
                        false,
                    )
                    .await
                }; // blockstore and headstore dropped here

                // Commit the transaction (all store references now dropped)
                commit_txn(txn, collection_name, "update").await?;

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
                Ok(update_result_from_commit(
                    doc,
                    fields_modified,
                    commit_result,
                ))
            }
            Err(e) => {
                // Discard the transaction on error
                discard_txn(txn, collection_name, "update");
                Err(e)
            }
        }
    }
}
