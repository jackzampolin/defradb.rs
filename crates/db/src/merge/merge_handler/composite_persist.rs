use super::composite::{CompositeMergeContext, CompositeMergeState};
use super::*;

/// Marker byte indicating a document is deleted (matches Go's DeletedObjectMarker).
const DELETED_MARKER: u8 = 0x01;

/// Build the deletion marker key: /del/{collection_id}/{doc_short_id}
fn build_deleted_key(collection_id: &str, doc_short_id: u64) -> Vec<u8> {
    storage::keys::deleted_doc_key(collection_id, doc_short_id)
}

impl<S: Store, B: blockstore::Blockstore> DbMergeHandler<S, B> {
    pub(crate) async fn persist_merged_document(
        &self,
        datastore: &mut NamespaceView,
        systemstore: &NamespaceView,
        context: &CompositeMergeContext<'_, '_>,
        state: &mut CompositeMergeState,
    ) -> std::result::Result<(), MergeError> {
        let collection = context.collection.as_ref().ok_or_else(|| {
            MergeError::MissingMetadata(format!(
                "Collection not found for schema_version_id: {}",
                context.payload.schema_version_id
            ))
        })?;

        state.is_branchable = collection.schema().is_branchable;

        if context.payload.status == 2 {
            self.handle_deletion(datastore, context, collection).await?;
            return Ok(());
        }

        if state.field_values.is_empty() {
            return Ok(());
        }

        let doc_id = DocID::from_string(context.doc_id_str)
            .map_err(|e| MergeError::MergeFailed(format!("Invalid doc_id: {}", e)))?;

        let (mut doc, old_doc) = match collection
            .get_with_datastore(datastore, context.doc_short_id, &doc_id)
            .await
        {
            Ok(Some(existing)) => {
                let old = existing.clone();
                (existing, Some(old))
            }
            _ => {
                let mut new_doc = Document::new();
                new_doc.set_id(doc_id.clone());
                (new_doc, None)
            }
        };

        doc.set_schema_version_id(&context.payload.schema_version_id);

        for (field_name, value) in &state.field_values {
            doc.set(field_name, value.clone());
        }

        let known_fields: std::collections::HashSet<&str> = collection
            .schema()
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect();
        let all_field_names: Vec<String> = doc.field_names().map(|name| name.to_string()).collect();
        for field_name in &all_field_names {
            if !known_fields.contains(field_name.as_str()) {
                doc.remove(field_name);
            }
        }

        // @immutable enforcement happens in process_linked_field_blocks (phase 1),
        // BEFORE any field block is persisted, so a rejected block leaves no
        // partial write.

        collection
            .save_with_datastore(datastore, &doc, context.doc_short_id)
            .await
            .map_err(MergeError::Database)?;

        // A merged write onto a logically-deleted document must not touch the
        // indexes: the document is dead, and indexing it re-mints exactly the
        // stale unique entry that blocks the value forever (source-inc/gents#700's
        // out-of-order create-after-delete arrival). Go skips index sync when
        // the merged doc reads back absent for the same reason.
        let deleted_marker_key =
            build_deleted_key(collection.collection_id(), context.doc_short_id);
        let doc_is_tombstoned = datastore
            .has(&deleted_marker_key)
            .await
            .map_err(|e| MergeError::Database(crate::error::Error::Storage(e)))?;

        let short_id = collection.resolved_root_id();
        match IndexManager::from_collection(short_id, collection.schema()) {
            Ok(index_manager) if !doc_is_tombstoned => {
                // Merge-path index maintenance resolves live unique conflicts
                // deterministically (smallest public DocID wins) instead of
                // failing: a CRDT merge cannot preserve cross-replica
                // uniqueness, and failing here wedges the document's entire
                // forward history in permanent retry on both replicas (#1111).
                // Both documents persist; the index converges to the same
                // winner everywhere.
                let index_result = match &old_doc {
                    Some(old_doc) => {
                        index_manager
                            .on_document_update_merge(
                                datastore,
                                systemstore,
                                old_doc,
                                &doc,
                                context.doc_short_id,
                                collection.schema(),
                            )
                            .await
                    }
                    None => {
                        index_manager
                            .on_document_create_merge(
                                datastore,
                                systemstore,
                                &doc,
                                context.doc_short_id,
                                collection.schema(),
                            )
                            .await
                    }
                };
                if let Err(e) = index_result {
                    // The merge variants above already resolve the common live
                    // unique-conflict case deterministically without erroring.
                    // What still surfaces `UniqueConstraintViolation` here are
                    // the degenerate arms (non-Unique index type mismatch,
                    // empty collection_id, `conflicting_doc_id` returning
                    // None) — internal index-state inconsistency, not a
                    // transient storage failure. Classify and reject rather
                    // than retrying forever.
                    if is_unique_constraint_violation(&e) {
                        return Err(MergeError::UniqueConstraintViolation(e.to_string()));
                    }
                    let message = if context.mode.is_standalone() {
                        "Failed to update indexes after merge"
                    } else {
                        "Failed to update indexes after batch merge"
                    };
                    return Err(MergeError::MergeFailed(format!("{message}: {e}")));
                }
            }
            Ok(_) => {
                tracing::info!(
                    doc_id = %context.doc_id_str,
                    collection = %collection.name(),
                    "skipping index maintenance for merge onto a tombstoned document"
                );
            }
            Err(e) => {
                // Previously swallowed silently, skipping ALL index maintenance
                // with no trace — enforcement disappeared invisibly (#1111).
                tracing::error!(
                    doc_id = %context.doc_id_str,
                    collection = %collection.name(),
                    error = %e,
                    "index manager could not be built from the collection schema; \
                     skipping index maintenance for this merge"
                );
            }
        }

        if let Some(enc_key) = self.se_enc_key() {
            if let Err(e) = se_merge::generate_merge_artifacts(
                datastore,
                collection.schema(),
                context.doc_id_str,
                &state.field_values,
                enc_key,
                None,
            )
            .await
            {
                let message = if context.mode.is_standalone() {
                    "Failed to generate SE artifacts after merge"
                } else {
                    "Failed to generate SE artifacts after batch merge"
                };
                tracing::warn!(
                    doc_id = %context.doc_id_str,
                    error = %e,
                    "{message}"
                );
            }
        }

        if context.mode.is_standalone() {
            tracing::info!(
                doc_id = %context.doc_id_str,
                collection = %collection.name(),
                fields_count = state.field_values.len(),
                any_applied = state.any_field_applied,
                "Document stored for queries"
            );
        }

        Ok(())
    }

    pub(crate) async fn handle_deletion(
        &self,
        datastore: &NamespaceView,
        context: &CompositeMergeContext<'_, '_>,
        collection: &Collection,
    ) -> std::result::Result<(), MergeError> {
        if let Ok(doc_id) = DocID::from_string(context.doc_id_str) {
            if let Ok(Some(old_doc)) = collection
                .get_with_datastore(datastore, context.doc_short_id, &doc_id)
                .await
            {
                let short_id = collection.resolved_root_id();
                if let Ok(index_manager) =
                    IndexManager::from_collection(short_id, collection.schema())
                {
                    if let Err(e) = index_manager
                        .on_document_delete(
                            datastore,
                            &old_doc,
                            context.doc_short_id,
                            collection.schema(),
                        )
                        .await
                    {
                        let message = if context.mode.is_standalone() {
                            "Failed to delete indexes after merge"
                        } else {
                            "Failed to delete indexes after batch merge"
                        };
                        return Err(MergeError::MergeFailed(format!("{message}: {e}")));
                    }
                }
            }
        }

        let deleted_key = build_deleted_key(collection.collection_id(), context.doc_short_id);
        datastore
            .set(&deleted_key, &[DELETED_MARKER])
            .await
            .map_err(|e| MergeError::Database(crate::error::Error::Storage(e)))?;

        if context.mode.is_standalone() {
            tracing::info!(
                doc_id = %context.doc_id_str,
                collection = %collection.name(),
                "Deletion marker set after P2P merge"
            );
        }

        Ok(())
    }
}

/// True if `e` represents a deterministic unique-index rejection rather than a
/// transient storage failure during merge-time index maintenance. Only this
/// case should be converted into `MergeOutcome::Rejected`; every other
/// `crate::index::Error` must keep surfacing as `Err` so transient failures retry.
fn is_unique_constraint_violation(e: &crate::index::Error) -> bool {
    matches!(e, crate::index::Error::Storage(se) if se.is_unique_constraint_violation())
}

#[cfg(test)]
mod classify_tests {
    use super::is_unique_constraint_violation;

    #[test]
    fn unique_constraint_violation_is_classified() {
        let e = crate::index::Error::Storage(storage::Error::UniqueConstraintViolation);
        assert!(is_unique_constraint_violation(&e));
    }

    #[test]
    fn non_unique_storage_error_is_not_classified() {
        let e = crate::index::Error::Storage(storage::Error::Other("disk full".to_string()));
        assert!(!is_unique_constraint_violation(&e));
    }

    #[test]
    fn non_storage_index_error_is_not_classified() {
        let e = crate::index::Error::Other("index misconfigured".to_string());
        assert!(!is_unique_constraint_violation(&e));
    }
}
