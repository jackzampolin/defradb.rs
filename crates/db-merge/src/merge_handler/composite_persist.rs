use super::composite::{CompositeMergeContext, CompositeMergeState};
use super::*;

/// Marker byte indicating a document is deleted (matches Go's DeletedObjectMarker).
const DELETED_MARKER: u8 = 0x01;

/// Build the deletion marker key: /del/{collection_id}/{doc_id}
fn build_deleted_key(collection_id: &str, doc_id: &str) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(b"/del/");
    key.extend_from_slice(collection_id.as_bytes());
    key.push(b'/');
    key.extend_from_slice(doc_id.as_bytes());
    key
}

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    pub(crate) async fn persist_merged_document(
        &self,
        datastore: &mut NamespaceView,
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

        let (mut doc, old_doc) = match collection.get_with_datastore(datastore, &doc_id).await {
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

        if let Some(old_doc) = old_doc.as_ref() {
            for field in collection
                .schema()
                .fields
                .iter()
                .filter(|field| field.immutable)
            {
                if old_doc.get(&field.name) != doc.get(&field.name) {
                    return Err(MergeError::MergeFailed(format!(
                        "immutable field '{}' cannot be changed",
                        field.name
                    )));
                }
            }
        }

        collection
            .save_with_datastore(datastore, &doc)
            .await
            .map_err(MergeError::Database)?;

        let short_id = collection.resolved_root_id();
        if let Ok(index_manager) = IndexManager::from_collection(short_id, collection.schema()) {
            let index_result = match &old_doc {
                Some(old_doc) => {
                    index_manager
                        .on_document_update(datastore, old_doc, &doc, collection.schema())
                        .await
                }
                None => {
                    index_manager
                        .on_document_create(datastore, &doc, collection.schema())
                        .await
                }
            };
            if let Err(e) = index_result {
                let message = if context.mode.is_standalone() {
                    "Failed to update indexes after merge"
                } else {
                    "Failed to update indexes after batch merge"
                };
                return Err(MergeError::MergeFailed(format!("{message}: {e}")));
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
            if let Ok(Some(old_doc)) = collection.get_with_datastore(datastore, &doc_id).await {
                let short_id = collection.resolved_root_id();
                if let Ok(index_manager) =
                    IndexManager::from_collection(short_id, collection.schema())
                {
                    if let Err(e) = index_manager
                        .on_document_delete(datastore, &old_doc, collection.schema())
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

        let deleted_key = build_deleted_key(collection.collection_id(), context.doc_id_str);
        datastore
            .set(&deleted_key, &[DELETED_MARKER])
            .await
            .map_err(|e| MergeError::Database(db::error::Error::Storage(e)))?;

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
