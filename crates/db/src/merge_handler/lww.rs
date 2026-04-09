use super::*;
use crate::block_builder::decode_priority_varint;

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    async fn current_field_priority(
        &self,
        headstore: &NamespaceView,
        doc_id: &str,
        field_name: &str,
    ) -> std::result::Result<u64, MergeError> {
        let mut iter = headstore
            .iterator(storage::corekv::IterOptions::new().with_prefix(
                storage::keys::headstore::HeadstoreDocKey::field_prefix(doc_id, field_name),
            ))
            .await
            .map_err(|e| MergeError::Storage(e.to_string()))?;

        let mut max_priority = 0_u64;
        while let Some(pair) = iter
            .next()
            .await
            .map_err(|e| MergeError::Storage(e.to_string()))?
        {
            max_priority = max_priority.max(decode_priority_varint(&pair.value));
        }

        iter.close()
            .await
            .map_err(|e| MergeError::Storage(e.to_string()))?;

        Ok(max_priority)
    }

    async fn seed_lww_from_existing_doc(
        &self,
        datastore: &mut NamespaceView,
        headstore: &NamespaceView,
        payload: &defra_core::block::LwwDeltaPayload,
        fallback_collection_id: Option<&str>,
        lww: &Lww,
        doc_id_str: &str,
    ) -> std::result::Result<bool, MergeError> {
        let collection = self
            .db
            .find_collection_by_id(&payload.schema_version_id)?
            .or(fallback_collection_id
                .and_then(|cid| self.db.find_collection_by_id(cid).ok().flatten()));

        let Some(collection) = collection else {
            return Ok(false);
        };

        if crdt::traits::PriorityReader::priority(lww, datastore)
            .await
            .map_err(|e| MergeError::MergeFailed(e.to_string()))?
            != 0
        {
            return Ok(false);
        }

        let doc_id = match DocID::from_string(doc_id_str) {
            Ok(doc_id) => doc_id,
            Err(_) => return Ok(false),
        };

        let Some(existing_doc) = collection.get_with_datastore(datastore, &doc_id).await? else {
            return Ok(false);
        };

        let Some(field_value) = existing_doc.get(&payload.field_name) else {
            return Ok(true);
        };

        let priority = self
            .current_field_priority(headstore, doc_id_str, &payload.field_name)
            .await?;
        if priority == 0 {
            return Ok(true);
        }

        let mut value_bytes = Vec::new();
        ciborium::into_writer(field_value, &mut value_bytes)
            .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        let seed_delta = LwwDelta::new(
            payload.doc_id.clone(),
            payload.field_name.clone(),
            priority,
            payload.schema_version_id.clone(),
            value_bytes,
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        let seed_ctx = Context {
            doc_id: DocId::new(doc_id_str),
            schema_version: payload.schema_version_id.clone(),
            is_create: true,
        };

        lww.merge(datastore, &seed_ctx, &seed_delta)
            .await
            .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        Ok(true)
    }

    /// Process an LWW delta from a block (standalone, with its own transaction).
    pub(crate) async fn process_lww_delta(
        &self,
        cid: &Cid,
        payload: &defra_core::block::LwwDeltaPayload,
        metadata: &BlockMetadata<'_>,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        tracing::debug!(
            cid = %cid,
            field_name = %payload.field_name,
            priority = payload.priority,
            "Processing LWW delta"
        );

        // Create a new transaction for this merge
        let txn = self.db.new_txn(false).await?;

        // Create the LWW CRDT for this field
        let lww = Lww::new(
            payload.schema_version_id.clone(),
            &payload.doc_id,
            payload.field_name.clone(),
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Create the delta
        let delta = LwwDelta::new(
            payload.doc_id.clone(),
            payload.field_name.clone(),
            payload.priority,
            payload.schema_version_id.clone(),
            payload.data.clone(),
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        let doc_id_str = String::from_utf8_lossy(&payload.doc_id).to_string();

        // Perform the merge in a scoped block to ensure datastore reference is dropped
        // before we try to commit/discard the transaction.
        let result = {
            let mut datastore = txn.datastore()?;
            let headstore = txn.headstore()?;
            let doc_exists = self
                .seed_lww_from_existing_doc(
                    &mut datastore,
                    &headstore,
                    payload,
                    metadata.collection_id,
                    &lww,
                    &doc_id_str,
                )
                .await?;
            let ctx = Context {
                doc_id: DocId::new(&doc_id_str),
                schema_version: payload.schema_version_id.clone(),
                is_create: payload.priority == 1 && !doc_exists,
            };
            lww.merge(&mut datastore, &ctx, &delta).await
        };

        match result {
            Ok(merge_result) => {
                if merge_result.was_applied() {
                    // Commit the transaction
                    txn.force_commit().await?;
                    tracing::info!(
                        cid = %cid,
                        field_name = %payload.field_name,
                        doc_id = %doc_id_str,
                        "LWW delta merged successfully"
                    );
                    Ok(MergeOutcome::Merged)
                } else if merge_result.was_rejected() {
                    // Discard the transaction - nothing to commit
                    if let Err(e) = txn.force_discard() {
                        tracing::error!(
                            cid = %cid,
                            error = %e,
                            "Failed to discard transaction after CRDT rejection - potential resource leak"
                        );
                    }
                    tracing::debug!(
                        cid = %cid,
                        field_name = %payload.field_name,
                        "LWW delta rejected by CRDT (lower priority or tie-break)"
                    );
                    Ok(MergeOutcome::terminal_skip(
                        "rejected by CRDT conflict resolution",
                    ))
                } else {
                    // Skipped (already applied)
                    if let Err(e) = txn.force_discard() {
                        tracing::error!(
                            cid = %cid,
                            error = %e,
                            "Failed to discard transaction after skip - potential resource leak"
                        );
                    }
                    tracing::debug!(
                        cid = %cid,
                        field_name = %payload.field_name,
                        "LWW delta skipped (already applied)"
                    );
                    Ok(MergeOutcome::terminal_skip("already applied"))
                }
            }
            Err(e) => {
                if let Err(discard_err) = txn.force_discard() {
                    tracing::error!(
                        cid = %cid,
                        discard_error = %discard_err,
                        merge_error = %e,
                        "Failed to discard transaction after merge error - potential resource leak"
                    );
                }
                Err(MergeError::MergeFailed(e.to_string()))
            }
        }
    }

    /// Process an LWW delta within an existing transaction, returning the merge result
    /// and the winning value for document reconstruction.
    pub(crate) async fn process_lww_delta_in_txn(
        &self,
        datastore: &mut NamespaceView,
        headstore: &NamespaceView,
        cid: &Cid,
        payload: &defra_core::block::LwwDeltaPayload,
        fallback_collection_id: Option<&str>,
    ) -> std::result::Result<LwwMergeResult, MergeError> {
        tracing::debug!(
            cid = %cid,
            field_name = %payload.field_name,
            priority = payload.priority,
            "Processing LWW delta in transaction"
        );

        // Create the LWW CRDT for this field
        let lww = Lww::new(
            payload.schema_version_id.clone(),
            &payload.doc_id,
            payload.field_name.clone(),
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Create the delta
        let delta = LwwDelta::new(
            payload.doc_id.clone(),
            payload.field_name.clone(),
            payload.priority,
            payload.schema_version_id.clone(),
            payload.data.clone(),
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        let doc_id_str = String::from_utf8_lossy(&payload.doc_id).to_string();
        let doc_exists = self
            .seed_lww_from_existing_doc(
                datastore,
                headstore,
                payload,
                fallback_collection_id,
                &lww,
                &doc_id_str,
            )
            .await?;
        let ctx = Context {
            doc_id: DocId::new(&doc_id_str),
            schema_version: payload.schema_version_id.clone(),
            is_create: payload.priority == 1 && !doc_exists,
        };

        // Perform the merge
        let merge_result = lww
            .merge(datastore, &ctx, &delta)
            .await
            .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Determine the winning value for document reconstruction
        let (applied, value) = if merge_result.was_applied() {
            // Incoming value won - use it
            tracing::debug!(
                cid = %cid,
                field_name = %payload.field_name,
                "LWW delta applied - using incoming value"
            );
            let value = if !payload.data.is_empty() {
                match ciborium::from_reader::<NormalValue, _>(&payload.data[..]) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        tracing::error!(
                            field_name = %payload.field_name,
                            error = %e,
                            "Failed to decode applied field value from CBOR"
                        );
                        return Err(MergeError::BlockDecode(format!(
                            "Failed to decode field '{}': {}",
                            payload.field_name, e
                        )));
                    }
                }
            } else {
                None // Tombstone
            };
            (true, value)
        } else {
            // Incoming value was rejected - read the winning value from CRDT storage
            tracing::debug!(
                cid = %cid,
                field_name = %payload.field_name,
                result = ?merge_result,
                "LWW delta rejected - reading winning value from storage"
            );

            // Read the current (winning) value from storage
            let value = match crdt::traits::ValueReader::value(&lww, datastore).await {
                Ok(data) => {
                    if data.is_empty() {
                        None // Tombstone/deleted
                    } else {
                        match ciborium::from_reader::<NormalValue, _>(&data[..]) {
                            Ok(v) => Some(v),
                            Err(e) => {
                                tracing::warn!(
                                    field_name = %payload.field_name,
                                    error = %e,
                                    "Failed to decode existing field value from CBOR - skipping field"
                                );
                                None
                            }
                        }
                    }
                }
                Err(e) => {
                    // Field may not exist yet - this is not an error
                    tracing::debug!(
                        field_name = %payload.field_name,
                        error = %e,
                        "Could not read existing field value - field may not exist"
                    );
                    None
                }
            };
            (false, value)
        };

        Ok(LwwMergeResult { applied, value })
    }
}
