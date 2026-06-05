use super::*;

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    /// Process a Counter delta from a block (standalone, with its own transaction).
    pub(crate) async fn process_counter_delta(
        &self,
        cid: &Cid,
        payload: &defra_core::block::CounterDeltaPayload,
        metadata: &BlockMetadata<'_>,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        let doc_id_str = String::from_utf8_lossy(&payload.doc_id).to_string();
        let _guard = self.merge_queue.acquire(&doc_id_str).await;

        tracing::debug!(
            cid = %cid,
            field_name = %payload.field_name,
            doc_id = %doc_id_str,
            priority = payload.priority,
            nonce = payload.nonce,
            "Processing Counter delta"
        );

        if self.blockstore.is_merged(cid).await.unwrap_or(false) {
            tracing::debug!(
                cid = %cid,
                field_name = %payload.field_name,
                "Counter delta already marked merged, skipping standalone replay"
            );
            return Ok(MergeOutcome::terminal_skip("already merged"));
        }

        // Look up the collection to determine field kind and counter type,
        // with fallback to metadata's collection_id for cross-version sync
        let collection = self
            .db
            .find_collection_by_id(&payload.schema_version_id)?
            .or(metadata
                .collection_id
                .and_then(|cid| self.db.find_collection_by_id(cid).ok().flatten()))
            .ok_or_else(|| {
                MergeError::MissingMetadata(format!(
                    "Collection not found for schema_version_id: {}",
                    payload.schema_version_id
                ))
            })?;

        // Get field definition to determine numeric kind and allow_decrement
        let field = collection
            .schema()
            .field_by_name(&payload.field_name)
            .ok_or_else(|| {
                MergeError::MissingMetadata(format!(
                    "Field '{}' not found in collection",
                    payload.field_name
                ))
            })?;

        // Determine numeric kind from field type
        let numeric_kind = self.get_numeric_kind_from_field(field)?;

        // Determine if decrement is allowed (PnCounter allows, PCounter doesn't)
        let allow_decrement = field.crdt_type.allows_decrement();

        // Create a new transaction for this merge
        let txn = self.db.new_txn(false).await?;

        // Create the Counter CRDT
        let counter = Counter::new(
            payload.schema_version_id.clone(),
            &payload.doc_id,
            payload.field_name.clone(),
            allow_decrement,
            numeric_kind,
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Create the CounterDelta from payload
        let delta = self.create_counter_delta(payload, numeric_kind)?;

        // Perform the merge in a scoped block
        let result = {
            let mut datastore = txn.datastore()?;
            let mut doc_exists = false;

            // Reconcile the CRDT accumulation store up to the document's current
            // materialized value before merging. Local increments update only the
            // document blob, not the accumulation store, so a node that received
            // the document by replication first (accumulation store already
            // initialized) would otherwise drop its own local increments when this
            // remote delta re-materializes the blob. See `Counter::reconcile_int64`.
            if let Ok(doc_id) = DocID::from_string(&doc_id_str) {
                if let Ok(Some(existing_doc)) =
                    collection.get_with_datastore(&datastore, &doc_id).await
                {
                    doc_exists = true;
                    if let Some(field_value) = existing_doc.get(&payload.field_name) {
                        match (numeric_kind, field_value) {
                            (NumericKind::Int64, NormalValue::Int(v)) => {
                                let _ = counter.reconcile_int64(&mut datastore, *v).await;
                            }
                            (NumericKind::Float64, NormalValue::Float64(v)) => {
                                let _ = counter.reconcile_float64(&mut datastore, *v).await;
                            }
                            _ => {}
                        }
                    }
                }
            }

            let ctx = Context {
                doc_id: DocId::new(&doc_id_str)
                    .map_err(|e| MergeError::MergeFailed(e.to_string()))?,
                schema_version: payload.schema_version_id.clone(),
                is_create: payload.priority == 1 && !doc_exists,
            };

            counter.merge(&mut datastore, &ctx, &delta).await
        };

        match result {
            Ok(_merge_result) => {
                txn.force_commit().await?;
                self.best_effort_finalize_field_block_merge(cid).await;
                tracing::info!(
                    cid = %cid,
                    field_name = %payload.field_name,
                    doc_id = %doc_id_str,
                    "Counter delta merged successfully"
                );
                Ok(MergeOutcome::Merged)
            }
            Err(e) => {
                if let Err(discard_err) = txn.force_discard() {
                    tracing::error!(
                        cid = %cid,
                        discard_error = %discard_err,
                        merge_error = %e,
                        "Failed to discard transaction after merge error"
                    );
                }
                Err(MergeError::MergeFailed(e.to_string()))
            }
        }
    }

    /// Process a Counter delta within an existing transaction, returning the merge result
    /// and the accumulated value for document reconstruction.
    pub(crate) async fn process_counter_delta_in_txn(
        &self,
        datastore: &mut NamespaceView,
        cid: &Cid,
        payload: &defra_core::block::CounterDeltaPayload,
        fallback_collection_id: Option<&str>,
    ) -> std::result::Result<CounterMergeResult, MergeError> {
        tracing::debug!(
            cid = %cid,
            field_name = %payload.field_name,
            priority = payload.priority,
            nonce = payload.nonce,
            "Processing Counter delta in transaction"
        );

        // Look up the collection to determine field kind and counter type,
        // with fallback to metadata's collection_id for cross-version sync
        let collection = self
            .db
            .find_collection_by_id(&payload.schema_version_id)?
            .or(fallback_collection_id
                .and_then(|cid| self.db.find_collection_by_id(cid).ok().flatten()))
            .ok_or_else(|| {
                MergeError::MissingMetadata(format!(
                    "Collection not found for schema_version_id: {}",
                    payload.schema_version_id
                ))
            })?;

        // Get field definition
        let field = collection
            .schema()
            .field_by_name(&payload.field_name)
            .ok_or_else(|| {
                MergeError::MissingMetadata(format!(
                    "Field '{}' not found in collection",
                    payload.field_name
                ))
            })?;

        // Determine numeric kind and allow_decrement
        let numeric_kind = self.get_numeric_kind_from_field(field)?;
        let allow_decrement = field.crdt_type.allows_decrement();

        // Create the Counter CRDT
        let counter = Counter::new(
            payload.schema_version_id.clone(),
            &payload.doc_id,
            payload.field_name.clone(),
            allow_decrement,
            numeric_kind,
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Reconcile the CRDT accumulation store up to the document's current
        // materialized value before merging. Local increments (create AND update)
        // store the counter value in the document blob but not in the accumulation
        // store, so a node that received the document by replication first would
        // otherwise drop its own local increments when this remote delta
        // re-materializes the blob. See `Counter::reconcile_int64`.
        let doc_id_str = String::from_utf8_lossy(&payload.doc_id).to_string();
        let mut doc_exists = false;
        if let Ok(doc_id) = DocID::from_string(&doc_id_str) {
            if let Ok(Some(existing_doc)) = collection.get_with_datastore(datastore, &doc_id).await
            {
                doc_exists = true;
                if let Some(field_value) = existing_doc.get(&payload.field_name) {
                    match (numeric_kind, field_value) {
                        (NumericKind::Int64, NormalValue::Int(v)) => {
                            let _ = counter.reconcile_int64(datastore, *v).await;
                        }
                        (NumericKind::Float64, NormalValue::Float64(v)) => {
                            let _ = counter.reconcile_float64(datastore, *v).await;
                        }
                        _ => {}
                    }
                }
            }
        }

        if self.blockstore.is_merged(cid).await.unwrap_or(false) {
            let value = self
                .read_counter_value(&counter, datastore, numeric_kind, &payload.field_name)
                .await;
            return Ok(CounterMergeResult {
                applied: false,
                value,
            });
        }

        // Create the CounterDelta from payload
        let delta = self.create_counter_delta(payload, numeric_kind)?;
        let ctx = Context {
            doc_id: DocId::new(&doc_id_str).map_err(|e| MergeError::MergeFailed(e.to_string()))?,
            schema_version: payload.schema_version_id.clone(),
            is_create: payload.priority == 1 && !doc_exists,
        };

        // Perform the merge
        let merge_result = counter
            .merge(datastore, &ctx, &delta)
            .await
            .map_err(|e| MergeError::MergeFailed(e.to_string()))?;
        // Read the accumulated value (counters always accumulate, so we always read current)
        let value = self
            .read_counter_value(&counter, datastore, numeric_kind, &payload.field_name)
            .await;

        Ok(CounterMergeResult {
            applied: merge_result.was_applied(),
            value,
        })
    }

    pub(crate) async fn best_effort_finalize_field_block_merge(&self, cid: &Cid) {
        if let Err(error) = mark_field_blocks_merged(&self.blockstore, &[*cid]).await {
            tracing::warn!(
                cid = %cid,
                error = %error,
                "Failed to finalize merged field block"
            );
        }
    }

    pub(crate) async fn best_effort_finalize_linked_field_blocks(&self, cids: &[Cid]) {
        if cids.is_empty() {
            return;
        }

        if let Err(error) = mark_field_blocks_merged(&self.blockstore, cids).await {
            tracing::warn!(
                count = cids.len(),
                error = %error,
                "Failed to finalize merged linked field blocks"
            );
        }
    }
    /// Determine numeric kind from field definition
    fn get_numeric_kind_from_field(
        &self,
        field: &schema::FieldDescription,
    ) -> std::result::Result<NumericKind, MergeError> {
        use schema::FieldKind;

        match &field.kind {
            FieldKind::Scalar(scalar_kind) => {
                use schema::ScalarKind;
                match scalar_kind {
                    ScalarKind::Int => Ok(NumericKind::Int64),
                    ScalarKind::Float64 | ScalarKind::Float32 => Ok(NumericKind::Float64),
                    other => Err(MergeError::UnsupportedDelta(format!(
                        "Counter field '{}' has unsupported scalar kind: {:?}",
                        field.name, other
                    ))),
                }
            }
            other => Err(MergeError::UnsupportedDelta(format!(
                "Counter field '{}' has unsupported kind: {:?}",
                field.name, other
            ))),
        }
    }

    /// Create a CounterDelta from the block payload
    fn create_counter_delta(
        &self,
        payload: &defra_core::block::CounterDeltaPayload,
        kind: NumericKind,
    ) -> std::result::Result<CounterDelta, MergeError> {
        // Go encodes counter data as CBOR. We need to decode it first.
        // The payload.data contains CBOR-encoded i64 or f64
        match kind {
            NumericKind::Int64 => {
                let increment: i64 = ciborium::from_reader(&payload.data[..]).map_err(|e| {
                    MergeError::BlockDecode(format!(
                        "Failed to decode Counter Int64 increment: {}",
                        e
                    ))
                })?;
                CounterDelta::new_int64(
                    payload.doc_id.clone(),
                    payload.field_name.clone(),
                    payload.priority,
                    payload.nonce,
                    payload.schema_version_id.clone(),
                    increment,
                )
                .map_err(|e| MergeError::MergeFailed(e.to_string()))
            }
            NumericKind::Float64 => {
                let increment: f64 = ciborium::from_reader(&payload.data[..]).map_err(|e| {
                    MergeError::BlockDecode(format!(
                        "Failed to decode Counter Float64 increment: {}",
                        e
                    ))
                })?;
                CounterDelta::new_float64(
                    payload.doc_id.clone(),
                    payload.field_name.clone(),
                    payload.priority,
                    payload.nonce,
                    payload.schema_version_id.clone(),
                    increment,
                )
                .map_err(|e| MergeError::MergeFailed(e.to_string()))
            }
            other => Err(MergeError::UnsupportedDelta(format!(
                "unsupported NumericKind {:?} for counter delta",
                other
            ))),
        }
    }

    async fn read_counter_value(
        &self,
        counter: &Counter,
        datastore: &mut NamespaceView,
        numeric_kind: NumericKind,
        field_name: &str,
    ) -> Option<NormalValue> {
        match ValueReader::value(counter, datastore).await {
            Ok(bytes) => self.decode_counter_value(bytes, numeric_kind, field_name),
            Err(e) => {
                tracing::debug!(
                    field_name,
                    error = %e,
                    "Could not read counter value"
                );
                None
            }
        }
    }

    fn decode_counter_value(
        &self,
        bytes: Vec<u8>,
        numeric_kind: NumericKind,
        field_name: &str,
    ) -> Option<NormalValue> {
        if bytes.is_empty() {
            return None;
        }

        match numeric_kind {
            NumericKind::Int64 => {
                if bytes.len() == 8 {
                    let arr: [u8; 8] = bytes[..8].try_into().unwrap();
                    Some(NormalValue::Int(i64::from_be_bytes(arr)))
                } else {
                    tracing::warn!(
                        field_name = %field_name,
                        "Invalid counter value length for Int64"
                    );
                    None
                }
            }
            NumericKind::Float64 => {
                if bytes.len() == 8 {
                    let arr: [u8; 8] = bytes[..8].try_into().unwrap();
                    Some(NormalValue::Float64(f64::from_be_bytes(arr)))
                } else {
                    tracing::warn!(
                        field_name = %field_name,
                        "Invalid counter value length for Float64"
                    );
                    None
                }
            }
            other => {
                tracing::warn!(kind = ?other, "unsupported NumericKind in counter value decode");
                None
            }
        }
    }
}

/// Mark a batch of field-block CIDs as merged in the blockstore.
///
/// The blockstore's merged-set is the single source of truth for CRDT
/// idempotency in Rust (matching Go). Every ingest path upstream — PushLog,
/// DAG traversal, crash-recovery replay — gates on `is_merged(cid)` /
/// `get_unmerged()` and skips blocks that are already merged. See #847 for
/// the history of this contract.
async fn mark_field_blocks_merged<B: blockstore::Blockstore + Send + Sync>(
    blockstore: &Arc<B>,
    cids: &[Cid],
) -> Result<(), MergeError> {
    if cids.is_empty() {
        return Ok(());
    }

    blockstore
        .mark_batch_as_merged(cids)
        .await
        .map_err(|e| MergeError::Storage(e.to_string()))?;

    Ok(())
}
