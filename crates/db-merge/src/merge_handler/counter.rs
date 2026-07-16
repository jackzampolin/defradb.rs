use super::*;

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    /// Process a Counter delta from a block (standalone, with its own transaction).
    pub(crate) async fn process_counter_delta(
        &self,
        cid: &Cid,
        payload: &defra_core::block::CounterDeltaPayload,
        metadata: &BlockMetadata<'_>,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        let Some(doc_id_str) = self.resolve_field_block_doc_id(cid).await? else {
            return Ok(MergeOutcome::terminal_skip(
                "field block has no unambiguous owner; merged via its composite",
            ));
        };
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
        let doc_short_id = db::doc_id_map::get_doc_ref(&txn.systemstore()?, &doc_id_str)
            .await
            .map_err(MergeError::Database)?
            .ok_or_else(|| {
                MergeError::MissingMetadata(format!("document identity not found for {doc_id_str}"))
            })?
            .doc_short_id;

        // Create the Counter CRDT
        let counter = Counter::new(
            payload.schema_version_id.clone(),
            doc_id_str.as_bytes(),
            payload.field_name.clone(),
            allow_decrement,
            numeric_kind,
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Create the CounterDelta from payload
        let delta = self.create_counter_delta_for(payload, numeric_kind, &doc_id_str)?;

        // Perform the merge in a scoped block
        let result = {
            let mut datastore = txn.datastore()?;
            let headstore = txn.headstore()?;
            let mut doc_exists = false;

            // Seed the CRDT accumulation store from the document's current
            // materialized value only if the store is not yet initialized
            // (init-if-absent). The store is the single source of truth; local
            // writes and merges both RMW their delta into it, so once it holds a
            // value it is authoritative and must not be overwritten from a
            // possibly-stale blob. See `Counter::reconcile_int64`.
            let doc_id = DocID::from_string(&doc_id_str)
                .map_err(|e| MergeError::MergeFailed(format!("invalid doc ID: {e}")))?;
            if let Some(existing_doc) = collection
                .get_by_doc_id(&datastore, &txn.systemstore()?, &doc_id)
                .await
                .map_err(MergeError::Database)?
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

            let ctx = Context {
                doc_id: DocId::new(&doc_id_str)
                    .map_err(|e| MergeError::MergeFailed(e.to_string()))?,
                schema_version: payload.schema_version_id.clone(),
                is_create: payload.priority == 1 && !doc_exists,
            };

            self.merge_counter_once_for_document(
                &mut datastore,
                &headstore,
                cid,
                payload,
                doc_short_id,
                &counter,
                &ctx,
                &delta,
                numeric_kind,
            )
            .await
        };

        match result {
            Ok(result) => {
                txn.force_commit().await?;
                self.best_effort_finalize_field_block_merge(cid).await;
                tracing::info!(
                    cid = %cid,
                    field_name = %payload.field_name,
                    doc_id = %doc_id_str,
                    "Counter delta merged successfully"
                );
                if result.applied {
                    Ok(MergeOutcome::Merged)
                } else {
                    Ok(MergeOutcome::terminal_skip("already merged"))
                }
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn process_counter_delta_in_txn(
        &self,
        datastore: &mut NamespaceView,
        headstore: &NamespaceView,
        cid: &Cid,
        payload: &defra_core::block::CounterDeltaPayload,
        fallback_collection_id: Option<&str>,
        doc_id_str: &str,
        doc_short_id: u64,
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
            doc_id_str.as_bytes(),
            payload.field_name.clone(),
            allow_decrement,
            numeric_kind,
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Seed the CRDT accumulation store from the document's current
        // materialized value only if the store is not yet initialized
        // (init-if-absent). The store is the single source of truth; local writes
        // and merges both RMW their delta into it, so once it holds a value it is
        // authoritative and must not be overwritten from a possibly-stale blob.
        // See `Counter::reconcile_int64`.
        let mut doc_exists = false;
        let doc_id = DocID::from_string(doc_id_str)
            .map_err(|e| MergeError::MergeFailed(format!("invalid doc ID: {e}")))?;
        if let Some(existing_doc) = collection
            .get_with_datastore(datastore, doc_short_id, &doc_id)
            .await
            .map_err(MergeError::Database)?
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

        // Create the CounterDelta from payload
        let delta = self.create_counter_delta_for(payload, numeric_kind, doc_id_str)?;
        let ctx = Context {
            doc_id: DocId::new(doc_id_str).map_err(|e| MergeError::MergeFailed(e.to_string()))?,
            schema_version: payload.schema_version_id.clone(),
            is_create: payload.priority == 1 && !doc_exists,
        };

        self.merge_counter_once_for_document(
            datastore,
            headstore,
            cid,
            payload,
            doc_short_id,
            &counter,
            &ctx,
            &delta,
            numeric_kind,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn merge_counter_once_for_document(
        &self,
        datastore: &mut NamespaceView,
        headstore: &NamespaceView,
        cid: &Cid,
        payload: &defra_core::block::CounterDeltaPayload,
        doc_short_id: u64,
        counter: &Counter,
        context: &Context,
        delta: &CounterDelta,
        numeric_kind: NumericKind,
    ) -> std::result::Result<CounterMergeResult, MergeError> {
        let applied_key =
            storage::keys::HeadstorePriorityKey::new(doc_short_id, payload.priority, *cid);
        if headstore
            .has(&applied_key.bytes())
            .await
            .map_err(|e| MergeError::Storage(e.to_string()))?
        {
            let value = self
                .read_counter_value(counter, datastore, numeric_kind, &payload.field_name)
                .await;
            return Ok(CounterMergeResult {
                applied: false,
                value,
            });
        }

        let merge_result = counter
            .merge(datastore, context, delta)
            .await
            .map_err(|e| MergeError::MergeFailed(e.to_string()))?;
        headstore
            .set(&applied_key.bytes(), &[])
            .await
            .map_err(|e| MergeError::Storage(e.to_string()))?;
        let value = self
            .read_counter_value(counter, datastore, numeric_kind, &payload.field_name)
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
    fn create_counter_delta_for(
        &self,
        payload: &defra_core::block::CounterDeltaPayload,
        kind: NumericKind,
        doc_id_str: &str,
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
                    doc_id_str.as_bytes().to_vec(),
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
                    doc_id_str.as_bytes().to_vec(),
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
