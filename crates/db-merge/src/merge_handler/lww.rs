use super::*;
use db_blocks::decode_priority_varint;

impl<S: Store, B: blockstore::Blockstore> DbMergeHandler<S, B> {
    async fn current_field_priority(
        &self,
        headstore: &NamespaceView,
        doc_short_id: u64,
        field_name: &str,
    ) -> std::result::Result<u64, MergeError> {
        let mut iter = headstore
            .iterator(storage::corekv::IterOptions::new().with_prefix(
                storage::keys::headstore::HeadstoreDocKey::field_prefix(doc_short_id, field_name),
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

    #[allow(clippy::too_many_arguments)]
    async fn seed_lww_from_existing_doc(
        &self,
        datastore: &mut NamespaceView,
        headstore: &NamespaceView,
        payload: &defra_core::block::LwwDeltaPayload,
        fallback_collection_id: Option<&str>,
        lww: &Lww,
        doc_id_str: &str,
        doc_short_id: u64,
    ) -> std::result::Result<bool, MergeError> {
        let collection = self
            .db
            .find_collection_by_id(&payload.schema_version_id)?
            .or(fallback_collection_id
                .and_then(|cid| self.db.find_collection_by_id(cid).ok().flatten()));

        let Some(collection) = collection else {
            return Ok(false);
        };

        // Two priority stores can disagree for a field: the headstore tracks the
        // authoritative current priority (advanced by BOTH local writes and
        // merges), while the datastore LWW priority is advanced ONLY by merges.
        // A local write therefore pushes the headstore (and materialized doc)
        // ahead of a stale datastore LWW. Reconcile here: when the headstore is
        // ahead, re-seed the datastore LWW from the materialized doc value at the
        // headstore priority, so the merge resolves against the true current
        // state instead of the stale CRDT entry (otherwise a re-walked ancestor
        // delta clobbers a concurrent local write — replicas diverge).
        let hs_priority = self
            .current_field_priority(headstore, doc_short_id, &payload.field_name)
            .await?;
        let stored_priority = crdt::traits::PriorityReader::priority(lww, datastore)
            .await
            .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Fast path: the datastore LWW is strictly ahead of the headstore, so
        // nothing in the materialized doc can improve it. Equal priority still
        // needs the value tie-break below to preserve Go LWW behavior.
        if stored_priority != 0 && stored_priority > hs_priority {
            return Ok(true);
        }

        let doc_id = match DocID::from_string(doc_id_str) {
            Ok(doc_id) => doc_id,
            Err(_) => return Ok(false),
        };

        let Some(existing_doc) = collection
            .get_with_datastore(datastore, doc_short_id, &doc_id)
            .await?
        else {
            return Ok(false);
        };

        let Some(field_value) = existing_doc.get(&payload.field_name) else {
            return Ok(true);
        };

        let priority = hs_priority;
        if priority == 0 {
            return Ok(true);
        }

        let mut value_bytes = Vec::new();
        ciborium::into_writer(field_value, &mut value_bytes)
            .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        let should_seed = match priority.cmp(&stored_priority) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => {
                match crdt::traits::ValueReader::value(lww, datastore).await {
                    Ok(stored_value) => value_bytes > stored_value,
                    Err(_) => true,
                }
            }
        };

        if !should_seed {
            return Ok(true);
        }

        let seed_delta = LwwDelta::new(
            doc_id_str.as_bytes().to_vec(),
            payload.field_name.clone(),
            priority,
            payload.schema_version_id.clone(),
            value_bytes,
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        let seed_ctx = Context {
            doc_id: DocId::new(doc_id_str).map_err(|e| MergeError::MergeFailed(e.to_string()))?,
            schema_version: payload.schema_version_id.clone(),
            is_create: false,
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
        let Some(doc_id_str) = self.resolve_field_block_doc_id(cid).await? else {
            return Ok(MergeOutcome::terminal_skip(
                "field block has no unambiguous owner; merged via its composite",
            ));
        };
        let _guard = self.merge_queue.acquire(&doc_id_str).await;

        tracing::debug!(
            cid = %cid,
            field_name = %payload.field_name,
            priority = payload.priority,
            "Processing LWW delta"
        );

        // Create a new transaction for this merge
        let txn = self.db.new_txn(false).await?;
        let doc_short_id = {
            let systemstore = txn.systemstore()?;
            match db::docid::map::get_doc_ref(&systemstore, &doc_id_str)
                .await
                .map_err(MergeError::Database)?
            {
                Some(doc_ref) => doc_ref.doc_short_id,
                None => {
                    let _ = txn.force_discard();
                    return Ok(MergeOutcome::terminal_skip(
                        "owning document has no local identity; merged via its composite",
                    ));
                }
            }
        };

        // Create the LWW CRDT for this field
        let lww = Lww::new(
            payload.schema_version_id.clone(),
            doc_id_str.as_bytes(),
            payload.field_name.clone(),
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Create the delta
        let delta = LwwDelta::new(
            doc_id_str.as_bytes().to_vec(),
            payload.field_name.clone(),
            payload.priority,
            payload.schema_version_id.clone(),
            payload.data.clone(),
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

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
                    doc_short_id,
                )
                .await?;
            let ctx = Context {
                doc_id: DocId::new(&doc_id_str)
                    .map_err(|e| MergeError::MergeFailed(e.to_string()))?,
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn process_lww_delta_in_txn(
        &self,
        datastore: &mut NamespaceView,
        headstore: &NamespaceView,
        cid: &Cid,
        payload: &defra_core::block::LwwDeltaPayload,
        fallback_collection_id: Option<&str>,
        doc_id_str: &str,
        doc_short_id: u64,
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
            doc_id_str.as_bytes(),
            payload.field_name.clone(),
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        // Create the delta
        let delta = LwwDelta::new(
            doc_id_str.as_bytes().to_vec(),
            payload.field_name.clone(),
            payload.priority,
            payload.schema_version_id.clone(),
            payload.data.clone(),
        )
        .map_err(|e| MergeError::MergeFailed(e.to_string()))?;

        let doc_exists = self
            .seed_lww_from_existing_doc(
                datastore,
                headstore,
                payload,
                fallback_collection_id,
                &lww,
                doc_id_str,
                doc_short_id,
            )
            .await?;
        let ctx = Context {
            doc_id: DocId::new(doc_id_str).map_err(|e| MergeError::MergeFailed(e.to_string()))?,
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
