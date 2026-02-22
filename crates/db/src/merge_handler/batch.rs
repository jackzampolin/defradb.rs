use super::*;

use p2p::sync::MergeBlock;

/// Event collected during batch processing, emitted after commit.
pub(crate) struct PendingMergeEvent {
    pub message: Message,
}

impl<S: Store + 'static, B: blockstore::Blockstore + Send + Sync + 'static> DbMergeHandler<S, B> {
    /// Process blocks individually, each with its own transaction.
    pub(crate) async fn merge_blocks_individually(
        &self,
        blocks: &[MergeBlock],
    ) -> Vec<Result<MergeOutcome, MergeError>> {
        let mut results = Vec::with_capacity(blocks.len());
        for block in blocks {
            let metadata =
                BlockMetadata::normal(&block.doc_id, &block.collection_id, &block.creator);
            results.push(
                self.handle_block(&block.cid, &block.block_data, metadata)
                    .await,
            );
        }
        results
    }

    /// Attempt batch merge with binary-split retry on failure.
    ///
    /// Tries the whole batch first. On failure, splits into two halves and
    /// recurses on each half. Base case: single block falls back to individual
    /// processing. This isolates bad blocks with ~log2(N) batch attempts
    /// instead of falling back to N individual transactions.
    #[allow(clippy::type_complexity)]
    pub(crate) fn try_batch_merge_with_split<'a>(
        &'a self,
        blocks: &'a [MergeBlock],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Vec<Result<MergeOutcome, MergeError>>> + Send + 'a>,
    > {
        Box::pin(async move {
            if blocks.len() <= 1 {
                return self.merge_blocks_individually(blocks).await;
            }

            match self.try_batch_merge(blocks).await {
                Ok(results) => results,
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        batch_size = blocks.len(),
                        "Batch merge failed, splitting in half"
                    );
                    let mid = blocks.len() / 2;
                    let (left, right) = blocks.split_at(mid);
                    let mut results = self.try_batch_merge_with_split(left).await;
                    results.extend(self.try_batch_merge_with_split(right).await);
                    results
                }
            }
        })
    }

    /// Attempt to merge all blocks within a single shared transaction.
    ///
    /// If any block fails, the entire transaction is rolled back and the caller
    /// should fall back to per-block processing.
    pub(crate) async fn try_batch_merge(
        &self,
        blocks: &[MergeBlock],
    ) -> Result<Vec<Result<MergeOutcome, MergeError>>, MergeError> {
        let txn = self.db.new_txn(false).await?;
        let batch_merged: std::sync::Mutex<HashSet<Cid>> = std::sync::Mutex::new(HashSet::new());
        let pending_events: std::sync::Mutex<Vec<PendingMergeEvent>> =
            std::sync::Mutex::new(Vec::new());

        let mut results = Vec::with_capacity(blocks.len());
        let mut batch_error: Option<MergeError> = None;

        // Create NamespaceViews from the shared transaction. These use
        // Arc<SharedTxn> internally and are Send+Sync, avoiding the
        // "future cannot be sent between threads safely" error that
        // would occur if we passed &DbTxn<S> across await points.
        {
            let datastore = txn.datastore()?;
            let headstore = txn.headstore()?;

            for block in blocks {
                let metadata =
                    BlockMetadata::normal(&block.doc_id, &block.collection_id, &block.creator);
                match self
                    .process_block_in_txn(
                        &datastore,
                        &headstore,
                        &block.cid,
                        &block.block_data,
                        &metadata,
                        &batch_merged,
                        &pending_events,
                    )
                    .await
                {
                    Ok(outcome) => results.push(Ok(outcome)),
                    Err(e) => {
                        batch_error = Some(e);
                        break;
                    }
                }
            }
        } // NamespaceViews dropped here so txn can be committed

        if let Some(e) = batch_error {
            if let Err(de) = txn.force_discard() {
                tracing::error!(error = %de, "Failed to discard batch txn");
            }
            return Err(e);
        }

        txn.force_commit().await?;

        // Move batch-merged CIDs into the permanent dedup set
        {
            let batch = batch_merged.lock().unwrap();
            let mut merged = self.merged_composites.lock().unwrap();
            merged.extend(batch.iter());
        }

        // Emit all collected events
        if let Some(bus) = self.db.event_bus() {
            let events = pending_events.into_inner().unwrap();
            for event in events {
                bus.publish(event.message);
            }
        }

        Ok(results)
    }

    /// Decode a block and dispatch to the appropriate _in_txn handler.
    ///
    /// Does NOT commit/discard — caller manages the transaction lifecycle.
    #[allow(clippy::too_many_arguments)]
    async fn process_block_in_txn(
        &self,
        datastore: &NamespaceView,
        headstore: &NamespaceView,
        cid: &Cid,
        block_data: &[u8],
        metadata: &BlockMetadata<'_>,
        batch_merged: &std::sync::Mutex<HashSet<Cid>>,
        pending_events: &std::sync::Mutex<Vec<PendingMergeEvent>>,
    ) -> Result<MergeOutcome, MergeError> {
        // Decode the block from DAG-CBOR
        let block =
            Block::from_dag_cbor(block_data).map_err(|e| MergeError::BlockDecode(e.to_string()))?;

        // Decrypt delta data if the block has encryption
        let decrypted_block;
        let effective_block = if block.encryption.is_some() {
            match &block.delta {
                CrdtDelta::Lww(payload) => {
                    match self
                        .decrypt_block_data(&payload.data, block.encryption.as_ref())
                        .await
                    {
                        Ok(decrypted_data) => {
                            let mut new_payload = payload.clone();
                            new_payload.data = decrypted_data;
                            decrypted_block = Block {
                                delta: CrdtDelta::Lww(new_payload),
                                heads: block.heads.clone(),
                                links: block.links.clone(),
                                encryption: block.encryption,
                                signature: block.signature,
                            };
                            &decrypted_block
                        }
                        Err(_) => &block,
                    }
                }
                CrdtDelta::Counter(payload) => {
                    match self
                        .decrypt_block_data(&payload.data, block.encryption.as_ref())
                        .await
                    {
                        Ok(decrypted_data) => {
                            let mut new_payload = payload.clone();
                            new_payload.data = decrypted_data;
                            decrypted_block = Block {
                                delta: CrdtDelta::Counter(new_payload),
                                heads: block.heads.clone(),
                                links: block.links.clone(),
                                encryption: block.encryption,
                                signature: block.signature,
                            };
                            &decrypted_block
                        }
                        Err(_) => &block,
                    }
                }
                _ => &block,
            }
        } else {
            &block
        };

        // Dispatch based on delta type
        match &effective_block.delta {
            CrdtDelta::Composite(payload) => {
                self.process_composite_delta_in_txn(
                    datastore,
                    headstore,
                    cid,
                    &block,
                    payload,
                    metadata,
                    false,
                    batch_merged,
                    pending_events,
                    0,
                )
                .await
            }
            CrdtDelta::Collection(payload) => {
                self.process_collection_delta_in_txn(
                    datastore,
                    headstore,
                    cid,
                    &block,
                    payload,
                    metadata,
                    batch_merged,
                    pending_events,
                    0,
                )
                .await
            }
            CrdtDelta::Lww(payload) => {
                let mut ds = datastore.clone();
                let result = self.process_lww_delta_in_txn(&mut ds, cid, payload).await;
                match result {
                    Ok(r) if r.applied => Ok(MergeOutcome::Merged),
                    Ok(_) => Ok(MergeOutcome::skipped("rejected by CRDT")),
                    Err(e) => Err(e),
                }
            }
            CrdtDelta::Counter(payload) => {
                let mut ds = datastore.clone();
                let result = self
                    .process_counter_delta_in_txn(&mut ds, cid, payload, metadata.collection_id)
                    .await;
                match result {
                    Ok(r) if r.applied => Ok(MergeOutcome::Merged),
                    Ok(_) => Ok(MergeOutcome::skipped("rejected by CRDT")),
                    Err(e) => Err(e),
                }
            }
            CrdtDelta::FieldDefinition(_) => Ok(MergeOutcome::skipped(
                "field definition processed with collection",
            )),
            CrdtDelta::CollectionDefinition(payload) => {
                // CollectionDefinition uses its own txn (rare, not worth batching)
                self.process_collection_definition_delta(cid, &block, payload, metadata)
                    .await
            }
            CrdtDelta::CollectionSet(_) => Ok(MergeOutcome::skipped("collection set delta")),
        }
    }
}
