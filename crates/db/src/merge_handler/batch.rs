use super::hook::CompositePostCommitAction;
use super::*;

use p2p::sync::MergeBlock;

/// Event collected during batch processing, emitted after commit.
pub(crate) struct PendingMergeEvent {
    pub message: Message,
}

/// Async side effect collected during batch processing, executed after commit.
pub(crate) struct PendingPostCommitAction {
    pub action: Box<dyn CompositePostCommitAction>,
}

impl<S: Store + 'static, B: blockstore::Blockstore + Send + Sync + 'static> DbMergeHandler<S, B> {
    /// Process blocks individually, each with its own transaction.
    pub(crate) async fn merge_blocks_individually(
        &self,
        blocks: &[MergeBlock],
    ) -> Vec<Result<MergeOutcome, MergeError>> {
        let mut results = Vec::with_capacity(blocks.len());
        for block in blocks {
            let metadata = BlockMetadata::normal(
                &block.doc_id,
                &block.collection_id,
                &block.creator,
                block.sender_peer.as_deref(),
                block.is_explicit_replicator,
            )
            .with_explicit_replay_authorization(block.explicit_replay_authorization.clone());
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
        let pending_post_commit_actions: std::sync::Mutex<Vec<PendingPostCommitAction>> =
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
                let metadata = BlockMetadata::normal(
                    &block.doc_id,
                    &block.collection_id,
                    &block.creator,
                    block.sender_peer.as_deref(),
                    block.is_explicit_replicator,
                )
                .with_explicit_replay_authorization(block.explicit_replay_authorization.clone());
                match self
                    .process_block_in_txn(
                        &datastore,
                        &headstore,
                        &block.cid,
                        &block.block_data,
                        &metadata,
                        &batch_merged,
                        &pending_events,
                        &pending_post_commit_actions,
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

        let post_commit_actions = pending_post_commit_actions.into_inner().unwrap();
        for action in post_commit_actions {
            if let Err(error) = action.action.run().await {
                tracing::warn!(
                    error = %error,
                    "Post-commit batch merge action failed after commit"
                );
            }
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
        pending_post_commit_actions: &std::sync::Mutex<Vec<PendingPostCommitAction>>,
    ) -> Result<MergeOutcome, MergeError> {
        // Decode the block from DAG-CBOR
        let block =
            Block::from_dag_cbor(block_data).map_err(|e| MergeError::BlockDecode(e.to_string()))?;

        // Verify block signature (batch path). Clone metadata so we can
        // set verified_creator from the cryptographic verification result.
        let mut metadata = metadata.clone();
        if !metadata.is_recovery {
            let verified = self.verify_block_signature(cid, &block, block_data).await?;
            metadata.verified_creator = verified;
        }

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
                    &metadata,
                    false,
                    batch_merged,
                    pending_events,
                    pending_post_commit_actions,
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
                    &metadata,
                    batch_merged,
                    pending_events,
                    pending_post_commit_actions,
                    0,
                )
                .await
            }
            CrdtDelta::Lww(payload) => {
                let mut ds = datastore.clone();
                let result = self.process_lww_delta_in_txn(&mut ds, cid, payload).await;
                match result {
                    Ok(r) if r.applied => Ok(MergeOutcome::Merged),
                    Ok(_) => Ok(MergeOutcome::terminal_skip("rejected by CRDT")),
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
                    Ok(_) => Ok(MergeOutcome::terminal_skip("rejected by CRDT")),
                    Err(e) => Err(e),
                }
            }
            CrdtDelta::FieldDefinition(_) => Ok(MergeOutcome::terminal_skip(
                "field definition processed with collection",
            )),
            CrdtDelta::CollectionDefinition(payload) => {
                // CollectionDefinition uses its own txn (rare, not worth batching)
                self.process_collection_definition_delta(cid, &block, payload, &metadata)
                    .await
            }
            CrdtDelta::CollectionSet(_) => Ok(MergeOutcome::terminal_skip("collection set delta")),
        }
    }
}
