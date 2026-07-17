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

/// Field blocks that should be marked merged in the blockstore once the
/// surrounding batch transaction commits. The blockstore's merged-set is the
/// single source of CRDT idempotency; see #847.
pub(crate) struct PendingFieldBlockFinalization {
    pub cids: Vec<Cid>,
}

impl<S: Store + 'static, B: blockstore::Blockstore + Send + Sync + 'static> DbMergeHandler<S, B> {
    /// Process blocks individually, each with its own transaction.
    pub(crate) async fn merge_blocks_individually(
        &self,
        blocks: &[MergeBlock],
    ) -> Vec<Result<MergeOutcome, MergeError>> {
        let mut results = Vec::with_capacity(blocks.len());
        for block in blocks {
            if let Err(error) = self
                .validate_explicit_replay_authorization(
                    block.explicit_replay_authorization.as_ref(),
                    block,
                )
                .await
            {
                results.push(Err(error));
                continue;
            }

            let metadata = BlockMetadata::normal(
                &block.doc_id,
                &block.collection_id,
                &block.creator,
                block.sender_peer.as_deref(),
                block.is_explicit_replicator,
            )
            .with_explicit_replay_authorization(block.explicit_replay_authorization.clone());

            // Conflict retry lives inside `handle_block` (Go's MaxTxnRetries
            // parity), so every caller — including the parallel replication
            // workers — gets it, not just this batch fallback.
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
                Err(MergeError::GateContended) => {
                    // A long-lived local/interactive txn holds the per-doc batch
                    // gate. Don't block node-wide inbound replication — degrade to
                    // the gate-free per-block path, which is correct and takes only
                    // single per-doc guards (#1041).
                    tracing::debug!(
                        batch_size = blocks.len(),
                        "batch gate contended; merging per-block"
                    );
                    self.merge_blocks_individually(blocks).await
                }
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
        // Serialize this batch against concurrent same-doc writes/merges (#1021).
        // The per-block `_in_txn` handlers below do NOT take the per-doc guard
        // (they share one txn), so acquire it here for every DISTINCT doc in the
        // batch — in SORTED doc-id order so it can never deadlock against any other
        // guard taker (a single-doc update/merge, or another batch). Held for the
        // whole batch txn.
        let mut batch_doc_ids: Vec<String> = blocks.iter().map(|b| b.doc_id.clone()).collect();
        batch_doc_ids.sort();
        batch_doc_ids.dedup();
        let mut _doc_guards = Vec::with_capacity(batch_doc_ids.len());
        {
            // Hold the shared batch gate only while acquiring, so this multi-doc
            // acquirer can never deadlock against an incremental local mutation
            // batch (which holds the gate for its whole batch). Once all guards
            // are held the gate is released; the per-doc guards stay until commit.
            //
            // Acquire it NON-BLOCKING: batch merging is an optimization, so if the
            // gate is held by a long-lived local/interactive txn we signal the
            // caller to fall back to the gate-free per-block path rather than stall
            // node-wide inbound replication (#1041).
            let _batch_gate = self
                .merge_queue
                .try_acquire_batch_gate()
                .ok_or(MergeError::GateContended)?;
            for doc_id in &batch_doc_ids {
                _doc_guards.push(self.merge_queue.acquire(doc_id).await);
            }
        }

        let txn = self.db.new_txn(false).await?;
        let batch_merged: std::sync::Mutex<HashSet<Cid>> = std::sync::Mutex::new(HashSet::new());
        let batch_merged_collections: std::sync::Mutex<HashSet<Cid>> =
            std::sync::Mutex::new(HashSet::new());
        let pending_events: std::sync::Mutex<Vec<PendingMergeEvent>> =
            std::sync::Mutex::new(Vec::new());
        let pending_post_commit_actions: std::sync::Mutex<Vec<PendingPostCommitAction>> =
            std::sync::Mutex::new(Vec::new());
        let pending_field_block_finalizations: std::sync::Mutex<
            Vec<PendingFieldBlockFinalization>,
        > = std::sync::Mutex::new(Vec::new());

        let mut results = Vec::with_capacity(blocks.len());
        let mut batch_error: Option<MergeError> = None;

        // Create NamespaceViews from the shared transaction. These use
        // Arc<SharedTxn> internally and are Send+Sync, avoiding the
        // "future cannot be sent between threads safely" error that
        // would occur if we passed &DbTxn<S> across await points.
        {
            let datastore = txn.datastore()?;
            let headstore = txn.headstore()?;
            let systemstore = txn.systemstore()?;

            for block in blocks {
                self.validate_explicit_replay_authorization(
                    block.explicit_replay_authorization.as_ref(),
                    block,
                )
                .await?;

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
                        &systemstore,
                        &block.cid,
                        &block.block_data,
                        &metadata,
                        &batch_merged,
                        &batch_merged_collections,
                        &pending_events,
                        &pending_post_commit_actions,
                        &pending_field_block_finalizations,
                    )
                    .await
                {
                    // A Rejected outcome (unique-index violation) surfaces AFTER
                    // persist_merged_document has staged the doc's field data in
                    // the SHARED batch txn, and the shared txn cannot roll back a
                    // single block. Treat it as batch-poisoning: discard the whole
                    // attempt and let the caller fall back to per-block processing,
                    // whose per-CID txn discards cleanly and re-yields the clean
                    // Rejected outcome (partial `results` are dropped with the Err).
                    Ok(MergeOutcome::Rejected { reason }) => {
                        batch_error = Some(MergeError::UniqueConstraintViolation(reason));
                        break;
                    }
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
        {
            let batch = batch_merged_collections.lock().unwrap();
            let mut merged = self.merged_collections.lock().unwrap();
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

        let field_block_finalizations = pending_field_block_finalizations.into_inner().unwrap();
        for finalization in field_block_finalizations {
            self.best_effort_finalize_linked_field_blocks(&finalization.cids)
                .await;
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
        systemstore: &NamespaceView,
        cid: &Cid,
        block_data: &[u8],
        metadata: &BlockMetadata<'_>,
        batch_merged: &std::sync::Mutex<HashSet<Cid>>,
        batch_merged_collections: &std::sync::Mutex<HashSet<Cid>>,
        pending_events: &std::sync::Mutex<Vec<PendingMergeEvent>>,
        pending_post_commit_actions: &std::sync::Mutex<Vec<PendingPostCommitAction>>,
        pending_field_block_finalizations: &std::sync::Mutex<Vec<PendingFieldBlockFinalization>>,
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

        // Same ownership-before-decryption gate as the non-batch dispatch in
        // `mod.rs::handle_block`: a standalone encrypted field block can only
        // merge once the ownership index names it a single owner, so check
        // that before paying for a (possibly cross-network) KMS fetch whose
        // result would just be discarded by the dispatch below.
        if block.encryption.is_some()
            && matches!(block.delta, CrdtDelta::Lww(_) | CrdtDelta::Counter(_))
            && self
                .resolve_field_block_identity(systemstore, cid)
                .await?
                .is_none()
        {
            // Still REQUEST the DEK (detached) — see the twin gate in
            // `mod.rs::handle_block` for the Go-parity rationale.
            if let Some(enc_cid) = block.encryption {
                self.spawn_dek_prefetch(enc_cid, &metadata);
            }
            return Ok(MergeOutcome::terminal_skip(
                "field block has no unambiguous owner; merged via its composite",
            ));
        }

        // Decrypt delta data if the block has encryption
        let decrypted_block;
        let effective_block = if block.encryption.is_some() {
            match &block.delta {
                CrdtDelta::Lww(payload) => {
                    match self
                        .decrypt_block_data(
                            &payload.data,
                            block.encryption.as_ref(),
                            Some(&metadata),
                        )
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
                        Err(MergeError::Kms(kms::Error::AccessDenied { .. })) => {
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                        Err(error @ MergeError::Kms(_)) => return Err(error),
                        Err(_) => &block,
                    }
                }
                CrdtDelta::Counter(payload) => {
                    match self
                        .decrypt_block_data(
                            &payload.data,
                            block.encryption.as_ref(),
                            Some(&metadata),
                        )
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
                        Err(MergeError::Kms(kms::Error::AccessDenied { .. })) => {
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                        Err(error @ MergeError::Kms(_)) => return Err(error),
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
                    systemstore,
                    cid,
                    &block,
                    payload,
                    &metadata,
                    false,
                    batch_merged,
                    batch_merged_collections,
                    pending_events,
                    pending_post_commit_actions,
                    pending_field_block_finalizations,
                    0,
                )
                .await
            }
            CrdtDelta::Collection(payload) => {
                self.process_collection_delta_in_txn(
                    datastore,
                    headstore,
                    systemstore,
                    cid,
                    &block,
                    payload,
                    &metadata,
                    batch_merged,
                    batch_merged_collections,
                    pending_events,
                    pending_post_commit_actions,
                    pending_field_block_finalizations,
                    0,
                )
                .await
            }
            CrdtDelta::Lww(payload) => {
                let Some((doc_id_str, doc_short_id)) =
                    self.resolve_field_block_identity(systemstore, cid).await?
                else {
                    return Ok(MergeOutcome::terminal_skip(
                        "field block has no unambiguous owner; merged via its composite",
                    ));
                };
                let mut ds = datastore.clone();
                let result = self
                    .process_lww_delta_in_txn(
                        &mut ds,
                        headstore,
                        cid,
                        payload,
                        metadata.collection_id,
                        &doc_id_str,
                        doc_short_id,
                    )
                    .await;
                match result {
                    Ok(r) if r.applied => Ok(MergeOutcome::Merged),
                    Ok(_) => Ok(MergeOutcome::terminal_skip("rejected by CRDT")),
                    Err(e) => Err(e),
                }
            }
            CrdtDelta::Counter(payload) => {
                let Some((doc_id_str, doc_short_id)) =
                    self.resolve_field_block_identity(systemstore, cid).await?
                else {
                    return Ok(MergeOutcome::terminal_skip(
                        "field block has no unambiguous owner; merged via its composite",
                    ));
                };
                let mut ds = datastore.clone();
                let result = self
                    .process_counter_delta_in_txn(
                        &mut ds,
                        headstore,
                        cid,
                        payload,
                        metadata.collection_id,
                        &doc_id_str,
                        doc_short_id,
                    )
                    .await;
                match result {
                    Ok(r) if r.applied => {
                        pending_field_block_finalizations
                            .lock()
                            .unwrap_or_else(|e| {
                                tracing::warn!(
                                    "pending_field_block_finalizations lock poisoned, recovering"
                                );
                                e.into_inner()
                            })
                            .push(PendingFieldBlockFinalization { cids: vec![*cid] });
                        Ok(MergeOutcome::Merged)
                    }
                    Ok(_) => {
                        pending_field_block_finalizations
                            .lock()
                            .unwrap_or_else(|e| {
                                tracing::warn!(
                                    "pending_field_block_finalizations lock poisoned, recovering"
                                );
                                e.into_inner()
                            })
                            .push(PendingFieldBlockFinalization { cids: vec![*cid] });
                        Ok(MergeOutcome::terminal_skip("rejected by CRDT"))
                    }
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
            // Only the variant discriminant is reported — `CrdtDelta` carries
            // field-value bytes from user documents and must not be formatted
            // into error strings that may end up in logs.
            other => Err(MergeError::UnsupportedDelta(format!(
                "unhandled CrdtDelta variant in batch dispatch: {:?}",
                std::mem::discriminant(other)
            ))),
        }
    }
}
