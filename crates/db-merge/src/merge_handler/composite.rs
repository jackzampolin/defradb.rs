use super::batch::{PendingFieldBlockFinalization, PendingMergeEvent, PendingPostCommitAction};
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompositeMergeMode {
    Standalone,
    Batch,
}

impl CompositeMergeMode {
    pub(crate) fn is_standalone(self) -> bool {
        matches!(self, Self::Standalone)
    }
}

pub(crate) struct CompositeMergeContext<'a, 'b> {
    pub(crate) cid: &'a Cid,
    pub(crate) block: &'a Block,
    pub(crate) payload: &'a defra_core::block::CompositeDeltaPayload,
    pub(crate) metadata: &'a BlockMetadata<'b>,
    pub(crate) doc_id_str: &'a str,
    pub(crate) collection: Option<Collection>,
    pub(crate) mode: CompositeMergeMode,
}

impl<'a, 'b> CompositeMergeContext<'a, 'b> {
    fn new(
        cid: &'a Cid,
        block: &'a Block,
        payload: &'a defra_core::block::CompositeDeltaPayload,
        metadata: &'a BlockMetadata<'b>,
        doc_id_str: &'a str,
        collection: Option<Collection>,
        mode: CompositeMergeMode,
    ) -> Self {
        Self {
            cid,
            block,
            payload,
            metadata,
            doc_id_str,
            collection,
            mode,
        }
    }
}

#[derive(Default)]
pub(crate) struct CompositeMergeState {
    pub(crate) field_values: HashMap<String, NormalValue>,
    pub(crate) any_field_applied: bool,
    pub(crate) encrypted_policy_checked: bool,
    pub(crate) field_block_heads: HashMap<String, Vec<Cid>>,
    pub(crate) linked_field_cids: Vec<Cid>,
    pub(crate) is_branchable: bool,
}

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    /// Process a Composite delta from a block.
    ///
    /// Composite deltas contain links to the actual field LWW/Counter blocks.
    /// This method processes all linked blocks within a SINGLE transaction to ensure
    /// atomicity between CRDT field merges and document storage.
    ///
    /// When `from_collection` is true, this composite is being processed as part of
    /// a collection-level sync (BranchableSync). The caller (`process_collection_delta`)
    /// handles collection headstore updates, so we skip creating local collection blocks
    /// to avoid race conditions with _commits queries.
    pub(crate) async fn process_composite_delta(
        &self,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CompositeDeltaPayload,
        metadata: &BlockMetadata<'_>,
        from_collection: bool,
        depth: usize,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        if depth >= super::MAX_MERGE_DEPTH {
            return Err(MergeError::depth_exceeded(cid, depth));
        }

        {
            let merged = self.merged_composites.lock().unwrap_or_else(|e| {
                tracing::warn!("merged_composites lock poisoned, recovering");
                e.into_inner()
            });
            if merged.contains(cid) {
                tracing::debug!(cid = %cid, "Composite already merged, skipping");
                return Ok(MergeOutcome::terminal_skip("already merged"));
            }
        }

        let doc_id_str = String::from_utf8_lossy(&payload.doc_id).to_string();

        tracing::info!(
            cid = %cid,
            doc_id = %doc_id_str,
            priority = payload.priority,
            status = payload.status,
            links = ?block.links,
            heads = ?block.heads,
            "Processing Composite delta (document-level)"
        );

        let collection_lookup = self
            .db
            .find_collection_by_id(&payload.schema_version_id)
            .ok()
            .flatten()
            .or_else(|| {
                metadata
                    .collection_id
                    .and_then(|cid| self.db.find_collection_by_id(cid).ok().flatten())
            });

        if let Some(collection) = collection_lookup.as_ref() {
            if let Some(reason) = self
                .db
                .replicated_downsample_source_skip_reason(collection.schema())?
            {
                tracing::warn!(
                    collection = %collection.name(),
                    doc_id = %doc_id_str,
                    reason = %reason,
                    "Skipping replicated write into local-only downsample source"
                );
                return Ok(MergeOutcome::terminal_skip(reason));
            }

            if let Some(hook) = self.composite_merge_hook() {
                if let Some(outcome) = hook
                    .on_protected_composite(&doc_id_str, collection.schema(), metadata)
                    .await?
                {
                    return Ok(outcome);
                }
            }
        }

        // Recursively merge parent composites referenced in `heads` before
        // processing this block.  This matches Go's processLog which walks
        // the DAG backwards and merges from oldest to newest, ensuring all
        // prior CRDT deltas are applied before the current one.
        //
        // Dedup guard: use merged_composites to skip parents already processed
        // by another path. Go serializes merge events per-collection and checks
        // `mt.heads` in loadComposites. In Rust, dual broadcast (doc topic +
        // collection topic) can trigger concurrent recursive walks that
        // temporarily re-add stale headstore entries. The guard prevents
        // re-processing parents that were already merged.
        if let Some(heads) = &block.heads {
            for head_cid in heads {
                {
                    let merged = self.merged_composites.lock().unwrap_or_else(|e| {
                        tracing::warn!("merged_composites lock poisoned, recovering");
                        e.into_inner()
                    });
                    if merged.contains(head_cid) {
                        tracing::debug!(
                            parent_cid = %head_cid,
                            child_cid = %cid,
                            "Parent composite already merged, skipping recursive processing"
                        );
                        continue;
                    }
                }

                let head_data = match self.blockstore.get(head_cid).await {
                    Ok(Some(data)) => data,
                    Ok(None) => {
                        tracing::debug!(
                            parent_cid = %head_cid,
                            child_cid = %cid,
                            "Parent composite not in blockstore, skipping"
                        );
                        continue;
                    }
                    Err(e) => {
                        tracing::debug!(
                            parent_cid = %head_cid,
                            error = %e,
                            "Failed to load parent composite, skipping"
                        );
                        continue;
                    }
                };

                let head_block = match Block::from_dag_cbor(&head_data) {
                    Ok(block) => block,
                    Err(_) => continue,
                };

                if let CrdtDelta::Composite(head_payload) = &head_block.delta {
                    tracing::info!(
                        parent_cid = %head_cid,
                        child_cid = %cid,
                        "Recursively merging parent composite before current"
                    );
                    match Box::pin(self.process_composite_delta(
                        head_cid,
                        &head_block,
                        head_payload,
                        metadata,
                        from_collection,
                        depth + 1,
                    ))
                    .await
                    {
                        Ok(MergeOutcome::Merged) => {}
                        Ok(outcome) if outcome.is_terminal_skip() => {}
                        Ok(outcome) => return Ok(outcome),
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        let txn = self.db.new_txn(false).await?;
        let context = CompositeMergeContext::new(
            cid,
            block,
            payload,
            metadata,
            &doc_id_str,
            collection_lookup.clone(),
            CompositeMergeMode::Standalone,
        );
        let mut state = CompositeMergeState::default();

        let process_result: std::result::Result<Option<MergeOutcome>, MergeError> = {
            let mut datastore = match txn.datastore() {
                Ok(datastore) => datastore,
                Err(e) => {
                    let _ = txn.force_discard();
                    return Err(MergeError::Database(e));
                }
            };
            let headstore = match txn.headstore() {
                Ok(headstore) => headstore,
                Err(e) => {
                    let _ = txn.force_discard();
                    return Err(MergeError::Database(e));
                }
            };

            match self
                .process_linked_field_blocks(&mut datastore, &headstore, &context, &mut state)
                .await?
            {
                Some(outcome) => Ok(Some(outcome)),
                None => {
                    self.persist_merged_document(&mut datastore, &context, &mut state)
                        .await?;
                    Ok(None)
                }
            }
        };

        match process_result {
            Ok(Some(outcome)) => {
                txn.force_discard()?;
                if outcome.is_terminal_skip() && !from_collection {
                    if let Some(bus) = self.db.event_bus() {
                        let merge_complete = MergeCompleteData {
                            doc_id: doc_id_str.clone(),
                            subject_doc_id: None,
                            cid: *cid,
                            collection_id: metadata
                                .collection_id
                                .unwrap_or(&payload.schema_version_id)
                                .to_string(),
                            by_peer: metadata.sender_peer.unwrap_or("").to_string(),
                        };
                        bus.publish(Message::merge_complete(merge_complete));
                    }
                }
                Ok(outcome)
            }
            Ok(None) => {
                if let Ok(headstore) = txn.headstore() {
                    self.update_heads(&headstore, &context, &state).await;
                }

                txn.force_commit().await?;

                self.best_effort_finalize_linked_field_blocks(
                    &state.linked_field_cids,
                    metadata.collection_id,
                )
                .await;

                {
                    let mut merged = self.merged_composites.lock().unwrap_or_else(|e| {
                        tracing::warn!("merged_composites lock poisoned, recovering");
                        e.into_inner()
                    });
                    merged.insert(*cid);
                }

                tracing::info!(
                    cid = %cid,
                    doc_id = %doc_id_str,
                    fields_merged = state.field_values.len(),
                    "Composite delta processed and committed successfully"
                );

                if let (Some(collection), Some(hook)) =
                    (context.collection.as_ref(), self.composite_merge_hook())
                {
                    if let Some(action) =
                        hook.post_commit_action(&doc_id_str, collection.schema(), metadata)
                    {
                        if let Err(e) = action.run().await {
                            tracing::warn!(
                                cid = %cid,
                                doc_id = %doc_id_str,
                                error = %e,
                                "Post-commit composite merge action failed"
                            );
                        }
                    }
                }

                if let Some(bus) = self.db.event_bus() {
                    let update = Update::new(
                        doc_id_str.clone(),
                        *cid,
                        payload.schema_version_id.clone(),
                        vec![],
                        false,
                        true,
                    );
                    bus.publish(Message::update(update));

                    if !from_collection {
                        let merge_complete = MergeCompleteData {
                            doc_id: doc_id_str.clone(),
                            subject_doc_id: None,
                            cid: *cid,
                            collection_id: metadata
                                .collection_id
                                .unwrap_or(&payload.schema_version_id)
                                .to_string(),
                            by_peer: metadata.sender_peer.unwrap_or("").to_string(),
                        };
                        bus.publish(Message::merge_complete(merge_complete));
                    }

                    if state.is_branchable {
                        let merge_complete = MergeCompleteData {
                            doc_id: String::new(),
                            subject_doc_id: Some(doc_id_str.clone()),
                            cid: *cid,
                            collection_id: metadata
                                .collection_id
                                .unwrap_or(&payload.schema_version_id)
                                .to_string(),
                            by_peer: metadata.sender_peer.unwrap_or("").to_string(),
                        };
                        bus.publish(Message::merge_complete(merge_complete));
                    }
                }

                Ok(MergeOutcome::Merged)
            }
            Err(e) => {
                if let Err(discard_err) = txn.force_discard() {
                    tracing::error!(
                        cid = %cid,
                        discard_error = %discard_err,
                        merge_error = %e,
                        "Failed to discard transaction after composite merge error - potential resource leak"
                    );
                }
                Err(e)
            }
        }
    }

    /// Process a Composite delta within a shared transaction (batch mode).
    ///
    /// Same logic as `process_composite_delta` but:
    /// - Uses a shared transaction (no create/commit/discard)
    /// - Checks both `self.merged_composites` and `batch_merged` for dedup
    /// - Inserts into `batch_merged` on success
    /// - Collects events into `pending_events` instead of publishing
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn process_composite_delta_in_txn(
        &self,
        datastore: &NamespaceView,
        headstore: &NamespaceView,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CompositeDeltaPayload,
        metadata: &BlockMetadata<'_>,
        from_collection: bool,
        batch_merged: &std::sync::Mutex<HashSet<Cid>>,
        _batch_merged_collections: &std::sync::Mutex<HashSet<Cid>>,
        pending_events: &std::sync::Mutex<Vec<PendingMergeEvent>>,
        pending_post_commit_actions: &std::sync::Mutex<Vec<PendingPostCommitAction>>,
        pending_field_block_finalizations: &std::sync::Mutex<Vec<PendingFieldBlockFinalization>>,
        depth: usize,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        if depth >= super::MAX_MERGE_DEPTH {
            return Err(MergeError::depth_exceeded(cid, depth));
        }

        {
            let merged = self.merged_composites.lock().unwrap_or_else(|e| {
                tracing::warn!("merged_composites lock poisoned, recovering");
                e.into_inner()
            });
            if merged.contains(cid) {
                tracing::debug!(cid = %cid, "Composite already merged in permanent dedup set, skipping");
                return Ok(MergeOutcome::terminal_skip("already merged"));
            }
        }
        {
            let batch_merged_guard = batch_merged.lock().unwrap_or_else(|e| {
                tracing::warn!("batch_merged lock poisoned, recovering");
                e.into_inner()
            });
            if batch_merged_guard.contains(cid) {
                tracing::debug!(cid = %cid, "Composite already merged in batch dedup set, skipping");
                return Ok(MergeOutcome::terminal_skip("already merged"));
            }
        }

        let doc_id_str = String::from_utf8_lossy(&payload.doc_id).to_string();

        tracing::info!(
            cid = %cid,
            doc_id = %doc_id_str,
            priority = payload.priority,
            "Processing Composite delta in batch txn"
        );

        let collection_lookup = self
            .db
            .find_collection_by_id(&payload.schema_version_id)
            .ok()
            .flatten()
            .or_else(|| {
                metadata
                    .collection_id
                    .and_then(|cid| self.db.find_collection_by_id(cid).ok().flatten())
            });

        if let Some(collection) = collection_lookup.as_ref() {
            if let Some(reason) = self
                .db
                .replicated_downsample_source_skip_reason(collection.schema())?
            {
                tracing::warn!(
                    collection = %collection.name(),
                    doc_id = %doc_id_str,
                    reason = %reason,
                    "Skipping replicated write into local-only downsample source"
                );
                return Ok(MergeOutcome::terminal_skip(reason));
            }

            if let Some(hook) = self.composite_merge_hook() {
                if let Some(outcome) = hook
                    .on_protected_composite(&doc_id_str, collection.schema(), metadata)
                    .await?
                {
                    return Ok(outcome);
                }
            }
        }

        if let Some(heads) = &block.heads {
            for head_cid in heads {
                {
                    let merged = self.merged_composites.lock().unwrap_or_else(|e| {
                        tracing::warn!("merged_composites lock poisoned, recovering");
                        e.into_inner()
                    });
                    if merged.contains(head_cid) {
                        continue;
                    }
                }
                {
                    let batch_merged_guard = batch_merged.lock().unwrap_or_else(|e| {
                        tracing::warn!("batch_merged lock poisoned, recovering");
                        e.into_inner()
                    });
                    if batch_merged_guard.contains(head_cid) {
                        continue;
                    }
                }

                let head_data = match self.blockstore.get(head_cid).await {
                    Ok(Some(data)) => data,
                    _ => continue,
                };

                let head_block = match Block::from_dag_cbor(&head_data) {
                    Ok(block) => block,
                    Err(_) => continue,
                };

                if let CrdtDelta::Composite(head_payload) = &head_block.delta {
                    match Box::pin(self.process_composite_delta_in_txn(
                        datastore,
                        headstore,
                        head_cid,
                        &head_block,
                        head_payload,
                        metadata,
                        from_collection,
                        batch_merged,
                        _batch_merged_collections,
                        pending_events,
                        pending_post_commit_actions,
                        pending_field_block_finalizations,
                        depth + 1,
                    ))
                    .await
                    {
                        Ok(MergeOutcome::Merged) => {}
                        Ok(outcome) if outcome.is_terminal_skip() => {}
                        Ok(outcome) => return Ok(outcome),
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        let context = CompositeMergeContext::new(
            cid,
            block,
            payload,
            metadata,
            &doc_id_str,
            collection_lookup.clone(),
            CompositeMergeMode::Batch,
        );
        let mut state = CompositeMergeState::default();

        let process_result: std::result::Result<Option<MergeOutcome>, MergeError> = {
            let mut datastore = datastore.clone();

            match self
                .process_linked_field_blocks(&mut datastore, headstore, &context, &mut state)
                .await?
            {
                Some(outcome) => Ok(Some(outcome)),
                None => {
                    self.persist_merged_document(&mut datastore, &context, &mut state)
                        .await?;
                    Ok(None)
                }
            }
        };

        match process_result {
            Ok(Some(outcome)) => {
                if outcome.is_terminal_skip() && !from_collection {
                    let merge_complete = MergeCompleteData {
                        doc_id: doc_id_str.clone(),
                        subject_doc_id: None,
                        cid: *cid,
                        collection_id: metadata
                            .collection_id
                            .unwrap_or(&payload.schema_version_id)
                            .to_string(),
                        by_peer: metadata.sender_peer.unwrap_or("").to_string(),
                    };
                    pending_events
                        .lock()
                        .unwrap_or_else(|e| {
                            tracing::warn!("pending_events lock poisoned, recovering");
                            e.into_inner()
                        })
                        .push(PendingMergeEvent {
                            message: Message::merge_complete(merge_complete),
                        });
                }
                Ok(outcome)
            }
            Ok(None) => {
                self.update_heads(headstore, &context, &state).await;

                {
                    let mut batch_merged_guard = batch_merged.lock().unwrap_or_else(|e| {
                        tracing::warn!("batch_merged lock poisoned, recovering");
                        e.into_inner()
                    });
                    batch_merged_guard.insert(*cid);
                }

                if !state.linked_field_cids.is_empty() {
                    pending_field_block_finalizations
                        .lock()
                        .unwrap_or_else(|e| {
                            tracing::warn!(
                                "pending_field_block_finalizations lock poisoned, recovering"
                            );
                            e.into_inner()
                        })
                        .push(PendingFieldBlockFinalization {
                            cids: state.linked_field_cids.clone(),
                            fallback_collection_id: metadata.collection_id.map(ToOwned::to_owned),
                        });
                }

                if let (Some(collection), Some(hook)) =
                    (context.collection.as_ref(), self.composite_merge_hook())
                {
                    if let Some(action) =
                        hook.post_commit_action(&doc_id_str, collection.schema(), metadata)
                    {
                        pending_post_commit_actions
                            .lock()
                            .unwrap_or_else(|e| {
                                tracing::warn!(
                                    "pending_post_commit_actions lock poisoned, recovering"
                                );
                                e.into_inner()
                            })
                            .push(PendingPostCommitAction { action });
                    }
                }

                {
                    let mut pending_events_guard = pending_events.lock().unwrap_or_else(|e| {
                        tracing::warn!("pending_events lock poisoned, recovering");
                        e.into_inner()
                    });

                    let update = Update::new(
                        doc_id_str.clone(),
                        *cid,
                        payload.schema_version_id.clone(),
                        vec![],
                        false,
                        true,
                    );
                    pending_events_guard.push(PendingMergeEvent {
                        message: Message::update(update),
                    });

                    if !from_collection {
                        let merge_complete = MergeCompleteData {
                            doc_id: doc_id_str.clone(),
                            subject_doc_id: None,
                            cid: *cid,
                            collection_id: metadata
                                .collection_id
                                .unwrap_or(&payload.schema_version_id)
                                .to_string(),
                            by_peer: metadata.sender_peer.unwrap_or("").to_string(),
                        };
                        pending_events_guard.push(PendingMergeEvent {
                            message: Message::merge_complete(merge_complete),
                        });
                    }

                    if state.is_branchable {
                        let merge_complete = MergeCompleteData {
                            doc_id: String::new(),
                            subject_doc_id: Some(doc_id_str.clone()),
                            cid: *cid,
                            collection_id: metadata
                                .collection_id
                                .unwrap_or(&payload.schema_version_id)
                                .to_string(),
                            by_peer: metadata.sender_peer.unwrap_or("").to_string(),
                        };
                        pending_events_guard.push(PendingMergeEvent {
                            message: Message::merge_complete(merge_complete),
                        });
                    }
                }

                Ok(MergeOutcome::Merged)
            }
            Err(e) => Err(e),
        }
    }
}
