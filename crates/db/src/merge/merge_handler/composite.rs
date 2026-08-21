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
    pub(crate) doc_short_id: u64,
    pub(crate) collection: Option<Collection>,
    pub(crate) mode: CompositeMergeMode,
}

#[allow(clippy::too_many_arguments)]
impl<'a, 'b> CompositeMergeContext<'a, 'b> {
    fn new(
        cid: &'a Cid,
        block: &'a Block,
        payload: &'a defra_core::block::CompositeDeltaPayload,
        metadata: &'a BlockMetadata<'b>,
        doc_id_str: &'a str,
        doc_short_id: u64,
        collection: Option<Collection>,
        mode: CompositeMergeMode,
    ) -> Self {
        Self {
            cid,
            block,
            payload,
            metadata,
            doc_id_str,
            doc_short_id,
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
    pub(crate) owned_field_cids: Vec<Cid>,
    pub(crate) linked_field_cids: Vec<Cid>,
    pub(crate) linked_encryption_cids: Vec<Cid>,
    pub(crate) is_branchable: bool,
}

enum CompositeMergePreparation {
    Ready(Option<Box<Collection>>),
    Complete(MergeOutcome),
}

enum CompositeMergeFrame {
    Enter {
        cid: Cid,
        block: Option<Block>,
        payload: Option<defra_core::block::CompositeDeltaPayload>,
        child_cid: Option<Cid>,
        depth: usize,
        is_root: bool,
    },
    Exit {
        cid: Cid,
        block: Block,
        payload: defra_core::block::CompositeDeltaPayload,
        doc_id: String,
        collection: Option<Box<Collection>>,
        is_root: bool,
    },
}

impl<S: Store, B: blockstore::Blockstore> DbMergeHandler<S, B> {
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
        // Fast path: gossip dual-broadcast (doc topic + collection topic) and
        // batch-merge retries re-deliver the same composite repeatedly, so a
        // hit here is the common case. Skip identity resolution and the
        // per-document guard for an already-merged block; the guarded re-check
        // below still covers the concurrent-first-delivery race.
        {
            let merged = self.merged_composites.lock().unwrap_or_else(|e| {
                tracing::warn!("merged_composites lock poisoned, recovering");
                e.into_inner()
            });
            if merged.contains(cid) {
                return Ok(MergeOutcome::terminal_skip("already merged"));
            }
        }

        let doc_id_str = self.resolve_composite_doc_id(cid, block, depth).await?;
        let _guard = self.merge_queue.acquire(&doc_id_str).await;

        self.process_composite_delta_locked(
            cid,
            block,
            payload,
            metadata,
            from_collection,
            depth,
            doc_id_str,
        )
        .await
    }

    fn has_merged_composite(&self, cid: &Cid) -> bool {
        self.merged_composites
            .lock()
            .unwrap_or_else(|error| {
                tracing::warn!("merged_composites lock poisoned, recovering");
                error.into_inner()
            })
            .contains(cid)
    }

    fn has_batch_merged_composite(
        batch_merged: &std::sync::Mutex<HashSet<Cid>>,
        cid: &Cid,
    ) -> bool {
        batch_merged
            .lock()
            .unwrap_or_else(|error| {
                tracing::warn!("batch_merged lock poisoned, recovering");
                error.into_inner()
            })
            .contains(cid)
    }

    async fn load_parent_composite(&self, parent_cid: &Cid, child_cid: &Cid) -> Option<Block> {
        let data = match self.blockstore.get(parent_cid).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                tracing::debug!(
                    %parent_cid,
                    %child_cid,
                    "Parent composite not in blockstore, skipping"
                );
                return None;
            }
            Err(error) => {
                tracing::debug!(
                    %parent_cid,
                    %child_cid,
                    %error,
                    "Failed to load parent composite, skipping"
                );
                return None;
            }
        };

        Block::from_dag_cbor(&data).ok()
    }

    async fn prepare_composite_merge(
        &self,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CompositeDeltaPayload,
        metadata: &BlockMetadata<'_>,
        doc_id: &str,
        mode: CompositeMergeMode,
    ) -> std::result::Result<CompositeMergePreparation, MergeError> {
        if mode.is_standalone() {
            tracing::info!(
                %cid,
                %doc_id,
                priority = payload.priority,
                status = payload.status,
                links = ?block.links,
                heads = ?block.heads,
                "Processing Composite delta (document-level)"
            );
        } else {
            tracing::info!(
                %cid,
                %doc_id,
                priority = payload.priority,
                "Processing Composite delta in batch txn"
            );
        }

        let collection = self
            .db
            .find_collection_by_id(&payload.schema_version_id)
            .ok()
            .flatten()
            .or_else(|| {
                metadata
                    .collection_id
                    .and_then(|cid| self.db.find_collection_by_id(cid).ok().flatten())
            });

        if let Some(collection) = collection.as_ref() {
            if let Some(reason) = self
                .db
                .replicated_downsample_source_skip_reason(collection.schema())?
            {
                tracing::warn!(
                    collection = %collection.name(),
                    %doc_id,
                    %reason,
                    "Skipping replicated write into local-only downsample source"
                );
                return Ok(CompositeMergePreparation::Complete(
                    MergeOutcome::terminal_skip(reason),
                ));
            }

            if let Some(hook) = self.composite_merge_hook() {
                if let Some(outcome) = hook
                    .on_protected_composite(doc_id, collection.schema(), metadata)
                    .await?
                {
                    return Ok(CompositeMergePreparation::Complete(outcome));
                }
            }
        }

        Ok(CompositeMergePreparation::Ready(collection.map(Box::new)))
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_composite_delta_locked(
        &self,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CompositeDeltaPayload,
        metadata: &BlockMetadata<'_>,
        from_collection: bool,
        depth: usize,
        doc_id_str: String,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        let mut frames = vec![CompositeMergeFrame::Enter {
            cid: *cid,
            block: Some(block.clone()),
            payload: Some(payload.clone()),
            child_cid: None,
            depth,
            is_root: true,
        }];

        while let Some(frame) = frames.pop() {
            match frame {
                CompositeMergeFrame::Enter {
                    cid,
                    block,
                    payload,
                    child_cid,
                    depth,
                    is_root,
                } => {
                    self.ensure_merge_depth(&cid, depth)?;
                    if self.has_merged_composite(&cid) {
                        if is_root {
                            return Ok(MergeOutcome::terminal_skip("already merged"));
                        }
                        continue;
                    }

                    let block = match block {
                        Some(block) => block,
                        None => {
                            let Some(block) = self
                                .load_parent_composite(
                                    &cid,
                                    &child_cid.expect("parent frame has a child CID"),
                                )
                                .await
                            else {
                                continue;
                            };
                            block
                        }
                    };
                    let payload = match payload {
                        Some(payload) => payload,
                        None => {
                            let CrdtDelta::Composite(payload) = &block.delta else {
                                continue;
                            };
                            payload.clone()
                        }
                    };

                    match self
                        .prepare_composite_merge(
                            &cid,
                            &block,
                            &payload,
                            metadata,
                            &doc_id_str,
                            CompositeMergeMode::Standalone,
                        )
                        .await?
                    {
                        CompositeMergePreparation::Ready(collection) => {
                            let heads = block.heads.clone();
                            frames.push(CompositeMergeFrame::Exit {
                                cid,
                                block,
                                payload,
                                doc_id: doc_id_str.clone(),
                                collection,
                                is_root,
                            });
                            if let Some(heads) = heads {
                                for parent_cid in heads.into_iter().rev() {
                                    frames.push(CompositeMergeFrame::Enter {
                                        cid: parent_cid,
                                        block: None,
                                        payload: None,
                                        child_cid: Some(cid),
                                        depth: depth + 1,
                                        is_root: false,
                                    });
                                }
                            }
                        }
                        CompositeMergePreparation::Complete(outcome) => {
                            if is_root || !outcome.is_terminal_skip() {
                                return Ok(outcome);
                            }
                        }
                    }
                }
                CompositeMergeFrame::Exit {
                    cid,
                    block,
                    payload,
                    doc_id,
                    collection,
                    is_root,
                } => {
                    if self.has_merged_composite(&cid) {
                        if is_root {
                            return Ok(MergeOutcome::terminal_skip("already merged"));
                        }
                        continue;
                    }
                    let outcome = self
                        .process_composite_delta_body(
                            &cid,
                            &block,
                            &payload,
                            metadata,
                            from_collection,
                            &doc_id,
                            collection.map(|collection| *collection),
                        )
                        .await?;
                    if is_root || (!outcome.is_merged() && !outcome.is_terminal_skip()) {
                        return Ok(outcome);
                    }
                }
            }
        }

        Ok(MergeOutcome::terminal_skip("already merged"))
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_composite_delta_body(
        &self,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CompositeDeltaPayload,
        metadata: &BlockMetadata<'_>,
        from_collection: bool,
        doc_id_str: &str,
        collection_lookup: Option<Collection>,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        let txn = self.db.new_txn(false).await?;
        let doc_short_id = {
            let collection = collection_lookup.as_ref().ok_or_else(|| {
                MergeError::MissingMetadata(format!(
                    "Collection not found for schema_version_id: {}",
                    payload.schema_version_id
                ))
            })?;
            let systemstore = match txn.systemstore() {
                Ok(systemstore) => systemstore,
                Err(e) => {
                    let _ = txn.force_discard();
                    return Err(MergeError::Database(e));
                }
            };
            match self
                .db
                .resolve_or_allocate_doc_short_id(
                    &systemstore,
                    collection.resolved_root_id(),
                    doc_id_str,
                )
                .await
            {
                Ok(short_id) => short_id,
                Err(e) => {
                    let _ = txn.force_discard();
                    return Err(MergeError::Database(e));
                }
            }
        };
        let context = CompositeMergeContext::new(
            cid,
            block,
            payload,
            metadata,
            doc_id_str,
            doc_short_id,
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
            let systemstore = match txn.systemstore() {
                Ok(systemstore) => systemstore,
                Err(e) => {
                    let _ = txn.force_discard();
                    return Err(MergeError::Database(e));
                }
            };

            // process_linked_field_blocks validates @immutable fields BEFORE
            // persisting any field, so a rejected composite leaves no partial
            // write. A change is a deterministic content rejection: skip terminally
            // (and roll back) rather than retry.
            match self
                .process_linked_field_blocks(&mut datastore, &headstore, &context, &mut state)
                .await
            {
                Ok(Some(outcome)) => Ok(Some(outcome)),
                Ok(None) => match self
                    .persist_merged_document(&mut datastore, &systemstore, &context, &mut state)
                    .await
                {
                    Ok(()) => Ok(None),
                    Err(MergeError::UniqueConstraintViolation(reason)) => {
                        Ok(Some(MergeOutcome::rejected(reason)))
                    }
                    Err(e) => Err(e),
                },
                Err(MergeError::ImmutableFieldChanged(reason)) => {
                    Ok(Some(MergeOutcome::terminal_skip(reason)))
                }
                Err(e) => Err(e),
            }
        };

        match process_result {
            Ok(Some(outcome)) => {
                txn.force_discard()?;
                if outcome.is_terminal_skip() && !from_collection {
                    if let Some(bus) = self.db.event_bus() {
                        let merge_complete = MergeCompleteData {
                            doc_id: doc_id_str.to_string(),
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
                if let Ok(systemstore) = txn.systemstore() {
                    self.record_block_ownership(
                        &systemstore,
                        doc_id_str,
                        cid,
                        block,
                        &state.owned_field_cids,
                        &state.linked_encryption_cids,
                    )
                    .await?;
                }

                let event_block = block.to_dag_cbor().map_err(|error| {
                    MergeError::BlockDecode(format!(
                        "Failed to encode merged composite update block: {}",
                        error
                    ))
                })?;
                txn.force_commit().await?;

                self.best_effort_finalize_linked_field_blocks(&state.linked_field_cids)
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
                        hook.post_commit_action(doc_id_str, collection.schema(), metadata)
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
                        doc_id_str.to_string(),
                        *cid,
                        payload.schema_version_id.clone(),
                        event_block,
                        false,
                        true,
                    );
                    bus.publish(Message::update(update));

                    if !from_collection {
                        let merge_complete = MergeCompleteData {
                            doc_id: doc_id_str.to_string(),
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
                            subject_doc_id: Some(doc_id_str.to_string()),
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
        systemstore: &NamespaceView,
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
        self.ensure_merge_depth(cid, depth)?;
        if self.has_merged_composite(cid) || Self::has_batch_merged_composite(batch_merged, cid) {
            return Ok(MergeOutcome::terminal_skip("already merged"));
        }

        // Every composite in a valid ancestry belongs to the root document;
        // the standalone path likewise carries one root identity through all
        // frames. Resolve it against the shared transaction so mappings staged
        // earlier in the batch are visible. Resolving every Enter frame afresh
        // turns a depth-N replay into O(N^2) ancestry reads.
        let doc_id = self
            .resolve_composite_doc_id_in_txn(systemstore, cid, block, depth)
            .await?;
        let mut frames = vec![CompositeMergeFrame::Enter {
            cid: *cid,
            block: Some(block.clone()),
            payload: Some(payload.clone()),
            child_cid: None,
            depth,
            is_root: true,
        }];

        while let Some(frame) = frames.pop() {
            match frame {
                CompositeMergeFrame::Enter {
                    cid,
                    block,
                    payload,
                    child_cid,
                    depth,
                    is_root,
                } => {
                    self.ensure_merge_depth(&cid, depth)?;
                    if self.has_merged_composite(&cid)
                        || Self::has_batch_merged_composite(batch_merged, &cid)
                    {
                        if is_root {
                            return Ok(MergeOutcome::terminal_skip("already merged"));
                        }
                        continue;
                    }

                    let block = match block {
                        Some(block) => block,
                        None => {
                            let Some(block) = self
                                .load_parent_composite(
                                    &cid,
                                    &child_cid.expect("parent frame has a child CID"),
                                )
                                .await
                            else {
                                continue;
                            };
                            block
                        }
                    };
                    let payload = match payload {
                        Some(payload) => payload,
                        None => {
                            let CrdtDelta::Composite(payload) = &block.delta else {
                                continue;
                            };
                            payload.clone()
                        }
                    };
                    let doc_id = doc_id.clone();

                    match self
                        .prepare_composite_merge(
                            &cid,
                            &block,
                            &payload,
                            metadata,
                            &doc_id,
                            CompositeMergeMode::Batch,
                        )
                        .await?
                    {
                        CompositeMergePreparation::Ready(collection) => {
                            let heads = block.heads.clone();
                            frames.push(CompositeMergeFrame::Exit {
                                cid,
                                block,
                                payload,
                                doc_id,
                                collection,
                                is_root,
                            });
                            if let Some(heads) = heads {
                                for parent_cid in heads.into_iter().rev() {
                                    frames.push(CompositeMergeFrame::Enter {
                                        cid: parent_cid,
                                        block: None,
                                        payload: None,
                                        child_cid: Some(cid),
                                        depth: depth + 1,
                                        is_root: false,
                                    });
                                }
                            }
                        }
                        CompositeMergePreparation::Complete(outcome) => {
                            if is_root || !outcome.is_terminal_skip() {
                                return Ok(outcome);
                            }
                        }
                    }
                }
                CompositeMergeFrame::Exit {
                    cid,
                    block,
                    payload,
                    doc_id,
                    collection,
                    is_root,
                } => {
                    if self.has_merged_composite(&cid)
                        || Self::has_batch_merged_composite(batch_merged, &cid)
                    {
                        if is_root {
                            return Ok(MergeOutcome::terminal_skip("already merged"));
                        }
                        continue;
                    }
                    let outcome = self
                        .process_composite_delta_in_txn_body(
                            datastore,
                            headstore,
                            systemstore,
                            &cid,
                            &block,
                            &payload,
                            metadata,
                            from_collection,
                            batch_merged,
                            pending_events,
                            pending_post_commit_actions,
                            pending_field_block_finalizations,
                            &doc_id,
                            collection.map(|collection| *collection),
                        )
                        .await?;
                    if is_root || (!outcome.is_merged() && !outcome.is_terminal_skip()) {
                        return Ok(outcome);
                    }
                }
            }
        }

        Ok(MergeOutcome::terminal_skip("already merged"))
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_composite_delta_in_txn_body(
        &self,
        datastore: &NamespaceView,
        headstore: &NamespaceView,
        systemstore: &NamespaceView,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CompositeDeltaPayload,
        metadata: &BlockMetadata<'_>,
        from_collection: bool,
        batch_merged: &std::sync::Mutex<HashSet<Cid>>,
        pending_events: &std::sync::Mutex<Vec<PendingMergeEvent>>,
        pending_post_commit_actions: &std::sync::Mutex<Vec<PendingPostCommitAction>>,
        pending_field_block_finalizations: &std::sync::Mutex<Vec<PendingFieldBlockFinalization>>,
        doc_id_str: &str,
        collection_lookup: Option<Collection>,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        let doc_short_id = {
            let collection = collection_lookup.as_ref().ok_or_else(|| {
                MergeError::MissingMetadata(format!(
                    "Collection not found for schema_version_id: {}",
                    payload.schema_version_id
                ))
            })?;
            self.db
                .resolve_or_allocate_doc_short_id(
                    systemstore,
                    collection.resolved_root_id(),
                    doc_id_str,
                )
                .await
                .map_err(MergeError::Database)?
        };
        let context = CompositeMergeContext::new(
            cid,
            block,
            payload,
            metadata,
            doc_id_str,
            doc_short_id,
            collection_lookup.clone(),
            CompositeMergeMode::Batch,
        );
        let mut state = CompositeMergeState::default();

        let process_result: std::result::Result<Option<MergeOutcome>, MergeError> = {
            let mut datastore = datastore.clone();

            // process_linked_field_blocks validates @immutable fields BEFORE
            // persisting any field, so an immutable rejection leaves no partial
            // write in the shared batch txn and can terminally skip in place.
            // A unique-index rejection is different: it is detected AFTER
            // persist_merged_document has staged the doc in the shared txn,
            // which cannot roll back a single block — so its Rejected outcome
            // must poison the whole batch attempt. try_batch_merge discards
            // the txn and falls back to per-block processing, where the
            // standalone path's per-CID txn discards cleanly. Both are
            // deterministic content rejections, not retries.
            match self
                .process_linked_field_blocks(&mut datastore, headstore, &context, &mut state)
                .await
            {
                Ok(Some(outcome)) => Ok(Some(outcome)),
                Ok(None) => match self
                    .persist_merged_document(&mut datastore, systemstore, &context, &mut state)
                    .await
                {
                    Ok(()) => Ok(None),
                    Err(MergeError::UniqueConstraintViolation(reason)) => {
                        Ok(Some(MergeOutcome::rejected(reason)))
                    }
                    Err(e) => Err(e),
                },
                Err(MergeError::ImmutableFieldChanged(reason)) => {
                    Ok(Some(MergeOutcome::terminal_skip(reason)))
                }
                Err(e) => Err(e),
            }
        };

        match process_result {
            Ok(Some(outcome)) => {
                if outcome.is_terminal_skip() && !from_collection {
                    let merge_complete = MergeCompleteData {
                        doc_id: doc_id_str.to_string(),
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
                self.record_block_ownership(
                    systemstore,
                    doc_id_str,
                    cid,
                    block,
                    &state.owned_field_cids,
                    &state.linked_encryption_cids,
                )
                .await?;

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
                        });
                }

                if let (Some(collection), Some(hook)) =
                    (context.collection.as_ref(), self.composite_merge_hook())
                {
                    if let Some(action) =
                        hook.post_commit_action(doc_id_str, collection.schema(), metadata)
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
                        doc_id_str.to_string(),
                        *cid,
                        payload.schema_version_id.clone(),
                        block.to_dag_cbor().map_err(|error| {
                            MergeError::BlockDecode(format!(
                                "Failed to encode merged composite update block: {}",
                                error
                            ))
                        })?,
                        false,
                        true,
                    );
                    pending_events_guard.push(PendingMergeEvent {
                        message: Message::update(update),
                    });

                    if !from_collection {
                        let merge_complete = MergeCompleteData {
                            doc_id: doc_id_str.to_string(),
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
                            subject_doc_id: Some(doc_id_str.to_string()),
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
