use super::batch::{PendingFieldBlockFinalization, PendingMergeEvent, PendingPostCommitAction};
use super::*;

enum CollectionMergeFrame {
    Enter {
        cid: Cid,
        block: Option<Block>,
        payload: Option<defra_core::block::CollectionDeltaPayload>,
        child_cid: Option<Cid>,
        depth: usize,
        is_root: bool,
    },
    Exit {
        cid: Cid,
        block: Block,
        payload: defra_core::block::CollectionDeltaPayload,
        depth: usize,
        is_root: bool,
    },
}

impl<S: Store, B: blockstore::Blockstore> DbMergeHandler<S, B> {
    fn has_merged_collection(&self, cid: &Cid) -> bool {
        self.merged_collections
            .lock()
            .unwrap_or_else(|error| {
                tracing::warn!("merged_collections lock poisoned, recovering");
                error.into_inner()
            })
            .contains(cid)
    }

    fn has_batch_merged_collection(
        batch_merged_collections: &std::sync::Mutex<HashSet<Cid>>,
        cid: &Cid,
    ) -> bool {
        batch_merged_collections
            .lock()
            .unwrap_or_else(|error| {
                tracing::warn!("batch_merged_collections lock poisoned, recovering");
                error.into_inner()
            })
            .contains(cid)
    }

    async fn load_parent_collection(&self, parent_cid: &Cid, child_cid: &Cid) -> Option<Block> {
        let data = match self.blockstore.get(parent_cid).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                tracing::debug!(
                    %parent_cid,
                    %child_cid,
                    "Parent collection block not in blockstore, skipping"
                );
                return None;
            }
            Err(error) => {
                tracing::debug!(
                    %parent_cid,
                    %child_cid,
                    %error,
                    "Failed to load parent collection block, skipping"
                );
                return None;
            }
        };

        Block::from_dag_cbor(&data).ok()
    }

    /// Process a Collection delta from a block.
    ///
    /// Collection blocks are metadata containers that link to document composite
    /// blocks. The collection CRDT merge itself is a no-op (matching Go behavior).
    /// The real work is:
    /// 1. Process parent collection blocks from `heads`, oldest first
    /// 2. Process each linked document composite via `process_composite_delta`
    /// 3. Update the collection headstore with the new head CID
    pub async fn process_collection_delta(
        &self,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CollectionDeltaPayload,
        metadata: &BlockMetadata<'_>,
        depth: usize,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        let mut frames = vec![CollectionMergeFrame::Enter {
            cid: *cid,
            block: Some(block.clone()),
            payload: Some(payload.clone()),
            child_cid: None,
            depth,
            is_root: true,
        }];

        while let Some(frame) = frames.pop() {
            match frame {
                CollectionMergeFrame::Enter {
                    cid,
                    block,
                    payload,
                    child_cid,
                    depth,
                    is_root,
                } => {
                    if let Err(error) = self.ensure_merge_depth(&cid, depth) {
                        if is_root {
                            return Err(error);
                        }
                        tracing::debug!(
                            parent_cid = %cid,
                            %error,
                            "Parent collection merge failed"
                        );
                        continue;
                    }
                    if self.has_merged_collection(&cid) {
                        if is_root {
                            return Ok(MergeOutcome::terminal_skip("collection already merged"));
                        }
                        continue;
                    }

                    let block = match block {
                        Some(block) => block,
                        None => {
                            let Some(block) = self
                                .load_parent_collection(
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
                            let CrdtDelta::Collection(payload) = &block.delta else {
                                continue;
                            };
                            payload.clone()
                        }
                    };

                    tracing::debug!(
                        %cid,
                        schema_version = %payload.schema_version_id,
                        priority = payload.priority,
                        links_count = block.links.as_ref().map(|links| links.len()).unwrap_or(0),
                        heads_count = block.heads.as_ref().map(|heads| heads.len()).unwrap_or(0),
                        "Processing Collection delta"
                    );

                    let heads = block.heads.clone();
                    frames.push(CollectionMergeFrame::Exit {
                        cid,
                        block,
                        payload,
                        depth,
                        is_root,
                    });
                    if let Some(heads) = heads {
                        for parent_cid in heads.into_iter().rev() {
                            frames.push(CollectionMergeFrame::Enter {
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
                CollectionMergeFrame::Exit {
                    cid,
                    block,
                    payload,
                    depth,
                    is_root,
                } => {
                    if self.has_merged_collection(&cid) {
                        if is_root {
                            return Ok(MergeOutcome::terminal_skip("collection already merged"));
                        }
                        continue;
                    }
                    let outcome = match self
                        .process_collection_delta_body(&cid, &block, &payload, metadata, depth)
                        .await
                    {
                        Ok(outcome) => outcome,
                        Err(error) if !is_root => {
                            tracing::debug!(
                                parent_cid = %cid,
                                %error,
                                "Parent collection merge failed"
                            );
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    if is_root || (!outcome.is_merged() && !outcome.is_terminal_skip()) {
                        return Ok(outcome);
                    }
                }
            }
        }

        Ok(MergeOutcome::terminal_skip("collection already merged"))
    }

    async fn process_collection_delta_body(
        &self,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CollectionDeltaPayload,
        metadata: &BlockMetadata<'_>,
        depth: usize,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        // Process linked document composites
        let mut any_merged = false;
        let mut retryable_skip: Option<MergeOutcome> = None;
        if let Some(links) = &block.links {
            for dag_link in links {
                let link_cid = &dag_link.link;

                tracing::debug!(
                    collection_cid = %cid,
                    link_cid = %link_cid,
                    link_name = %dag_link.name,
                    "Processing linked block from Collection delta"
                );

                let linked_data = match self.blockstore.get(link_cid).await {
                    Ok(Some(data)) => data,
                    Ok(None) => {
                        tracing::warn!(
                            link_cid = %link_cid,
                            "Linked block not found in blockstore"
                        );
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(
                            link_cid = %link_cid,
                            error = %e,
                            "Failed to load linked block"
                        );
                        continue;
                    }
                };

                let linked_block = match Block::from_dag_cbor(&linked_data) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(
                            link_cid = %link_cid,
                            error = %e,
                            "Failed to decode linked block"
                        );
                        continue;
                    }
                };

                tracing::debug!(
                    link_cid = %link_cid,
                    delta_type = ?std::mem::discriminant(&linked_block.delta),
                    "Processing linked block from Collection"
                );

                match &linked_block.delta {
                    CrdtDelta::Composite(composite_payload) => {
                        let doc_id_str = self
                            .resolve_composite_doc_id(link_cid, &linked_block, depth + 1)
                            .await?;
                        tracing::debug!(
                            link_cid = %link_cid,
                            doc_id = %doc_id_str,
                            "Processing linked composite from Collection"
                        );
                        match self
                            .process_composite_delta(
                                link_cid,
                                &linked_block,
                                composite_payload,
                                metadata,
                                true, // from_collection: skip local collection block creation
                                depth + 1,
                            )
                            .await
                        {
                            Ok(MergeOutcome::Merged) => {
                                tracing::debug!(link_cid = %link_cid, "Composite merged successfully");
                                any_merged = true;

                                // Publish per-document MergeComplete so the Go test
                                // framework's WaitForSync can track each document.
                                if let Some(bus) = self.db.event_bus() {
                                    let col_id = metadata
                                        .collection_id
                                        .unwrap_or(&payload.schema_version_id)
                                        .to_string();
                                    bus.publish(Message::merge_complete(MergeCompleteData {
                                        doc_id: doc_id_str,
                                        subject_doc_id: None,
                                        cid: *link_cid,
                                        collection_id: col_id,
                                        by_peer: metadata.sender_peer.unwrap_or("").to_string(),
                                    }));
                                }
                            }
                            Ok(outcome) if outcome.is_terminal_skip() => {
                                tracing::debug!(
                                    link_cid = %link_cid,
                                    outcome = ?outcome,
                                    "Composite skipped"
                                );
                                if let Some(bus) = self.db.event_bus() {
                                    let col_id = metadata
                                        .collection_id
                                        .unwrap_or(&payload.schema_version_id)
                                        .to_string();
                                    bus.publish(Message::merge_complete(MergeCompleteData {
                                        doc_id: doc_id_str,
                                        subject_doc_id: None,
                                        cid: *link_cid,
                                        collection_id: col_id,
                                        by_peer: metadata.sender_peer.unwrap_or("").to_string(),
                                    }));
                                }
                            }
                            Ok(outcome) => {
                                tracing::debug!(
                                    link_cid = %link_cid,
                                    outcome = ?outcome,
                                    "Composite skipped and will be retried"
                                );
                                retryable_skip.get_or_insert(outcome);
                            }
                            Err(e) => {
                                tracing::debug!(link_cid = %link_cid, error = %e, "Composite merge failed");
                            }
                        }
                    }
                    other => {
                        tracing::debug!(
                            link_cid = %link_cid,
                            delta_type = ?std::mem::discriminant(other),
                            "Skipping non-composite link"
                        );
                    }
                }
            }
        }

        if let Some(outcome) = retryable_skip {
            return Ok(outcome);
        }

        // Update collection headstore using proper head merging.
        // Only remove heads that this block explicitly supersedes (listed in block.heads),
        // preserving concurrent branches for later merge via write_collection_block.
        let txn = self.db.new_txn(false).await?;
        let collection_id = metadata.collection_id.unwrap_or(&payload.schema_version_id);
        let short_id = if let Ok(systemstore) = txn.systemstore() {
            crate::collection::require_persisted_collection_short_id(&systemstore, collection_id)
                .await?
        } else {
            return Err(MergeError::Database(crate::error::Error::Other(
                "failed to access systemstore while resolving collection root_id".to_string(),
            )));
        };
        if let Ok(headstore) = txn.headstore() {
            // Remove only the heads that this block supersedes (its parents).
            // This preserves concurrent branches in the headstore.
            if let Some(heads) = &block.heads {
                for parent_cid in heads {
                    let parent_key =
                        storage::keys::headstore::HeadstoreColKey::new(short_id, *parent_cid);
                    let _ = headstore
                        .delete(
                            &<storage::keys::headstore::HeadstoreColKey as storage::corekv::Key>::bytes(
                                &parent_key,
                            ),
                        )
                        .await;
                }
            }

            // Add the new collection head (idempotent if already exists)
            let col_key = storage::keys::headstore::HeadstoreColKey::new(short_id, *cid);
            let priority_bytes = encode_priority_varint(payload.priority);
            if let Err(e) = headstore
                .set(
                    &<storage::keys::headstore::HeadstoreColKey as storage::corekv::Key>::bytes(
                        &col_key,
                    ),
                    &priority_bytes,
                )
                .await
            {
                tracing::warn!(
                    error = %e,
                    collection_id = %collection_id,
                    "Failed to write collection head to headstore"
                );
            }
        }
        txn.force_commit().await?;

        tracing::info!(
            cid = %cid,
            collection_id = %collection_id,
            short_id = short_id,
            any_merged = any_merged,
            "Collection delta processed"
        );

        {
            let mut merged = self.merged_collections.lock().unwrap_or_else(|e| {
                tracing::warn!("merged_collections lock poisoned, recovering");
                e.into_inner()
            });
            merged.insert(*cid);
        }

        if any_merged {
            Ok(MergeOutcome::Merged)
        } else {
            Ok(MergeOutcome::terminal_skip(
                "no linked composites needed merging",
            ))
        }
    }

    /// Process a Collection delta within a shared transaction (batch mode).
    ///
    /// Same logic as `process_collection_delta` but uses a shared transaction
    /// and delegates to `process_composite_delta_in_txn` for linked composites.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn process_collection_delta_in_txn(
        &self,
        datastore: &NamespaceView,
        headstore: &NamespaceView,
        systemstore: &NamespaceView,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CollectionDeltaPayload,
        metadata: &BlockMetadata<'_>,
        batch_merged: &std::sync::Mutex<HashSet<Cid>>,
        batch_merged_collections: &std::sync::Mutex<HashSet<Cid>>,
        pending_events: &std::sync::Mutex<Vec<PendingMergeEvent>>,
        pending_post_commit_actions: &std::sync::Mutex<Vec<PendingPostCommitAction>>,
        pending_field_block_finalizations: &std::sync::Mutex<Vec<PendingFieldBlockFinalization>>,
        depth: usize,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        let mut frames = vec![CollectionMergeFrame::Enter {
            cid: *cid,
            block: Some(block.clone()),
            payload: Some(payload.clone()),
            child_cid: None,
            depth,
            is_root: true,
        }];

        while let Some(frame) = frames.pop() {
            match frame {
                CollectionMergeFrame::Enter {
                    cid,
                    block,
                    payload,
                    child_cid,
                    depth,
                    is_root,
                } => {
                    if let Err(error) = self.ensure_merge_depth(&cid, depth) {
                        if is_root {
                            return Err(error);
                        }
                        tracing::debug!(
                            parent_cid = %cid,
                            %error,
                            "Parent collection merge failed in batch"
                        );
                        continue;
                    }
                    if self.has_merged_collection(&cid) {
                        if is_root {
                            return Ok(MergeOutcome::terminal_skip("collection already merged"));
                        }
                        continue;
                    }
                    if Self::has_batch_merged_collection(batch_merged_collections, &cid) {
                        if is_root {
                            return Ok(MergeOutcome::terminal_skip(
                                "collection already merged in batch",
                            ));
                        }
                        continue;
                    }

                    let block = match block {
                        Some(block) => block,
                        None => {
                            let Some(block) = self
                                .load_parent_collection(
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
                            let CrdtDelta::Collection(payload) = &block.delta else {
                                continue;
                            };
                            payload.clone()
                        }
                    };

                    tracing::debug!(
                        %cid,
                        schema_version = %payload.schema_version_id,
                        "Processing Collection delta in batch txn"
                    );

                    let heads = block.heads.clone();
                    frames.push(CollectionMergeFrame::Exit {
                        cid,
                        block,
                        payload,
                        depth,
                        is_root,
                    });
                    if let Some(heads) = heads {
                        for parent_cid in heads.into_iter().rev() {
                            frames.push(CollectionMergeFrame::Enter {
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
                CollectionMergeFrame::Exit {
                    cid,
                    block,
                    payload,
                    depth,
                    is_root,
                } => {
                    if self.has_merged_collection(&cid)
                        || Self::has_batch_merged_collection(batch_merged_collections, &cid)
                    {
                        if is_root {
                            return Ok(MergeOutcome::terminal_skip("collection already merged"));
                        }
                        continue;
                    }
                    let outcome = match self
                        .process_collection_delta_in_txn_body(
                            datastore,
                            headstore,
                            systemstore,
                            &cid,
                            &block,
                            &payload,
                            metadata,
                            batch_merged,
                            batch_merged_collections,
                            pending_events,
                            pending_post_commit_actions,
                            pending_field_block_finalizations,
                            depth,
                        )
                        .await
                    {
                        Ok(outcome) => outcome,
                        Err(error) if !is_root => {
                            tracing::debug!(
                                parent_cid = %cid,
                                %error,
                                "Parent collection merge failed in batch"
                            );
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    if is_root || (!outcome.is_merged() && !outcome.is_terminal_skip()) {
                        return Ok(outcome);
                    }
                }
            }
        }

        Ok(MergeOutcome::terminal_skip("collection already merged"))
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_collection_delta_in_txn_body(
        &self,
        datastore: &NamespaceView,
        headstore: &NamespaceView,
        systemstore: &NamespaceView,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CollectionDeltaPayload,
        metadata: &BlockMetadata<'_>,
        batch_merged: &std::sync::Mutex<HashSet<Cid>>,
        batch_merged_collections: &std::sync::Mutex<HashSet<Cid>>,
        pending_events: &std::sync::Mutex<Vec<PendingMergeEvent>>,
        pending_post_commit_actions: &std::sync::Mutex<Vec<PendingPostCommitAction>>,
        pending_field_block_finalizations: &std::sync::Mutex<Vec<PendingFieldBlockFinalization>>,
        depth: usize,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        // Process linked document composites
        let mut any_merged = false;
        let mut retryable_skip: Option<MergeOutcome> = None;
        if let Some(links) = &block.links {
            for dag_link in links {
                let link_cid = &dag_link.link;

                let linked_data = match self.blockstore.get(link_cid).await {
                    Ok(Some(data)) => data,
                    _ => continue,
                };

                let linked_block = match Block::from_dag_cbor(&linked_data) {
                    Ok(b) => b,
                    Err(_) => continue,
                };

                if let CrdtDelta::Composite(composite_payload) = &linked_block.delta {
                    let doc_id_str = self
                        .resolve_composite_doc_id(link_cid, &linked_block, depth + 1)
                        .await?;
                    match self
                        .process_composite_delta_in_txn(
                            datastore,
                            headstore,
                            systemstore,
                            link_cid,
                            &linked_block,
                            composite_payload,
                            metadata,
                            true,
                            batch_merged,
                            batch_merged_collections,
                            pending_events,
                            pending_post_commit_actions,
                            pending_field_block_finalizations,
                            depth + 1,
                        )
                        .await
                    {
                        Ok(MergeOutcome::Merged) => {
                            any_merged = true;
                            // Collect per-document MergeComplete event
                            let col_id = metadata
                                .collection_id
                                .unwrap_or(&payload.schema_version_id)
                                .to_string();
                            let mut pe = pending_events.lock().unwrap();
                            pe.push(PendingMergeEvent {
                                message: Message::merge_complete(MergeCompleteData {
                                    doc_id: doc_id_str,
                                    subject_doc_id: None,
                                    cid: *link_cid,
                                    collection_id: col_id,
                                    by_peer: metadata.sender_peer.unwrap_or("").to_string(),
                                }),
                            });
                        }
                        Ok(outcome) if outcome.is_terminal_skip() => {
                            tracing::debug!(link_cid = %link_cid, outcome = ?outcome, "Composite skipped in batch");
                            let col_id = metadata
                                .collection_id
                                .unwrap_or(&payload.schema_version_id)
                                .to_string();
                            let mut pe = pending_events.lock().unwrap_or_else(|e| {
                                tracing::warn!("pending_events lock poisoned, recovering");
                                e.into_inner()
                            });
                            pe.push(PendingMergeEvent {
                                message: Message::merge_complete(MergeCompleteData {
                                    doc_id: doc_id_str,
                                    subject_doc_id: None,
                                    cid: *link_cid,
                                    collection_id: col_id,
                                    by_peer: metadata.sender_peer.unwrap_or("").to_string(),
                                }),
                            });
                        }
                        Ok(outcome) => {
                            tracing::debug!(link_cid = %link_cid, outcome = ?outcome, "Composite skipped in batch and will be retried");
                            retryable_skip.get_or_insert(outcome);
                        }
                        Err(e) => {
                            tracing::debug!(link_cid = %link_cid, error = %e, "Composite merge failed in batch");
                        }
                    }
                }
            }
        }

        if let Some(outcome) = retryable_skip {
            return Ok(outcome);
        }

        // Update collection headstore using the shared headstore view
        let collection_id = metadata.collection_id.unwrap_or(&payload.schema_version_id);
        let short_id = {
            let txn = self.db.new_txn(true).await?;
            let short_id = if let Ok(systemstore) = txn.systemstore() {
                crate::collection::require_persisted_collection_short_id(
                    &systemstore,
                    collection_id,
                )
                .await?
            } else {
                return Err(MergeError::Database(crate::error::Error::Other(
                    "failed to access systemstore while resolving collection root_id".to_string(),
                )));
            };
            let _ = txn.discard();
            short_id
        };

        {
            let mut batch_merged_guard = batch_merged_collections.lock().unwrap_or_else(|e| {
                tracing::warn!("batch_merged_collections lock poisoned, recovering");
                e.into_inner()
            });
            batch_merged_guard.insert(*cid);
        }

        {
            if let Some(heads) = &block.heads {
                for parent_cid in heads {
                    let parent_key =
                        storage::keys::headstore::HeadstoreColKey::new(short_id, *parent_cid);
                    let _ = headstore
                        .delete(
                            &<storage::keys::headstore::HeadstoreColKey as storage::corekv::Key>::bytes(
                                &parent_key,
                            ),
                        )
                        .await;
                }
            }

            let col_key = storage::keys::headstore::HeadstoreColKey::new(short_id, *cid);
            let priority_bytes = encode_priority_varint(payload.priority);
            let _ = headstore
                .set(
                    &<storage::keys::headstore::HeadstoreColKey as storage::corekv::Key>::bytes(
                        &col_key,
                    ),
                    &priority_bytes,
                )
                .await;
        }

        if any_merged {
            Ok(MergeOutcome::Merged)
        } else {
            Ok(MergeOutcome::terminal_skip(
                "no linked composites needed merging",
            ))
        }
    }
}
