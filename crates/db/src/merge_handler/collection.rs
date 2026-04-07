use super::batch::{PendingMergeEvent, PendingPostCommitAction};
use super::*;

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    /// Process a Collection delta from a block.
    ///
    /// Collection blocks are metadata containers that link to document composite
    /// blocks. The collection CRDT merge itself is a no-op (matching Go behavior).
    /// The real work is:
    /// 1. Recursively process parent collection blocks from `heads`
    /// 2. Process each linked document composite via `process_composite_delta`
    /// 3. Update the collection headstore with the new head CID
    pub(crate) async fn process_collection_delta(
        &self,
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CollectionDeltaPayload,
        metadata: &BlockMetadata<'_>,
        depth: usize,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        if depth >= super::MAX_MERGE_DEPTH {
            return Err(MergeError::depth_exceeded(cid, depth));
        }

        tracing::debug!(
            cid = %cid,
            schema_version = %payload.schema_version_id,
            priority = payload.priority,
            links_count = block.links.as_ref().map(|l| l.len()).unwrap_or(0),
            heads_count = block.heads.as_ref().map(|h| h.len()).unwrap_or(0),
            "Processing Collection delta"
        );

        // Recursively process parent collection blocks from `heads` before
        // this block, ensuring older documents are merged first.
        if let Some(heads) = &block.heads {
            for head_cid in heads {
                let head_data = match self.blockstore.get(head_cid).await {
                    Ok(Some(data)) => data,
                    Ok(None) => {
                        tracing::debug!(
                            parent_cid = %head_cid,
                            child_cid = %cid,
                            "Parent collection block not in blockstore, skipping"
                        );
                        continue;
                    }
                    Err(e) => {
                        tracing::debug!(
                            parent_cid = %head_cid,
                            error = %e,
                            "Failed to load parent collection block, skipping"
                        );
                        continue;
                    }
                };

                let head_block = match Block::from_dag_cbor(&head_data) {
                    Ok(b) => b,
                    Err(_) => continue,
                };

                if let CrdtDelta::Collection(head_payload) = &head_block.delta {
                    tracing::info!(
                        parent_cid = %head_cid,
                        child_cid = %cid,
                        "Recursively merging parent collection block"
                    );
                    match Box::pin(self.process_collection_delta(
                        head_cid,
                        &head_block,
                        head_payload,
                        metadata,
                        depth + 1,
                    ))
                    .await
                    {
                        Ok(MergeOutcome::Merged) => {}
                        Ok(outcome) if outcome.is_terminal_skip() => {}
                        Ok(outcome) => return Ok(outcome),
                        Err(e) => {
                            tracing::debug!(
                                parent_cid = %head_cid,
                                error = %e,
                                "Parent collection merge failed"
                            );
                        }
                    }
                }
            }
        }

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
                        let doc_id_str =
                            String::from_utf8_lossy(&composite_payload.doc_id).to_string();
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
        let collection_id = metadata.collection_id.unwrap_or(&payload.schema_version_id);
        let short_id = collection_short_id(collection_id);

        let txn = self.db.new_txn(false).await?;
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
        cid: &Cid,
        block: &Block,
        payload: &defra_core::block::CollectionDeltaPayload,
        metadata: &BlockMetadata<'_>,
        batch_merged: &std::sync::Mutex<HashSet<Cid>>,
        pending_events: &std::sync::Mutex<Vec<PendingMergeEvent>>,
        pending_post_commit_actions: &std::sync::Mutex<Vec<PendingPostCommitAction>>,
        depth: usize,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        if depth >= super::MAX_MERGE_DEPTH {
            return Err(MergeError::depth_exceeded(cid, depth));
        }

        tracing::debug!(
            cid = %cid,
            schema_version = %payload.schema_version_id,
            "Processing Collection delta in batch txn"
        );

        // Recursively process parent collection blocks
        if let Some(heads) = &block.heads {
            for head_cid in heads {
                let head_data = match self.blockstore.get(head_cid).await {
                    Ok(Some(data)) => data,
                    _ => continue,
                };

                let head_block = match Block::from_dag_cbor(&head_data) {
                    Ok(b) => b,
                    Err(_) => continue,
                };

                if let CrdtDelta::Collection(head_payload) = &head_block.delta {
                    match Box::pin(self.process_collection_delta_in_txn(
                        datastore,
                        headstore,
                        head_cid,
                        &head_block,
                        head_payload,
                        metadata,
                        batch_merged,
                        pending_events,
                        pending_post_commit_actions,
                        depth + 1,
                    ))
                    .await
                    {
                        Ok(MergeOutcome::Merged) => {}
                        Ok(outcome) if outcome.is_terminal_skip() => {}
                        Ok(outcome) => return Ok(outcome),
                        Err(e) => {
                            tracing::debug!(
                                parent_cid = %head_cid,
                                error = %e,
                                "Parent collection merge failed in batch"
                            );
                        }
                    }
                }
            }
        }

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
                    let doc_id_str = String::from_utf8_lossy(&composite_payload.doc_id).to_string();
                    match self
                        .process_composite_delta_in_txn(
                            datastore,
                            headstore,
                            link_cid,
                            &linked_block,
                            composite_payload,
                            metadata,
                            true,
                            batch_merged,
                            pending_events,
                            pending_post_commit_actions,
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
        let short_id = collection_short_id(collection_id);

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
