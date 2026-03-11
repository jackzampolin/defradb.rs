use super::batch::{PendingMergeEvent, PendingPostCommitAction};
use super::*;

/// Marker byte indicating a document is deleted (matches Go's DeletedObjectMarker).
const DELETED_MARKER: u8 = 0x01;

/// Build the deletion marker key: /del/{collection_id}/{doc_id}
fn build_deleted_key(collection_id: &str, doc_id: &str) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(b"/del/");
    key.extend_from_slice(collection_id.as_bytes());
    key.push(b'/');
    key.extend_from_slice(doc_id.as_bytes());
    key
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
                // Skip parents already processed by this or another path.
                {
                    let merged = self.merged_composites.lock().unwrap();
                    if merged.contains(head_cid) {
                        tracing::debug!(
                            parent_cid = %head_cid,
                            child_cid = %cid,
                            "Parent composite already merged, skipping recursive processing"
                        );
                        continue;
                    }
                }

                // Load the parent block from blockstore
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
                    Ok(b) => b,
                    Err(_) => continue,
                };

                if let CrdtDelta::Composite(head_payload) = &head_block.delta {
                    tracing::info!(
                        parent_cid = %head_cid,
                        child_cid = %cid,
                        "Recursively merging parent composite before current"
                    );
                    // Recursive call — the parent will in turn merge its own parents.
                    // Each composite opens its own transaction so ordering is safe.
                    // Box::pin is required because recursive async fns are unsized.
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

        // Create a SINGLE transaction for all field merges AND document storage
        let txn = self.db.new_txn(false).await?;

        let collection_for_policy = self
            .db
            .find_collection_by_id(&payload.schema_version_id)
            .ok()
            .flatten()
            .or_else(|| {
                metadata
                    .collection_id
                    .and_then(|cid| self.db.find_collection_by_id(cid).ok().flatten())
            });

        // Collect winning field values for document reconstruction
        // These are the values that WON conflict resolution, not just the incoming values
        let mut field_values: HashMap<String, NormalValue> = HashMap::new();
        let mut any_field_applied = false;
        let mut process_error: Option<MergeError> = None;
        let mut skip_outcome: Option<MergeOutcome> = None;
        let mut is_branchable = false;
        let mut encrypted_policy_checked = false;
        // Collect field block heads for proper headstore merging during concurrent updates
        let mut field_block_heads: HashMap<String, Vec<Cid>> = HashMap::new();

        // Process linked blocks within the transaction scope
        // Use a scoped block to ensure datastore is dropped before commit/discard
        {
            let mut datastore = match txn.datastore() {
                Ok(ds) => ds,
                Err(e) => {
                    let _ = txn.force_discard();
                    return Err(MergeError::Database(e));
                }
            };

            if let Some(links) = &block.links {
                tracing::info!(
                    cid = %cid,
                    links_count = links.len(),
                    "Processing linked blocks from Composite delta"
                );

                for dag_link in links {
                    let link_name = &dag_link.name;
                    let link_cid = &dag_link.link;

                    tracing::debug!(
                        parent_cid = %cid,
                        link_cid = %link_cid,
                        link_name = %link_name,
                        "Processing linked block"
                    );

                    // Load the linked block from storage
                    let linked_block_data = match self.blockstore.get(link_cid).await {
                        Ok(Some(data)) => data,
                        Ok(None) => {
                            tracing::error!(
                                parent_cid = %cid,
                                link_cid = %link_cid,
                                "Linked block not found in blockstore"
                            );
                            process_error = Some(MergeError::Storage(format!(
                                "Linked block {} not found in blockstore",
                                link_cid
                            )));
                            break;
                        }
                        Err(e) => {
                            tracing::error!(
                                parent_cid = %cid,
                                link_cid = %link_cid,
                                error = %e,
                                "Failed to load linked block from blockstore"
                            );
                            process_error = Some(MergeError::Storage(e.to_string()));
                            break;
                        }
                    };

                    // Decode and process the linked block
                    let linked_block = match Block::from_dag_cbor(&linked_block_data) {
                        Ok(b) => b,
                        Err(e) => {
                            process_error = Some(MergeError::BlockDecode(e.to_string()));
                            break;
                        }
                    };

                    // Collect field block heads for proper headstore merging.
                    // During concurrent updates, we must only delete the heads that
                    // this field block explicitly supersedes, not ALL heads for the field.
                    if let Some(heads) = &linked_block.heads {
                        field_block_heads.insert(link_name.clone(), heads.clone());
                    }

                    if linked_block.encryption.is_some() && !encrypted_policy_checked {
                        encrypted_policy_checked = true;
                        if let (Some(collection), Some(hook)) =
                            (collection_for_policy.as_ref(), self.composite_merge_hook())
                        {
                            if let Some(outcome) = hook
                                .on_encrypted_link(&doc_id_str, collection.schema(), metadata)
                                .await?
                            {
                                skip_outcome = Some(outcome);
                                break;
                            }
                        }
                    }

                    // Decrypt linked block data if it has encryption
                    let effective_linked_delta = match &linked_block.delta {
                        CrdtDelta::Lww(p) if linked_block.encryption.is_some() => {
                            match self
                                .decrypt_block_data(&p.data, linked_block.encryption.as_ref())
                                .await
                            {
                                Ok(decrypted) => {
                                    let mut dp = p.clone();
                                    dp.data = decrypted;
                                    CrdtDelta::Lww(dp)
                                }
                                Err(_) => linked_block.delta.clone(),
                            }
                        }
                        CrdtDelta::Counter(p) if linked_block.encryption.is_some() => {
                            match self
                                .decrypt_block_data(&p.data, linked_block.encryption.as_ref())
                                .await
                            {
                                Ok(decrypted) => {
                                    let mut dp = p.clone();
                                    dp.data = decrypted;
                                    CrdtDelta::Counter(dp)
                                }
                                Err(_) => linked_block.delta.clone(),
                            }
                        }
                        other => other.clone(),
                    };

                    match &effective_linked_delta {
                        CrdtDelta::Lww(lww_payload) => {
                            // Process the LWW delta within our transaction
                            match self
                                .process_lww_delta_in_txn(&mut datastore, link_cid, lww_payload)
                                .await
                            {
                                Ok(result) => {
                                    if result.applied {
                                        any_field_applied = true;
                                    }
                                    // Collect the WINNING value for document reconstruction
                                    if let Some(value) = result.value {
                                        field_values.insert(lww_payload.field_name.clone(), value);
                                    }
                                }
                                Err(e) => {
                                    process_error = Some(e);
                                    break;
                                }
                            }
                        }
                        CrdtDelta::Counter(counter_payload) => {
                            // Process the Counter delta within our transaction
                            match self
                                .process_counter_delta_in_txn(
                                    &mut datastore,
                                    link_cid,
                                    counter_payload,
                                    metadata.collection_id,
                                )
                                .await
                            {
                                Ok(result) => {
                                    if result.applied {
                                        any_field_applied = true;
                                    }
                                    // Collect the accumulated value for document reconstruction
                                    if let Some(value) = result.value {
                                        field_values
                                            .insert(counter_payload.field_name.clone(), value);
                                    }
                                }
                                Err(e) => {
                                    process_error = Some(e);
                                    break;
                                }
                            }
                        }
                        other => {
                            tracing::error!(
                                parent_cid = %cid,
                                link_cid = %link_cid,
                                delta_type = ?std::mem::discriminant(other),
                                "Unexpected delta type in linked block - expected LWW or Counter"
                            );
                            process_error = Some(MergeError::UnsupportedDelta(format!(
                                "Unexpected delta type in linked block: {:?}",
                                std::mem::discriminant(other)
                            )));
                            break;
                        }
                    }
                }
            }

            // Find the collection by schema version ID, with fallback to
            // the P2P metadata's collection_id (handles cross-version sync
            // where the incoming block's schema version differs from local)
            if process_error.is_none() {
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

                let is_delete = payload.status == 2;

                match collection_lookup {
                    Some(collection) => {
                        is_branchable = collection.schema().is_branchable;
                        if is_delete {
                            // Handle delete: remove index entries, then write deletion marker.
                            // Must load the old document first so we know which index
                            // entries to remove (Go's syncIndexedDoc does the same).
                            if let Ok(doc_id) = DocID::from_string(&doc_id_str) {
                                if let Ok(Some(old_doc)) =
                                    collection.get_with_datastore(&datastore, &doc_id).await
                                {
                                    let short_id = collection_short_id(collection.collection_id());
                                    if let Ok(index_manager) =
                                        IndexManager::from_collection(short_id, collection.schema())
                                    {
                                        if let Err(e) = index_manager
                                            .on_document_delete(
                                                &datastore,
                                                &old_doc,
                                                collection.schema(),
                                            )
                                            .await
                                        {
                                            process_error = Some(MergeError::MergeFailed(format!(
                                                "Failed to delete indexes after merge: {}",
                                                e
                                            )));
                                        }
                                    }
                                }
                            }

                            if process_error.is_none() {
                                let deleted_key =
                                    build_deleted_key(collection.collection_id(), &doc_id_str);
                                if let Err(e) = datastore.set(&deleted_key, &[DELETED_MARKER]).await
                                {
                                    process_error =
                                        Some(MergeError::Database(crate::error::Error::Storage(e)));
                                } else {
                                    tracing::info!(
                                        doc_id = %doc_id_str,
                                        collection = %collection.name(),
                                        "Deletion marker set after P2P merge"
                                    );
                                }
                            }
                        } else if !field_values.is_empty() {
                            // Store the reconstructed document within the same transaction.
                            // Load the existing document first so unmodified fields (e.g.
                            // foreign keys like _AuthorID) are preserved across partial
                            // updates that only touch a subset of fields.
                            match DocID::from_string(&doc_id_str) {
                                Ok(doc_id) => {
                                    let (mut doc, old_doc) = match collection
                                        .get_with_datastore(&datastore, &doc_id)
                                        .await
                                    {
                                        Ok(Some(existing)) => {
                                            let old = existing.clone();
                                            (existing, Some(old))
                                        }
                                        _ => {
                                            let mut new_doc = Document::new();
                                            new_doc.set_id(doc_id.clone());
                                            (new_doc, None)
                                        }
                                    };

                                    // Set the schema version from the incoming block so the
                                    // lensed fetcher can detect version mismatches and apply
                                    // migrations at query time (matches Go's composite merge).
                                    doc.set_schema_version_id(&payload.schema_version_id);

                                    // Overlay new/winning field values on top of existing fields
                                    for (field_name, value) in &field_values {
                                        doc.set(field_name, value.clone());
                                    }

                                    // Only store fields that the local collection knows about,
                                    // so cross-version syncs don't leak unknown fields into
                                    // query results.
                                    let known_fields: std::collections::HashSet<&str> = collection
                                        .schema()
                                        .fields
                                        .iter()
                                        .map(|f| f.name.as_str())
                                        .collect();
                                    let all_field_names: Vec<String> =
                                        doc.field_names().map(|s| s.to_string()).collect();
                                    for fname in &all_field_names {
                                        if !known_fields.contains(fname.as_str()) {
                                            doc.remove(fname);
                                        }
                                    }

                                    if let Err(e) =
                                        collection.save_with_datastore(&datastore, &doc).await
                                    {
                                        process_error = Some(MergeError::Database(e));
                                    } else {
                                        // Update indexes for the merged document.
                                        // Index failure blocks the transaction — index and document
                                        // storage must remain consistent.
                                        let short_id =
                                            collection_short_id(collection.collection_id());
                                        if let Ok(index_manager) = IndexManager::from_collection(
                                            short_id,
                                            collection.schema(),
                                        ) {
                                            let index_result = match &old_doc {
                                                Some(old) => {
                                                    index_manager
                                                        .on_document_update(
                                                            &datastore,
                                                            old,
                                                            &doc,
                                                            collection.schema(),
                                                        )
                                                        .await
                                                }
                                                None => {
                                                    index_manager
                                                        .on_document_create(
                                                            &datastore,
                                                            &doc,
                                                            collection.schema(),
                                                        )
                                                        .await
                                                }
                                            };
                                            if let Err(e) = index_result {
                                                process_error =
                                                    Some(MergeError::MergeFailed(format!(
                                                        "Failed to update indexes after merge: {}",
                                                        e
                                                    )));
                                            }
                                        }

                                        if process_error.is_none() {
                                            // Generate SE artifacts for replicated doc
                                            if let Some(enc_key) = self.se_enc_key() {
                                                if let Err(e) = se_merge::generate_merge_artifacts(
                                                    &mut datastore,
                                                    collection.schema(),
                                                    &doc_id_str,
                                                    &field_values,
                                                    enc_key,
                                                    None,
                                                )
                                                .await
                                                {
                                                    tracing::warn!(
                                                        doc_id = %doc_id_str,
                                                        error = %e,
                                                        "Failed to generate SE artifacts after merge"
                                                    );
                                                }
                                            }

                                            tracing::info!(
                                                doc_id = %doc_id_str,
                                                collection = %collection.name(),
                                                fields_count = field_values.len(),
                                                any_applied = any_field_applied,
                                                "Document stored for queries"
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    process_error = Some(MergeError::MergeFailed(format!(
                                        "Invalid doc_id: {}",
                                        e
                                    )));
                                }
                            }
                        }
                    }
                    None => {
                        process_error = Some(MergeError::MissingMetadata(format!(
                            "Collection not found for schema_version_id: {}",
                            payload.schema_version_id
                        )));
                    }
                }
            }
        } // datastore dropped here

        if let Some(outcome) = skip_outcome {
            txn.force_discard()?;
            return Ok(outcome);
        }

        // Write headstore entries so _version / _commits queries work on
        // the receiving node.  The headstore tracks the latest CID for each
        // field and for the composite ("C"), keyed by doc_id.
        //
        // IMPORTANT: Use proper head merging — only delete heads that this block
        // explicitly supersedes (listed in block.heads / field block heads).
        // This preserves concurrent branches during concurrent P2P updates.
        if process_error.is_none() {
            if let Ok(headstore) = txn.headstore() {
                let priority_bytes = encode_priority_varint(payload.priority);

                // Composite head: only delete heads listed in block.heads
                if let Some(heads) = &block.heads {
                    for parent_cid in heads {
                        let parent_key = storage::keys::headstore::HeadstoreDocKey::new(
                            &doc_id_str,
                            "C",
                            *parent_cid,
                        );
                        let _ = headstore
                            .delete(
                                &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(&parent_key),
                            )
                            .await;
                    }
                }
                // Add new composite head
                let composite_head_key =
                    storage::keys::headstore::HeadstoreDocKey::new(&doc_id_str, "C", *cid);
                if let Err(e) = headstore
                    .set(
                        &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(
                            &composite_head_key,
                        ),
                        &priority_bytes,
                    )
                    .await
                {
                    tracing::warn!(error = %e, "Failed to write composite head to headstore");
                }
                let composite_priority_key = storage::keys::headstore::HeadstorePriorityKey::new(
                    &doc_id_str,
                    payload.priority,
                    *cid,
                );
                if let Err(e) = headstore
                    .set(
                        &<storage::keys::headstore::HeadstorePriorityKey as storage::corekv::Key>::bytes(
                            &composite_priority_key,
                        ),
                        &[],
                    )
                    .await
                {
                    tracing::warn!(error = %e, "Failed to write composite priority index");
                }

                // Field heads: only delete heads that each field block supersedes
                if let Some(links) = &block.links {
                    for dag_link in links {
                        // Delete only the parent field heads (from the field block's heads)
                        if let Some(parent_cids) = field_block_heads.get(&dag_link.name) {
                            for parent_cid in parent_cids {
                                let parent_key = storage::keys::headstore::HeadstoreDocKey::new(
                                    &doc_id_str,
                                    &dag_link.name,
                                    *parent_cid,
                                );
                                let _ = headstore
                                    .delete(
                                        &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(&parent_key),
                                    )
                                    .await;
                            }
                        }
                        // Add new field head
                        let field_head_key = storage::keys::headstore::HeadstoreDocKey::new(
                            &doc_id_str,
                            &dag_link.name,
                            dag_link.link,
                        );
                        if let Err(e) = headstore
                            .set(
                                &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(&field_head_key),
                                &priority_bytes,
                            )
                            .await
                        {
                            tracing::warn!(
                                field = %dag_link.name,
                                error = %e,
                                "Failed to write field head to headstore"
                            );
                        }
                        let field_priority_key =
                            storage::keys::headstore::HeadstorePriorityKey::new(
                                &doc_id_str,
                                payload.priority,
                                dag_link.link,
                            );
                        if let Err(e) = headstore
                            .set(
                                &<storage::keys::headstore::HeadstorePriorityKey as storage::corekv::Key>::bytes(
                                    &field_priority_key,
                                ),
                                &[],
                            )
                            .await
                        {
                            tracing::warn!(
                                field = %dag_link.name,
                                error = %e,
                                "Failed to write field priority index"
                            );
                        }
                    }
                }
            }
        }

        // For branchable collections, the sender broadcasts the collection block
        // separately (dual broadcast), so we don't create local collection blocks here.
        // The sender's collection block arrives via handle_block → process_collection_delta
        // which preserves the exact collection CID for cross-node consistency.

        // Handle transaction commit/discard based on result
        match process_error {
            None => {
                // Commit the entire transaction (all field merges + document storage + headstore)
                txn.force_commit().await?;

                // Mark this composite as merged so concurrent/recursive paths
                // skip re-processing it (prevents stale headstore entries).
                {
                    let mut merged = self.merged_composites.lock().unwrap();
                    merged.insert(*cid);
                }

                tracing::info!(
                    cid = %cid,
                    doc_id = %doc_id_str,
                    fields_merged = field_values.len(),
                    "Composite delta processed and committed successfully"
                );

                if let (Some(collection), Some(hook)) =
                    (collection_for_policy.as_ref(), self.composite_merge_hook())
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

                // Emit update event for subscriptions (P2P relay)
                if let Some(bus) = self.db.event_bus() {
                    let update = Update::new(
                        doc_id_str.clone(),
                        *cid,
                        payload.schema_version_id.clone(),
                        vec![], // Block data not needed for subscription re-query
                        false,  // is_retry
                        true,   // is_relay (P2P update)
                    );
                    bus.publish(Message::update(update));

                    // For branchable collections, emit a collection-level merge_complete
                    // event. Uses composite CID to match the sender's Update event CID.
                    if is_branchable {
                        let by_peer = metadata.sender_peer.unwrap_or("").to_string();
                        let mc = MergeCompleteData {
                            doc_id: String::new(), // empty → keyed by collection_id
                            cid: *cid,
                            collection_id: metadata
                                .collection_id
                                .unwrap_or(&payload.schema_version_id)
                                .to_string(),
                            by_peer,
                        };
                        bus.publish(Message::merge_complete(mc));
                    }
                }

                Ok(MergeOutcome::Merged)
            }
            Some(e) => {
                // Discard the transaction - rollback all changes
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
        pending_events: &std::sync::Mutex<Vec<PendingMergeEvent>>,
        pending_post_commit_actions: &std::sync::Mutex<Vec<PendingPostCommitAction>>,
        depth: usize,
    ) -> std::result::Result<MergeOutcome, MergeError> {
        if depth >= super::MAX_MERGE_DEPTH {
            return Err(MergeError::depth_exceeded(cid, depth));
        }

        let doc_id_str = String::from_utf8_lossy(&payload.doc_id).to_string();

        tracing::info!(
            cid = %cid,
            doc_id = %doc_id_str,
            priority = payload.priority,
            "Processing Composite delta in batch txn"
        );

        // Recursively merge parent composites (using batch dedup)
        if let Some(heads) = &block.heads {
            for head_cid in heads {
                // Check both permanent and batch-local dedup sets
                {
                    let merged = self.merged_composites.lock().unwrap();
                    if merged.contains(head_cid) {
                        continue;
                    }
                }
                {
                    let bm = batch_merged.lock().unwrap();
                    if bm.contains(head_cid) {
                        continue;
                    }
                }

                let head_data = match self.blockstore.get(head_cid).await {
                    Ok(Some(data)) => data,
                    _ => continue,
                };

                let head_block = match Block::from_dag_cbor(&head_data) {
                    Ok(b) => b,
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
                        pending_events,
                        pending_post_commit_actions,
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

        let collection_for_policy = self
            .db
            .find_collection_by_id(&payload.schema_version_id)
            .ok()
            .flatten()
            .or_else(|| {
                metadata
                    .collection_id
                    .and_then(|cid| self.db.find_collection_by_id(cid).ok().flatten())
            });

        // Process field links within the shared transaction
        let mut field_values: HashMap<String, NormalValue> = HashMap::new();
        let mut _any_field_applied = false;
        let mut process_error: Option<MergeError> = None;
        let mut skip_outcome: Option<MergeOutcome> = None;
        let mut is_branchable = false;
        let mut encrypted_policy_checked = false;
        let mut field_block_heads: HashMap<String, Vec<Cid>> = HashMap::new();

        {
            let mut datastore = datastore.clone();

            if let Some(links) = &block.links {
                for dag_link in links {
                    let link_name = &dag_link.name;
                    let link_cid = &dag_link.link;

                    let linked_block_data = match self.blockstore.get(link_cid).await {
                        Ok(Some(data)) => data,
                        Ok(None) => {
                            process_error = Some(MergeError::Storage(format!(
                                "Linked block {} not found in blockstore",
                                link_cid
                            )));
                            break;
                        }
                        Err(e) => {
                            process_error = Some(MergeError::Storage(e.to_string()));
                            break;
                        }
                    };

                    let linked_block = match Block::from_dag_cbor(&linked_block_data) {
                        Ok(b) => b,
                        Err(e) => {
                            process_error = Some(MergeError::BlockDecode(e.to_string()));
                            break;
                        }
                    };

                    if let Some(heads) = &linked_block.heads {
                        field_block_heads.insert(link_name.clone(), heads.clone());
                    }

                    if linked_block.encryption.is_some() && !encrypted_policy_checked {
                        encrypted_policy_checked = true;
                        if let (Some(collection), Some(hook)) =
                            (collection_for_policy.as_ref(), self.composite_merge_hook())
                        {
                            if let Some(outcome) = hook
                                .on_encrypted_link(&doc_id_str, collection.schema(), metadata)
                                .await?
                            {
                                skip_outcome = Some(outcome);
                                break;
                            }
                        }
                    }

                    // Decrypt linked block data if encrypted
                    let effective_linked_delta = match &linked_block.delta {
                        CrdtDelta::Lww(p) if linked_block.encryption.is_some() => {
                            match self
                                .decrypt_block_data(&p.data, linked_block.encryption.as_ref())
                                .await
                            {
                                Ok(decrypted) => {
                                    let mut dp = p.clone();
                                    dp.data = decrypted;
                                    CrdtDelta::Lww(dp)
                                }
                                Err(_) => linked_block.delta.clone(),
                            }
                        }
                        CrdtDelta::Counter(p) if linked_block.encryption.is_some() => {
                            match self
                                .decrypt_block_data(&p.data, linked_block.encryption.as_ref())
                                .await
                            {
                                Ok(decrypted) => {
                                    let mut dp = p.clone();
                                    dp.data = decrypted;
                                    CrdtDelta::Counter(dp)
                                }
                                Err(_) => linked_block.delta.clone(),
                            }
                        }
                        other => other.clone(),
                    };

                    match &effective_linked_delta {
                        CrdtDelta::Lww(lww_payload) => {
                            match self
                                .process_lww_delta_in_txn(&mut datastore, link_cid, lww_payload)
                                .await
                            {
                                Ok(result) => {
                                    if result.applied {
                                        _any_field_applied = true;
                                    }
                                    if let Some(value) = result.value {
                                        field_values.insert(lww_payload.field_name.clone(), value);
                                    }
                                }
                                Err(e) => {
                                    process_error = Some(e);
                                    break;
                                }
                            }
                        }
                        CrdtDelta::Counter(counter_payload) => {
                            match self
                                .process_counter_delta_in_txn(
                                    &mut datastore,
                                    link_cid,
                                    counter_payload,
                                    metadata.collection_id,
                                )
                                .await
                            {
                                Ok(result) => {
                                    if result.applied {
                                        _any_field_applied = true;
                                    }
                                    if let Some(value) = result.value {
                                        field_values
                                            .insert(counter_payload.field_name.clone(), value);
                                    }
                                }
                                Err(e) => {
                                    process_error = Some(e);
                                    break;
                                }
                            }
                        }
                        other => {
                            process_error = Some(MergeError::UnsupportedDelta(format!(
                                "Unexpected delta type in linked block: {:?}",
                                std::mem::discriminant(other)
                            )));
                            break;
                        }
                    }
                }
            }

            // Store document if no errors
            if process_error.is_none() {
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

                let is_delete = payload.status == 2;

                match collection_lookup {
                    Some(collection) => {
                        is_branchable = collection.schema().is_branchable;
                        if is_delete {
                            if let Ok(doc_id) = DocID::from_string(&doc_id_str) {
                                if let Ok(Some(old_doc)) =
                                    collection.get_with_datastore(&datastore, &doc_id).await
                                {
                                    let short_id = collection_short_id(collection.collection_id());
                                    if let Ok(index_manager) =
                                        IndexManager::from_collection(short_id, collection.schema())
                                    {
                                        if let Err(e) = index_manager
                                            .on_document_delete(
                                                &datastore,
                                                &old_doc,
                                                collection.schema(),
                                            )
                                            .await
                                        {
                                            process_error = Some(MergeError::MergeFailed(format!(
                                                "Failed to delete indexes after batch merge: {}",
                                                e
                                            )));
                                        }
                                    }
                                }
                            }

                            if process_error.is_none() {
                                let deleted_key =
                                    build_deleted_key(collection.collection_id(), &doc_id_str);
                                if let Err(e) = datastore.set(&deleted_key, &[DELETED_MARKER]).await
                                {
                                    process_error =
                                        Some(MergeError::Database(crate::error::Error::Storage(e)));
                                }
                            }
                        } else if !field_values.is_empty() {
                            match DocID::from_string(&doc_id_str) {
                                Ok(doc_id) => {
                                    let (mut doc, old_doc) = match collection
                                        .get_with_datastore(&datastore, &doc_id)
                                        .await
                                    {
                                        Ok(Some(existing)) => {
                                            let old = existing.clone();
                                            (existing, Some(old))
                                        }
                                        _ => {
                                            let mut new_doc = Document::new();
                                            new_doc.set_id(doc_id.clone());
                                            (new_doc, None)
                                        }
                                    };

                                    doc.set_schema_version_id(&payload.schema_version_id);
                                    for (field_name, value) in &field_values {
                                        doc.set(field_name, value.clone());
                                    }

                                    let known_fields: std::collections::HashSet<&str> = collection
                                        .schema()
                                        .fields
                                        .iter()
                                        .map(|f| f.name.as_str())
                                        .collect();
                                    let all_field_names: Vec<String> =
                                        doc.field_names().map(|s| s.to_string()).collect();
                                    for fname in &all_field_names {
                                        if !known_fields.contains(fname.as_str()) {
                                            doc.remove(fname);
                                        }
                                    }

                                    if let Err(e) =
                                        collection.save_with_datastore(&datastore, &doc).await
                                    {
                                        process_error = Some(MergeError::Database(e));
                                    } else {
                                        // Index failure blocks the transaction — index and document
                                        // storage must remain consistent.
                                        let short_id =
                                            collection_short_id(collection.collection_id());
                                        if let Ok(index_manager) = IndexManager::from_collection(
                                            short_id,
                                            collection.schema(),
                                        ) {
                                            let index_result = match &old_doc {
                                                Some(old) => {
                                                    index_manager
                                                        .on_document_update(
                                                            &datastore,
                                                            old,
                                                            &doc,
                                                            collection.schema(),
                                                        )
                                                        .await
                                                }
                                                None => {
                                                    index_manager
                                                        .on_document_create(
                                                            &datastore,
                                                            &doc,
                                                            collection.schema(),
                                                        )
                                                        .await
                                                }
                                            };
                                            if let Err(e) = index_result {
                                                process_error = Some(MergeError::MergeFailed(
                                                    format!("Failed to update indexes after batch merge: {}", e),
                                                ));
                                            }
                                        }

                                        // Generate SE artifacts for replicated doc (batch path)
                                        if process_error.is_none() {
                                            if let Some(enc_key) = self.se_enc_key() {
                                                if let Err(e) = se_merge::generate_merge_artifacts(
                                                    &mut datastore,
                                                    collection.schema(),
                                                    &doc_id_str,
                                                    &field_values,
                                                    enc_key,
                                                    None,
                                                )
                                                .await
                                                {
                                                    tracing::warn!(
                                                        doc_id = %doc_id_str,
                                                        error = %e,
                                                        "Failed to generate SE artifacts after batch merge"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    process_error = Some(MergeError::MergeFailed(format!(
                                        "Invalid doc_id: {}",
                                        e
                                    )));
                                }
                            }
                        }
                    }
                    None => {
                        process_error = Some(MergeError::MissingMetadata(format!(
                            "Collection not found for schema_version_id: {}",
                            payload.schema_version_id
                        )));
                    }
                }
            }
        } // datastore dropped here

        if let Some(outcome) = skip_outcome {
            return Ok(outcome);
        }

        // Write headstore entries using the shared headstore view
        if process_error.is_none() {
            {
                let priority_bytes = encode_priority_varint(payload.priority);

                if let Some(heads) = &block.heads {
                    for parent_cid in heads {
                        let parent_key = storage::keys::headstore::HeadstoreDocKey::new(
                            &doc_id_str,
                            "C",
                            *parent_cid,
                        );
                        let _ = headstore
                            .delete(
                                &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(&parent_key),
                            )
                            .await;
                    }
                }

                let composite_head_key =
                    storage::keys::headstore::HeadstoreDocKey::new(&doc_id_str, "C", *cid);
                let _ = headstore
                    .set(
                        &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(
                            &composite_head_key,
                        ),
                        &priority_bytes,
                    )
                    .await;
                let composite_priority_key = storage::keys::headstore::HeadstorePriorityKey::new(
                    &doc_id_str,
                    payload.priority,
                    *cid,
                );
                let _ = headstore
                    .set(
                        &<storage::keys::headstore::HeadstorePriorityKey as storage::corekv::Key>::bytes(
                            &composite_priority_key,
                        ),
                        &[],
                    )
                    .await;

                if let Some(links) = &block.links {
                    for dag_link in links {
                        if let Some(parent_cids) = field_block_heads.get(&dag_link.name) {
                            for parent_cid in parent_cids {
                                let parent_key = storage::keys::headstore::HeadstoreDocKey::new(
                                    &doc_id_str,
                                    &dag_link.name,
                                    *parent_cid,
                                );
                                let _ = headstore
                                    .delete(
                                        &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(&parent_key),
                                    )
                                    .await;
                            }
                        }
                        let field_head_key = storage::keys::headstore::HeadstoreDocKey::new(
                            &doc_id_str,
                            &dag_link.name,
                            dag_link.link,
                        );
                        let _ = headstore
                            .set(
                                &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(&field_head_key),
                                &priority_bytes,
                            )
                            .await;
                        let field_priority_key =
                            storage::keys::headstore::HeadstorePriorityKey::new(
                                &doc_id_str,
                                payload.priority,
                                dag_link.link,
                            );
                        let _ = headstore
                            .set(
                                &<storage::keys::headstore::HeadstorePriorityKey as storage::corekv::Key>::bytes(
                                    &field_priority_key,
                                ),
                                &[],
                            )
                            .await;
                    }
                }
            }
        }

        // Handle result (NO commit/discard — caller manages transaction)
        match process_error {
            None => {
                // Mark as merged in batch-local set
                {
                    let mut bm = batch_merged.lock().unwrap();
                    bm.insert(*cid);
                }

                if let (Some(collection), Some(hook)) =
                    (collection_for_policy.as_ref(), self.composite_merge_hook())
                {
                    if let Some(action) =
                        hook.post_commit_action(&doc_id_str, collection.schema(), metadata)
                    {
                        pending_post_commit_actions
                            .lock()
                            .unwrap()
                            .push(PendingPostCommitAction { action });
                    }
                }

                // Collect events for deferred publishing
                {
                    let mut pe = pending_events.lock().unwrap();
                    let update = Update::new(
                        doc_id_str.clone(),
                        *cid,
                        payload.schema_version_id.clone(),
                        vec![],
                        false,
                        true,
                    );
                    pe.push(PendingMergeEvent {
                        message: Message::update(update),
                    });

                    if is_branchable {
                        let by_peer = metadata.sender_peer.unwrap_or("").to_string();
                        let mc = MergeCompleteData {
                            doc_id: String::new(),
                            cid: *cid,
                            collection_id: metadata
                                .collection_id
                                .unwrap_or(&payload.schema_version_id)
                                .to_string(),
                            by_peer,
                        };
                        pe.push(PendingMergeEvent {
                            message: Message::merge_complete(mc),
                        });
                    }
                }

                Ok(MergeOutcome::Merged)
            }
            Some(e) => Err(e),
        }
    }
}
