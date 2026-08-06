use std::collections::HashSet;

use super::*;

impl<S: Store, B: blockstore::Blockstore> DbMergeHandler<S, B> {
    /// Resolve the owning DocID of a composite block.
    ///
    /// Mirrors Go's `resolveCompositeBlockDocRef`: prefer the recorded
    /// owner when unambiguous, otherwise derive it from the block itself —
    /// a genesis composite's CID is the DocID seed, an update inherits it
    /// from the genesis reached through its heads.
    pub(crate) async fn resolve_composite_doc_id(
        &self,
        cid: &Cid,
        block: &Block,
    ) -> std::result::Result<String, MergeError> {
        // A genesis composite (no heads) seeds its own DocID from its CID, so
        // the ownership index could only ever hold that same derived value.
        // Short-circuit before opening a read txn — this is the hot path for
        // freshly created documents arriving over replication.
        if block.heads.as_deref().is_none_or(<[Cid]>::is_empty) {
            return Ok(db_blocks::derive_doc_id(cid));
        }

        let txn = self.db.new_txn(true).await?;
        let systemstore = txn.systemstore().map_err(MergeError::Database)?;
        let mut visited = HashSet::new();
        self.resolve_composite_doc_id_inner(&systemstore, cid, block, &mut visited)
            .await
    }

    /// Iterative DFS over the composite ancestry with an explicit worklist.
    ///
    /// This walk MUST NOT recurse: composite ancestry is as deep as a
    /// document's update history (thousands of blocks for long-lived docs),
    /// and one async frame per ancestor overflows small thread stacks — iOS
    /// FFI workers crash-looped on exactly this path before the merge
    /// walkers in `composite.rs`/`collection.rs` got the same explicit-frame
    /// treatment. Heads are pushed in reverse so the first head is explored
    /// first, preserving the recursive predecessor's DFS order; `visited`
    /// bounds the walk to each unique ancestor once. A subtree that dead-ends
    /// (missing/undecodable/non-composite blocks) simply leaves more of the
    /// worklist to drain — the "no reachable genesis" error only fires once
    /// every reachable path is exhausted, matching the recursive semantics.
    async fn resolve_composite_doc_id_inner(
        &self,
        systemstore: &NamespaceView,
        cid: &Cid,
        block: &Block,
        visited: &mut HashSet<Cid>,
    ) -> std::result::Result<String, MergeError> {
        // A head's owner lookup, blockstore read, and decode are all DEFERRED
        // until its frame is popped: probing later siblings eagerly would let
        // their owner entries (or their storage errors) preempt the first
        // head's subtree — diverging from the recursive version's
        // first-head-first resolution, error propagation, and visited
        // insertion order.
        enum IdentityFrame {
            /// The entry composite, already decoded by the caller.
            Loaded(Cid, Block),
            /// A discovered head; nothing has been probed yet.
            Pending(Cid),
        }

        let mut worklist: Vec<IdentityFrame> = vec![IdentityFrame::Loaded(*cid, block.clone())];

        while let Some(frame) = worklist.pop() {
            let (node_cid, node_block) = match frame {
                IdentityFrame::Loaded(node_cid, node_block) => (node_cid, node_block),
                IdentityFrame::Pending(head_cid) => {
                    if !visited.insert(head_cid) {
                        continue;
                    }

                    let head_owners =
                        db::doc_id_map::get_doc_ids_for_block(systemstore, &head_cid.to_string())
                            .await
                            .map_err(MergeError::Database)?;
                    // Field blocks can be co-owned, so the owner index is
                    // only authoritative when it names exactly one document.
                    if head_owners.len() == 1 {
                        return Ok(head_owners.into_iter().next().expect("len checked"));
                    }

                    let head_data = match self.blockstore.get(&head_cid).await {
                        Ok(Some(data)) => data,
                        // A genuinely absent head can't help resolve
                        // identity; a blockstore failure is infrastructure
                        // and must not be silently treated as "head missing".
                        Ok(None) => continue,
                        Err(e) => return Err(MergeError::Storage(e.to_string())),
                    };
                    let Ok(head_block) = Block::from_dag_cbor(&head_data) else {
                        continue;
                    };
                    if !matches!(head_block.delta, CrdtDelta::Composite(_)) {
                        continue;
                    }
                    (head_cid, head_block)
                }
            };

            // Genesis composite: identity is derived from the CID itself, so
            // no ownership lookup is needed (see `resolve_composite_doc_id`).
            let heads = node_block.heads.as_deref().unwrap_or(&[]);
            if heads.is_empty() {
                return Ok(db_blocks::derive_doc_id(&node_cid));
            }

            // Field blocks can be co-owned, so the owner index is only
            // authoritative for a composite when it names exactly one
            // document. (For Pending nodes this re-checks what their frame
            // already probed — the recursive predecessor performed the same
            // harmless re-check at the top of each recursion.)
            let owners = db::doc_id_map::get_doc_ids_for_block(systemstore, &node_cid.to_string())
                .await
                .map_err(MergeError::Database)?;
            if owners.len() == 1 {
                return Ok(owners.into_iter().next().expect("len checked"));
            }

            // Reversed so the FIRST head is popped — and only then probed —
            // first: DFS parity with the recursive predecessor.
            for head_cid in heads.iter().rev() {
                worklist.push(IdentityFrame::Pending(*head_cid));
            }
        }

        Err(MergeError::MergeFailed(format!(
            "cannot resolve document identity for composite block {cid}: no recorded owner and no reachable genesis"
        )))
    }

    /// Resolve the owning DocID of a standalone field block via the owner
    /// index. Field blocks carry no identity and can be co-owned, so this
    /// only resolves when ownership is unambiguous; otherwise the block is
    /// merged later through its composite.
    pub(crate) async fn resolve_field_block_doc_id(
        &self,
        cid: &Cid,
    ) -> std::result::Result<Option<String>, MergeError> {
        let txn = self.db.new_txn(true).await?;
        let systemstore = txn.systemstore().map_err(MergeError::Database)?;
        let mut owners = db::doc_id_map::get_doc_ids_for_block(&systemstore, &cid.to_string())
            .await
            .map_err(MergeError::Database)?;
        if owners.len() == 1 {
            return Ok(Some(owners.remove(0)));
        }
        Ok(None)
    }

    /// Resolve a standalone field block's owning document AND its short ID
    /// using an existing systemstore view (batch txn path).
    pub(crate) async fn resolve_field_block_identity(
        &self,
        systemstore: &NamespaceView,
        cid: &Cid,
    ) -> std::result::Result<Option<(String, u64)>, MergeError> {
        let mut owners = db::doc_id_map::get_doc_ids_for_block(systemstore, &cid.to_string())
            .await
            .map_err(MergeError::Database)?;
        if owners.len() != 1 {
            return Ok(None);
        }
        let doc_id = owners.remove(0);
        let doc_ref = db::doc_id_map::get_doc_ref(systemstore, &doc_id)
            .await
            .map_err(MergeError::Database)?;
        Ok(doc_ref.map(|r| (doc_id, r.doc_short_id)))
    }

    /// Record block ownership for a merged composite: the composite CID
    /// itself, its linked field blocks, and its encryption block (mirrors
    /// Go's setBlockDocIDMapping/setLinkedBlockDocIDMappings).
    pub(crate) async fn record_block_ownership(
        &self,
        systemstore: &NamespaceView,
        doc_id: &str,
        composite_cid: &Cid,
        block: &Block,
        linked_field_cids: &[Cid],
        linked_encryption_cids: &[Cid],
    ) -> std::result::Result<(), MergeError> {
        db::doc_id_map::set_block_doc_id_mapping(systemstore, &composite_cid.to_string(), doc_id)
            .await
            .map_err(MergeError::Database)?;
        for field_cid in linked_field_cids {
            db::doc_id_map::set_block_doc_id_mapping(systemstore, &field_cid.to_string(), doc_id)
                .await
                .map_err(MergeError::Database)?;
        }
        for encryption_cid in linked_encryption_cids {
            db::doc_id_map::set_block_doc_id_mapping(
                systemstore,
                &encryption_cid.to_string(),
                doc_id,
            )
            .await
            .map_err(MergeError::Database)?;
        }
        if let Some(enc_cid) = &block.encryption {
            db::doc_id_map::set_block_doc_id_mapping(systemstore, &enc_cid.to_string(), doc_id)
                .await
                .map_err(MergeError::Database)?;
        }
        Ok(())
    }
}
