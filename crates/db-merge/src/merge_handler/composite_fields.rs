use super::composite::{CompositeMergeContext, CompositeMergeState};
use super::*;

enum EffectiveLinkedDelta {
    Delta(CrdtDelta),
    Skip(MergeOutcome),
    SkipField,
}

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    pub(crate) async fn process_linked_field_blocks(
        &self,
        datastore: &mut NamespaceView,
        headstore: &NamespaceView,
        context: &CompositeMergeContext<'_, '_>,
        state: &mut CompositeMergeState,
    ) -> std::result::Result<Option<MergeOutcome>, MergeError> {
        let Some(links) = &context.block.links else {
            return Ok(None);
        };

        if context.mode.is_standalone() {
            tracing::info!(
                cid = %context.cid,
                links_count = links.len(),
                "Processing linked blocks from Composite delta"
            );
        }

        // Load the prior value once (deleted-inclusive, so a delete+recreate cannot
        // change an immutable field) when the composite links any @immutable field.
        let immutable_fields: HashSet<&str> = context
            .collection
            .as_ref()
            .map(|collection| {
                collection
                    .schema()
                    .fields
                    .iter()
                    // JSON fields encode numbers differently in the document store
                    // (json_to_cbor_value) than in the field block (ciborium), so a
                    // cross-encoding merge comparison would false-reject. The local
                    // write path compares same-representation values and still
                    // enforces immutability for JSON fields.
                    .filter(|field| {
                        field.immutable
                            && !matches!(field.kind, FieldKind::Scalar(ScalarKind::Json))
                    })
                    .map(|field| field.name.as_str())
                    .collect()
            })
            .unwrap_or_default();
        let immutable_baseline = if context.collection.as_ref().is_some_and(|_| {
            links
                .iter()
                .any(|l| immutable_fields.contains(l.name.as_str()))
        }) {
            let doc_id = DocID::from_string(context.doc_id_str)
                .map_err(|e| MergeError::MergeFailed(format!("invalid doc_id: {e}")))?;
            context
                .collection
                .as_ref()
                .unwrap()
                .get_with_datastore_include_deleted(datastore, &doc_id, false)
                .await
                .map_err(MergeError::Database)?
                .map(|(doc, _)| doc)
        } else {
            None
        };

        // Phase 1: decode/decrypt every linked field ONCE and validate @immutable
        // BEFORE persisting anything, so a rejected composite leaves no partial write.
        let mut pending: Vec<(Cid, CrdtDelta)> = Vec::with_capacity(links.len());
        for dag_link in links {
            let link_name = &dag_link.name;
            let link_cid = &dag_link.link;

            let linked_block_data = match self.blockstore.get(link_cid).await {
                Ok(Some(data)) => data,
                Ok(None) => {
                    return Err(MergeError::Storage(format!(
                        "Linked block {} not found in blockstore",
                        link_cid
                    )));
                }
                Err(e) => return Err(MergeError::Storage(e.to_string())),
            };
            let linked_block = Block::from_dag_cbor(&linked_block_data)
                .map_err(|e| MergeError::BlockDecode(e.to_string()))?;

            if let Some(heads) = &linked_block.heads {
                state
                    .field_block_heads
                    .insert(link_name.clone(), heads.clone());
            }

            let effective = match self
                .handle_encryption(context, &linked_block, &mut state.encrypted_policy_checked)
                .await?
            {
                EffectiveLinkedDelta::Delta(delta) => delta,
                EffectiveLinkedDelta::Skip(outcome) => return Ok(Some(outcome)),
                EffectiveLinkedDelta::SkipField => continue,
            };

            if immutable_fields.contains(link_name.as_str()) {
                if let Some(baseline) = immutable_baseline.as_ref() {
                    Self::check_immutable_delta(baseline, link_name, &effective)?;
                }
            }

            pending.push((*link_cid, effective));
        }

        // Phase 2: persist the decoded deltas.
        for (link_cid, effective) in pending {
            match effective {
                CrdtDelta::Lww(lww_payload) => {
                    let result = self
                        .process_lww_delta_in_txn(
                            datastore,
                            headstore,
                            &link_cid,
                            &lww_payload,
                            context.metadata.collection_id,
                        )
                        .await?;
                    if result.applied {
                        state.any_field_applied = true;
                    }
                    if let Some(value) = result.value {
                        state
                            .field_values
                            .insert(lww_payload.field_name.clone(), value);
                    }
                }
                CrdtDelta::Counter(counter_payload) => {
                    let result = self
                        .process_counter_delta_in_txn(
                            datastore,
                            &link_cid,
                            &counter_payload,
                            context.metadata.collection_id,
                        )
                        .await?;
                    if result.applied {
                        state.any_field_applied = true;
                    }
                    if let Some(value) = result.value {
                        state
                            .field_values
                            .insert(counter_payload.field_name.clone(), value);
                    }
                }
                other => {
                    return Err(MergeError::UnsupportedDelta(format!(
                        "Unexpected delta type in linked block: {:?}",
                        std::mem::discriminant(&other)
                    )));
                }
            }
            state.linked_field_cids.push(link_cid);
        }

        Ok(None)
    }

    /// Reject an incoming immutable-field delta that does not exactly preserve a
    /// prior value — a different value, or a tombstone/clear (empty payload) — so a
    /// crafted block cannot diverge the field store from the document store.
    ///
    /// A field with no prior value is allowed to be set: out-of-order sync can
    /// materialize a partial document before the create-composite carrying the
    /// immutable field merges, so rejecting first-set would false-reject the
    /// legitimate block.
    fn check_immutable_delta(
        baseline: &Document,
        field_name: &str,
        effective: &CrdtDelta,
    ) -> std::result::Result<(), MergeError> {
        let Some(prior) = baseline.get(field_name) else {
            return Ok(());
        };
        let CrdtDelta::Lww(payload) = effective else {
            return Ok(());
        };
        let preserves_prior = !payload.data.is_empty()
            && ciborium::from_reader::<NormalValue, _>(payload.data.as_slice())
                .map(|incoming| incoming == *prior)
                .unwrap_or(false);
        if !preserves_prior {
            return Err(MergeError::ImmutableFieldChanged(format!(
                "immutable field '{field_name}' cannot be changed"
            )));
        }
        Ok(())
    }

    async fn handle_encryption(
        &self,
        context: &CompositeMergeContext<'_, '_>,
        linked_block: &Block,
        encrypted_policy_checked: &mut bool,
    ) -> std::result::Result<EffectiveLinkedDelta, MergeError> {
        if linked_block.encryption.is_some() && !*encrypted_policy_checked {
            *encrypted_policy_checked = true;
            if let (Some(collection), Some(hook)) =
                (context.collection.as_ref(), self.composite_merge_hook())
            {
                if let Some(outcome) = hook
                    .on_encrypted_link(context.doc_id_str, collection.schema(), context.metadata)
                    .await?
                {
                    return Ok(EffectiveLinkedDelta::Skip(outcome));
                }
            }
        }

        let effective_linked_delta = match &linked_block.delta {
            CrdtDelta::Lww(payload) if linked_block.encryption.is_some() => {
                match self
                    .decrypt_block_data(
                        &payload.data,
                        linked_block.encryption.as_ref(),
                        Some(context.metadata),
                    )
                    .await
                {
                    Ok(decrypted) => {
                        let mut decrypted_payload = payload.clone();
                        decrypted_payload.data = decrypted;
                        CrdtDelta::Lww(decrypted_payload)
                    }
                    Err(e) => {
                        tracing::debug!(
                            error = %e,
                            "Cannot decrypt LWW field block, skipping field (canRead=false)"
                        );
                        return Ok(EffectiveLinkedDelta::SkipField);
                    }
                }
            }
            CrdtDelta::Counter(payload) if linked_block.encryption.is_some() => {
                match self
                    .decrypt_block_data(
                        &payload.data,
                        linked_block.encryption.as_ref(),
                        Some(context.metadata),
                    )
                    .await
                {
                    Ok(decrypted) => {
                        let mut decrypted_payload = payload.clone();
                        decrypted_payload.data = decrypted;
                        CrdtDelta::Counter(decrypted_payload)
                    }
                    Err(e) => {
                        tracing::debug!(
                            error = %e,
                            "Cannot decrypt Counter field block, skipping field (canRead=false)"
                        );
                        return Ok(EffectiveLinkedDelta::SkipField);
                    }
                }
            }
            other => other.clone(),
        };

        Ok(EffectiveLinkedDelta::Delta(effective_linked_delta))
    }
}
