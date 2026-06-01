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
        if let Some(links) = &context.block.links {
            if context.mode.is_standalone() {
                tracing::info!(
                    cid = %context.cid,
                    links_count = links.len(),
                    "Processing linked blocks from Composite delta"
                );
            }

            for dag_link in links {
                if let Some(outcome) = self
                    .process_field_block(datastore, headstore, context, dag_link, state)
                    .await?
                {
                    return Ok(Some(outcome));
                }
            }
        }

        Ok(None)
    }

    async fn process_field_block(
        &self,
        datastore: &mut NamespaceView,
        headstore: &NamespaceView,
        context: &CompositeMergeContext<'_, '_>,
        dag_link: &defra_core::block::DAGLink,
        state: &mut CompositeMergeState,
    ) -> std::result::Result<Option<MergeOutcome>, MergeError> {
        let link_name = &dag_link.name;
        let link_cid = &dag_link.link;

        if context.mode.is_standalone() {
            tracing::debug!(
                parent_cid = %context.cid,
                link_cid = %link_cid,
                link_name = %link_name,
                "Processing linked block"
            );
        }

        let linked_block_data = match self.blockstore.get(link_cid).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                if context.mode.is_standalone() {
                    tracing::error!(
                        parent_cid = %context.cid,
                        link_cid = %link_cid,
                        "Linked block not found in blockstore"
                    );
                }
                return Err(MergeError::Storage(format!(
                    "Linked block {} not found in blockstore",
                    link_cid
                )));
            }
            Err(e) => {
                if context.mode.is_standalone() {
                    tracing::error!(
                        parent_cid = %context.cid,
                        link_cid = %link_cid,
                        error = %e,
                        "Failed to load linked block from blockstore"
                    );
                }
                return Err(MergeError::Storage(e.to_string()));
            }
        };

        let linked_block = Block::from_dag_cbor(&linked_block_data)
            .map_err(|e| MergeError::BlockDecode(e.to_string()))?;

        if let Some(heads) = &linked_block.heads {
            state
                .field_block_heads
                .insert(link_name.clone(), heads.clone());
        }

        let effective_linked_delta = match self
            .handle_encryption(context, &linked_block, &mut state.encrypted_policy_checked)
            .await?
        {
            EffectiveLinkedDelta::Delta(delta) => delta,
            EffectiveLinkedDelta::Skip(outcome) => return Ok(Some(outcome)),
            EffectiveLinkedDelta::SkipField => return Ok(None),
        };

        match &effective_linked_delta {
            CrdtDelta::Lww(lww_payload) => {
                let result = self
                    .process_lww_delta_in_txn(
                        datastore,
                        headstore,
                        link_cid,
                        lww_payload,
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
                        link_cid,
                        counter_payload,
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
                if context.mode.is_standalone() {
                    tracing::error!(
                        parent_cid = %context.cid,
                        link_cid = %link_cid,
                        delta_type = ?std::mem::discriminant(other),
                        "Unexpected delta type in linked block - expected LWW or Counter"
                    );
                }
                return Err(MergeError::UnsupportedDelta(format!(
                    "Unexpected delta type in linked block: {:?}",
                    std::mem::discriminant(other)
                )));
            }
        }

        state.linked_field_cids.push(*link_cid);

        Ok(None)
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
