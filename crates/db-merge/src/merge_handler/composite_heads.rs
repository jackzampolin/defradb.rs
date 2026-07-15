use super::composite::{CompositeMergeContext, CompositeMergeState};
use super::*;

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    pub(crate) async fn update_heads(
        &self,
        headstore: &NamespaceView,
        context: &CompositeMergeContext<'_, '_>,
        state: &CompositeMergeState,
    ) {
        let priority_bytes = encode_priority_varint(context.payload.priority);

        if let Some(heads) = &context.block.heads {
            for parent_cid in heads {
                let parent_key = storage::keys::headstore::HeadstoreDocKey::new(
                    context.doc_short_id,
                    "C",
                    *parent_cid,
                );
                let _ = headstore
                    .delete(
                        &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(
                            &parent_key,
                        ),
                    )
                    .await;
            }
        }

        let composite_head_key =
            storage::keys::headstore::HeadstoreDocKey::new(context.doc_short_id, "C", *context.cid);
        if let Err(e) = headstore
            .set(
                &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(
                    &composite_head_key,
                ),
                &priority_bytes,
            )
            .await
        {
            if context.mode.is_standalone() {
                tracing::warn!(error = %e, "Failed to write composite head to headstore");
            }
        }

        let composite_priority_key = storage::keys::headstore::HeadstorePriorityKey::new(
            context.doc_short_id,
            context.payload.priority,
            *context.cid,
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
            if context.mode.is_standalone() {
                tracing::warn!(error = %e, "Failed to write composite priority index");
            }
        }

        if let Some(links) = &context.block.links {
            for dag_link in links {
                if let Some(parent_cids) = state.field_block_heads.get(&dag_link.name) {
                    for parent_cid in parent_cids {
                        let parent_key = storage::keys::headstore::HeadstoreDocKey::new(
                            context.doc_short_id,
                            &dag_link.name,
                            *parent_cid,
                        );
                        let _ = headstore
                            .delete(
                                &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(
                                    &parent_key,
                                ),
                            )
                            .await;
                    }
                }

                let field_head_key = storage::keys::headstore::HeadstoreDocKey::new(
                    context.doc_short_id,
                    &dag_link.name,
                    dag_link.link,
                );
                if let Err(e) = headstore
                    .set(
                        &<storage::keys::headstore::HeadstoreDocKey as storage::corekv::Key>::bytes(
                            &field_head_key,
                        ),
                        &priority_bytes,
                    )
                    .await
                {
                    if context.mode.is_standalone() {
                        tracing::warn!(
                            field = %dag_link.name,
                            error = %e,
                            "Failed to write field head to headstore"
                        );
                    }
                }

                let field_priority_key = storage::keys::headstore::HeadstorePriorityKey::new(
                    context.doc_short_id,
                    context.payload.priority,
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
                    if context.mode.is_standalone() {
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
}
