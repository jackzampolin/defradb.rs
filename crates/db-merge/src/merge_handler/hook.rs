use async_trait::async_trait;
use defra_core::merge::{BlockMetadata, MergeOutcome};
use defra_core::thread_bounds::{MaybeSend, MaybeSendSync};
use schema::CollectionVersion;

use super::MergeError;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub(crate) trait CompositePostCommitAction: MaybeSend {
    async fn run(self: Box<Self>) -> Result<(), MergeError>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub(crate) trait CompositeMergeHook: MaybeSendSync {
    async fn on_protected_composite(
        &self,
        _doc_id: &str,
        _collection: &CollectionVersion,
        _metadata: &BlockMetadata<'_>,
    ) -> Result<Option<MergeOutcome>, MergeError> {
        Ok(None)
    }

    async fn on_encrypted_link(
        &self,
        _doc_id: &str,
        _collection: &CollectionVersion,
        _metadata: &BlockMetadata<'_>,
    ) -> Result<Option<MergeOutcome>, MergeError> {
        Ok(None)
    }

    fn post_commit_action(
        &self,
        _doc_id: &str,
        _collection: &CollectionVersion,
        _metadata: &BlockMetadata<'_>,
    ) -> Option<Box<dyn CompositePostCommitAction>> {
        None
    }
}
