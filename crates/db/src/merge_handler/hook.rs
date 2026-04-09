use async_trait::async_trait;
use p2p::sync::{BlockMetadata, MergeOutcome};
use schema::CollectionVersion;

use super::MergeError;

#[async_trait]
pub(crate) trait CompositePostCommitAction: Send {
    async fn run(self: Box<Self>) -> Result<(), MergeError>;
}

#[async_trait]
pub(crate) trait CompositeMergeHook: Send + Sync {
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
