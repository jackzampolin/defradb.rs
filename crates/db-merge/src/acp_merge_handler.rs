use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use acp::{DocumentACP, DocumentPermission, Identity};
use async_trait::async_trait;
use cid::Cid;
use identity::Identity as _;
use p2p::sync::{BlockMetadata, MergeBlock, MergeHandler, MergeOutcome};
use schema::CollectionVersion;
use storage::corekv::Store;

use crate::merge_handler::hook::{CompositeMergeHook, CompositePostCommitAction};
use crate::merge_handler::{DbMergeHandler, MergeError};

pub type AcpMergeError = MergeError;

struct RegisterReplicatedDocAction {
    acp: Arc<dyn DocumentACP>,
    did: identity::Did,
    policy_id: String,
    resource_name: String,
    doc_id: String,
}

#[async_trait]
impl CompositePostCommitAction for RegisterReplicatedDocAction {
    async fn run(self: Box<Self>) -> Result<(), MergeError> {
        self.acp
            .register_doc_object(
                &self.did,
                &self.policy_id,
                &self.resource_name,
                &self.doc_id,
            )
            .await
            .map_err(|e| MergeError::MergeFailed(format!("ACP registration failed: {}", e)))
    }
}

struct AcpCompositeMergeHook {
    document_acp: std::sync::OnceLock<Arc<dyn DocumentACP>>,
    local_identity: Option<Identity>,
    strict_replicated_doc_access: AtomicBool,
}

impl AcpCompositeMergeHook {
    fn new(local_identity: Option<Identity>) -> Self {
        Self {
            document_acp: std::sync::OnceLock::new(),
            local_identity,
            strict_replicated_doc_access: AtomicBool::new(false),
        }
    }

    fn set_document_acp(&self, acp: Arc<dyn DocumentACP>) {
        let _ = self.document_acp.set(acp);
    }

    fn set_strict_replicated_doc_access(&self, strict: bool) {
        self.strict_replicated_doc_access
            .store(strict, Ordering::Relaxed);
    }

    fn document_acp(&self) -> Option<&Arc<dyn DocumentACP>> {
        self.document_acp.get()
    }
}

#[async_trait]
impl CompositeMergeHook for AcpCompositeMergeHook {
    async fn on_protected_composite(
        &self,
        doc_id: &str,
        collection: &CollectionVersion,
        metadata: &BlockMetadata<'_>,
    ) -> Result<Option<MergeOutcome>, MergeError> {
        let Some(acp) = self.document_acp() else {
            return Ok(None);
        };
        let Some(policy) = &collection.policy else {
            return Ok(None);
        };
        if !self.strict_replicated_doc_access.load(Ordering::Relaxed) {
            return Ok(None);
        }
        if metadata.allows_explicit_replay_for(&collection.collection_id) {
            return Ok(None);
        }
        let Some(identity) = self.local_identity.as_ref() else {
            return Ok(None);
        };

        let is_registered = acp
            .is_doc_registered(&policy.id, &policy.resource_name, doc_id)
            .await
            .map_err(|e| {
                MergeError::MergeFailed(format!("ACP registration lookup failed: {}", e))
            })?;
        if !is_registered {
            return Ok(Some(MergeOutcome::retryable_skip(
                "replicated protected document is not yet registered in local ACP",
            )));
        }

        let has_access = acp
            .check_doc_access(
                identity,
                DocumentPermission::Read,
                &policy.id,
                &policy.resource_name,
                doc_id,
            )
            .await
            .map_err(|e| MergeError::MergeFailed(format!("ACP access check failed: {}", e)))?;

        if has_access {
            return Ok(None);
        }

        Ok(Some(MergeOutcome::retryable_skip(
            "replicated protected document is not yet readable by local node",
        )))
    }

    async fn on_encrypted_link(
        &self,
        doc_id: &str,
        collection: &CollectionVersion,
        metadata: &BlockMetadata<'_>,
    ) -> Result<Option<MergeOutcome>, MergeError> {
        let Some(acp) = self.document_acp() else {
            return Ok(None);
        };
        let Some(policy) = &collection.policy else {
            return Ok(None);
        };

        if metadata.allows_explicit_replay_for(&collection.collection_id) {
            tracing::info!(
                doc_id = %doc_id,
                sender_peer = metadata.sender_peer.unwrap_or(""),
                authorizer = metadata
                    .explicit_replay_authorization
                    .as_ref()
                    .map(|authorization| authorization.authorizer_did.as_str())
                    .unwrap_or(""),
                creator = metadata.effective_creator().unwrap_or(""),
                "Allowing encrypted merge via explicit replicator path"
            );
            return Ok(None);
        }

        let is_registered = acp
            .is_doc_registered(&policy.id, &policy.resource_name, doc_id)
            .await
            .map_err(|e| {
                MergeError::MergeFailed(format!("ACP registration lookup failed: {}", e))
            })?;

        if is_registered {
            return Ok(None);
        }

        Ok(Some(MergeOutcome::retryable_skip(
            "encrypted replicated document is not yet registered in local ACP",
        )))
    }

    fn post_commit_action(
        &self,
        doc_id: &str,
        collection: &CollectionVersion,
        metadata: &BlockMetadata<'_>,
    ) -> Option<Box<dyn CompositePostCommitAction>> {
        let acp = self.document_acp()?.clone();
        let policy = collection.policy.as_ref()?;
        let creator = metadata
            .explicit_replay_authorizer_for(&collection.collection_id)
            .or_else(|| metadata.effective_creator())?;
        let did = identity::Did::new(creator).ok()?;

        Some(Box::new(RegisterReplicatedDocAction {
            acp,
            did,
            policy_id: policy.id.clone(),
            resource_name: policy.resource_name.clone(),
            doc_id: doc_id.to_string(),
        }))
    }
}

pub struct AcpMergeHandler<S: Store, B: blockstore::Blockstore> {
    inner: Arc<DbMergeHandler<S, B>>,
    hook: Arc<AcpCompositeMergeHook>,
}

impl<S: Store, B: blockstore::Blockstore + Send + Sync> AcpMergeHandler<S, B> {
    pub fn new(inner: Arc<DbMergeHandler<S, B>>) -> Self {
        let local_identity = inner
            .db
            .node_identity()
            .and_then(|identity| identity.did().ok().map(Identity::from));
        let hook = Arc::new(AcpCompositeMergeHook::new(local_identity));
        inner.set_composite_merge_hook(hook.clone());
        Self { inner, hook }
    }

    pub fn with_document_acp(self, acp: Arc<dyn DocumentACP>) -> Self {
        self.hook.set_document_acp(acp);
        self
    }

    pub fn set_document_acp(&self, acp: Arc<dyn DocumentACP>) {
        self.hook.set_document_acp(acp);
    }

    pub fn set_strict_replicated_doc_access(&self, strict: bool) {
        self.hook.set_strict_replicated_doc_access(strict);
    }

    pub fn inner(&self) -> &Arc<DbMergeHandler<S, B>> {
        &self.inner
    }
}

#[async_trait]
impl<S, B> MergeHandler for AcpMergeHandler<S, B>
where
    S: Store + 'static,
    B: blockstore::Blockstore + Send + Sync + 'static,
{
    type Error = AcpMergeError;

    async fn handle_block(
        &self,
        cid: &Cid,
        block_data: &[u8],
        metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        self.inner.handle_block(cid, block_data, metadata).await
    }

    async fn handle_block_batch(
        &self,
        blocks: &[MergeBlock],
    ) -> Vec<Result<MergeOutcome, Self::Error>> {
        self.inner.handle_block_batch(blocks).await
    }
}
