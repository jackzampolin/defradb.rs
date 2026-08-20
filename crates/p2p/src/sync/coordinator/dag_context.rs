//! Context carried while fetching a DAG before merge.

use cid::Cid;
use defra_core::Block as DefraBlock;

use crate::sync::manager::SyncEvent;
use crate::transport::PeerId;
use crate::ExplicitReplayAuthorization;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BlockContext {
    pub(crate) collection_id: Option<String>,
}

/// Extract collection context from a block payload. Deltas carry no
/// document identity (Go #4838) — the DocID travels only in the PushLog
/// envelope or is derived from the genesis composite CID at merge time.
pub(crate) fn block_context_from_data(block_data: &[u8]) -> BlockContext {
    let Ok(block) = DefraBlock::from_dag_cbor(block_data) else {
        return BlockContext::default();
    };

    BlockContext {
        collection_id: block.delta.schema_version_id().map(ToString::to_string),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DagFetchContext {
    pub(crate) doc_id: String,
    pub(crate) collection_id: String,
    pub(crate) creator: String,
    pub(crate) source_peer: PeerId,
    alternate_providers: Vec<PeerId>,
    pub(crate) is_explicit_replicator: bool,
    explicit_replicator_collections: Option<Vec<String>>,
    pub(crate) explicit_replay_authorization: Option<ExplicitReplayAuthorization>,
    pending_lease: Option<crate::sync::manager::PendingDagLease>,
    block_sync_completions: Option<crate::sync::manager::BlockSyncCompletionTracker>,
    rooted_car_completions: Option<crate::sync::manager::RootedCarCompletionTracker>,
    rooted_provider_discovery: bool,
}

impl DagFetchContext {
    pub(crate) fn new(
        doc_id: String,
        collection_id: String,
        creator: String,
        source_peer: PeerId,
    ) -> Self {
        Self {
            doc_id,
            collection_id,
            creator,
            source_peer,
            alternate_providers: Vec::new(),
            is_explicit_replicator: false,
            explicit_replicator_collections: None,
            explicit_replay_authorization: None,
            pending_lease: None,
            block_sync_completions: None,
            rooted_car_completions: None,
            rooted_provider_discovery: false,
        }
    }

    pub(crate) fn with_alternate_providers(mut self, providers: Vec<PeerId>) -> Self {
        self.alternate_providers.clear();
        for peer in providers {
            if peer != self.source_peer && !self.alternate_providers.contains(&peer) {
                self.alternate_providers.push(peer);
            }
            if self.alternate_providers.len()
                == crate::sync::pending_store::MAX_PENDING_DAG_ALTERNATE_PROVIDERS
            {
                break;
            }
        }
        self
    }

    /// Ordered fetch providers: the announcing peer first, then alternates.
    pub(crate) fn providers(&self) -> Vec<PeerId> {
        let mut providers = Vec::with_capacity(1 + self.alternate_providers.len());
        providers.push(self.source_peer.clone());
        providers.extend(self.alternate_providers.iter().cloned());
        providers
    }

    pub(crate) fn with_explicit_replicator(mut self, is_explicit_replicator: bool) -> Self {
        self.is_explicit_replicator = is_explicit_replicator;
        self
    }

    pub(crate) fn with_explicit_replicator_collections(mut self, collections: Vec<String>) -> Self {
        self.explicit_replicator_collections = Some(collections);
        self.refresh_explicit_replicator_from_collection();
        self
    }

    pub(crate) fn with_explicit_replay_authorization(
        mut self,
        authorization: Option<ExplicitReplayAuthorization>,
    ) -> Self {
        self.explicit_replay_authorization = authorization;
        self
    }

    pub(crate) fn with_pending_lease(
        mut self,
        lease: crate::sync::manager::PendingDagLease,
    ) -> Self {
        self.pending_lease = Some(lease);
        self
    }

    pub(crate) fn is_current(&self) -> bool {
        self.pending_lease
            .as_ref()
            .is_none_or(|lease| lease.is_current())
    }

    pub(crate) fn with_block_sync_completions(
        mut self,
        tracker: crate::sync::manager::BlockSyncCompletionTracker,
    ) -> Self {
        self.block_sync_completions = Some(tracker);
        self
    }

    pub(crate) fn with_rooted_car_completions(
        mut self,
        tracker: crate::sync::manager::RootedCarCompletionTracker,
    ) -> Self {
        self.rooted_car_completions = Some(tracker);
        self
    }

    pub(crate) fn with_rooted_provider_discovery(mut self) -> Self {
        self.rooted_provider_discovery = true;
        self
    }

    pub(crate) fn needs_rooted_provider_discovery(&self) -> bool {
        self.rooted_provider_discovery
    }

    pub(crate) fn track_block_sync(
        &self,
        query_id: crate::QueryId,
    ) -> Option<tokio::sync::oneshot::Receiver<crate::sync::manager::FetchCompletion>> {
        self.block_sync_completions
            .as_ref()
            .map(|tracker| tracker.register(query_id))
    }

    pub(crate) fn cancel_block_sync_tracking(&self, query_id: crate::QueryId) {
        if let Some(tracker) = &self.block_sync_completions {
            tracker.cancel(query_id);
        }
    }

    pub(crate) fn track_rooted_car(
        &self,
        root_cid: Cid,
    ) -> Option<tokio::sync::oneshot::Receiver<crate::sync::manager::FetchCompletion>> {
        self.rooted_car_completions
            .as_ref()
            .map(|tracker| tracker.register(root_cid))
    }

    pub(crate) fn cancel_rooted_car_tracking(&self, root_cid: Cid) {
        if let Some(tracker) = &self.rooted_car_completions {
            tracker.cancel(root_cid);
        }
    }

    pub(crate) fn fill_missing_from_block(&mut self, block_data: &[u8]) {
        let block_context = block_context_from_data(block_data);
        if self.collection_id.is_empty() {
            if let Some(collection_id) = block_context.collection_id {
                self.collection_id = collection_id;
            }
        }
        self.refresh_explicit_replicator_from_collection();
    }

    /// Derive the document identity from a genesis composite block CID when
    /// it is not otherwise known (Go #4838). Used on the branchable-collection
    /// sync path, where blocks arrive without a per-document PushLog envelope.
    pub(crate) fn fill_missing_doc_id_from_genesis(&mut self, cid: &Cid, block_data: &[u8]) {
        if !self.doc_id.is_empty() {
            return;
        }
        let Ok(block) = DefraBlock::from_dag_cbor(block_data) else {
            return;
        };
        let is_genesis_composite = matches!(block.delta, defra_core::CrdtDelta::Composite(_))
            && block.heads.as_ref().is_none_or(Vec::is_empty);
        if is_genesis_composite {
            self.doc_id = document::DocID::new_v0(*cid).to_string();
        }
    }

    fn refresh_explicit_replicator_from_collection(&mut self) {
        let Some(collections) = &self.explicit_replicator_collections else {
            return;
        };
        self.is_explicit_replicator = !self.collection_id.is_empty()
            && collections
                .iter()
                .any(|collection_id| collection_id == &self.collection_id);
    }

    pub(crate) fn into_dag_ready(self, root_cid: Cid) -> SyncEvent {
        SyncEvent::DagReady {
            root_cid,
            doc_id: self.doc_id,
            collection_id: self.collection_id,
            creator: self.creator,
            sender_peer: Some(self.source_peer.to_string()),
            is_explicit_replicator: self.is_explicit_replicator,
            explicit_replay_authorization: self.explicit_replay_authorization,
        }
    }
}
