//! Context carried while fetching a DAG before merge.

use cid::Cid;
use defra_core::Block as DefraBlock;

use crate::sync::manager::SyncEvent;
use crate::transport::PeerId;
use crate::ExplicitReplayAuthorization;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BlockContext {
    pub(crate) doc_id: Option<String>,
    pub(crate) collection_id: Option<String>,
}

pub(crate) fn block_context_from_data(block_data: &[u8]) -> BlockContext {
    let Ok(block) = DefraBlock::from_dag_cbor(block_data) else {
        return BlockContext::default();
    };

    BlockContext {
        doc_id: block
            .delta
            .doc_id()
            .map(|doc_id| String::from_utf8_lossy(doc_id).to_string()),
        collection_id: block.delta.schema_version_id().map(ToString::to_string),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DagFetchContext {
    pub(crate) doc_id: String,
    pub(crate) collection_id: String,
    pub(crate) creator: String,
    pub(crate) source_peer: PeerId,
    pub(crate) is_explicit_replicator: bool,
    explicit_replicator_collections: Option<Vec<String>>,
    pub(crate) explicit_replay_authorization: Option<ExplicitReplayAuthorization>,
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
            is_explicit_replicator: false,
            explicit_replicator_collections: None,
            explicit_replay_authorization: None,
        }
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

    pub(crate) fn fill_missing_from_block(&mut self, block_data: &[u8]) {
        let block_context = block_context_from_data(block_data);
        if self.doc_id.is_empty() {
            if let Some(doc_id) = block_context.doc_id {
                self.doc_id = doc_id;
            }
        }
        if self.collection_id.is_empty() {
            if let Some(collection_id) = block_context.collection_id {
                self.collection_id = collection_id;
            }
        }
        self.refresh_explicit_replicator_from_collection();
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
