use async_lock::Mutex as TokioMutex;
use async_trait::async_trait;
use blockstore::Blockstore;
use cid::Cid;
use document::{DocID, Document};
use p2p::sync::SyncCoordinator;
use p2p::transport::P2PTransport;
use query::mutator::{
    BroadcastStatus, CreateResult, DeleteResult, DocMutator, MutationBatchController, UpdateResult,
};
use std::collections::HashSet;
use std::sync::Arc;
use storage::corekv::Store;
use tracing::{error, warn};

use db::auto_commit_mutator::BatchMutator;
use db::database::DB;
use db_blocks::BlockResult;

#[derive(Clone, Copy)]
enum BroadcastKind {
    DagPush,
    SingleBlockPush,
}

fn document_json_value(doc: &Document) -> Option<serde_json::Value> {
    Some(serde_json::Value::Object(
        doc.to_map().ok()?.into_iter().collect(),
    ))
}

struct PendingBroadcast {
    kind: BroadcastKind,
    cid: Cid,
    block: Vec<u8>,
    doc_id: String,
    collection_id: String,
    collection_name: String,
    document_json: Option<serde_json::Value>,
    creator_did: Option<String>,
    broadcast_cid: Option<Cid>,
    broadcast_block: Option<Vec<u8>>,
}

struct BroadcastCapture<'a> {
    kind: BroadcastKind,
    collection_name: &'a str,
    doc_id: &'a str,
    commit_cid: Option<Cid>,
    commit_block: Option<&'a Vec<u8>>,
    document_json: Option<serde_json::Value>,
    broadcast_cid: Option<Cid>,
    broadcast_block: Option<&'a Vec<u8>>,
}

pub(crate) struct BroadcastBatchMutator<
    S: Store,
    B: Blockstore,
    T: P2PTransport = p2p::Libp2pTransport,
> {
    inner: Arc<BatchMutator<S>>,
    sync: Arc<SyncCoordinator<B, T>>,
    db: Arc<DB<S>>,
    pending_broadcasts: TokioMutex<Vec<PendingBroadcast>>,
    inner_controller: Arc<dyn MutationBatchController>,
}

impl<S: Store, B: Blockstore + 'static, T: P2PTransport + 'static> BroadcastBatchMutator<S, B, T> {
    pub(crate) fn new(
        inner: Arc<BatchMutator<S>>,
        inner_controller: Arc<dyn MutationBatchController>,
        sync: Arc<SyncCoordinator<B, T>>,
        db: Arc<DB<S>>,
    ) -> Self {
        Self {
            inner,
            sync,
            db,
            pending_broadcasts: TokioMutex::new(Vec::new()),
            inner_controller,
        }
    }

    fn get_collection_id(&self, collection_name: &str) -> query::error::Result<String> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;
        Ok(collection.collection_id().to_string())
    }

    async fn capture_broadcast(&self, capture: BroadcastCapture<'_>) -> query::error::Result<()> {
        let BroadcastCapture {
            kind,
            collection_name,
            doc_id,
            commit_cid,
            commit_block,
            document_json,
            broadcast_cid,
            broadcast_block,
        } = capture;

        let (cid, block) = match (commit_cid, commit_block) {
            (Some(cid), Some(block)) => (cid, block.clone()),
            _ => {
                warn!(
                    collection = %collection_name,
                    doc_id = %doc_id,
                    "Missing commit block data for batched broadcast; skipping deferred broadcast"
                );
                return Ok(());
            }
        };

        let collection_id = self.get_collection_id(collection_name)?;
        self.pending_broadcasts.lock().await.push(PendingBroadcast {
            kind,
            cid,
            block,
            doc_id: doc_id.to_string(),
            collection_id,
            collection_name: collection_name.to_string(),
            document_json,
            creator_did: defra_core::signing::get_broadcast_creator_did(),
            broadcast_cid,
            broadcast_block: broadcast_block.cloned(),
        });

        Ok(())
    }

    async fn broadcast_pending_static(sync: &SyncCoordinator<B, T>, pending: PendingBroadcast) {
        let PendingBroadcast {
            kind,
            cid,
            block,
            doc_id,
            collection_id,
            collection_name,
            document_json,
            creator_did,
            broadcast_cid,
            broadcast_block,
        } = pending;

        let creator_ref = creator_did.as_deref();

        match (kind, document_json.as_ref()) {
            (_, Some(document_json)) => {
                sync.push_document_to_replicators_with_creator(
                    &cid,
                    &block,
                    &doc_id,
                    &collection_id,
                    document_json,
                    creator_ref,
                )
                .await;
            }
            (BroadcastKind::DagPush | BroadcastKind::SingleBlockPush, None) => {
                sync.push_to_replicators_with_creator(
                    &cid,
                    &block,
                    &doc_id,
                    &collection_id,
                    creator_ref,
                )
                .await;
            }
        }

        let block_result = BlockResult {
            cid,
            block,
            doc_id: doc_id.clone(),
            field_cids: vec![],
        };
        let broadcast_status = super::broadcast::broadcast_with_retry_with_creator(
            sync,
            &block_result,
            &collection_id,
            &collection_name,
            creator_ref,
        )
        .await;

        if let BroadcastStatus::Failed(error) = &broadcast_status {
            error!(
                doc_id = %doc_id,
                collection = %collection_name,
                error = %error,
                "Deferred batch broadcast failed — document committed locally but NOT replicated"
            );
        }

        if let (Some(col_cid), Some(col_block)) = (broadcast_cid, broadcast_block) {
            let col_block_result = BlockResult {
                cid: col_cid,
                block: col_block,
                doc_id: String::new(),
                field_cids: vec![],
            };
            sync.push_to_replicators_with_creator(
                &col_block_result.cid,
                &col_block_result.block,
                &col_block_result.doc_id,
                &collection_id,
                creator_ref,
            )
            .await;

            let collection_broadcast_status = super::broadcast::broadcast_with_retry_with_creator(
                sync,
                &col_block_result,
                &collection_id,
                &collection_name,
                creator_ref,
            )
            .await;

            if let BroadcastStatus::Failed(error) = &collection_broadcast_status {
                error!(
                    doc_id = %col_block_result.doc_id,
                    collection = %collection_name,
                    error = %error,
                    "Deferred branchable collection broadcast failed — NOT replicated"
                );
            }
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static, B: Blockstore + 'static, T: P2PTransport> DocMutator
    for BroadcastBatchMutator<S, B, T>
{
    async fn create(
        &self,
        collection_name: &str,
        doc: Document,
    ) -> query::error::Result<CreateResult> {
        let result = self.inner.create(collection_name, doc).await?;
        let doc_id = result.doc_id.to_string();
        let document_json = document_json_value(&result.document);
        self.capture_broadcast(BroadcastCapture {
            kind: BroadcastKind::DagPush,
            collection_name,
            doc_id: &doc_id,
            commit_cid: result.commit_cid,
            commit_block: result.commit_block.as_ref(),
            document_json,
            broadcast_cid: result.broadcast_cid,
            broadcast_block: result.broadcast_block.as_ref(),
        })
        .await?;
        Ok(result)
    }

    async fn update(
        &self,
        collection_name: &str,
        doc: Document,
        modified_fields: HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        let result = self
            .inner
            .update(collection_name, doc, modified_fields)
            .await?;

        if let Some(doc_id) = result.document.id() {
            let doc_id = doc_id.to_string();
            let document_json = document_json_value(&result.document);
            self.capture_broadcast(BroadcastCapture {
                kind: BroadcastKind::DagPush,
                collection_name,
                doc_id: &doc_id,
                commit_cid: result.commit_cid,
                commit_block: result.commit_block.as_ref(),
                document_json,
                broadcast_cid: result.broadcast_cid,
                broadcast_block: result.broadcast_block.as_ref(),
            })
            .await?;
        } else {
            warn!(
                collection = %collection_name,
                "Updated document is missing _docID; skipping deferred broadcast"
            );
        }

        Ok(result)
    }

    async fn delete(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        let document_json = self
            .inner
            .get_for_update(collection_name, doc_id)
            .await?
            .and_then(|doc| document_json_value(&doc));
        let result = self.inner.delete(collection_name, doc_id).await?;
        let doc_id = result.doc_id.to_string();
        // Pass through the branchable collection block so it gets broadcast
        // alongside the composite delete (matches the non-batch BroadcastMutator
        // path and Go's two-update emit for branchable mutations).
        self.capture_broadcast(BroadcastCapture {
            kind: BroadcastKind::SingleBlockPush,
            collection_name,
            doc_id: &doc_id,
            commit_cid: result.commit_cid,
            commit_block: result.commit_block.as_ref(),
            document_json,
            broadcast_cid: result.broadcast_cid,
            broadcast_block: result.broadcast_block.as_ref(),
        })
        .await?;
        Ok(result)
    }

    async fn exists(&self, collection_name: &str, doc_id: &DocID) -> query::error::Result<bool> {
        self.inner.exists(collection_name, doc_id).await
    }

    async fn get_for_update(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<Option<Document>> {
        self.inner.get_for_update(collection_name, doc_id).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static, B: Blockstore + 'static, T: P2PTransport> MutationBatchController
    for BroadcastBatchMutator<S, B, T>
{
    async fn commit(&self) -> query::error::Result<()> {
        if let Err(err) = self.inner_controller.commit().await {
            self.pending_broadcasts.lock().await.clear();
            return Err(err);
        }

        let pending_broadcasts = std::mem::take(&mut *self.pending_broadcasts.lock().await);
        if !pending_broadcasts.is_empty() {
            let sync = self.sync.clone();
            tokio::spawn(async move {
                for pending in pending_broadcasts {
                    Self::broadcast_pending_static(&sync, pending).await;
                }
            });
        }

        Ok(())
    }

    async fn rollback(&self) -> query::error::Result<()> {
        let rollback_result = self.inner_controller.rollback().await;
        self.pending_broadcasts.lock().await.clear();
        rollback_result
    }
}
