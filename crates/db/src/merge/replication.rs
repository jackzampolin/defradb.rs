//! Small replication-facing facade over the concrete db-merge module layout.
//!
//! This keeps startup and doc-pusher code from depending on individual module
//! paths, which makes later internal decomposition less invasive.

use std::sync::Arc;

use crate::database::DB;
use blockstore::Blockstore;
use cid::Cid;
use p2p::sync::{DocumentHeadProvider, PushFailure, SyncCoordinator};
use p2p::transport::P2PTransport;
use storage::corekv::Store;
use tokio::sync::mpsc;

use crate::event::emission::TxnBroadcaster;
use crate::merge::acp_merge_handler::AcpMergeHandler;
use crate::merge::broadcast_mutator::BroadcastMutator;
use crate::merge::head_provider::DbHeadProvider;
use crate::merge::merge_handler::DbMergeHandler;
use crate::merge::txn_broadcaster::SyncTxnBroadcaster;

pub struct ReplicationStack<S: Store, B: Blockstore + Send + Sync, T: P2PTransport> {
    pub merge_handler_inner: Arc<DbMergeHandler<S, B>>,
    pub merge_handler: Arc<AcpMergeHandler<S, B>>,
    pub broadcast_mutator: Arc<BroadcastMutator<S, B, T>>,
    /// Broadcaster for transactional writes. Wire this into
    /// `DbTransactionRegistry::with_broadcaster` so committed `/tx` mutations
    /// reach P2P peers just like single-mutation auto-commit writes do.
    pub txn_broadcaster: Arc<dyn TxnBroadcaster>,
}

pub fn create_head_provider<S: Store>(db: Arc<DB<S>>) -> DbHeadProvider<S> {
    DbHeadProvider::new(db)
}

pub fn create_merge_handler<S: Store, B: Blockstore + Send + Sync>(
    db: Arc<DB<S>>,
    blockstore: Arc<B>,
) -> DbMergeHandler<S, B> {
    DbMergeHandler::new(db, blockstore)
}

pub fn create_acp_merge_handler<S: Store, B: Blockstore + Send + Sync>(
    inner: Arc<DbMergeHandler<S, B>>,
) -> AcpMergeHandler<S, B> {
    AcpMergeHandler::new(inner)
}

pub fn create_broadcast_mutator<S: Store, B: Blockstore + 'static, T: P2PTransport>(
    db: Arc<DB<S>>,
    sync: Arc<SyncCoordinator<B, T>>,
) -> BroadcastMutator<S, B, T> {
    BroadcastMutator::new(db, sync)
}

pub fn create_replication_stack<
    S: Store,
    B: Blockstore + Send + Sync + 'static,
    T: P2PTransport + 'static,
>(
    db: Arc<DB<S>>,
    blockstore: Arc<B>,
    sync: Arc<SyncCoordinator<B, T>>,
) -> ReplicationStack<S, B, T> {
    create_replication_stack_with_max_merge_depth(
        db,
        blockstore,
        sync,
        crate::merge::DEFAULT_MAX_MERGE_DEPTH,
    )
}

pub fn create_replication_stack_with_max_merge_depth<
    S: Store,
    B: Blockstore + Send + Sync + 'static,
    T: P2PTransport + 'static,
>(
    db: Arc<DB<S>>,
    blockstore: Arc<B>,
    sync: Arc<SyncCoordinator<B, T>>,
    max_merge_depth: usize,
) -> ReplicationStack<S, B, T> {
    let merge_handler_inner = Arc::new(DbMergeHandler::new_with_max_merge_depth(
        db.clone(),
        blockstore,
        max_merge_depth,
    ));
    let merge_handler = Arc::new(create_acp_merge_handler(merge_handler_inner.clone()));
    let broadcast_mutator = Arc::new(create_broadcast_mutator(db, sync.clone()));
    let txn_broadcaster: Arc<dyn TxnBroadcaster> = Arc::new(SyncTxnBroadcaster::new(sync));

    ReplicationStack {
        merge_handler_inner,
        merge_handler,
        broadcast_mutator,
        txn_broadcaster,
    }
}

pub fn attach_failure_channel<B: Blockstore + 'static, T: P2PTransport>(
    coordinator: &mut SyncCoordinator<B, T>,
    capacity: usize,
) -> mpsc::Receiver<PushFailure> {
    let (failure_tx, failure_rx) = mpsc::channel::<PushFailure>(capacity);
    coordinator.set_failure_channel(failure_tx);
    failure_rx
}

pub async fn load_persisted_collections<B: Blockstore + 'static, T: P2PTransport>(
    coordinator: &Arc<SyncCoordinator<B, T>>,
) -> Result<usize, String> {
    coordinator
        .load_p2p_collections()
        .await
        .map_err(|error| error.to_string())
}

pub async fn load_document_head_blocks<S: Store + 'static>(
    db: &Arc<DB<S>>,
    doc_id: &str,
) -> Result<Vec<(Cid, Vec<u8>)>, String> {
    let provider = create_head_provider(db.clone());
    let heads = <DbHeadProvider<S> as DocumentHeadProvider>::get_document_heads(&provider, doc_id)
        .await
        .map_err(|error| format!("failed to load document heads: {error}"))?;

    let txn = db
        .new_txn(true)
        .await
        .map_err(|error| format!("failed to create read transaction: {error}"))?;
    let blockstore = txn
        .blockstore()
        .map_err(|error| format!("failed to get blockstore: {error}"))?;

    let mut blocks = Vec::with_capacity(heads.len());
    for cid in heads {
        let bytes = blockstore
            .get(&cid.to_bytes())
            .await
            .map_err(|error| format!("failed to read head block {cid}: {error}"))?
            .ok_or_else(|| format!("head block {cid} not found"))?;
        blocks.push((cid, bytes));
    }

    let _ = txn.discard();
    Ok(blocks)
}
