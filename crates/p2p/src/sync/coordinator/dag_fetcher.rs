//! Poll-based DAG fetcher for DocSync and BranchableSync.
//!
//! Tries CAR fetch first (single round-trip for entire DAG), then falls back
//! to batched selective block fetch + blockstore polling for any remaining blocks.
//! Stalled batches rotate across the context's providers under a per-attempt
//! stall budget, timed-out transport queries are cancelled at the end of their
//! poll window, and incomplete fetches are retried with backoff before the
//! failure is escalated (#1093).

use std::sync::Arc;
use std::time::Duration;

use blockstore::Blockstore;
use cid::Cid;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use super::dag_context::DagFetchContext;
use super::dag_retry::{retry_backoff, ProviderRotation, MAX_FETCH_ATTEMPTS};
use super::DagFetchLimiter;
use crate::sync::manager::links::find_all_missing_links;
use crate::sync::manager::SyncEvent;
use crate::transport::{P2PTransport, PeerId};

const SELECTIVE_FETCH_BATCH_SIZE: usize = 2048;

/// Defensive ceiling on selective-fetch DAG-walk iterations.
///
/// Each iteration reveals and fetches the next frontier of missing blocks
/// (`find_all_missing_links` can only traverse into blocks already present), so
/// the walk runs roughly once per layer of the missing sub-DAG. The real loop
/// terminator is the `!made_progress` break — this ceiling only guards against a
/// peer that dribbles blocks indefinitely. It must stay well above any realistic
/// DAG depth: the previous fixed cap of 20 stranded any document with a longer
/// unreplicated update history unmerged, even while the walk was still making
/// progress every iteration.
const MAX_DAG_WALK_ITERATIONS: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchBatchOutcome {
    Complete,
    Partial,
    NoProgress,
}

/// Outcome of one provider's poll window, as seen by the rotation loop.
///
/// `Stalled` (a full window with zero blocks) consumes attempt stall budget;
/// `SendFailed` (the query never left) costs a rotation slot but no time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderWindowOutcome {
    Complete,
    Partial,
    Stalled,
    SendFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchAttemptOutcome {
    Complete,
    Incomplete { remaining: usize },
}

/// Fetch an entire DAG rooted at `root_cid`.
///
/// Strategy: try CAR fetch first (one round-trip), then selective block fetch
/// for any missing blocks, rotating providers on stalls. Incomplete attempts
/// are retried with backoff; terminal failure is logged at ERROR because the
/// document cannot converge until the root is announced again.
///
/// A limiter permit is held only for the duration of each attempt — it is
/// released during backoff sleeps so a fetch stuck on dead providers does not
/// starve other roots under fan-in pressure. Every issued transport query is
/// reaped via `cancel_sync` at the end of its poll window, so releasing the
/// permit never leaves stalled fetch work running in the transport.
pub async fn poll_fetch_dag<B: Blockstore + 'static, T: P2PTransport>(
    transport: T,
    blockstore: Arc<B>,
    event_tx: mpsc::Sender<SyncEvent>,
    root_cid: Cid,
    context: DagFetchContext,
    limiter: DagFetchLimiter,
) {
    let mut providers = ProviderRotation::new(context.providers());
    debug!(
        root_cid = %root_cid,
        doc_id = %context.doc_id,
        collection_id = %context.collection_id,
        source_peer = %context.source_peer,
        provider_count = providers.len(),
        is_explicit_replicator = context.is_explicit_replicator,
        "Starting DAG fetch (CAR-first, selective block fallback)"
    );

    let mut remaining_count = 0usize;
    for attempt in 1..=MAX_FETCH_ATTEMPTS {
        if attempt > 1 {
            let backoff = retry_backoff(attempt);
            warn!(
                root_cid = %root_cid,
                doc_id = %context.doc_id,
                attempt = attempt,
                remaining_count = remaining_count,
                backoff_ms = backoff.as_millis() as u64,
                "DAG fetch incomplete, retrying after backoff"
            );
            tokio::time::sleep(backoff).await;
        }

        let Some(_permits) = limiter.acquire(&context.source_peer).await else {
            debug!(
                root_cid = %root_cid,
                doc_id = %context.doc_id,
                attempt = attempt,
                "DAG fetch limiter closed, abandoning fetch"
            );
            return;
        };

        match fetch_dag_attempt(
            &transport,
            &blockstore,
            &event_tx,
            root_cid,
            &context,
            &mut providers,
        )
        .await
        {
            FetchAttemptOutcome::Complete => return,
            FetchAttemptOutcome::Incomplete { remaining } => remaining_count = remaining,
        }
    }

    error!(
        root_cid = %root_cid,
        doc_id = %context.doc_id,
        collection_id = %context.collection_id,
        remaining_count = remaining_count,
        attempts = MAX_FETCH_ATTEMPTS,
        providers = ?providers.peers(),
        "DAG fetch failed after exhausting retries and providers; document will not converge until the root is re-announced"
    );
}

/// Connected peers to offer as alternate providers for a DAG fetch.
///
/// A transport lookup failure costs only rotation capability, not the fetch
/// itself, so it degrades to source-peer-only fetching — with a WARN trace,
/// because a silently empty alternates list is indistinguishable from a
/// healthy but sparsely connected node.
pub(crate) async fn connected_alternate_providers<T: P2PTransport>(
    transport: &T,
    root_cid: &Cid,
) -> Vec<PeerId> {
    match transport.connected_peers().await {
        Ok(peers) => peers,
        Err(e) => {
            warn!(
                root_cid = %root_cid,
                error = %e,
                "Failed to list connected peers for DAG fetch alternates; fetching from source peer only"
            );
            Vec::new()
        }
    }
}

/// One full CAR-first + selective-walk attempt over the current provider set.
async fn fetch_dag_attempt<B: Blockstore + 'static, T: P2PTransport>(
    transport: &T,
    blockstore: &Arc<B>,
    event_tx: &mpsc::Sender<SyncEvent>,
    root_cid: Cid,
    context: &DagFetchContext,
    providers: &mut ProviderRotation,
) -> FetchAttemptOutcome {
    // One full rotation's worth of stalled 30s windows per attempt. Productive
    // windows (any block arrived) never consume it, so a large DAG that is
    // actually transferring is unbounded, while a dead provider set costs at
    // most providers × 30s of stalled waiting per attempt regardless of how
    // many batches the missing frontier spans.
    let mut stall_budget = providers.len();

    let car_missing_watch = match blockstore.get(&root_cid).await {
        Ok(Some(root_data)) => find_all_missing_links(blockstore.as_ref(), &root_data)
            .await
            .ok()
            .filter(|missing| !missing.is_empty()),
        _ => None,
    };

    // Try CAR fetch first
    if try_car_fetch(
        transport,
        blockstore,
        &root_cid,
        providers.current(),
        car_missing_watch.as_deref(),
    )
    .await
    {
        if let Ok(Some(root_data)) = blockstore.get(&root_cid).await {
            let missing = find_all_missing_links(blockstore.as_ref(), &root_data)
                .await
                .unwrap_or_default();
            if missing.is_empty() {
                info!(root_cid = %root_cid, doc_id = %context.doc_id, "DAG fetch complete via CAR");
                emit_dag_ready(event_tx, root_cid, context, &root_data).await;
                return FetchAttemptOutcome::Complete;
            }
            debug!(
                root_cid = %root_cid,
                missing_count = missing.len(),
                "CAR fetch was partial, falling through to selective block fetch"
            );
        }
    }

    // Fallback fetch: fetch the root block first so we can enumerate missing links.
    if !matches!(
        poll_fetch_blocks_rotating(
            &root_cid,
            std::slice::from_ref(&root_cid),
            transport,
            blockstore,
            providers,
            &mut stall_budget,
        )
        .await,
        FetchBatchOutcome::Complete
    ) {
        warn!(root_cid = %root_cid, "Failed to fetch root block");
        return FetchAttemptOutcome::Incomplete { remaining: 1 };
    }

    // Walk DAG, fetching missing blocks level by level. The walk stops as soon
    // as an iteration makes no progress (`!made_progress` below); the iteration
    // ceiling is only a defensive backstop, not a functional depth limit.
    for iteration in 0..MAX_DAG_WALK_ITERATIONS {
        let root_data = match blockstore.get(&root_cid).await {
            Ok(Some(data)) => data,
            _ => {
                warn!(root_cid = %root_cid, "Root block disappeared from blockstore");
                return FetchAttemptOutcome::Incomplete { remaining: 1 };
            }
        };

        let missing = match find_all_missing_links(blockstore.as_ref(), &root_data).await {
            Ok(m) => m,
            Err(e) => {
                warn!(root_cid = %root_cid, error = %e, "find_all_missing_links failed");
                return FetchAttemptOutcome::Incomplete { remaining: 0 };
            }
        };

        if missing.is_empty() {
            break;
        }

        debug!(
            root_cid = %root_cid,
            iteration = iteration,
            missing_count = missing.len(),
            "Fetching missing DAG blocks via selective block fetch"
        );

        let mut made_progress = false;
        for batch in missing.chunks(SELECTIVE_FETCH_BATCH_SIZE) {
            match poll_fetch_blocks_rotating(
                &root_cid,
                batch,
                transport,
                blockstore,
                providers,
                &mut stall_budget,
            )
            .await
            {
                FetchBatchOutcome::Complete => {
                    made_progress = true;
                }
                FetchBatchOutcome::Partial => {
                    made_progress = true;
                    debug!(
                        root_cid = %root_cid,
                        requested_count = batch.len(),
                        "Selective block batch made partial progress; continuing DAG walk"
                    );
                }
                FetchBatchOutcome::NoProgress => {
                    warn!(
                        root_cid = %root_cid,
                        requested_count = batch.len(),
                        providers_tried = providers.len(),
                        "Timeout fetching selective block batch (30s per provider)"
                    );
                }
            }
        }
        if !made_progress {
            break;
        }
    }

    // Verify DAG is complete
    let root_data = match blockstore.get(&root_cid).await {
        Ok(Some(data)) => data,
        _ => return FetchAttemptOutcome::Incomplete { remaining: 1 },
    };
    let remaining = find_all_missing_links(blockstore.as_ref(), &root_data)
        .await
        .unwrap_or_default();

    if remaining.is_empty() {
        info!(root_cid = %root_cid, doc_id = %context.doc_id, "DAG fetch complete");
        emit_dag_ready(event_tx, root_cid, context, &root_data).await;
        FetchAttemptOutcome::Complete
    } else {
        FetchAttemptOutcome::Incomplete {
            remaining: remaining.len(),
        }
    }
}

async fn emit_dag_ready(
    event_tx: &mpsc::Sender<SyncEvent>,
    root_cid: Cid,
    context: &DagFetchContext,
    root_data: &[u8],
) {
    let mut context = context.clone();
    context.fill_missing_from_block(root_data);
    if event_tx
        .send(context.into_dag_ready(root_cid))
        .await
        .is_err()
    {
        warn!(
            root_cid = %root_cid,
            "Failed to emit DagReady after DAG fetch"
        );
    }
}

/// Ceiling on the CAR request round-trip itself.
///
/// On iroh, `send_car_request` resolves only after the full connect /
/// stream-open / response-read cycle (up to ~50s against a half-open peer),
/// which would silently extend the attempt's CAR phase well past its intended
/// 10s budget while the limiter permit is held. CAR requests have no
/// cancellation handle, so on timeout the transport-side task is left to
/// self-terminate within its own bounded internal timeouts (at most one per
/// attempt); any late response still lands in the blockstore, where the
/// selective phase or a later attempt picks it up.
const CAR_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Try to fetch an entire DAG via a single CAR request.
async fn try_car_fetch<B: Blockstore, T: P2PTransport>(
    transport: &T,
    blockstore: &Arc<B>,
    root_cid: &Cid,
    source_peer: &PeerId,
    watch_missing: Option<&[Cid]>,
) -> bool {
    match tokio::time::timeout(
        CAR_REQUEST_TIMEOUT,
        transport.send_car_request(source_peer, *root_cid),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            debug!(root_cid = %root_cid, error = %e, "CAR request failed, will use selective block fetch");
            return false;
        }
        Err(_) => {
            debug!(
                root_cid = %root_cid,
                timeout_secs = CAR_REQUEST_TIMEOUT.as_secs(),
                "CAR request did not resolve within budget, falling back to selective block fetch"
            );
            return false;
        }
    }

    let timeout = Duration::from_secs(10);
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(missing) = watch_missing {
            let mut remaining = 0usize;
            for cid in missing {
                if !matches!(blockstore.has(cid).await, Ok(true)) {
                    remaining += 1;
                }
            }
            if remaining < missing.len() {
                return true;
            }
        } else if let Ok(true) = blockstore.has(root_cid).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    debug!(root_cid = %root_cid, "CAR fetch timed out (10s), falling back to selective block fetch");
    false
}

/// Fetch one batch of exact blocks, rotating to the next provider whenever
/// the current one yields nothing within the fetch window.
///
/// `stall_budget` caps the stalled (timed-out) provider windows one attempt
/// may burn across all of its batches; once it reaches zero, stalled batches
/// fail fast without issuing further transport queries.
async fn poll_fetch_blocks_rotating<B: Blockstore, T: P2PTransport>(
    root_cid: &Cid,
    cids: &[Cid],
    transport: &T,
    blockstore: &Arc<B>,
    providers: &mut ProviderRotation,
    stall_budget: &mut usize,
) -> FetchBatchOutcome {
    for _ in 0..providers.len() {
        if *stall_budget == 0 {
            debug!(
                root_cid = %root_cid,
                requested_count = cids.len(),
                "Attempt stall budget exhausted, failing batch without fetching"
            );
            return FetchBatchOutcome::NoProgress;
        }
        let provider = providers.current().clone();
        match poll_fetch_blocks(root_cid, cids, transport, blockstore, &provider).await {
            ProviderWindowOutcome::Complete => return FetchBatchOutcome::Complete,
            ProviderWindowOutcome::Partial => return FetchBatchOutcome::Partial,
            ProviderWindowOutcome::Stalled => {
                *stall_budget -= 1;
                providers.advance();
                if providers.len() > 1 {
                    warn!(
                        root_cid = %root_cid,
                        provider = %provider,
                        requested_count = cids.len(),
                        "No blocks from provider within fetch window, rotating to next provider"
                    );
                }
            }
            ProviderWindowOutcome::SendFailed => providers.advance(),
        }
    }
    FetchBatchOutcome::NoProgress
}

/// Fetch one batch of exact blocks via the transport's block-sync path.
///
/// The issued transport query is always reaped via `cancel_sync` before this
/// returns, so no query outlives its poll window: a stalled provider's
/// transport work cannot pile up behind rotation, keep consuming connections
/// after the fetch's limiter permit is released, or deliver blocks after the
/// fetch has terminally failed.
async fn poll_fetch_blocks<B: Blockstore, T: P2PTransport>(
    root_cid: &Cid,
    cids: &[Cid],
    transport: &T,
    blockstore: &Arc<B>,
    source_peer: &PeerId,
) -> ProviderWindowOutcome {
    let mut missing = Vec::new();
    for cid in cids {
        if matches!(blockstore.has(cid).await, Ok(true)) {
            continue;
        }
        missing.push(*cid);
    }

    if missing.is_empty() {
        return ProviderWindowOutcome::Complete;
    }

    let query_id = match transport
        .sync_blocks(*root_cid, vec![source_peer.clone()], missing.clone())
        .await
    {
        Ok(query_id) => query_id,
        Err(e) => {
            warn!(
                root_cid = %root_cid,
                requested_count = missing.len(),
                error = %e,
                "selective block fetch failed"
            );
            return ProviderWindowOutcome::SendFailed;
        }
    };

    let timeout = Duration::from_secs(30);
    let start = Instant::now();
    let mut outcome = ProviderWindowOutcome::Stalled;
    while start.elapsed() < timeout {
        let mut remaining = 0usize;
        for cid in &missing {
            if !matches!(blockstore.has(cid).await, Ok(true)) {
                remaining += 1;
            }
        }
        if remaining == 0 {
            outcome = ProviderWindowOutcome::Complete;
            break;
        }
        if remaining < missing.len() {
            outcome = ProviderWindowOutcome::Partial;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    if let Err(e) = transport.cancel_sync(query_id).await {
        debug!(
            root_cid = %root_cid,
            query_id = query_id.0,
            error = %e,
            "Failed to cancel block-sync query after poll window"
        );
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result as P2PResult;
    use crate::message::{
        BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
        PushLogReply, PushLogRequest, PushSEArtifactsRequest,
    };
    use crate::sync::manager::SyncEvent;
    use crate::topics::DefraTopic;
    use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId};
    use crate::{QueryId, ReplicatorInfo};
    use async_trait::async_trait;
    use blockstore::{Blockstore, DefraBlockstore};
    use ipld_core::{codec::Codec, ipld, ipld::Ipld};
    use multihash_codetable::{Code, MultihashDigest};
    use serde_ipld_dagcbor::codec::DagCborCodec;
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use storage::backends::MemoryStore;
    use tokio::sync::mpsc;

    fn make_cid(data: &[u8]) -> Cid {
        let hash = Code::Sha2_256.digest(data);
        Cid::new_v1(0x71, hash)
    }

    fn encode_ipld(ipld: Ipld) -> Vec<u8> {
        DagCborCodec::encode_to_vec(&ipld).unwrap()
    }

    #[derive(Clone)]
    struct TestTransport {
        peer_id: PeerId,
        pubkey: Vec<u8>,
        blockstore: Arc<DefraBlockstore<MemoryStore>>,
        root_cid: Cid,
        root_data: Vec<u8>,
        car_blocks: Arc<HashMap<Cid, Vec<u8>>>,
        selective_blocks: Arc<HashMap<Cid, Vec<u8>>>,
        car_requests: Arc<AtomicUsize>,
        sync_batches: Arc<Mutex<Vec<Vec<Cid>>>>,
        sync_providers: Arc<Mutex<Vec<String>>>,
        dead_providers: Arc<Mutex<HashSet<String>>>,
        skip_serving_syncs: Arc<AtomicUsize>,
        fail_connected_peers: Arc<AtomicBool>,
        cancelled_queries: Arc<Mutex<Vec<u64>>>,
        hang_car_requests: Arc<AtomicBool>,
    }

    impl TestTransport {
        fn new(
            blockstore: Arc<DefraBlockstore<MemoryStore>>,
            root_cid: Cid,
            root_data: Vec<u8>,
            car_blocks: HashMap<Cid, Vec<u8>>,
            selective_blocks: HashMap<Cid, Vec<u8>>,
        ) -> Self {
            Self {
                peer_id: PeerId::new("local-peer".to_string()),
                pubkey: vec![1, 2, 3],
                blockstore,
                root_cid,
                root_data,
                car_blocks: Arc::new(car_blocks),
                selective_blocks: Arc::new(selective_blocks),
                car_requests: Arc::new(AtomicUsize::new(0)),
                sync_batches: Arc::new(Mutex::new(Vec::new())),
                sync_providers: Arc::new(Mutex::new(Vec::new())),
                dead_providers: Arc::new(Mutex::new(HashSet::new())),
                skip_serving_syncs: Arc::new(AtomicUsize::new(0)),
                fail_connected_peers: Arc::new(AtomicBool::new(false)),
                cancelled_queries: Arc::new(Mutex::new(Vec::new())),
                hang_car_requests: Arc::new(AtomicBool::new(false)),
            }
        }

        fn car_request_count(&self) -> usize {
            self.car_requests.load(Ordering::SeqCst)
        }

        fn sync_batches(&self) -> Vec<Vec<Cid>> {
            self.sync_batches.lock().unwrap().clone()
        }

        fn sync_providers(&self) -> Vec<String> {
            self.sync_providers.lock().unwrap().clone()
        }

        fn mark_provider_dead(&self, peer: &str) {
            self.dead_providers.lock().unwrap().insert(peer.to_string());
        }

        fn set_skip_serving_syncs(&self, count: usize) {
            self.skip_serving_syncs.store(count, Ordering::SeqCst);
        }

        fn set_fail_connected_peers(&self) {
            self.fail_connected_peers.store(true, Ordering::SeqCst);
        }

        fn cancelled_queries(&self) -> Vec<u64> {
            self.cancelled_queries.lock().unwrap().clone()
        }

        fn set_hang_car_requests(&self) {
            self.hang_car_requests.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl P2PTransport for TestTransport {
        type ResponseToken = ();

        fn local_peer_id(&self) -> &PeerId {
            &self.peer_id
        }

        fn local_public_key_proto(&self) -> &[u8] {
            &self.pubkey
        }

        fn sign(&self, _data: &[u8]) -> P2PResult<Vec<u8>> {
            Ok(vec![0])
        }

        async fn dial(&self, _peer_id: &PeerId, _addrs: Vec<PeerAddr>) -> P2PResult<()> {
            Ok(())
        }

        async fn disconnect(&self, _peer_id: &PeerId) -> P2PResult<()> {
            Ok(())
        }

        async fn listen(&self, _addr: PeerAddr) -> P2PResult<()> {
            Ok(())
        }

        async fn connected_peers(&self) -> P2PResult<Vec<PeerId>> {
            if self.fail_connected_peers.load(Ordering::SeqCst) {
                return Err(crate::error::Error::Transport(
                    "peer listing unavailable".to_string(),
                ));
            }
            Ok(Vec::new())
        }

        async fn listen_addresses(&self) -> P2PResult<Vec<PeerAddr>> {
            Ok(Vec::new())
        }

        async fn poll_until_connected(
            &self,
            _peer_id: &PeerId,
            _timeout: Duration,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn peer_addresses(&self) -> P2PResult<Vec<String>> {
            Ok(Vec::new())
        }

        async fn topic_peers(&self, _topic: DefraTopic) -> P2PResult<Vec<PeerId>> {
            Ok(Vec::new())
        }

        async fn subscribe(&self, _topic: DefraTopic) -> P2PResult<bool> {
            Ok(true)
        }

        async fn unsubscribe(&self, _topic: DefraTopic) -> P2PResult<bool> {
            Ok(true)
        }

        async fn publish(
            &self,
            _topic: DefraTopic,
            _msg: PushLogBroadcast,
        ) -> P2PResult<MessageId> {
            Ok(MessageId::new("noop".to_string()))
        }

        async fn send_pushlog_response(
            &self,
            _token: Self::ResponseToken,
            _reply: PushLogReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_two_stream_request(
            &self,
            _peer_id: &PeerId,
            _req: PushLogRequest,
        ) -> P2PResult<PushLogReply> {
            Ok(PushLogReply::success("noop"))
        }

        async fn send_two_stream_response(
            &self,
            _peer_id: &PeerId,
            _reply: PushLogReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_doc_sync_request(
            &self,
            _peer_id: &PeerId,
            _req: DocSyncRequest,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_doc_sync_response(
            &self,
            _peer_id: &PeerId,
            _reply: DocSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_branchable_sync_request(
            &self,
            _peer_id: &PeerId,
            _req: BranchableSyncRequest,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_branchable_sync_response(
            &self,
            _peer_id: &PeerId,
            _reply: BranchableSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_car_request(&self, _peer_id: &PeerId, root_cid: Cid) -> P2PResult<()> {
            assert_eq!(root_cid, self.root_cid);
            self.car_requests.fetch_add(1, Ordering::SeqCst);
            if self.hang_car_requests.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_secs(600)).await;
                return Ok(());
            }
            self.blockstore
                .put(&self.root_cid, &self.root_data)
                .await
                .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
            for (cid, data) in self.car_blocks.iter() {
                self.blockstore
                    .put(cid, data)
                    .await
                    .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
            }
            Ok(())
        }

        async fn send_car_response(&self, _peer_id: &PeerId, _car_data: Vec<u8>) -> P2PResult<()> {
            Ok(())
        }

        async fn send_car_response_token(
            &self,
            _token: Self::ResponseToken,
            _car_data: Vec<u8>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_doc_sync_response_token(
            &self,
            _token: Self::ResponseToken,
            _reply: DocSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_branchable_sync_response_token(
            &self,
            _token: Self::ResponseToken,
            _reply: BranchableSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_se_artifacts(
            &self,
            _peer_id: &PeerId,
            _req: PushSEArtifactsRequest,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn sync_blocks(
            &self,
            _root: Cid,
            providers: Vec<PeerId>,
            missing: Vec<Cid>,
        ) -> P2PResult<QueryId> {
            let call_index = {
                let mut batches = self.sync_batches.lock().unwrap();
                batches.push(missing.clone());
                batches.len() - 1
            };
            self.sync_providers
                .lock()
                .unwrap()
                .extend(providers.iter().map(|peer| peer.to_string()));
            let query_id = QueryId(call_index as u64 + 1);
            if call_index < self.skip_serving_syncs.load(Ordering::SeqCst) {
                return Ok(query_id);
            }
            let all_dead = {
                let dead = self.dead_providers.lock().unwrap();
                providers.iter().all(|peer| dead.contains(peer.as_str()))
            };
            if all_dead {
                return Ok(query_id);
            }
            for cid in missing {
                if let Some(data) = self.selective_blocks.get(&cid) {
                    self.blockstore
                        .put(&cid, data)
                        .await
                        .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
                }
            }
            Ok(query_id)
        }

        async fn cancel_sync(&self, query_id: QueryId) -> P2PResult<bool> {
            self.cancelled_queries.lock().unwrap().push(query_id.0);
            Ok(true)
        }

        async fn create_replicator(
            &self,
            _peer_id: &PeerId,
            _collections: Vec<String>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn delete_replicator(&self, _peer_id: &PeerId) -> P2PResult<()> {
            Ok(())
        }

        async fn list_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>> {
            Ok(Vec::new())
        }

        async fn get_replicator(&self, _peer_id: &PeerId) -> P2PResult<Option<ReplicatorInfo>> {
            Ok(None)
        }

        async fn remove_replicator_collections(
            &self,
            _peer_id: &PeerId,
            _collections: Vec<String>,
        ) -> P2PResult<bool> {
            Ok(false)
        }

        async fn shutdown(&self) -> P2PResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn poll_fetch_dag_recovers_partial_car_with_batched_selective_fetch() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));

        let child_one_data = encode_ipld(ipld!({ "value": 1 }));
        let child_one_cid = make_cid(&child_one_data);
        let child_two_data = encode_ipld(ipld!({ "value": 2 }));
        let child_two_cid = make_cid(&child_two_data);
        let root_data = encode_ipld(ipld!({ "children": [child_one_cid, child_two_cid] }));
        let root_cid = make_cid(&root_data);

        let selective_blocks = HashMap::from([
            (child_one_cid, child_one_data.clone()),
            (child_two_cid, child_two_data.clone()),
        ]);
        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::new(),
            selective_blocks,
        );

        let (event_tx, mut event_rx) = mpsc::channel(1);
        let source_peer = PeerId::new("remote-peer".to_string());

        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            DagFetchContext::new(
                "doc-id".to_string(),
                "collection-id".to_string(),
                "creator-id".to_string(),
                source_peer.clone(),
            )
            .with_explicit_replicator(true),
            DagFetchLimiter::new(2),
        )
        .await;

        match event_rx.recv().await {
            Some(SyncEvent::DagReady {
                root_cid: ready_cid,
                doc_id,
                collection_id,
                creator,
                sender_peer,
                is_explicit_replicator,
                ..
            }) => {
                assert_eq!(ready_cid, root_cid);
                assert_eq!(doc_id, "doc-id");
                assert_eq!(collection_id, "collection-id");
                assert_eq!(creator, "creator-id");
                assert_eq!(sender_peer.as_deref(), Some(source_peer.as_str()));
                assert!(is_explicit_replicator);
            }
            other => panic!("expected DagReady, got {:?}", other),
        }
        assert!(matches!(blockstore.has(&child_one_cid).await, Ok(true)));
        assert!(matches!(blockstore.has(&child_two_cid).await, Ok(true)));
        assert_eq!(transport.car_request_count(), 1);

        let batches = transport.sync_batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 2);

        let requested: HashSet<_> = batches[0].iter().copied().collect();
        assert_eq!(requested, HashSet::from([child_one_cid, child_two_cid]));
    }

    #[tokio::test]
    async fn poll_fetch_dag_does_not_treat_preexisting_root_as_car_success() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));

        let child_data = encode_ipld(ipld!({ "value": 1 }));
        let child_cid = make_cid(&child_data);
        let root_data = encode_ipld(ipld!({ "child": child_cid }));
        let root_cid = make_cid(&root_data);

        blockstore.put(&root_cid, &root_data).await.unwrap();

        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::from([(child_cid, child_data.clone())]),
            HashMap::new(),
        );

        let (event_tx, mut event_rx) = mpsc::channel(1);
        let source_peer = PeerId::new("remote-peer".to_string());

        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            DagFetchContext::new(
                "doc-id".to_string(),
                "collection-id".to_string(),
                "creator-id".to_string(),
                source_peer,
            ),
            DagFetchLimiter::new(2),
        )
        .await;

        assert!(matches!(
            event_rx.recv().await,
            Some(SyncEvent::DagReady { root_cid: ready_cid, .. }) if ready_cid == root_cid
        ));
        assert!(matches!(blockstore.has(&child_cid).await, Ok(true)));
        assert_eq!(transport.car_request_count(), 1);
        assert!(transport.sync_batches().is_empty());
    }

    #[tokio::test]
    async fn poll_fetch_dag_continues_after_partial_selective_batch_progress() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));

        let leaf_one_data = encode_ipld(ipld!({ "value": 1 }));
        let leaf_one_cid = make_cid(&leaf_one_data);
        let leaf_two_data = encode_ipld(ipld!({ "value": 2 }));
        let leaf_two_cid = make_cid(&leaf_two_data);

        let mid_one_data = encode_ipld(ipld!({ "child": leaf_one_cid }));
        let mid_one_cid = make_cid(&mid_one_data);
        let mid_two_data = encode_ipld(ipld!({ "child": leaf_two_cid }));
        let mid_two_cid = make_cid(&mid_two_data);

        let root_data = encode_ipld(ipld!({ "children": [mid_one_cid, mid_two_cid] }));
        let root_cid = make_cid(&root_data);

        let selective_blocks = HashMap::from([
            (mid_one_cid, mid_one_data.clone()),
            (mid_two_cid, mid_two_data.clone()),
            (leaf_one_cid, leaf_one_data.clone()),
            (leaf_two_cid, leaf_two_data.clone()),
        ]);
        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::new(),
            selective_blocks,
        );

        let (event_tx, mut event_rx) = mpsc::channel(1);
        let source_peer = PeerId::new("remote-peer".to_string());

        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            DagFetchContext::new(
                "doc-id".to_string(),
                "collection-id".to_string(),
                "creator-id".to_string(),
                source_peer,
            ),
            DagFetchLimiter::new(2),
        )
        .await;

        assert!(matches!(
            event_rx.recv().await,
            Some(SyncEvent::DagReady { root_cid: ready_cid, .. }) if ready_cid == root_cid
        ));

        let batches = transport.sync_batches();
        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches[0].iter().copied().collect::<HashSet<_>>(),
            HashSet::from([mid_one_cid, mid_two_cid])
        );
        assert_eq!(
            batches[1].iter().copied().collect::<HashSet<_>>(),
            HashSet::from([leaf_one_cid, leaf_two_cid])
        );
    }

    /// A linear DAG deeper than the old fixed 20-iteration cap must still fully
    /// reconcile. Each selective-fetch iteration reveals one deeper layer, so a
    /// 25-deep chain needs 24 selective iterations; the previous `0..20` cap
    /// abandoned it unmerged (no `DagReady`) even though every iteration was
    /// still making progress.
    #[tokio::test]
    async fn poll_fetch_dag_completes_dag_deeper_than_legacy_iteration_cap() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));

        const DEPTH: usize = 25;

        // Build a linear chain nodes[0] (leaf) -> ... -> nodes[DEPTH-1] (root),
        // each node linking its single child by CID.
        let mut nodes: Vec<(Cid, Vec<u8>)> = Vec::with_capacity(DEPTH);
        let mut child: Option<Cid> = None;
        for i in 0..DEPTH {
            let data = match child {
                Some(c) => encode_ipld(ipld!({ "i": i as i64, "child": c })),
                None => encode_ipld(ipld!({ "i": i as i64 })),
            };
            let cid = make_cid(&data);
            child = Some(cid);
            nodes.push((cid, data));
        }
        let (root_cid, root_data) = nodes.last().unwrap().clone();

        // Root arrives via CAR; every ancestor is fetched one layer per iteration.
        let selective_blocks: HashMap<Cid, Vec<u8>> = nodes[..DEPTH - 1]
            .iter()
            .map(|(cid, data)| (*cid, data.clone()))
            .collect();
        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::new(),
            selective_blocks,
        );

        let (event_tx, mut event_rx) = mpsc::channel(1);
        let source_peer = PeerId::new("remote-peer".to_string());

        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            DagFetchContext::new(
                "doc-id".to_string(),
                "collection-id".to_string(),
                "creator-id".to_string(),
                source_peer,
            ),
            DagFetchLimiter::new(2),
        )
        .await;

        assert!(
            matches!(
                event_rx.recv().await,
                Some(SyncEvent::DagReady { root_cid: ready_cid, .. }) if ready_cid == root_cid
            ),
            "a DAG deeper than the legacy 20-iteration cap must fully reconcile and emit DagReady"
        );
        for (cid, _) in &nodes {
            assert!(matches!(blockstore.has(cid).await, Ok(true)));
        }
        // One selective iteration per ancestor: DEPTH - 1 batches.
        assert_eq!(transport.sync_batches().len(), DEPTH - 1);
    }

    fn single_child_dag() -> (Cid, Vec<u8>, Cid, Vec<u8>) {
        let child_data = encode_ipld(ipld!({ "value": 1 }));
        let child_cid = make_cid(&child_data);
        let root_data = encode_ipld(ipld!({ "child": child_cid }));
        let root_cid = make_cid(&root_data);
        (root_cid, root_data, child_cid, child_data)
    }

    /// A dead source peer must not fail the walk: the batch rotates to the
    /// alternate provider and the fetch completes on the first attempt.
    #[tokio::test(start_paused = true)]
    async fn poll_fetch_dag_rotates_to_alternate_provider_on_no_progress() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (root_cid, root_data, child_cid, child_data) = single_child_dag();

        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::new(),
            HashMap::from([(child_cid, child_data)]),
        );
        transport.mark_provider_dead("dead-peer");

        let (event_tx, mut event_rx) = mpsc::channel(1);
        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            DagFetchContext::new(
                "doc-id".to_string(),
                "collection-id".to_string(),
                "creator-id".to_string(),
                PeerId::new("dead-peer".to_string()),
            )
            .with_alternate_providers(vec![PeerId::new("alt-peer".to_string())]),
            DagFetchLimiter::new(2),
        )
        .await;

        assert!(matches!(
            event_rx.recv().await,
            Some(SyncEvent::DagReady { root_cid: ready_cid, .. }) if ready_cid == root_cid
        ));
        assert!(matches!(blockstore.has(&child_cid).await, Ok(true)));
        assert_eq!(transport.car_request_count(), 1);
        assert_eq!(
            transport.sync_providers(),
            vec!["dead-peer".to_string(), "alt-peer".to_string()]
        );
    }

    /// An attempt that stalls (no blocks served) must be retried after
    /// backoff and succeed once the provider starts serving.
    #[tokio::test(start_paused = true)]
    async fn poll_fetch_dag_retries_incomplete_fetch_and_succeeds() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (root_cid, root_data, child_cid, child_data) = single_child_dag();

        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::new(),
            HashMap::from([(child_cid, child_data)]),
        );
        transport.set_skip_serving_syncs(1);

        let (event_tx, mut event_rx) = mpsc::channel(1);
        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            DagFetchContext::new(
                "doc-id".to_string(),
                "collection-id".to_string(),
                "creator-id".to_string(),
                PeerId::new("remote-peer".to_string()),
            ),
            DagFetchLimiter::new(2),
        )
        .await;

        assert!(matches!(
            event_rx.recv().await,
            Some(SyncEvent::DagReady { root_cid: ready_cid, .. }) if ready_cid == root_cid
        ));
        assert!(matches!(blockstore.has(&child_cid).await, Ok(true)));
        // One CAR try and one selective batch per attempt; success on attempt 2.
        assert_eq!(transport.car_request_count(), 2);
        assert_eq!(transport.sync_batches().len(), 2);
    }

    /// When every attempt stalls against every provider the fetcher stops
    /// after MAX_FETCH_ATTEMPTS without emitting DagReady (terminal failure).
    #[tokio::test(start_paused = true)]
    async fn poll_fetch_dag_exhausted_retries_do_not_emit_dag_ready() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (root_cid, root_data, child_cid, child_data) = single_child_dag();

        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::new(),
            HashMap::from([(child_cid, child_data)]),
        );
        transport.mark_provider_dead("dead-peer");

        let (event_tx, mut event_rx) = mpsc::channel(1);
        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            DagFetchContext::new(
                "doc-id".to_string(),
                "collection-id".to_string(),
                "creator-id".to_string(),
                PeerId::new("dead-peer".to_string()),
            ),
            DagFetchLimiter::new(2),
        )
        .await;

        assert!(
            event_rx.recv().await.is_none(),
            "terminal failure must not emit DagReady"
        );
        assert!(matches!(blockstore.has(&child_cid).await, Ok(false)));
        // One CAR try and one selective batch per attempt, all exhausted.
        assert_eq!(transport.car_request_count(), MAX_FETCH_ATTEMPTS as usize);
        assert_eq!(transport.sync_batches().len(), MAX_FETCH_ATTEMPTS as usize);
    }

    /// Every issued block-sync query must be reaped via `cancel_sync` at the
    /// end of its poll window: a stalled provider's transport-side work must
    /// not outlive rotation, the limiter permit, or terminal failure.
    #[tokio::test(start_paused = true)]
    async fn poll_fetch_dag_cancels_every_issued_query() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (root_cid, root_data, child_cid, child_data) = single_child_dag();

        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::new(),
            HashMap::from([(child_cid, child_data)]),
        );
        transport.mark_provider_dead("dead-peer");

        let (event_tx, mut event_rx) = mpsc::channel(1);
        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            DagFetchContext::new(
                "doc-id".to_string(),
                "collection-id".to_string(),
                "creator-id".to_string(),
                PeerId::new("dead-peer".to_string()),
            ),
            DagFetchLimiter::new(2),
        )
        .await;

        assert!(event_rx.recv().await.is_none());
        let issued: Vec<u64> = (1..=transport.sync_batches().len() as u64).collect();
        assert_eq!(
            transport.cancelled_queries(),
            issued,
            "every stalled query must be cancelled, in issue order"
        );
    }

    /// The per-attempt stall budget must cap stalled-batch work: once every
    /// provider has burned a full window, remaining batches in the attempt
    /// fail fast without issuing transport queries, so dead-provider attempt
    /// time does not scale with the width of the missing frontier.
    #[tokio::test(start_paused = true)]
    async fn poll_fetch_dag_stall_budget_caps_stalled_batches_per_attempt() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));

        let width = SELECTIVE_FETCH_BATCH_SIZE + 1;
        let mut children = Vec::with_capacity(width);
        for i in 0..width {
            let data = encode_ipld(ipld!({ "value": i as i64 }));
            children.push(make_cid(&data));
        }
        let root_data = encode_ipld(Ipld::List(
            children.iter().map(|cid| Ipld::Link(*cid)).collect(),
        ));
        let root_cid = make_cid(&root_data);
        blockstore.put(&root_cid, &root_data).await.unwrap();

        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::new(),
            HashMap::new(),
        );
        transport.mark_provider_dead("dead-peer");

        let (event_tx, mut event_rx) = mpsc::channel(1);
        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            DagFetchContext::new(
                "doc-id".to_string(),
                "collection-id".to_string(),
                "creator-id".to_string(),
                PeerId::new("dead-peer".to_string()),
            ),
            DagFetchLimiter::new(2),
        )
        .await;

        assert!(event_rx.recv().await.is_none());
        // Two batches per attempt are missing, but the single provider's stall
        // budget is spent on the first, so the second issues no query: one
        // stalled (and cancelled) query per attempt, not two.
        let batches = transport.sync_batches();
        assert_eq!(batches.len(), MAX_FETCH_ATTEMPTS as usize);
        for batch in &batches {
            assert_eq!(batch.len(), SELECTIVE_FETCH_BATCH_SIZE);
        }
        assert_eq!(
            transport.cancelled_queries(),
            (1..=MAX_FETCH_ATTEMPTS as u64).collect::<Vec<_>>()
        );
    }

    /// A CAR request that never resolves (half-open peer: connected but
    /// unresponsive) must not stall the attempt beyond the CAR budget — the
    /// fetch falls back to the selective path and completes, instead of
    /// waiting out the transport's full internal timeout chain.
    #[tokio::test(start_paused = true)]
    async fn poll_fetch_dag_bounds_hung_car_request() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (root_cid, root_data, child_cid, child_data) = single_child_dag();

        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data.clone(),
            HashMap::new(),
            HashMap::from([(root_cid, root_data), (child_cid, child_data)]),
        );
        transport.set_hang_car_requests();

        let started = Instant::now();
        let (event_tx, mut event_rx) = mpsc::channel(1);
        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            DagFetchContext::new(
                "doc-id".to_string(),
                "collection-id".to_string(),
                "creator-id".to_string(),
                PeerId::new("remote-peer".to_string()),
            ),
            DagFetchLimiter::new(2),
        )
        .await;

        assert!(matches!(
            event_rx.recv().await,
            Some(SyncEvent::DagReady { root_cid: ready_cid, .. }) if ready_cid == root_cid
        ));
        assert!(matches!(blockstore.has(&child_cid).await, Ok(true)));
        assert_eq!(transport.car_request_count(), 1);
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "hung CAR request must be cut at its budget, not awaited to transport timeouts; elapsed: {:?}",
            started.elapsed()
        );
    }

    /// A `connected_peers()` failure must degrade to an empty alternates list
    /// (source-peer-only rotation), not abort the fetch: composed exactly as
    /// the event-handler call sites do, the fetch still completes from the
    /// source peer.
    #[tokio::test(start_paused = true)]
    async fn poll_fetch_dag_completes_from_source_when_peer_listing_fails() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (root_cid, root_data, child_cid, child_data) = single_child_dag();

        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::new(),
            HashMap::from([(child_cid, child_data)]),
        );
        transport.set_fail_connected_peers();

        let alternate_providers = connected_alternate_providers(&transport, &root_cid).await;
        assert!(
            alternate_providers.is_empty(),
            "transport failure must degrade to no alternates"
        );

        let (event_tx, mut event_rx) = mpsc::channel(1);
        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            DagFetchContext::new(
                "doc-id".to_string(),
                "collection-id".to_string(),
                "creator-id".to_string(),
                PeerId::new("remote-peer".to_string()),
            )
            .with_alternate_providers(alternate_providers),
            DagFetchLimiter::new(2),
        )
        .await;

        assert!(matches!(
            event_rx.recv().await,
            Some(SyncEvent::DagReady { root_cid: ready_cid, .. }) if ready_cid == root_cid
        ));
        assert!(matches!(blockstore.has(&child_cid).await, Ok(true)));
        assert_eq!(transport.sync_providers(), vec!["remote-peer".to_string()]);
    }

    /// The limiter permit must be released during retry backoff: a second
    /// waiter acquires the single permit while the first fetch is sleeping
    /// between attempts, not after all of its retries complete.
    #[tokio::test(start_paused = true)]
    async fn poll_fetch_dag_releases_limiter_permit_during_backoff() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (root_cid, root_data, child_cid, child_data) = single_child_dag();

        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::new(),
            HashMap::from([(child_cid, child_data)]),
        );
        transport.mark_provider_dead("dead-peer");

        let limiter = DagFetchLimiter::new(1);
        let (event_tx, mut event_rx) = mpsc::channel(1);
        let fetch = tokio::spawn(poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            DagFetchContext::new(
                "doc-id".to_string(),
                "collection-id".to_string(),
                "creator-id".to_string(),
                PeerId::new("dead-peer".to_string()),
            ),
            limiter.clone(),
        ));

        // Let attempt 1 start (and therefore hold the only permit) before
        // competing for it.
        while transport.sync_batches().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Resolves as soon as attempt 1's permit drops (start of backoff);
        // if the permit spanned all attempts this would only resolve after
        // the fetch task finished.
        let permits = limiter
            .acquire(&PeerId::new("other-peer".to_string()))
            .await
            .expect("limiter must grant a permit while the fetch is backing off");
        assert!(
            !fetch.is_finished(),
            "fetch must still be mid-retry while another waiter holds the permit"
        );
        assert_eq!(transport.sync_batches().len(), 1);

        drop(permits);
        fetch.await.unwrap();

        assert!(event_rx.recv().await.is_none());
        assert_eq!(transport.sync_batches().len(), MAX_FETCH_ATTEMPTS as usize);
    }
}
