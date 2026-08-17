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
use crate::sync::manager::SyncDiagnostics;
use crate::sync::manager::SyncEvent;
use crate::transport::{P2PTransport, PeerId};

const SELECTIVE_FETCH_BATCH_SIZE: usize = 2048;
/// Outer owner watchdog for a transport block-sync response.
///
/// Iroh bounds its request/response read at 30 seconds. The receiver owner must
/// outlive that bound so the transport completion event, rather than a racing
/// shorter poll window, decides when a productive CAR stream is reaped.
const BLOCK_SYNC_COMPLETION_WATCHDOG: Duration = Duration::from_secs(35);
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
    diagnostics: Arc<SyncDiagnostics>,
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
        if !context.is_current() {
            debug!(
                root_cid = %root_cid,
                doc_id = %context.doc_id,
                collection_id = %context.collection_id,
                "Stopping fetch for superseded sender/scope head"
            );
            return;
        }
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
        if !context.is_current() {
            return;
        }

        match fetch_dag_attempt(
            &transport,
            &blockstore,
            &event_tx,
            root_cid,
            &context,
            &mut providers,
            diagnostics.as_ref(),
        )
        .await
        {
            FetchAttemptOutcome::Complete => return,
            FetchAttemptOutcome::Incomplete { remaining } => remaining_count = remaining,
        }
    }

    diagnostics.record_pending_dag_fetch_exhausted();
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
    diagnostics: &SyncDiagnostics,
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

    // Sender demotion removes per-CID provider advertisements. Exercise the
    // rooted authority at the announcing publisher first (with a cancellable,
    // bounded query), then request only the still-missing exact frontier.
    // This lets a directly connected hub cache a leaf's DAG for the rest of a
    // star without returning to unbounded recursive-first ownership.
    let rooted_outcome = if let Some(rooted_watch) = car_missing_watch.as_deref() {
        if context.needs_rooted_provider_discovery() {
            if transport.supports_cancellable_rooted_sync() {
                poll_fetch_rooted_provider(
                    &root_cid,
                    rooted_watch,
                    transport,
                    blockstore,
                    providers.current(),
                    context,
                )
                .await
            } else {
                poll_fetch_rooted_car(
                    &root_cid,
                    rooted_watch,
                    transport,
                    blockstore,
                    providers.current(),
                    context,
                )
                .await
            }
        } else {
            ProviderWindowOutcome::SendFailed
        }
    } else if try_car_fetch(transport, blockstore, &root_cid, providers.current()).await {
        ProviderWindowOutcome::Partial
    } else {
        ProviderWindowOutcome::SendFailed
    };
    if matches!(
        rooted_outcome,
        ProviderWindowOutcome::Complete | ProviderWindowOutcome::Partial
    ) {
        if let Ok(Some(root_data)) = blockstore.get(&root_cid).await {
            let missing = find_all_missing_links(blockstore.as_ref(), &root_data)
                .await
                .unwrap_or_default();
            if missing.is_empty() {
                info!(root_cid = %root_cid, doc_id = %context.doc_id, "DAG fetch complete via rooted CAR");
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
            &mut ProviderFetchState {
                providers,
                stall_budget: &mut stall_budget,
                diagnostics,
            },
            context,
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
                &mut ProviderFetchState {
                    providers,
                    stall_budget: &mut stall_budget,
                    diagnostics,
                },
                context,
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

/// Root-absent DocSync/BranchableSync discovery retains the established CAR
/// request seam. PushLog recovery already has the root and uses the cancellable
/// rooted-provider probe below instead.
async fn try_car_fetch<B: Blockstore, T: P2PTransport>(
    transport: &T,
    blockstore: &Arc<B>,
    root_cid: &Cid,
    source_peer: &PeerId,
) -> bool {
    match tokio::time::timeout(
        Duration::from_secs(10),
        transport.send_car_request(source_peer, *root_cid),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            debug!(root_cid = %root_cid, error = %error, "CAR discovery request failed");
            return false;
        }
        Err(_) => return false,
    }

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        if matches!(blockstore.has(root_cid).await, Ok(true)) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Exercise the established libp2p CAR request protocol when the transport's
/// block-sync primitive cannot express a recursive root request.  Sending the
/// request and receiving its CAR response are separate streams on libp2p, so
/// completion is established from the bounded rooted frontier in blockstore.
async fn poll_fetch_rooted_car<B: Blockstore, T: P2PTransport>(
    root_cid: &Cid,
    watch_cids: &[Cid],
    transport: &T,
    blockstore: &Arc<B>,
    source_peer: &PeerId,
    context: &DagFetchContext,
) -> ProviderWindowOutcome {
    let mut initially_missing = 0usize;
    for cid in watch_cids {
        if !matches!(blockstore.has(cid).await, Ok(true)) {
            initially_missing += 1;
        }
    }
    if initially_missing == 0 {
        return ProviderWindowOutcome::Complete;
    }

    let mut completion = context.track_rooted_car(*root_cid);

    match tokio::time::timeout(
        Duration::from_secs(10),
        transport.send_car_request(source_peer, *root_cid),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            context.cancel_rooted_car_tracking(*root_cid);
            debug!(root_cid = %root_cid, source_peer = %source_peer, error = %error, "Rooted CAR request failed before dispatch");
            return ProviderWindowOutcome::SendFailed;
        }
        Err(_) => {
            context.cancel_rooted_car_tracking(*root_cid);
            return ProviderWindowOutcome::Stalled;
        }
    }

    let observe = |remaining: usize| {
        if remaining == 0 {
            ProviderWindowOutcome::Complete
        } else if remaining < initially_missing {
            ProviderWindowOutcome::Partial
        } else {
            ProviderWindowOutcome::Stalled
        }
    };
    let remaining_now = count_missing(blockstore, watch_cids).await;
    if remaining_now < initially_missing {
        context.cancel_rooted_car_tracking(*root_cid);
        return observe(remaining_now);
    }

    if let Some(receiver) = completion.as_mut() {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if !context.is_current() {
                context.cancel_rooted_car_tracking(*root_cid);
                return ProviderWindowOutcome::Stalled;
            }
            tokio::select! {
                _ = &mut *receiver => break,
                _ = tokio::time::sleep_until(deadline) => break,
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        }
    } else {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(10) && context.is_current() {
            let remaining = count_missing(blockstore, watch_cids).await;
            if remaining < initially_missing {
                return observe(remaining);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    context.cancel_rooted_car_tracking(*root_cid);
    observe(count_missing(blockstore, watch_cids).await)
}

async fn count_missing<B: Blockstore>(blockstore: &Arc<B>, cids: &[Cid]) -> usize {
    let mut remaining = 0usize;
    for cid in cids {
        if !matches!(blockstore.has(cid).await, Ok(true)) {
            remaining += 1;
        }
    }
    remaining
}

/// Exercise one authenticated provider's rooted CAR authority before exact-CID
/// recovery. The query is cancellable and bounded, so an unavailable provider
/// cannot retain a transport task or monopolize the receiver fetch owner.
async fn poll_fetch_rooted_provider<B: Blockstore, T: P2PTransport>(
    root_cid: &Cid,
    watch_cids: &[Cid],
    transport: &T,
    blockstore: &Arc<B>,
    source_peer: &PeerId,
    context: &DagFetchContext,
) -> ProviderWindowOutcome {
    let mut initially_missing = 0usize;
    for cid in watch_cids {
        if !matches!(blockstore.has(cid).await, Ok(true)) {
            initially_missing += 1;
        }
    }
    if initially_missing == 0 {
        return ProviderWindowOutcome::Complete;
    }
    let query_id = match transport
        .sync_blocks(*root_cid, vec![source_peer.clone()], Vec::new())
        .await
    {
        Ok(query_id) => query_id,
        Err(error) => {
            debug!(root_cid = %root_cid, source_peer = %source_peer, error = %error, "Rooted CAR request failed before dispatch");
            return ProviderWindowOutcome::SendFailed;
        }
    };
    let mut completion = context.track_block_sync(query_id);
    let completion_is_observable = completion.is_some();
    let start = Instant::now();
    let timeout = BLOCK_SYNC_COMPLETION_WATCHDOG;
    let mut outcome = ProviderWindowOutcome::Stalled;
    let mut transport_complete = false;
    while start.elapsed() < timeout && context.is_current() {
        let mut remaining = 0usize;
        for cid in watch_cids {
            if !matches!(blockstore.has(cid).await, Ok(true)) {
                remaining += 1;
            }
        }
        if remaining == 0 {
            outcome = ProviderWindowOutcome::Complete;
            // Even a locally complete DAG does not imply that the peer has
            // finished the response framing.  When completion is observable,
            // drain it; otherwise a direct unit transport can only use its
            // blockstore state.  If the DAG is not yet complete, this is only
            // the frontier known before the rooted request and descendants can
            // still be streaming.
            if !completion_is_observable {
                break;
            }
        }
        if remaining < initially_missing {
            outcome = ProviderWindowOutcome::Partial;
            // A CAR response is a stream.  Seeing its first useful block is
            // progress, not completion: cancelling here truncates the rooted
            // response and leaves the newly discovered frontier unavailable
            // from the only authoritative publisher.  Production contexts
            // carry the transport completion tracker, so let that response
            // drain.  Direct unit transports without completion events retain
            // the blockstore-only fallback.
            if !completion_is_observable {
                break;
            }
        }
        if transport_complete {
            break;
        }
        if let Some(receiver) = completion.as_mut() {
            tokio::select! {
                result = receiver => {
                    let success = result.unwrap_or(false);
                    completion = None;
                    transport_complete = true;
                    if !success {
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    context.cancel_block_sync_tracking(query_id);
    if let Err(error) = transport.cancel_sync(query_id).await {
        debug!(root_cid = %root_cid, query_id = query_id.0, error = %error, "Failed to cancel rooted CAR query");
    }
    outcome
}

/// Fetch one batch of exact blocks, rotating to the next provider whenever
/// the current one yields nothing within the fetch window.
///
/// `stall_budget` caps the stalled (timed-out) provider windows one attempt
/// may burn across all of its batches; once it reaches zero, stalled batches
/// fail fast without issuing further transport queries.
struct ProviderFetchState<'a> {
    providers: &'a mut ProviderRotation,
    stall_budget: &'a mut usize,
    diagnostics: &'a SyncDiagnostics,
}

async fn poll_fetch_blocks_rotating<B: Blockstore, T: P2PTransport>(
    root_cid: &Cid,
    cids: &[Cid],
    transport: &T,
    blockstore: &Arc<B>,
    state: &mut ProviderFetchState<'_>,
    context: &DagFetchContext,
) -> FetchBatchOutcome {
    for _ in 0..state.providers.len() {
        if *state.stall_budget == 0 {
            debug!(
                root_cid = %root_cid,
                requested_count = cids.len(),
                "Attempt stall budget exhausted, failing batch without fetching"
            );
            return FetchBatchOutcome::NoProgress;
        }
        let provider = state.providers.current().clone();
        match poll_fetch_blocks(root_cid, cids, transport, blockstore, &provider, context).await {
            ProviderWindowOutcome::Complete => return FetchBatchOutcome::Complete,
            ProviderWindowOutcome::Partial => return FetchBatchOutcome::Partial,
            ProviderWindowOutcome::Stalled => {
                *state.stall_budget -= 1;
                state.providers.advance();
                state.diagnostics.record_provider_rotation();
                if state.providers.len() > 1 {
                    warn!(
                        root_cid = %root_cid,
                        provider = %provider,
                        requested_count = cids.len(),
                        "No blocks from provider within fetch window, rotating to next provider"
                    );
                }
            }
            ProviderWindowOutcome::SendFailed => {
                state.providers.advance();
                state.diagnostics.record_provider_rotation();
            }
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
    context: &DagFetchContext,
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
    let mut completion = context.track_block_sync(query_id);
    let completion_is_observable = completion.is_some();
    let mut transport_complete = false;

    let timeout = BLOCK_SYNC_COMPLETION_WATCHDOG;
    let start = Instant::now();
    let mut outcome = ProviderWindowOutcome::Stalled;
    while start.elapsed() < timeout {
        if !context.is_current() {
            break;
        }
        let mut remaining = 0usize;
        for cid in &missing {
            if !matches!(blockstore.has(cid).await, Ok(true)) {
                remaining += 1;
            }
        }
        if remaining == 0 {
            outcome = ProviderWindowOutcome::Complete;
            if !completion_is_observable {
                break;
            }
        }
        if remaining < missing.len() {
            outcome = ProviderWindowOutcome::Partial;
            // Do not abort a productive selective CAR stream after its first
            // block.  Its remaining requested blocks are still in flight.
            if !completion_is_observable {
                break;
            }
        }
        if transport_complete {
            break;
        }
        if let Some(receiver) = completion.as_mut() {
            tokio::select! {
                result = receiver => {
                    let success = result.unwrap_or(false);
                    completion = None;
                    transport_complete = true;
                    if !success {
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    context.cancel_block_sync_tracking(query_id);
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
#[path = "dag_fetcher_tests.rs"]
mod tests;
