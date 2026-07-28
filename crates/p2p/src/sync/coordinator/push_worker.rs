//! Fixed worker pool draining the outbound push backlog (#1099).
//!
//! Exactly `worker_count` tasks are spawned once at coordinator construction
//! and live until shutdown; they are the only long-lived push handles, and
//! they own the expensive parts of a push — DAG expansion from the blockstore
//! and request signing — so queued jobs stay compact.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use blockstore::Blockstore;
use bytes::Bytes;
use cid::Cid;
use parking_lot::Mutex;

use super::{PushFailure, SyncShutdownHandle};
use crate::message::PushLogRequest;
use crate::signing::sign_with_transport;
use crate::sync::push_backlog::{JobCompletion, PushBacklog, PushJobSpec};
use crate::sync::push_encode_cache::PushPayload;
use crate::transport::P2PTransport;

pub(super) struct PushWorkerContext<B, T> {
    pub(super) transport: T,
    pub(super) blockstore: Arc<B>,
    pub(super) backlog: Arc<PushBacklog>,
    pub(super) selective_car_access: Arc<super::selective_car_access::SelectiveCarAccess>,
    pub(super) failure_tx: Arc<Mutex<Option<tokio::sync::mpsc::Sender<PushFailure>>>>,
    pub(super) send_timeout: Duration,
}

/// Hand a failed/rejected push to the persisted retry ladder. Losslessly:
/// a full channel applies backpressure to the reporter instead of dropping
/// the retry record — dropping here would silently lose the exact overflow
/// this queue exists to make durable. Only a closed channel (recorder gone,
/// process shutting down) is logged and released.
pub(super) async fn report_push_failure(
    failure_tx: &Arc<Mutex<Option<tokio::sync::mpsc::Sender<PushFailure>>>>,
    peer_id: &crate::transport::PeerId,
    doc_id: String,
    collection_id: String,
    cid: Option<Cid>,
    head_priority: u64,
) {
    report_push_event(
        failure_tx,
        peer_id,
        doc_id,
        collection_id,
        cid,
        head_priority,
        true,
    )
    .await;
}

pub(super) async fn report_observed_head(
    failure_tx: &Arc<Mutex<Option<tokio::sync::mpsc::Sender<PushFailure>>>>,
    job: &PushJobSpec,
) {
    report_push_event(
        failure_tx,
        &job.peer_id,
        job.doc_id.clone(),
        job.collection_id.clone(),
        Some(job.root_cid),
        job.head_priority(),
        false,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn report_push_event(
    failure_tx: &Arc<Mutex<Option<tokio::sync::mpsc::Sender<PushFailure>>>>,
    peer_id: &crate::transport::PeerId,
    doc_id: String,
    collection_id: String,
    cid: Option<Cid>,
    head_priority: u64,
    create_retry: bool,
) {
    // Collection commits are doc-less: their obligation is CID-scoped. They are
    // recorded in the ledger's collection-commit keyspace and replayed by CID
    // (defradb#1113). Dropping them here made a failed collection-commit push
    // permanent — the receiver kept heads whose parents never arrived, so its
    // pending-DAG registrations could never complete (source-inc/gents#696).
    //
    // A doc-less failure with no CID (the versionless SE-artifact path) still
    // has nothing to replay: no document to re-resolve and no CID to re-send.
    if doc_id.is_empty() && cid.is_none() {
        if create_retry {
            tracing::warn!(
                peer_id = %peer_id,
                collection_id,
                "Versionless collection-scoped push failed with no CID; nothing to replay"
            );
        }
        return;
    }
    let tx = failure_tx.lock().clone();
    if let Some(tx) = tx {
        let failure = PushFailure {
            peer_id: peer_id.to_string(),
            doc_id,
            collection_id,
            cid: cid.map(|cid| cid.to_string()).unwrap_or_default(),
            head_priority,
            create_retry,
        };
        // reserve() is cancel-safe, so waiting in a loop lets sustained
        // recorder backpressure surface in the logs instead of silently
        // stalling the push pool.
        loop {
            match tokio::time::timeout(Duration::from_secs(5), tx.reserve()).await {
                Ok(Ok(permit)) => {
                    permit.send(failure);
                    return;
                }
                Ok(Err(_closed)) => {
                    tracing::warn!(
                        peer_id = %peer_id,
                        doc_id = %failure.doc_id,
                        "Push failure recorder is gone; dropping retry record"
                    );
                    return;
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        peer_id = %peer_id,
                        doc_id = %failure.doc_id,
                        "Push failure recorder is backlogged; still waiting to hand off retry record"
                    );
                }
            }
        }
    }
}

pub(super) fn spawn_push_workers<B, T>(
    context: Arc<PushWorkerContext<B, T>>,
    shutdown: &SyncShutdownHandle,
) where
    B: Blockstore + 'static,
    T: P2PTransport,
{
    for _ in 0..context.backlog.worker_count() {
        let context = Arc::clone(&context);
        let handle = tokio::spawn(async move {
            while let Some(job) = context.backlog.next_job().await {
                let completion = run_push_job(&context, &job).await;
                context.backlog.job_done(&job, completion);
            }
        });
        shutdown.register_task(handle);
    }
}

async fn run_push_job<B, T>(context: &PushWorkerContext<B, T>, job: &PushJobSpec) -> JobCompletion
where
    B: Blockstore + 'static,
    T: P2PTransport,
{
    if !context.backlog.is_current(job) {
        return JobCompletion::Retired;
    }

    let payload = job
        .encoded_payload
        .clone()
        .unwrap_or_else(|| Arc::new(PushPayload::from_job(job)));
    let root_request = payload
        .root_request
        .get_or_try_init(|| async {
            build_request(
                &context.transport,
                &payload,
                job.root_cid,
                job.head_block.clone(),
            )
            .ok_or(())
        })
        .await
        .ok()
        .cloned();
    let root_missing = root_request.is_none();
    let (dependencies, dependency_failed) = if job.expand_dag {
        match payload
            .dependency_requests
            .get_or_try_init(|| async {
                let (blocks, complete) = load_ordered_dag_blocks(
                    context.blockstore.as_ref(),
                    job.root_cid,
                    job.head_block.clone(),
                )
                .await;
                if !complete {
                    return Err(());
                }
                blocks
                    .into_iter()
                    .filter(|(cid, _)| *cid != job.root_cid)
                    .map(|(cid, block)| build_request(&context.transport, &payload, cid, block))
                    .collect::<Option<Vec<_>>>()
                    .map(Arc::new)
                    .ok_or(())
            })
            .await
        {
            Ok(requests) => (Arc::clone(requests), false),
            Err(()) => (Arc::new(Vec::new()), true),
        }
    } else {
        (Arc::new(Vec::new()), false)
    };
    let mut requests = Vec::with_capacity(dependencies.len() + usize::from(root_request.is_some()));
    requests.extend(dependencies.iter().cloned());
    if let Some(root_request) = root_request {
        requests.push(root_request);
    }
    let send_outcome = if requests.is_empty() {
        // Every block failed to sign: report so the persisted retry ladder
        // regenerates and re-pushes instead of silently losing the doc.
        PushSendOutcome {
            failed: true,
            at_capacity: false,
        }
    } else {
        // Authorize the receiver's post-ack recovery pull from the DAG
        // itself, not from `requests` (what this job happened to push).
        // A root-only push (`expand_dag = false`) still grants the full
        // local DAG, decoupling recovery from payload expansion so a future
        // removal of payload expansion (#1116 stage 3) keeps recovery
        // working. Fall back to the pushed CIDs on a walk error: never send
        // a push whose recovery pull we could not authorize.
        let grant_cids = match crate::sync::car::collect_dag_cids(
            context.blockstore.as_ref(),
            &job.root_cid,
            crate::sync::car::CAR_MAX_BLOCKS,
        )
        .await
        {
            Ok(cids) => cids,
            Err(_) => requests.iter().map(|(cid, _)| *cid).collect(),
        };
        let _car_access =
            context
                .selective_car_access
                .register(job.peer_id.clone(), job.root_cid, grant_cids);
        send_ordered_pushlogs_via_transport(
            &context.transport,
            &job.peer_id,
            requests,
            context.send_timeout,
        )
        .await
    };
    // A saturated receiver parks the whole peer: every CID we would push next
    // is going to be rejected for the same reason (defradb#1112).
    if send_outcome.at_capacity {
        context.backlog.park_peer_at_capacity(&job.peer_id);
        for queued_job in context.backlog.take_queued_for_peer(&job.peer_id) {
            let peer_id = queued_job.peer_id.clone();
            let head_priority = queued_job.head_priority();
            report_push_failure(
                &context.failure_tx,
                &peer_id,
                queued_job.doc_id,
                queued_job.collection_id,
                Some(queued_job.root_cid),
                head_priority,
            )
            .await;
        }
    }
    let send_failed = send_outcome.failed;
    let any_failed = root_missing || dependency_failed || send_failed;

    if any_failed && context.backlog.is_current(job) {
        report_push_failure(
            &context.failure_tx,
            &job.peer_id,
            job.doc_id.clone(),
            job.collection_id.clone(),
            Some(job.root_cid),
            job.head_priority(),
        )
        .await;
        JobCompletion::Failed
    } else if context.backlog.is_current(job) {
        JobCompletion::Succeeded
    } else {
        JobCompletion::Retired
    }
}

fn build_request<T: P2PTransport>(
    transport: &T,
    payload: &PushPayload,
    block_cid: Cid,
    block_data: Bytes,
) -> Option<(Cid, PushLogRequest)> {
    let mut request = PushLogRequest::new(
        payload.doc_id.clone(),
        Bytes::from(block_cid.to_bytes()),
        payload.collection_id.clone(),
        payload.creator.clone(),
        block_data,
    );
    match sign_with_transport(transport, &mut request) {
        Ok(()) => Some((block_cid, request)),
        Err(error) => {
            tracing::debug!(cid = %block_cid, error = %error, "Failed to sign PushLog request");
            None
        }
    }
}

/// Load every transitive block in a document DAG, with dependencies first.
pub(super) async fn load_ordered_dag_blocks<B: Blockstore>(
    blockstore: &B,
    root_cid: Cid,
    root_bytes: Bytes,
) -> (Vec<(Cid, Bytes)>, bool) {
    let mut ordered = Vec::new();
    let mut complete = true;
    let mut visited = HashSet::new();
    let mut stack = vec![(root_cid, root_bytes, false)];

    while let Some((cid, data, expanded)) = stack.pop() {
        if expanded {
            ordered.push((cid, data));
            continue;
        }

        if !visited.insert(cid) {
            continue;
        }

        let linked_cids = defra_core::Block::from_dag_cbor(&data)
            .ok()
            .and_then(|block| defra_core::collect_block_links(&block).ok())
            .unwrap_or_default();

        stack.push((cid, data, true));

        for linked_cid in linked_cids.into_iter().rev() {
            match blockstore.get(&linked_cid).await {
                Ok(Some(linked_data)) => stack.push((linked_cid, linked_data, false)),
                Ok(None) => {
                    complete = false;
                    tracing::debug!(
                        root_cid = %root_cid,
                        linked_cid = %linked_cid,
                        "Linked DAG block not found in blockstore"
                    );
                }
                Err(error) => {
                    complete = false;
                    tracing::debug!(
                        root_cid = %root_cid,
                        linked_cid = %linked_cid,
                        error = %error,
                        "Failed to load linked DAG block"
                    );
                }
            }
        }
    }

    (ordered, complete)
}

/// Send PushLog requests to a peer in order via the transport, waiting for
/// each to complete. Returns true when any request failed terminally.
/// Outcome of an ordered push to one peer.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct PushSendOutcome {
    /// Any block failed to land.
    pub failed: bool,
    /// The receiver reported its pending-DAG registry FULL. This is peer-wide
    /// and structural, so the caller parks the whole peer rather than just this
    /// CID (defradb#1112).
    pub at_capacity: bool,
}

pub(super) async fn send_ordered_pushlogs_via_transport<T: P2PTransport>(
    transport: &T,
    peer_id: &crate::transport::PeerId,
    requests: Vec<(Cid, PushLogRequest)>,
    send_timeout: Duration,
) -> PushSendOutcome {
    use crate::error::{is_at_capacity_message, is_rate_limited_message};

    let mut any_failed = false;
    let mut at_capacity = false;
    'requests: for (cid, request) in requests {
        let mut rate_limited_attempts = 0;
        loop {
            match tokio::time::timeout(
                send_timeout,
                transport.send_two_stream_request(peer_id, request.clone()),
            )
            .await
            {
                Err(_) => {
                    tracing::warn!(
                        peer_id = %peer_id,
                        cid = %cid,
                        timeout_ms = send_timeout.as_millis(),
                        "PushLog to replicator timed out"
                    );
                    any_failed = true;
                    break 'requests;
                }
                Ok(Err(e)) => {
                    if e.is_connection_like() {
                        tracing::debug!(
                            peer_id = %peer_id,
                            cid = %cid,
                            error = %e,
                            "PushLog to replicator failed because the connection became unavailable; stopping replay for this peer"
                        );
                        any_failed = true;
                        break 'requests;
                    }

                    tracing::debug!(
                        peer_id = %peer_id,
                        cid = %cid,
                        error = %e,
                        "PushLog to replicator failed"
                    );
                    any_failed = true;
                    break;
                }
                Ok(Ok(reply)) => {
                    let Some(error_message) = reply.err_message.as_deref() else {
                        tracing::debug!(
                            target: "p2p::sync::restart_recovery",
                            peer_id = %peer_id,
                            cid = %cid,
                            doc_id = %request.doc_id,
                            "PushLog accepted by replicator"
                        );
                        break;
                    };

                    // A saturated receiver is a PEER-WIDE, structural condition:
                    // it cannot accept any new root until it drains. Resending
                    // the same block at pacing intervals just burns receiver
                    // work (a block write + a full DAG traversal per attempt),
                    // and because the failure cooldown is keyed per-CID, every
                    // other CID for that peer would start its own burst. Stop
                    // immediately, park the whole PEER, and let the persisted
                    // retry sweep resume once the receiver can drain
                    // (defradb#1112).
                    if is_at_capacity_message(error_message) {
                        tracing::debug!(
                            peer_id = %peer_id,
                            cid = %cid,
                            "PushLog rejected: receiver at capacity; parking peer and deferring \
                             to persisted retry"
                        );
                        at_capacity = true;
                        any_failed = true;
                        break 'requests;
                    }

                    if is_rate_limited_message(error_message) {
                        rate_limited_attempts += 1;
                        if rate_limited_attempts > super::broadcast::MAX_RATE_LIMITED_PUSH_ATTEMPTS
                        {
                            tracing::warn!(
                                peer_id = %peer_id,
                                cid = %cid,
                                attempts = rate_limited_attempts,
                                "PushLog to replicator remained rate-limited; stopping ordered push"
                            );
                            any_failed = true;
                            break 'requests;
                        }

                        let delay =
                            super::broadcast::rate_limited_push_delay(rate_limited_attempts);
                        tracing::debug!(
                            peer_id = %peer_id,
                            cid = %cid,
                            attempt = rate_limited_attempts,
                            delay_ms = delay.as_millis(),
                            "PushLog to replicator was rate-limited; backing off before retry"
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }

                    tracing::warn!(
                        peer_id = %peer_id,
                        cid = %cid,
                        error = %error_message,
                        "PushLog to replicator was rejected"
                    );
                    any_failed = true;
                    break;
                }
            }
        }
    }
    PushSendOutcome {
        failed: any_failed,
        at_capacity,
    }
}

#[cfg(test)]
#[path = "push_worker_tests.rs"]
mod tests;
