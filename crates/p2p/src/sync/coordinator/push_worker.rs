//! Fixed worker pool draining the outbound push backlog (#1099).
//!
//! Exactly `worker_count` tasks are spawned once at coordinator construction
//! and live until shutdown; they are the only long-lived push handles, and
//! they own request signing and transport sends, so queued jobs stay compact.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cid::Cid;
use parking_lot::Mutex;

use super::{PushFailure, SyncShutdownHandle};
use crate::message::PushLogRequest;
use crate::signing::sign_with_transport;
use crate::sync::push_backlog::{HeadHintFailureReason, JobCompletion, PushBacklog, PushJobSpec};
use crate::transport::P2PTransport;

pub(super) struct PushWorkerContext<T> {
    pub(super) transport: T,
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
) -> bool {
    report_push_event(
        failure_tx,
        peer_id,
        doc_id,
        collection_id,
        cid,
        head_priority,
        true,
        false,
    )
    .await
}

pub(super) async fn report_observed_head(
    failure_tx: &Arc<Mutex<Option<tokio::sync::mpsc::Sender<PushFailure>>>>,
    job: &PushJobSpec,
) -> bool {
    report_push_event(
        failure_tx,
        &job.peer_id,
        job.doc_id.clone(),
        job.collection_id.clone(),
        Some(job.root_cid),
        job.head_priority(),
        false,
        false,
    )
    .await
}

pub(super) async fn report_push_ack(
    failure_tx: &Arc<Mutex<Option<tokio::sync::mpsc::Sender<PushFailure>>>>,
    job: &PushJobSpec,
) -> bool {
    report_push_event(
        failure_tx,
        &job.peer_id,
        job.doc_id.clone(),
        job.collection_id.clone(),
        Some(job.root_cid),
        job.head_priority(),
        false,
        true,
    )
    .await
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
    acknowledged: bool,
) -> bool {
    // Collection commits are doc-less. Their durable obligation is a collection
    // marker whose retry rederives the current collection heads. The CID below
    // identifies only this live attempt and is never persisted as delivery state.
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
        return false;
    }
    let tx = failure_tx.lock().clone();
    if let Some(tx) = tx {
        let (durable_tx, durable_rx) = if create_retry || acknowledged {
            (None, None)
        } else {
            let (tx, rx) = tokio::sync::oneshot::channel();
            (Some(tx), Some(rx))
        };
        let failure = PushFailure {
            peer_id: peer_id.to_string(),
            doc_id,
            collection_id,
            cid: cid.map(|cid| cid.to_string()).unwrap_or_default(),
            head_priority,
            create_retry,
            acknowledged,
            durable_tx,
        };
        // reserve() is cancel-safe, so waiting in a loop lets sustained
        // recorder backpressure surface in the logs instead of silently
        // stalling the push pool.
        loop {
            match tokio::time::timeout(Duration::from_secs(5), tx.reserve()).await {
                Ok(Ok(permit)) => {
                    permit.send(failure);
                    return match durable_rx {
                        Some(rx) => rx.await.unwrap_or(false),
                        None => true,
                    };
                }
                Ok(Err(_closed)) => {
                    tracing::warn!(
                        peer_id = %peer_id,
                        doc_id = %failure.doc_id,
                        "Push failure recorder is gone; dropping retry record"
                    );
                    return false;
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
    false
}

pub(super) fn spawn_push_workers<T>(
    context: Arc<PushWorkerContext<T>>,
    shutdown: &SyncShutdownHandle,
) where
    T: P2PTransport,
{
    for _ in 0..context.backlog.worker_count() {
        let context = Arc::clone(&context);
        shutdown.spawn_task(async move {
            while let Some(job) = context.backlog.next_job().await {
                let completion = run_push_job(&context, &job).await;
                context.backlog.job_done(&job, completion);
            }
        });
    }
}

async fn run_push_job<T>(context: &PushWorkerContext<T>, job: &PushJobSpec) -> JobCompletion
where
    T: P2PTransport,
{
    if !context.backlog.is_current(job) {
        return JobCompletion::Retired;
    }

    let root_request = build_request(&context.transport, job);
    let root_missing = root_request.is_none();
    let send_outcome = if let Some(root_request) = root_request {
        // The receiver owns DAG completion. Install a bounded root capability
        // before announcing the head; linked-CID reachability is validated on
        // demand in the CAR handler, so announcement work stays O(heads×peers).
        let Some(_car_access) = context
            .selective_car_access
            .register(job.peer_id.clone(), job.root_cid)
        else {
            context
                .backlog
                .record_head_hint_failure(HeadHintFailureReason::Local);
            return JobCompletion::Failed;
        };
        context.backlog.record_head_hint_sent(job);
        send_head_hint_via_transport(
            &context.transport,
            &job.peer_id,
            (job.root_cid, root_request),
            context.send_timeout,
        )
        .await
    } else {
        // Every block failed to sign: report so the persisted retry ladder
        // regenerates and re-pushes instead of silently losing the doc.
        PushSendOutcome {
            failed: true,
            at_capacity: false,
            failure_reason: Some(HeadHintFailureReason::Local),
        }
    };
    if let Some(reason) = send_outcome.failure_reason {
        context.backlog.record_head_hint_failure(reason);
    }
    // A saturated receiver parks the whole peer: every CID we would push next
    // is going to be rejected for the same reason (defradb#1112).
    if send_outcome.at_capacity {
        context.backlog.park_peer_at_capacity(&job.peer_id);
        for queued_job in context.backlog.take_queued_for_peer(&job.peer_id) {
            let peer_id = queued_job.peer_id.clone();
            let head_priority = queued_job.head_priority();
            let _ = report_push_failure(
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
    let any_failed = root_missing || send_failed;

    if any_failed && context.backlog.is_current(job) {
        let _ = report_push_failure(
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
        let _ = report_push_ack(&context.failure_tx, job).await;
        JobCompletion::Succeeded
    } else {
        JobCompletion::Retired
    }
}

fn build_request<T: P2PTransport>(transport: &T, job: &PushJobSpec) -> Option<PushLogRequest> {
    let mut request = PushLogRequest::new(
        job.doc_id.clone(),
        Bytes::from(job.root_cid.to_bytes()),
        job.collection_id.clone(),
        job.creator.clone(),
        job.head_block.clone(),
    );
    match sign_with_transport(transport, &mut request) {
        Ok(()) => Some(request),
        Err(error) => {
            tracing::debug!(cid = %job.root_cid, error = %error, "Failed to sign PushLog request");
            None
        }
    }
}

/// Outcome of announcing one head hint to one peer.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct PushSendOutcome {
    /// Any block failed to land.
    pub failed: bool,
    /// The receiver reported its pending-DAG registry FULL. This is peer-wide
    /// and structural, so the caller parks the whole peer rather than just this
    /// CID (defradb#1112).
    pub at_capacity: bool,
    pub failure_reason: Option<HeadHintFailureReason>,
}

pub(super) async fn send_head_hint_via_transport<T: P2PTransport>(
    transport: &T,
    peer_id: &crate::transport::PeerId,
    (cid, request): (Cid, PushLogRequest),
    send_timeout: Duration,
) -> PushSendOutcome {
    use crate::error::is_at_capacity_message;

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
            PushSendOutcome {
                failed: true,
                at_capacity: false,
                failure_reason: Some(HeadHintFailureReason::Transport),
            }
        }
        Ok(Err(e)) => {
            if e.is_connection_like() {
                tracing::debug!(
                    peer_id = %peer_id,
                    cid = %cid,
                    error = %e,
                    "PushLog to replicator failed because the connection became unavailable"
                );
            } else {
                tracing::debug!(
                        peer_id = %peer_id,
                        cid = %cid,
                        error = %e,
                        "PushLog to replicator failed"
                );
            }
            PushSendOutcome {
                failed: true,
                at_capacity: false,
                failure_reason: Some(HeadHintFailureReason::Transport),
            }
        }
        Ok(Ok(reply)) => {
            let Some(error_message) = reply.err_message.as_deref() else {
                tracing::debug!(
                    target: "p2p::sync::restart_recovery",
                    peer_id = %peer_id,
                    cid = %cid,
                    doc_id = %request.doc_id,
                    "PushLog head hint accepted by replicator"
                );
                return PushSendOutcome {
                    failed: false,
                    at_capacity: false,
                    failure_reason: None,
                };
            };
            if is_at_capacity_message(error_message) {
                tracing::debug!(
                    peer_id = %peer_id,
                    cid = %cid,
                    "PushLog rejected: receiver at capacity; parking peer and deferring to persisted retry"
                );
                return PushSendOutcome {
                    failed: true,
                    at_capacity: true,
                    failure_reason: Some(HeadHintFailureReason::CapacityNack),
                };
            }
            tracing::warn!(
                peer_id = %peer_id,
                cid = %cid,
                error = %error_message,
                "PushLog head hint was rejected"
            );
            PushSendOutcome {
                failed: true,
                at_capacity: false,
                failure_reason: Some(HeadHintFailureReason::OtherNack),
            }
        }
    }
}

#[cfg(test)]
#[path = "push_worker_tests.rs"]
mod tests;
