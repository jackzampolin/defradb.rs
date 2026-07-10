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
use crate::sync::push_backlog::{PushBacklog, PushJobSpec};
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
) {
    let tx = failure_tx.lock().clone();
    if let Some(tx) = tx {
        let failure = PushFailure {
            peer_id: peer_id.to_string(),
            doc_id,
            collection_id,
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
                let peer_id = job.peer_id.clone();
                let succeeded = run_push_job(&context, job).await;
                context.backlog.job_done(&peer_id, succeeded);
            }
        });
        shutdown.register_task(handle);
    }
}

async fn run_push_job<B, T>(context: &PushWorkerContext<B, T>, job: PushJobSpec) -> bool
where
    B: Blockstore + 'static,
    T: P2PTransport,
{
    let blocks: Vec<(Cid, Bytes)> = if job.expand_dag {
        load_ordered_dag_blocks(
            context.blockstore.as_ref(),
            job.root_cid,
            job.head_block.clone(),
        )
        .await
    } else {
        vec![(job.root_cid, job.head_block.clone())]
    };
    let pushed_cids = job
        .expand_dag
        .then(|| blocks.iter().map(|(cid, _)| *cid).collect::<Vec<_>>());

    let mut requests: Vec<(Cid, PushLogRequest)> = Vec::with_capacity(blocks.len());
    for (block_cid, block_data) in blocks {
        let mut request = PushLogRequest::new(
            job.doc_id.clone(),
            Bytes::from(block_cid.to_bytes()),
            job.collection_id.clone(),
            job.creator.clone(),
            block_data,
        );
        match sign_with_transport(&context.transport, &mut request) {
            Ok(()) => requests.push((block_cid, request)),
            Err(error) => {
                tracing::debug!(cid = %block_cid, error = %error, "Failed to sign PushLog request");
            }
        }
    }

    let any_failed = if requests.is_empty() {
        // Every block failed to sign: report so the persisted retry ladder
        // regenerates and re-pushes instead of silently losing the doc.
        true
    } else {
        let _car_access = pushed_cids.map(|cids| {
            context
                .selective_car_access
                .register(job.peer_id.clone(), cids)
        });
        send_ordered_pushlogs_via_transport(
            &context.transport,
            &job.peer_id,
            requests,
            context.send_timeout,
        )
        .await
    };

    if any_failed {
        report_push_failure(
            &context.failure_tx,
            &job.peer_id,
            job.doc_id,
            job.collection_id,
        )
        .await;
    }
    !any_failed
}

/// Load every transitive block in a document DAG, with dependencies first.
pub(super) async fn load_ordered_dag_blocks<B: Blockstore>(
    blockstore: &B,
    root_cid: Cid,
    root_bytes: Bytes,
) -> Vec<(Cid, Bytes)> {
    let mut ordered = Vec::new();
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
                    tracing::debug!(
                        root_cid = %root_cid,
                        linked_cid = %linked_cid,
                        "Linked DAG block not found in blockstore"
                    );
                }
                Err(error) => {
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

    ordered
}

/// Send PushLog requests to a peer in order via the transport, waiting for
/// each to complete. Returns true when any request failed terminally.
pub(super) async fn send_ordered_pushlogs_via_transport<T: P2PTransport>(
    transport: &T,
    peer_id: &crate::transport::PeerId,
    requests: Vec<(Cid, PushLogRequest)>,
    send_timeout: Duration,
) -> bool {
    use crate::error::is_rate_limited_message;

    let mut any_failed = false;
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
                        break;
                    };

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
    any_failed
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use cid::Cid;
    use multihash_codetable::{Code, MultihashDigest};
    use parking_lot::Mutex;

    use super::super::broadcast::tests::TestTransport;
    use super::*;
    use crate::transport::PeerId;

    type TestBlockstore = blockstore::DefraBlockstore<storage::backends::MemoryStore>;

    fn test_context(
        transport: TestTransport,
        backlog: Arc<PushBacklog>,
        send_timeout: Duration,
    ) -> (
        Arc<PushWorkerContext<TestBlockstore, TestTransport>>,
        tokio::sync::mpsc::Receiver<PushFailure>,
    ) {
        let store = Arc::new(storage::backends::MemoryStore::new());
        let blockstore = Arc::new(blockstore::DefraBlockstore::new(store, true));
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let context = Arc::new(PushWorkerContext {
            transport,
            blockstore,
            backlog,
            selective_car_access: Arc::new(
                super::super::selective_car_access::SelectiveCarAccess::default(),
            ),
            failure_tx: Arc::new(Mutex::new(Some(tx))),
            send_timeout,
        });
        (context, rx)
    }

    fn job(peer: &str, cid_seed: &[u8]) -> PushJobSpec {
        PushJobSpec {
            peer_id: PeerId::new(peer.to_string()),
            doc_id: format!("doc-{peer}"),
            collection_id: "collection".to_string(),
            creator: "creator".to_string(),
            root_cid: Cid::new_v1(0x55, Code::Sha2_256.digest(cid_seed)),
            head_block: Bytes::from_static(b"head-block"),
            expand_dag: false,
        }
    }

    /// #1099 fairness: a nonresponsive peer occupying its per-peer cap must
    /// not stop healthy peers from draining through the remaining workers.
    #[tokio::test]
    async fn slow_peer_does_not_starve_healthy_peers() {
        let backlog = PushBacklog::new(1024, usize::MAX, 1, 2);
        let transport = TestTransport::new(Vec::new()).with_stalled_peer("slow");
        let (context, _failure_rx) = test_context(
            transport.clone(),
            Arc::clone(&backlog),
            Duration::from_secs(60),
        );
        let shutdown = SyncShutdownHandle::new();
        spawn_push_workers(context, &shutdown);

        backlog.try_enqueue(job("slow", b"slow-1"));
        for index in 0..5u8 {
            backlog.try_enqueue(job("healthy", &[index]));
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let healthy_sent = transport
                .sent()
                .iter()
                .filter(|(peer, _)| peer == "healthy")
                .count();
            if healthy_sent == 5 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "healthy peer starved: only {healthy_sent}/5 sends completed while a slow peer holds a worker"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let snap = backlog.snapshot();
        assert!(snap.active_jobs <= 2);
        assert!(snap.completed_total >= 5);
        backlog.close();
    }

    /// A stalled send fails via the send timeout, frees the worker, and lands
    /// in the persisted retry ladder through the failure channel.
    #[tokio::test]
    async fn stalled_send_times_out_and_reports_push_failure() {
        let backlog = PushBacklog::new(1024, usize::MAX, 1, 1);
        let transport = TestTransport::new(Vec::new()).with_stalled_peer("slow");
        let (context, mut failure_rx) =
            test_context(transport, Arc::clone(&backlog), Duration::from_millis(50));
        let shutdown = SyncShutdownHandle::new();
        spawn_push_workers(context, &shutdown);

        backlog.try_enqueue(job("slow", b"slow-1"));

        let failure = tokio::time::timeout(Duration::from_secs(5), failure_rx.recv())
            .await
            .expect("timed-out push must report a failure")
            .expect("failure channel open");
        assert_eq!(failure.peer_id, "slow");
        assert_eq!(failure.doc_id, "doc-slow");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while backlog.snapshot().failed_total == 0 {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(backlog.snapshot().active_jobs, 0);
        backlog.close();
    }

    /// The rejection-to-retry handoff must be lossless: a full failure
    /// channel applies backpressure instead of dropping the retry record.
    #[tokio::test]
    async fn report_push_failure_backpressures_instead_of_dropping() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<PushFailure>(1);
        tx.send(PushFailure {
            peer_id: "occupant".to_string(),
            doc_id: "occupant-doc".to_string(),
            collection_id: "collection".to_string(),
        })
        .await
        .unwrap();
        let slot = Arc::new(Mutex::new(Some(tx)));

        let peer = PeerId::new("slow".to_string());
        let reporter = {
            let slot = Arc::clone(&slot);
            tokio::spawn(async move {
                report_push_failure(
                    &slot,
                    &peer,
                    "doc-slow".to_string(),
                    "collection".to_string(),
                )
                .await;
            })
        };

        // Channel full: the reporter must wait, not drop.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !reporter.is_finished(),
            "reporter must block on a full channel"
        );

        assert_eq!(rx.recv().await.unwrap().doc_id, "occupant-doc");
        let delivered = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("failure must be delivered once capacity frees")
            .unwrap();
        assert_eq!(delivered.doc_id, "doc-slow");
        reporter.await.unwrap();
    }

    /// Workers exit promptly when the backlog closes; the worker handles are
    /// the only retained push handles.
    #[tokio::test]
    async fn workers_exit_on_close() {
        let backlog = PushBacklog::new(1024, usize::MAX, 4, 3);
        let transport = TestTransport::new(Vec::new());
        let (context, _failure_rx) =
            test_context(transport, Arc::clone(&backlog), Duration::from_secs(1));
        let shutdown = SyncShutdownHandle::new();
        spawn_push_workers(context, &shutdown);
        assert_eq!(shutdown.retained_task_count(), 3);

        backlog.close();
        let started = tokio::time::Instant::now();
        shutdown.shutdown().await;
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
