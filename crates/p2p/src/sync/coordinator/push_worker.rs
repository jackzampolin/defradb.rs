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
    if doc_id.is_empty() {
        if create_retry {
            tracing::warn!(
                peer_id = %peer_id,
                collection_id,
                cid = ?cid,
                "Collection-commit push failed; document retry ledger cannot replay CID-scoped work"
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
    let send_failed = if requests.is_empty() {
        // Every block failed to sign: report so the persisted retry ladder
        // regenerates and re-pushes instead of silently losing the doc.
        true
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
    use crate::sync::push_backlog::EnqueueOutcome;
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
        PushJobSpec::new(
            PeerId::new(peer.to_string()),
            format!("doc-{peer}-{}", hex::encode(cid_seed)),
            "collection".to_string(),
            "creator".to_string(),
            Cid::new_v1(0x55, Code::Sha2_256.digest(cid_seed)),
            Bytes::from_static(b"head-block"),
            false,
        )
    }

    fn versioned_job(peer: &str, priority: u64) -> PushJobSpec {
        use defra_core::{Block, CompositeDeltaPayload, CrdtDelta};

        let doc_id = "doc-versioned";
        let block = Block::new_with_options(
            CrdtDelta::Composite(CompositeDeltaPayload {
                doc_id: doc_id.as_bytes().to_vec(),
                schema_version_id: "schema".to_string(),
                priority,
                status: 1,
            }),
            vec![],
            vec![],
            None,
            None,
        );
        let head_block = Bytes::from(block.to_dag_cbor().unwrap());
        PushJobSpec::new(
            PeerId::new(peer.to_string()),
            doc_id.to_string(),
            "collection".to_string(),
            "creator".to_string(),
            defra_core::block::generate_cid_from_bytes(&head_block).unwrap(),
            head_block,
            false,
        )
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
        assert_eq!(failure.doc_id, "doc-slow-736c6f772d31");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while backlog.snapshot().failed_total == 0 {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(backlog.snapshot().active_jobs, 0);
        backlog.close();
    }

    #[tokio::test]
    async fn fanout_signs_one_payload_once_for_all_peers() {
        let backlog = PushBacklog::new(1024, usize::MAX, 1, 2);
        let transport = TestTransport::new(Vec::new());
        let (context, _failure_rx) = test_context(
            transport.clone(),
            Arc::clone(&backlog),
            Duration::from_secs(1),
        );
        let cache = crate::sync::push_encode_cache::PushEncodeCache::default();
        let base = job("a", b"shared");
        let mut first = PushJobSpec::new(
            base.peer_id,
            "doc-shared".to_string(),
            base.collection_id,
            base.creator,
            base.root_cid,
            base.head_block,
            base.expand_dag,
        );
        first.encoded_payload = Some(cache.acquire(&first));
        let mut second = PushJobSpec::new(
            PeerId::new("b".to_string()),
            first.doc_id.clone(),
            first.collection_id.clone(),
            first.creator.clone(),
            first.root_cid,
            first.head_block.clone(),
            first.expand_dag,
        );
        second.encoded_payload = Some(cache.acquire(&second));

        let shutdown = SyncShutdownHandle::new();
        spawn_push_workers(context, &shutdown);
        backlog.try_enqueue(first);
        backlog.try_enqueue(second);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while backlog.snapshot().completed_total < 2 {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(transport.sign_count(), 1);
        assert_eq!(cache.hits(), 1);
        backlog.close();
    }

    #[tokio::test]
    async fn transient_root_sign_failure_is_not_cached_across_fanout() {
        let backlog = PushBacklog::new(1024, usize::MAX, 1, 2);
        let transport = TestTransport::new(Vec::new()).with_sign_failures(1);
        let (context, mut failure_rx) = test_context(
            transport.clone(),
            Arc::clone(&backlog),
            Duration::from_secs(1),
        );
        let cache = crate::sync::push_encode_cache::PushEncodeCache::default();
        let base = job("a", b"shared-sign-retry");
        let mut first = PushJobSpec::new(
            base.peer_id,
            "doc-shared".to_string(),
            base.collection_id,
            base.creator,
            base.root_cid,
            base.head_block,
            base.expand_dag,
        );
        first.encoded_payload = Some(cache.acquire(&first));
        let mut second = PushJobSpec::new(
            PeerId::new("b".to_string()),
            first.doc_id.clone(),
            first.collection_id.clone(),
            first.creator.clone(),
            first.root_cid,
            first.head_block.clone(),
            first.expand_dag,
        );
        second.encoded_payload = Some(cache.acquire(&second));

        let shutdown = SyncShutdownHandle::new();
        spawn_push_workers(context, &shutdown);
        backlog.try_enqueue(first);
        backlog.try_enqueue(second);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while backlog.snapshot().completed_total + backlog.snapshot().failed_total < 2 {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(transport.sign_count(), 2);
        assert_eq!(transport.sent().len(), 1);
        assert!(failure_rx.try_recv().is_ok());
        backlog.close();
    }

    #[tokio::test]
    async fn missing_root_request_fails_even_when_dependency_send_succeeds() {
        use defra_core::{Block, CompositeDeltaPayload, CrdtDelta};

        let backlog = PushBacklog::new(1024, usize::MAX, 1, 1);
        let transport = TestTransport::new(Vec::new()).with_sign_failures(1);
        let (context, mut failure_rx) = test_context(
            transport.clone(),
            Arc::clone(&backlog),
            Duration::from_secs(1),
        );
        let dependency = Bytes::from_static(b"dependency");
        let dependency_cid = defra_core::block::generate_cid_from_bytes(&dependency).unwrap();
        context
            .blockstore
            .put(&dependency_cid, &dependency)
            .await
            .unwrap();
        let root = Block::new_with_options(
            CrdtDelta::Composite(CompositeDeltaPayload {
                doc_id: b"doc".to_vec(),
                schema_version_id: "schema".to_string(),
                priority: 1,
                status: 1,
            }),
            vec![dependency_cid],
            vec![],
            None,
            None,
        );
        let root = Bytes::from(root.to_dag_cbor().unwrap());
        let root_cid = defra_core::block::generate_cid_from_bytes(&root).unwrap();
        let job = PushJobSpec::new(
            PeerId::new("peer".to_string()),
            "doc".to_string(),
            "collection".to_string(),
            "creator".to_string(),
            root_cid,
            root,
            true,
        );
        assert_eq!(backlog.try_enqueue(job), EnqueueOutcome::Enqueued);
        let active = backlog.next_job().await.unwrap();

        let completion = run_push_job(&context, &active).await;

        assert_eq!(completion, JobCompletion::Failed);
        assert_eq!(transport.sign_count(), 2);
        assert_eq!(transport.sent().len(), 1);
        assert_eq!(failure_rx.recv().await.unwrap().cid, root_cid.to_string());
        backlog.job_done(&active, completion);
        backlog.close();
    }

    /// A root-only push (`expand_dag = false`) sends just the root block, but
    /// the selective-CAR grant must still cover the full local DAG so the
    /// receiver's post-ack recovery pull can fetch missing dependents (#1116
    /// stage 2): the grant is authorized from the blockstore's DAG shape, not
    /// from the pushed payload set.
    #[tokio::test]
    async fn root_only_push_still_grants_full_dag_for_recovery() {
        use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink};

        let backlog = PushBacklog::new(1024, usize::MAX, 1, 1);
        let transport = TestTransport::new(Vec::new());
        let (context, _failure_rx) = test_context(
            transport.clone(),
            Arc::clone(&backlog),
            Duration::from_secs(1),
        );

        let child_data = Bytes::from_static(b"child-block");
        let child_cid = defra_core::block::generate_cid_from_bytes(&child_data).unwrap();
        context
            .blockstore
            .put(&child_cid, &child_data)
            .await
            .unwrap();

        let root = Block::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
                doc_id: b"doc".to_vec(),
                schema_version_id: "schema".to_string(),
                priority: 1,
                status: 1,
            }),
            vec![],
            vec![DAGLink::new("child", child_cid)],
        );
        let root_bytes = Bytes::from(root.to_dag_cbor().unwrap());
        let root_cid = defra_core::block::generate_cid_from_bytes(&root_bytes).unwrap();
        context
            .blockstore
            .put(&root_cid, &root_bytes)
            .await
            .unwrap();

        let peer = PeerId::new("peer".to_string());
        let job = PushJobSpec::new(
            peer.clone(),
            "doc".to_string(),
            "collection".to_string(),
            "creator".to_string(),
            root_cid,
            root_bytes,
            false,
        );
        assert_eq!(backlog.try_enqueue(job), EnqueueOutcome::Enqueued);
        let active = backlog.next_job().await.unwrap();

        let completion = run_push_job(&context, &active).await;

        assert_eq!(completion, JobCompletion::Succeeded);
        assert!(
            context
                .selective_car_access
                .allows(&peer, &root_cid, &child_cid),
            "root-only push must still grant the child block for receiver recovery"
        );
        backlog.job_done(&active, completion);
        backlog.close();
    }

    #[tokio::test]
    async fn superseded_active_failure_never_enters_persisted_retry() {
        let backlog = PushBacklog::new(1024, usize::MAX, 1, 1);
        let transport = TestTransport::new(vec![
            crate::message::PushLogReply::error("old", "rejected"),
            crate::message::PushLogReply::success("new"),
        ])
        .with_send_delay(Duration::from_millis(50));
        let (context, mut failure_rx) = test_context(
            transport.clone(),
            Arc::clone(&backlog),
            Duration::from_secs(1),
        );
        let shutdown = SyncShutdownHandle::new();
        spawn_push_workers(context, &shutdown);

        backlog.try_enqueue(versioned_job("peer", 1));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while transport.sent().is_empty() {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        backlog.try_enqueue(versioned_job("peer", 2));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while backlog.snapshot().completed_total < 1 {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(failure_rx.try_recv().is_err());
        assert_eq!(backlog.snapshot().stale_head_retirements_total, 1);
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
            cid: Cid::new_v1(0x55, Code::Sha2_256.digest(b"occupant")).to_string(),
            head_priority: 0,
            create_retry: true,
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
                    Some(Cid::new_v1(0x55, Code::Sha2_256.digest(b"slow"))),
                    0,
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

    #[tokio::test]
    async fn collection_commit_failure_does_not_enter_document_retry_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<PushFailure>(1);
        let slot = Arc::new(Mutex::new(Some(tx)));

        report_push_failure(
            &slot,
            &PeerId::new("peer".to_string()),
            String::new(),
            "collection".to_string(),
            Some(Cid::new_v1(
                0x55,
                Code::Sha2_256.digest(b"collection-commit"),
            )),
            1,
        )
        .await;

        assert!(rx.try_recv().is_err());
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
