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
async fn capacity_nack_demotes_queued_peer_work_to_persisted_retry() {
    let backlog = PushBacklog::new(64, usize::MAX, 1, 1);
    let capacity_nack = crate::error::Error::PendingDagCapacity { max: 1 }
        .backpressure_reply_message()
        .expect("capacity error maps to the capacity sentinel");
    let transport = TestTransport::new(vec![crate::message::PushLogReply::error(
        "full",
        capacity_nack,
    )]);
    let (context, mut failure_rx) =
        test_context(transport, Arc::clone(&backlog), Duration::from_secs(1));

    let active = job("peer", b"active");
    let queued_a = job("peer", b"queued-a");
    let queued_b = job("peer", b"queued-b");
    let mut expected_docs = vec![
        active.doc_id.clone(),
        queued_a.doc_id.clone(),
        queued_b.doc_id.clone(),
    ];
    for job in [active, queued_a, queued_b] {
        assert_eq!(backlog.try_enqueue(job), EnqueueOutcome::Enqueued);
    }
    let active = backlog.next_job().await.expect("active job");

    let completion = run_push_job(&context, &active).await;

    assert_eq!(completion, JobCompletion::Failed);
    let mut failed_docs = Vec::new();
    for _ in 0..expected_docs.len() {
        let failure = tokio::time::timeout(Duration::from_secs(1), failure_rx.recv())
            .await
            .expect("capacity failure must reach the retry recorder")
            .expect("failure channel open");
        assert!(failure.create_retry);
        failed_docs.push(failure.doc_id);
    }
    expected_docs.sort();
    failed_docs.sort();
    assert_eq!(failed_docs, expected_docs);
    assert_eq!(backlog.snapshot().queued_items, 0);
    assert_eq!(backlog.snapshot().queued_bytes, 0);

    backlog.job_done(&active, completion);
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
async fn collection_commit_failure_enters_the_retry_channel_with_its_cid() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<PushFailure>(1);
    let slot = Arc::new(Mutex::new(Some(tx)));
    let cid = Cid::new_v1(0x55, Code::Sha2_256.digest(b"collection-commit"));

    report_push_failure(
        &slot,
        &PeerId::new("peer".to_string()),
        String::new(),
        "collection".to_string(),
        Some(cid),
        1,
    )
    .await;

    // defradb#1113: the obligation must reach the ledger. It is doc-less, so
    // it is keyed and replayed by CID; dropping it made failed
    // collection-commit pushes permanent (source-inc/gents#696).
    let failure = rx.try_recv().expect("commit failure must be recorded");
    assert_eq!(failure.doc_id, "");
    assert_eq!(failure.collection_id, "collection");
    assert_eq!(failure.cid, cid.to_string());
    assert!(failure.create_retry);
}

/// A doc-less failure with no CID has nothing to replay and is still
/// dropped (the versionless SE-artifact path).
#[tokio::test]
async fn versionless_collection_failure_is_not_recorded() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<PushFailure>(1);
    let slot = Arc::new(Mutex::new(Some(tx)));

    report_push_failure(
        &slot,
        &PeerId::new("peer".to_string()),
        String::new(),
        "collection".to_string(),
        None,
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
