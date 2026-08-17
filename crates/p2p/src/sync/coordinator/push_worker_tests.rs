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

fn test_context(
    transport: TestTransport,
    backlog: Arc<PushBacklog>,
    send_timeout: Duration,
) -> (
    Arc<PushWorkerContext<TestTransport>>,
    tokio::sync::mpsc::Receiver<PushFailure>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let context = Arc::new(PushWorkerContext {
        transport,
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
            .filter(|push| push.peer_id == "healthy")
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
async fn head_hint_signing_failure_sends_no_dependency_pushlogs() {
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
    );
    assert_eq!(backlog.try_enqueue(job), EnqueueOutcome::Enqueued);
    let active = backlog.next_job().await.unwrap();

    let completion = run_push_job(&context, &active).await;

    assert_eq!(completion, JobCompletion::Failed);
    assert_eq!(transport.sign_count(), 1);
    assert!(transport.sent().is_empty());
    assert_eq!(failure_rx.recv().await.unwrap().cid, root_cid.to_string());
    backlog.job_done(&active, completion);
    backlog.close();
}

/// A root-only push installs its bounded root capability before sending. The
/// CAR handler validates requested linked CIDs against that root on demand.
#[tokio::test]
async fn root_only_push_installs_receiver_pull_authority() {
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

    let peer = PeerId::new("peer".to_string());
    let job = PushJobSpec::new(
        peer.clone(),
        "doc".to_string(),
        "collection".to_string(),
        "creator".to_string(),
        root_cid,
        root_bytes,
    );
    assert_eq!(backlog.try_enqueue(job), EnqueueOutcome::Enqueued);
    let active = backlog.next_job().await.unwrap();

    let completion = run_push_job(&context, &active).await;

    assert_eq!(completion, JobCompletion::Succeeded);
    assert!(
        context.selective_car_access.allows_root(&peer, &root_cid),
        "root-only push must still grant the child block for receiver recovery"
    );
    backlog.job_done(&active, completion);
    backlog.close();
}

#[derive(Debug)]
struct OwnershipArm {
    scheduled: u64,
    transmitted: usize,
    announced_bytes: usize,
    terminal_success: u64,
    child_announced_as_head: bool,
    child_car_authorized: bool,
}

async fn run_ownership_arm(expand_dag: bool) -> OwnershipArm {
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};

    let backlog = PushBacklog::new(8, usize::MAX, 1, 1);
    let transport = TestTransport::new(Vec::new());
    let (context, _failure_rx) = test_context(
        transport.clone(),
        Arc::clone(&backlog),
        Duration::from_secs(1),
    );

    let child = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "value".to_string(),
            priority: 1,
            schema_version_id: "schema".to_string(),
            data: b"current".to_vec(),
        }),
        vec![],
        vec![],
    );
    let child_bytes = Bytes::from(child.to_dag_cbor().unwrap());
    let child_cid = defra_core::block::generate_cid_from_bytes(&child_bytes).unwrap();

    let root = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "schema".to_string(),
            priority: 1,
            status: 1,
        }),
        vec![],
        vec![DAGLink::new("value", child_cid)],
    );
    let root_bytes = Bytes::from(root.to_dag_cbor().unwrap());
    let root_cid = defra_core::block::generate_cid_from_bytes(&root_bytes).unwrap();

    let peer = PeerId::new("receiver".to_string());
    if expand_dag {
        // Frozen reference arm for the superseded sender: dependencies were
        // signed and presented as standalone PushLog heads before the root.
        // Keeping this only in the A/B harness ensures the regression test
        // remains capable of distinguishing the two ownership models after
        // production expansion is removed.
        let mut child_request = crate::message::PushLogRequest::new(
            "doc".to_string(),
            Bytes::from(child_cid.to_bytes()),
            "collection".to_string(),
            "creator".to_string(),
            child_bytes,
        );
        crate::signing::sign_with_transport(&transport, &mut child_request).unwrap();
        transport
            .send_two_stream_request(&peer, child_request)
            .await
            .unwrap();
    }
    let job = PushJobSpec::new(
        peer.clone(),
        "doc".to_string(),
        "collection".to_string(),
        "creator".to_string(),
        root_cid,
        root_bytes,
    );
    assert_eq!(backlog.try_enqueue(job), EnqueueOutcome::Enqueued);
    let active = backlog.next_job().await.unwrap();
    let completion = run_push_job(&context, &active).await;
    assert_eq!(completion, JobCompletion::Succeeded);
    backlog.job_done(&active, completion);

    let sent = transport.sent();
    let snapshot = backlog.snapshot();
    let result = OwnershipArm {
        scheduled: snapshot.enqueued_total,
        transmitted: sent.len(),
        announced_bytes: sent.iter().map(|push| push.block_bytes).sum(),
        terminal_success: snapshot.completed_total,
        child_announced_as_head: sent.iter().any(|push| push.cid == child_cid.to_bytes()),
        child_car_authorized: context.selective_car_access.allows_root(&peer, &root_cid),
    };
    backlog.close();
    result
}

/// Delivery-shape fence for #1116: the frozen full-DAG arm announces a field
/// block as a standalone PushLog, while the production arm announces one
/// composite head and preserves rooted CAR authority. Receiver convergence is
/// covered separately by
/// `ownership_ab_full_dag_amplifies_admission_and_requires_sender_retry`.
#[tokio::test]
async fn delivery_shape_full_dag_vs_head_hint_only() {
    let full_dag = run_ownership_arm(true).await;
    let head_hint = run_ownership_arm(false).await;

    assert_eq!(full_dag.scheduled, 1);
    assert_eq!(head_hint.scheduled, 1);
    assert_eq!(full_dag.terminal_success, 1);
    assert_eq!(head_hint.terminal_success, 1);
    assert!(full_dag.child_announced_as_head);
    assert!(!head_hint.child_announced_as_head);
    assert_eq!(full_dag.transmitted, 2);
    assert_eq!(head_hint.transmitted, 1);
    assert!(full_dag.announced_bytes > head_hint.announced_bytes);
    assert!(full_dag.child_car_authorized);
    assert!(head_hint.child_car_authorized);
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
    let events: Vec<_> = std::iter::from_fn(|| failure_rx.try_recv().ok()).collect();
    assert!(events.iter().any(|event| !event.create_retry));
    assert!(events.iter().all(|event| !event.create_retry));
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
        acknowledged: false,
        durable_tx: None,
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
