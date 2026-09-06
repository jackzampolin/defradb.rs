use db::merge::push_docs_replay::ReplayDocumentFailure;
use db::merge::push_docs_replay::*;
use p2p::message::PushLogReply;
use p2p::transport::PeerId;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

#[tokio::test]
async fn replay_push_gate_caps_concurrent_sends() {
    let gate = Arc::new(ReplayPushGate::new(ReplayPushConfig {
        max_concurrent_document_tasks: 8,
        max_concurrent_outbound_pushes: 2,
        per_peer_rate_limit_burst: 100,
        per_peer_rate_limit_rate: 100.0,
        send_timeout: Duration::from_secs(1),
        ..Default::default()
    }));
    let current = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let peer = PeerId::new("peer-1".to_string());

    let mut handles = Vec::new();
    for _ in 0..8 {
        let gate = gate.clone();
        let current = current.clone();
        let max_seen = max_seen.clone();
        let peer = peer.clone();
        handles.push(tokio::spawn(async move {
            gate.send_pushlog(&peer, async move {
                let active = current.fetch_add(1, Ordering::SeqCst) + 1;
                record_max(&max_seen, active);
                tokio::time::sleep(Duration::from_millis(50)).await;
                current.fetch_sub(1, Ordering::SeqCst);
                Ok(PushLogReply::success("message"))
            })
            .await
            .unwrap();
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(max_seen.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn replay_push_gate_paces_after_peer_burst() {
    let gate = ReplayPushGate::new(ReplayPushConfig {
        max_concurrent_document_tasks: 1,
        max_concurrent_outbound_pushes: 1,
        per_peer_rate_limit_burst: 1,
        per_peer_rate_limit_rate: 10.0,
        send_timeout: Duration::from_secs(1),
        ..Default::default()
    });
    let peer = PeerId::new("peer-1".to_string());

    let start = Instant::now();
    for _ in 0..3 {
        gate.send_pushlog(&peer, async { Ok(PushLogReply::success("message")) })
            .await
            .unwrap();
    }

    assert!(start.elapsed() >= Duration::from_millis(150));
}

fn record_max(max_seen: &AtomicUsize, value: usize) {
    let mut observed = max_seen.load(Ordering::SeqCst);
    while value > observed {
        match max_seen.compare_exchange(observed, value, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return,
            Err(current) => observed = current,
        }
    }
}

#[tokio::test]
async fn unfinished_replay_uses_configured_schedule_and_marks_replicator_inactive() {
    use storage::RegolithStore;

    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = storage::stores::Peerstore::new(store.clone())
        .with_retry_schedule(storage::stores::RetrySchedule::new(vec![3600]).unwrap());
    let peer = PeerId::new("peer-durable".to_string());
    let info =
        p2p::ReplicatorInfo::from_raw(peer.to_string(), vec!["collection".to_string()], Vec::new());
    peerstore
        .create_replicator(peer.as_str(), &info.to_bytes().unwrap())
        .await
        .unwrap();

    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    persist_replay_failures(
        &peerstore,
        &peer,
        &[ReplayDocumentFailure {
            doc_id: "doc-1".to_string(),
            collection_id: "collection".to_string(),
        }],
    )
    .await
    .unwrap();

    let retries = peerstore.get_retry_documents(peer.as_str()).await.unwrap();
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].doc_id, "doc-1");
    assert_eq!(retries[0].scope, storage::stores::RetryScope::Document);
    assert!(!retries[0].is_collection_commit());
    assert!(!retries[0].retry_info.is_due());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(retries[0].retry_info.next_retry_unix >= before + 1800);
    assert!(retries[0].retry_info.next_retry_unix <= now + 3600);
    let saved = peerstore
        .get_replicator(peer.as_str())
        .await
        .unwrap()
        .unwrap();
    let saved = p2p::ReplicatorInfo::from_bytes(&saved).unwrap();
    assert_eq!(saved.status, p2p::ReplicatorStatus::Inactive);
}

#[tokio::test]
async fn unfinished_replay_uses_the_peer_retry_writer() {
    use storage::RegolithStore;

    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = storage::stores::Peerstore::new(store.clone());
    let peer = PeerId::new("peer-durable".to_string());
    let info =
        p2p::ReplicatorInfo::from_raw(peer.to_string(), vec!["collection".to_string()], Vec::new());
    peerstore
        .create_replicator(peer.as_str(), &info.to_bytes().unwrap())
        .await
        .unwrap();

    let writer = peerstore
        .acquire_replicator_retry_guard(peer.as_str())
        .await
        .unwrap()
        .unwrap();
    let persistence_peer = peer.clone();
    let mut persistence = tokio::spawn(async move {
        persist_replay_failures(
            &storage::stores::Peerstore::new(store),
            &persistence_peer,
            &[ReplayDocumentFailure {
                doc_id: "doc-1".to_string(),
                collection_id: "collection".to_string(),
            }],
        )
        .await
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut persistence)
            .await
            .is_err(),
        "replay failure persistence bypassed the peer retry writer"
    );

    drop(writer);
    tokio::time::timeout(Duration::from_secs(1), persistence)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
