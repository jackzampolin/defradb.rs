use std::sync::Arc;
use storage::corekv::Store;
use storage::stores::{Peerstore, RetryInfo, RetrySchedule};
use storage::RegolithStore;
use web_time::{SystemTime, UNIX_EPOCH};

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[test]
fn retry_schedule_rejects_empty_and_zero_intervals() {
    for intervals in [vec![], vec![0], vec![1, 0, 3]] {
        assert!(RetrySchedule::new(intervals).is_err());
    }
}

#[test]
fn custom_intervals_advance_and_repeat_the_last_cap() {
    let schedule = RetrySchedule::new(vec![1, 8, 100]).unwrap();
    let mut info = RetryInfo::new_initial();
    for (attempt, cap) in [1, 8, 100, 100].into_iter().enumerate() {
        let before = now();
        info.bump_with_schedule("peer", &schedule);
        assert!(info.next_retry_unix >= before + (cap / 2).max(1));
        assert!(info.next_retry_unix <= now() + cap);
        assert_eq!(info.num_retries, attempt as u32 + 1);
    }
}

#[test]
fn default_schedule_preserves_existing_deadlines() {
    for attempt in 0..10 {
        let mut default = RetryInfo::new_initial();
        default.num_retries = attempt;
        let mut explicit = default.clone();
        let before = now();
        default.bump_for("peer");
        explicit.bump_with_schedule("peer", &RetrySchedule::default());
        assert!(explicit.next_retry_unix.abs_diff(default.next_retry_unix) <= now() - before);
        assert_eq!(default.num_retries, explicit.num_retries);
    }
}

#[test]
fn retry_counter_and_deadline_saturate_instead_of_overflowing() {
    let mut info = RetryInfo::new_initial();
    info.num_retries = u32::MAX;
    info.bump_with_schedule("peer", &RetrySchedule::new(vec![u64::MAX]).unwrap());
    assert_eq!(info.num_retries, u32::MAX);
    assert!(info.next_retry_unix > now());
}

async fn info(peerstore: &Peerstore<RegolithStore>) -> RetryInfo {
    RetryInfo::from_bytes(&peerstore.get_retry_info("peer").await.unwrap().unwrap()).unwrap()
}

#[tokio::test]
async fn configured_schedule_survives_scope_updates_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(RegolithStore::open(dir.path()).unwrap());
    let peerstore = Peerstore::new(store.clone())
        .with_retry_schedule(RetrySchedule::new(vec![1, 600]).unwrap());
    peerstore
        .create_replicator("peer", b"replicator")
        .await
        .unwrap();
    let before = now();
    peerstore
        .observe_push_head("peer", "doc", "collection")
        .await
        .unwrap();
    let first = info(&peerstore).await;
    assert!(first.next_retry_unix > before && first.next_retry_unix <= now() + 1);
    assert_eq!(first.num_retries, 1);

    peerstore
        .observe_push_head("peer", "", "collection")
        .await
        .unwrap();
    assert_eq!(
        info(&peerstore).await.next_retry_unix,
        first.next_retry_unix
    );
    let before = now();
    peerstore.reschedule_retry_peer("peer", None).await.unwrap();
    let second = info(&peerstore).await;
    assert!(second.next_retry_unix >= before + 300 && second.next_retry_unix <= now() + 600);
    drop(peerstore);
    store.close().await.unwrap();
    drop(store);

    let reopened = Arc::new(RegolithStore::open(dir.path()).unwrap());
    let peerstore =
        Peerstore::new(reopened.clone()).with_retry_schedule(RetrySchedule::new(vec![1]).unwrap());
    assert_eq!(
        info(&peerstore).await.next_retry_unix,
        second.next_retry_unix
    );
    assert_eq!(
        peerstore.get_retry_documents("peer").await.unwrap().len(),
        2
    );
    let before = now();
    peerstore.reschedule_retry_peer("peer", None).await.unwrap();
    let third = info(&peerstore).await;
    assert!(third.next_retry_unix > before && third.next_retry_unix <= now() + 1);
    assert_eq!(third.num_retries, 3);
    drop(peerstore);
    reopened.close().await.unwrap();
}
