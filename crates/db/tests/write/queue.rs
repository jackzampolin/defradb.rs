use db::write::queue::*;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn same_doc_serializes() {
    let queue = Arc::new(DocWriteQueue::new());
    let counter = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];
    for _ in 0..10 {
        let q = queue.clone();
        let c = counter.clone();
        let m = max_concurrent.clone();
        handles.push(tokio::spawn(async move {
            let _guard = q.acquire("doc-1").await;
            let current = c.fetch_add(1, Ordering::SeqCst) + 1;
            m.fetch_max(current, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            c.fetch_sub(1, Ordering::SeqCst);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(max_concurrent.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn batch_gate_is_exclusive() {
    let queue = Arc::new(DocWriteQueue::new());
    let gate = queue.acquire_batch_gate().await;

    let q2 = queue.clone();
    let waiter = tokio::spawn(async move { q2.acquire_batch_gate().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !waiter.is_finished(),
        "a second batch-gate acquire must block while the gate is held"
    );

    drop(gate);
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("second acquire completes once the gate is released")
        .unwrap();
}

#[tokio::test]
async fn batch_gate_does_not_block_per_doc_guards() {
    // The gate only serializes the acquisition phase of multi-doc writers; a
    // single-doc per-doc guard is independent and must proceed while the gate
    // is held (otherwise the common single-doc path would stall behind a batch).
    let queue = Arc::new(DocWriteQueue::new());
    let _gate = queue.acquire_batch_gate().await;
    tokio::time::timeout(Duration::from_secs(1), queue.acquire("doc-1"))
        .await
        .expect("per-doc guard acquisition must not block on the batch gate");
}

#[tokio::test]
async fn try_acquire_batch_gate_reflects_held_state() {
    // The non-blocking gate signal that the batch-merge path relies on to
    // degrade to per-block when an interactive txn holds the gate (#1041).
    let queue = Arc::new(DocWriteQueue::new());
    let held = queue
        .try_acquire_batch_gate()
        .expect("try_acquire must succeed when the gate is free");
    assert!(
        queue.try_acquire_batch_gate().is_none(),
        "try_acquire must return None (not block) while the gate is held"
    );
    drop(held);
    assert!(
        queue.try_acquire_batch_gate().is_some(),
        "try_acquire must succeed again once the gate is released"
    );
}

#[tokio::test]
async fn different_docs_run_in_parallel() {
    let queue = Arc::new(DocWriteQueue::new());
    let counter = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];
    for i in 0..10 {
        let q = queue.clone();
        let c = counter.clone();
        let m = max_concurrent.clone();
        let key = format!("doc-{}", i);
        handles.push(tokio::spawn(async move {
            let _guard = q.acquire(&key).await;
            let current = c.fetch_add(1, Ordering::SeqCst) + 1;
            m.fetch_max(current, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            c.fetch_sub(1, Ordering::SeqCst);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert!(max_concurrent.load(Ordering::SeqCst) > 1);
}
