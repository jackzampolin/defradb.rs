use async_lock::{Mutex as AsyncMutex, MutexGuardArc};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

const PRUNE_THRESHOLD: usize = 10_000;

/// Per-document write serialization queue.
///
/// Serializes mutations that touch the same document so that a local write and
/// a P2P merge (or two of either) never interleave their read-modify-write on a
/// document's CRDT state. This is required for counter convergence: a local
/// increment and an incoming merge both read-modify-write the counter
/// accumulation store, and without per-doc serialization their txns can race in
/// a way the underlying store's optimistic-conflict detection does not always
/// catch, dropping increments while the commit DAG still converges (#1021).
///
/// The merge handler shares the DB's instance of this queue (it already holds an
/// `Arc<DB>`), so local writes and merges contend on the same per-doc lock.
/// Different documents proceed in parallel. Mirrors Go DefraDB's per-doc merge
/// queue, extended to also cover local writes.
pub struct DocWriteQueue {
    locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    /// Serializes the guard-ACQUISITION phase of multi-document writers (local
    /// mutation batches, batch merges) against one another. A caller that will
    /// hold more than one per-doc guard at once must hold this gate while
    /// acquiring them, so two multi-doc acquirers can never grab overlapping
    /// documents in opposite orders and deadlock. Single-doc callers never take
    /// it, so the common path (single-doc writes and merges) is unaffected.
    ///
    /// Deadlock-freedom also relies on `new_txn()` being non-blocking (the
    /// backends use optimistic MVCC: a write txn snapshots on open and acquires
    /// the exclusive store write lock only at commit, which holds neither the gate
    /// nor any per-doc guard). That keeps the gate the only resource ever
    /// contended across a txn open, so the two acquirer orderings (BatchMutator
    /// opens its txn before taking the gate; create_many / try_batch_merge take
    /// the gate before opening their txn) cannot invert into a cycle. A future
    /// blocking-writer backend would need to revisit this.
    batch_gate: Arc<AsyncMutex<()>>,
}

impl Default for DocWriteQueue {
    fn default() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
            batch_gate: Arc::new(AsyncMutex::new(())),
        }
    }
}

impl DocWriteQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquire the write lock for a document.
    ///
    /// Returns an owned guard that serializes access. Different documents
    /// proceed in parallel; the same document blocks until the previous holder
    /// drops the guard.
    pub async fn acquire(&self, doc_id: &str) -> MutexGuardArc<()> {
        let mutex = {
            let mut map = self.locks.lock();
            if map.len() > PRUNE_THRESHOLD {
                map.retain(|_, v| Arc::strong_count(v) > 1);
            }
            map.entry(doc_id.to_string())
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };
        mutex.lock_arc().await
    }

    /// Acquire the multi-document batch gate.
    ///
    /// Any caller that will simultaneously hold more than one per-doc guard must
    /// hold this gate while acquiring those guards. An incremental acquirer (a
    /// local mutation batch that discovers its documents one mutation at a time)
    /// holds it for the whole batch; an upfront acquirer (a batch merge, or
    /// `create_many`) holds it only while taking its sorted guards, then releases
    /// it. This makes the per-doc guards deadlock-free across multi-doc writers.
    pub async fn acquire_batch_gate(&self) -> MutexGuardArc<()> {
        self.batch_gate.lock_arc().await
    }

    /// Non-blocking variant of [`Self::acquire_batch_gate`]. Returns `None` if the
    /// gate is currently held. A caller for whom batching is an optimization (the
    /// batch-merge path) uses this to degrade to the gate-free per-block path
    /// instead of blocking behind a long-lived gate holder (e.g. an interactive
    /// transaction that holds the gate across its user-controlled lifetime, #1041).
    pub fn try_acquire_batch_gate(&self) -> Option<MutexGuardArc<()>> {
        self.batch_gate.try_lock_arc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        use std::time::Duration;
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
        use std::time::Duration;
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
}
