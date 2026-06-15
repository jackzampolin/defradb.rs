use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OwnedMutexGuard;

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
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl Default for DocWriteQueue {
    fn default() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
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
    pub async fn acquire(&self, doc_id: &str) -> OwnedMutexGuard<()> {
        let mutex = {
            let mut map = self.locks.lock();
            if map.len() > PRUNE_THRESHOLD {
                map.retain(|_, v| Arc::strong_count(v) > 1);
            }
            map.entry(doc_id.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        mutex.lock_owned().await
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
