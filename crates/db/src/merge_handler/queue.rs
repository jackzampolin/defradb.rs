use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OwnedMutexGuard;

const PRUNE_THRESHOLD: usize = 10_000;

/// Per-key async merge serialization queue.
///
/// Ensures that merges for the same document (or collection, for branchable
/// types) are processed one at a time, while merges for different keys run
/// in parallel. Matches Go DefraDB's `docMergeQueue`/`colMergeQueue`.
pub struct MergeQueue {
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl Default for MergeQueue {
    fn default() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
        }
    }
}

impl MergeQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquire the merge lock for a given key.
    ///
    /// Returns an owned guard that serializes access. Different keys
    /// proceed in parallel; the same key blocks until the previous
    /// holder drops the guard.
    pub async fn acquire(&self, key: &str) -> OwnedMutexGuard<()> {
        let mutex = {
            let mut map = self.locks.lock();
            if map.len() > PRUNE_THRESHOLD {
                map.retain(|_, v| Arc::strong_count(v) > 1);
            }
            map.entry(key.to_string())
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
    async fn same_key_serializes() {
        let queue = Arc::new(MergeQueue::new());
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
    async fn different_keys_run_in_parallel() {
        let queue = Arc::new(MergeQueue::new());
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
