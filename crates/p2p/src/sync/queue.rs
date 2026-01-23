
//! Process queue for serializing concurrent sync operations.
//!
//! This matches Go's `processQueue` in `p2p.go` which prevents multiple
//! goroutines from syncing the same CID concurrently, avoiding transaction
//! conflicts during merge.

use cid::Cid;
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::oneshot;

/// A queue that serializes processing of the same CID.
///
/// When multiple sync requests arrive for the same CID concurrently,
/// only the first one proceeds while others wait. Once the first
/// completes, all waiters are released (they can then check if the
/// block is already merged and skip processing).
///
/// # Go Compatibility
///
/// This matches Go's `processQueue` pattern in `p2p.go:565-611`.
#[derive(Clone)]
pub struct ProcessQueue {
    inner: Arc<ProcessQueueInner>,
}

impl std::fmt::Debug for ProcessQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessQueue").finish_non_exhaustive()
    }
}

struct ProcessQueueInner {
    /// Map of CID -> list of waiters
    waiters: Mutex<HashMap<Cid, Vec<oneshot::Sender<()>>>>,
}

impl ProcessQueue {
    /// Create a new process queue.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ProcessQueueInner {
                waiters: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Get the number of CIDs currently being processed.
    ///
    /// Useful for monitoring and debugging.
    pub fn active_count(&self) -> usize {
        self.inner.waiters.lock().len()
    }

    /// Get all CIDs currently being processed.
    ///
    /// Useful for debugging stuck operations.
    pub fn active_cids(&self) -> Vec<Cid> {
        self.inner.waiters.lock().keys().cloned().collect()
    }

    /// Force release a stuck CID.
    ///
    /// Use this to recover from situations where a ProcessGuard was dropped
    /// outside a tokio runtime and the CID became permanently locked.
    ///
    /// # Safety
    ///
    /// Only call this for CIDs that you are certain are stuck. Calling this
    /// while a legitimate processing operation is in progress may cause
    /// duplicate processing.
    ///
    /// # Returns
    ///
    /// Returns `true` if the CID was released, `false` if it wasn't locked.
    pub fn force_release(&self, cid: &Cid) -> bool {
        let mut waiters = self.inner.waiters.lock();
        if let Some(waiting) = waiters.remove(cid) {
            tracing::warn!(
                ?cid,
                waiter_count = waiting.len(),
                "Force-releasing stuck CID"
            );
            // Notify any waiters that processing is "complete"
            for tx in waiting {
                let _ = tx.send(());
            }
            true
        } else {
            false
        }
    }

    /// Force release all stuck CIDs.
    ///
    /// Use this during cleanup or recovery to release all locked CIDs.
    ///
    /// # Returns
    ///
    /// Returns the number of CIDs that were released.
    pub fn force_release_all(&self) -> usize {
        let mut waiters = self.inner.waiters.lock();
        let count = waiters.len();
        if count > 0 {
            tracing::warn!(count = count, "Force-releasing all stuck CIDs");
            for (cid, waiting) in waiters.drain() {
                tracing::debug!(?cid, "Force-releasing CID");
                for tx in waiting {
                    let _ = tx.send(());
                }
            }
        }
        count
    }

    /// Try to acquire exclusive processing rights for a CID.
    ///
    /// Returns:
    /// - `Ok(ProcessGuard)` if this caller should process the CID
    /// - `Err(receiver)` if another caller is already processing; wait on the receiver
    ///
    /// # Example
    ///
    /// ```ignore
    /// let queue = ProcessQueue::new();
    ///
    /// match queue.try_acquire(&cid).await {
    ///     Ok(guard) => {
    ///         // We're the first - do the sync work
    ///         do_sync(&cid).await;
    ///         // Guard drop notifies waiters
    ///     }
    ///     Err(rx) => {
    ///         // Another task is processing - wait for it
    ///         let _ = rx.await;
    ///         // Now check if block is merged and proceed accordingly
    ///     }
    /// }
    /// ```
    pub async fn try_acquire(&self, cid: &Cid) -> Result<ProcessGuard, oneshot::Receiver<()>> {
        let mut waiters = self.inner.waiters.lock();

        if waiters.contains_key(cid) {
            // Someone else is processing - add ourselves as a waiter
            let (tx, rx) = oneshot::channel();
            waiters.get_mut(cid).unwrap().push(tx);
            Err(rx)
        } else {
            // We're the first - create the entry and return a guard
            waiters.insert(*cid, Vec::new());
            Ok(ProcessGuard {
                cid: *cid,
                queue: self.clone(),
            })
        }
    }

    /// Release the CID and notify all waiters (synchronous version).
    fn release_sync(&self, cid: &Cid) {
        let mut waiters = self.inner.waiters.lock();
        if let Some(waiting) = waiters.remove(cid) {
            // Notify all waiters that processing is complete
            let waiter_count = waiting.len();
            let mut notified = 0;
            for tx in waiting {
                if tx.send(()).is_ok() {
                    notified += 1;
                }
            }
            if notified < waiter_count {
                tracing::debug!(
                    ?cid,
                    notified,
                    total = waiter_count,
                    "Some waiters were cancelled before notification"
                );
            }
        }
    }
}

impl Default for ProcessQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Guard that releases the CID from the queue when dropped.
#[derive(Debug)]
pub struct ProcessGuard {
    cid: Cid,
    queue: ProcessQueue,
}

impl ProcessGuard {
    /// Get the CID being processed.
    pub fn cid(&self) -> &Cid {
        &self.cid
    }

    /// Explicitly release the guard.
    ///
    /// This is the preferred way to release as it provides explicit control.
    /// The guard will also be released automatically on drop.
    pub async fn release(self) {
        self.queue.release_sync(&self.cid);
        // Prevent Drop from running
        std::mem::forget(self);
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        // Release synchronously - this works both inside and outside tokio runtime
        // because we use parking_lot::Mutex which supports synchronous locking.
        self.queue.release_sync(&self.cid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use std::time::Duration;

    fn test_cid() -> Cid {
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
    }

    fn test_cid2() -> Cid {
        Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap()
    }

    #[tokio::test]
    async fn test_first_caller_acquires() {
        let queue = ProcessQueue::new();
        let cid = test_cid();

        let result = queue.try_acquire(&cid).await;
        assert!(result.is_ok(), "First caller should acquire");
    }

    #[tokio::test]
    async fn test_second_caller_waits() {
        let queue = ProcessQueue::new();
        let cid = test_cid();

        // First caller acquires
        let _guard = queue.try_acquire(&cid).await.unwrap();

        // Second caller should get a waiter
        let result = queue.try_acquire(&cid).await;
        assert!(result.is_err(), "Second caller should wait");
    }

    #[tokio::test]
    async fn test_different_cids_independent() {
        let queue = ProcessQueue::new();
        let cid1 = test_cid();
        let cid2 = test_cid2();

        // First CID acquired
        let _guard1 = queue.try_acquire(&cid1).await.unwrap();

        // Second CID should also acquire (different CID)
        let result = queue.try_acquire(&cid2).await;
        assert!(result.is_ok(), "Different CIDs should be independent");
    }

    #[tokio::test]
    async fn test_waiter_notified_on_release() {
        let queue = ProcessQueue::new();
        let cid = test_cid();

        // First caller acquires
        let guard = queue.try_acquire(&cid).await.unwrap();

        // Spawn second caller that waits
        let queue_clone = queue.clone();
        let waiter = tokio::spawn(async move {
            let rx = queue_clone.try_acquire(&cid).await.unwrap_err();
            // Wait for notification
            rx.await.unwrap();
            true
        });

        // Give the waiter time to register
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Explicitly release guard
        guard.release().await;

        // Waiter should complete
        let result = tokio::time::timeout(Duration::from_millis(100), waiter).await;
        assert!(result.is_ok(), "Waiter should be notified");
        assert!(
            result.unwrap().unwrap(),
            "Waiter should complete successfully"
        );
    }

    #[tokio::test]
    async fn test_multiple_waiters_all_notified() {
        let queue = ProcessQueue::new();
        let cid = test_cid();

        // First caller acquires
        let guard = queue.try_acquire(&cid).await.unwrap();

        // Spawn multiple waiters
        let mut handles = Vec::new();
        for _ in 0..5 {
            let queue_clone = queue.clone();
            handles.push(tokio::spawn(async move {
                let rx = queue_clone.try_acquire(&cid).await.unwrap_err();
                rx.await.unwrap();
            }));
        }

        // Give waiters time to register
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Explicitly release guard
        guard.release().await;

        // All waiters should complete
        for handle in handles {
            let result = tokio::time::timeout(Duration::from_millis(100), handle).await;
            assert!(result.is_ok(), "All waiters should be notified");
        }
    }

    #[tokio::test]
    async fn test_reacquire_after_release() {
        let queue = ProcessQueue::new();
        let cid = test_cid();

        // First acquisition
        {
            let guard = queue.try_acquire(&cid).await.unwrap();
            guard.release().await;
        }

        // Should be able to acquire again immediately
        let result = queue.try_acquire(&cid).await;
        assert!(result.is_ok(), "Should be able to reacquire after release");
    }

    #[tokio::test]
    async fn test_drop_releases() {
        let queue = ProcessQueue::new();
        let cid = test_cid();

        // Acquire and drop (not explicit release)
        {
            let _guard = queue.try_acquire(&cid).await.unwrap();
            // Guard dropped here - release is now synchronous
        }

        // Should be able to acquire again immediately (drop is now synchronous)
        let result = queue.try_acquire(&cid).await;
        assert!(result.is_ok(), "Should be able to reacquire after drop");
    }

    #[test]
    fn test_drop_releases_outside_tokio() {
        // Test that dropping ProcessGuard outside tokio runtime works correctly
        // (previously this would permanently lock the CID)
        let queue = ProcessQueue::new();
        let cid = test_cid();

        // Use block_on to acquire in async context, then drop synchronously
        let rt = tokio::runtime::Runtime::new().unwrap();
        let guard = rt.block_on(async { queue.try_acquire(&cid).await.unwrap() });

        // Drop the guard outside async context
        drop(guard);

        // Should be able to acquire again
        let result = rt.block_on(async { queue.try_acquire(&cid).await });
        assert!(
            result.is_ok(),
            "Should be able to reacquire after synchronous drop"
        );
    }

    #[tokio::test]
    async fn test_cancelled_waiters_handled_gracefully() {
        let queue = ProcessQueue::new();
        let cid = test_cid();

        // First caller acquires
        let guard = queue.try_acquire(&cid).await.unwrap();

        // Create waiters then drop them (simulating cancelled tasks)
        {
            let rx1 = queue.try_acquire(&cid).await.unwrap_err();
            let rx2 = queue.try_acquire(&cid).await.unwrap_err();
            // Drop receivers without awaiting - simulates task cancellation
            drop(rx1);
            drop(rx2);
        }

        // Release should handle cancelled receivers gracefully (not panic)
        guard.release().await;

        // Queue should be in clean state
        let result = queue.try_acquire(&cid).await;
        assert!(
            result.is_ok(),
            "Queue should be clean after handling cancelled waiters"
        );
    }

    #[tokio::test]
    async fn test_mixed_cancelled_and_waiting() {
        let queue = ProcessQueue::new();
        let cid = test_cid();

        // First caller acquires
        let guard = queue.try_acquire(&cid).await.unwrap();

        // Create a waiter that will be cancelled
        let rx1 = queue.try_acquire(&cid).await.unwrap_err();
        drop(rx1); // Cancel immediately

        // Create a waiter that will actually wait
        let queue_clone = queue.clone();
        let waiter = tokio::spawn(async move {
            let rx = queue_clone.try_acquire(&cid).await.unwrap_err();
            rx.await.unwrap();
            true
        });

        // Give waiter time to register
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Release - should notify the waiting task even though one was cancelled
        guard.release().await;

        // Active waiter should still be notified
        let result = tokio::time::timeout(Duration::from_millis(100), waiter).await;
        assert!(result.is_ok(), "Active waiter should be notified");
        assert!(
            result.unwrap().unwrap(),
            "Active waiter should complete successfully"
        );
    }
}
