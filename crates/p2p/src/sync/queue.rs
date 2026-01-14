// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Process queue for serializing concurrent sync operations.
//!
//! This matches Go's `processQueue` in `p2p.go` which prevents multiple
//! goroutines from syncing the same CID concurrently, avoiding transaction
//! conflicts during merge.

use cid::Cid;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{oneshot, Mutex};

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
        let mut waiters = self.inner.waiters.lock().await;

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

    /// Release the CID and notify all waiters.
    async fn release(&self, cid: &Cid) {
        let mut waiters = self.inner.waiters.lock().await;
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

    /// Explicitly release the guard (same as drop, but async).
    pub async fn release(self) {
        self.queue.release(&self.cid).await;
        // Prevent Drop from running
        std::mem::forget(self);
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        // Clone data we need before spawning
        let cid = self.cid;
        let queue = self.queue.clone();

        // Only spawn if we're in a tokio runtime context
        // This prevents panics during shutdown or when used outside async code
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                queue.release(&cid).await;
            });
        } else {
            // If no runtime is available, we can't release asynchronously.
            // This is a critical failure - the CID will remain permanently locked
            // in the waiters map, causing future sync attempts for this CID to hang.
            // Callers should prefer using the explicit `release().await` method.
            tracing::error!(
                ?cid,
                "CRITICAL: ProcessGuard dropped outside tokio runtime - CID {} is PERMANENTLY LOCKED. \
                 Future sync operations for this CID will hang. Use ProcessGuard::release().await instead of drop.",
                cid
            );
        }
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
            // Guard dropped here
        }

        // Give time for async drop to complete
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Should be able to acquire again
        let result = queue.try_acquire(&cid).await;
        assert!(result.is_ok(), "Should be able to reacquire after drop");
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
