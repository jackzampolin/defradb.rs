//! Short-window coalescing for rapid document gossip updates (#1102).

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cid::Cid;
use parking_lot::Mutex;
use tokio::sync::Notify;

use super::BroadcastResult;
use crate::message::PushLogBroadcast;

pub(crate) const DEFAULT_BROADCAST_COALESCING_WINDOW: Duration = Duration::from_millis(250);

type SharedResult = std::result::Result<BroadcastResult, String>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BroadcastKey {
    collection_id: String,
    doc_id: String,
}

struct PendingBroadcast {
    broadcast: Mutex<PushLogBroadcast>,
    last_update: Mutex<tokio::time::Instant>,
    result: Mutex<Option<SharedResult>>,
    notify: Notify,
}

pub(crate) struct BroadcastCoalescer {
    pending: Mutex<HashMap<BroadcastKey, Arc<PendingBroadcast>>>,
    window: Duration,
    coalesced: AtomicU64,
}

impl Default for BroadcastCoalescer {
    fn default() -> Self {
        Self::with_window(DEFAULT_BROADCAST_COALESCING_WINDOW)
    }
}

impl BroadcastCoalescer {
    fn with_window(window: Duration) -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            window,
            coalesced: AtomicU64::new(0),
        }
    }

    pub(crate) fn coalesced(&self) -> u64 {
        self.coalesced.load(Ordering::Relaxed)
    }

    pub(crate) async fn run<F, Fut>(&self, broadcast: PushLogBroadcast, send: F) -> SharedResult
    where
        F: FnOnce(PushLogBroadcast) -> Fut,
        Fut: Future<Output = SharedResult>,
    {
        let key = BroadcastKey {
            collection_id: broadcast.collection_id.clone(),
            doc_id: broadcast.doc_id.clone(),
        };
        let (pending, leader) = {
            let mut all = self.pending.lock();
            if let Some(pending) = all.get(&key) {
                let mut current = pending.broadcast.lock();
                if version(&broadcast) > version(&current) {
                    *current = broadcast;
                }
                *pending.last_update.lock() = tokio::time::Instant::now();
                self.coalesced.fetch_add(1, Ordering::Relaxed);
                (Arc::clone(pending), false)
            } else {
                let pending = Arc::new(PendingBroadcast {
                    broadcast: Mutex::new(broadcast),
                    last_update: Mutex::new(tokio::time::Instant::now()),
                    result: Mutex::new(None),
                    notify: Notify::new(),
                });
                all.insert(key.clone(), Arc::clone(&pending));
                (pending, true)
            }
        };

        if leader {
            wait_for_quiet(&pending.last_update, self.window).await;
            {
                let mut all = self.pending.lock();
                if all
                    .get(&key)
                    .is_some_and(|current| Arc::ptr_eq(current, &pending))
                {
                    all.remove(&key);
                }
            }
            let latest = pending.broadcast.lock().clone();
            let result = send(latest).await;
            *pending.result.lock() = Some(result.clone());
            pending.notify.notify_waiters();
            return result;
        }

        loop {
            let notified = pending.notify.notified();
            if let Some(result) = pending.result.lock().clone() {
                return result;
            }
            notified.await;
        }
    }
}

pub(super) async fn wait_for_quiet(last_update: &Mutex<tokio::time::Instant>, window: Duration) {
    loop {
        let deadline = *last_update.lock() + window;
        tokio::time::sleep_until(deadline).await;
        if tokio::time::Instant::now() >= *last_update.lock() + window {
            return;
        }
    }
}

fn version(broadcast: &PushLogBroadcast) -> (u64, Vec<u8>) {
    let priority = defra_core::Block::from_dag_cbor(&broadcast.block)
        .map(|block| block.delta.priority())
        .unwrap_or(0);
    let cid = Cid::try_from(broadcast.cid.as_ref())
        .map(|cid| cid.to_bytes())
        .unwrap_or_else(|_| broadcast.cid.to_vec());
    (priority, cid)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use bytes::Bytes;
    use multihash_codetable::{Code, MultihashDigest};

    use super::*;

    fn broadcast(seed: &[u8]) -> PushLogBroadcast {
        let cid = Cid::new_v1(0x55, Code::Sha2_256.digest(seed));
        PushLogBroadcast::new(
            "doc".to_string(),
            Bytes::from(cid.to_bytes()),
            "collection".to_string(),
            "creator".to_string(),
            Bytes::copy_from_slice(seed),
        )
    }

    #[tokio::test]
    async fn rapid_updates_publish_only_the_greatest_version() {
        let coalescer = Arc::new(BroadcastCoalescer::with_window(Duration::from_millis(10)));
        let sends = Arc::new(AtomicUsize::new(0));
        let sent_cid = Arc::new(Mutex::new(None));
        let updates: Vec<_> = [b"1".as_slice(), b"2".as_slice(), b"3".as_slice()]
            .into_iter()
            .map(broadcast)
            .collect();
        let expected_cid = updates
            .iter()
            .max_by_key(|update| version(update))
            .unwrap()
            .cid
            .clone();
        let mut tasks = Vec::new();
        for update in updates {
            let coalescer = Arc::clone(&coalescer);
            let sends = Arc::clone(&sends);
            let sent_cid = Arc::clone(&sent_cid);
            tasks.push(tokio::spawn(async move {
                coalescer
                    .run(update, move |latest| async move {
                        sends.fetch_add(1, Ordering::Relaxed);
                        *sent_cid.lock() = Some(latest.cid);
                        Ok(BroadcastResult::Success)
                    })
                    .await
            }));
        }
        for task in tasks {
            assert_eq!(task.await.unwrap().unwrap(), BroadcastResult::Success);
        }
        assert_eq!(sends.load(Ordering::Relaxed), 1);
        assert_eq!(*sent_cid.lock(), Some(expected_cid));
        assert_eq!(coalescer.coalesced(), 2);
    }

    #[tokio::test]
    async fn sequential_update_resets_the_quiet_window() {
        let window = Duration::from_millis(40);
        let coalescer = Arc::new(BroadcastCoalescer::with_window(window));
        let sends = Arc::new(AtomicUsize::new(0));
        let leader = {
            let coalescer = Arc::clone(&coalescer);
            let sends = Arc::clone(&sends);
            tokio::spawn(async move {
                coalescer
                    .run(broadcast(b"first"), move |_| async move {
                        sends.fetch_add(1, Ordering::Relaxed);
                        Ok(BroadcastResult::Success)
                    })
                    .await
            })
        };
        tokio::time::sleep(window / 2).await;
        let follower = {
            let coalescer = Arc::clone(&coalescer);
            let sends = Arc::clone(&sends);
            tokio::spawn(async move {
                coalescer
                    .run(broadcast(b"second"), move |_| async move {
                        sends.fetch_add(1, Ordering::Relaxed);
                        Ok(BroadcastResult::Success)
                    })
                    .await
            })
        };

        tokio::time::sleep(window * 3 / 4).await;
        assert_eq!(sends.load(Ordering::Relaxed), 0);
        leader.await.unwrap().unwrap();
        follower.await.unwrap().unwrap();
        assert_eq!(sends.load(Ordering::Relaxed), 1);
    }
}
