//! Short-window coalescing for rapid document gossip updates (#1102).

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cid::Cid;
use parking_lot::Mutex;
use tokio::sync::Notify;

use super::BroadcastResult;
use crate::message::PushLogBroadcast;

pub(crate) const DEFAULT_BROADCAST_COALESCING_WINDOW: Duration = Duration::from_millis(250);
pub(crate) const DEFAULT_BROADCAST_MAX_COALESCING_DELAY: Duration = Duration::from_secs(1);

type SharedResult = std::result::Result<BroadcastResult, String>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BroadcastKey {
    collection_id: String,
    doc_id: String,
}

struct PendingBroadcast {
    broadcast: Mutex<PushLogBroadcast>,
    version: Mutex<(u64, Cid)>,
    started_at: tokio::time::Instant,
    last_update: Mutex<tokio::time::Instant>,
    result: Mutex<Option<SharedResult>>,
    cancelled: AtomicBool,
    notify: Notify,
}

pub(crate) struct BroadcastCoalescer {
    pending: Mutex<HashMap<BroadcastKey, Arc<PendingBroadcast>>>,
    window: Duration,
    max_delay: Duration,
    coalesced: AtomicU64,
}

struct BroadcastLeaderGuard<'a> {
    coalescer: &'a BroadcastCoalescer,
    key: BroadcastKey,
    pending: Arc<PendingBroadcast>,
    armed: bool,
}

impl BroadcastLeaderGuard<'_> {
    fn complete(&mut self) {
        self.armed = false;
    }
}

impl Drop for BroadcastLeaderGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut all = self.coalescer.pending.lock();
        if all
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.pending))
        {
            all.remove(&self.key);
        }
        drop(all);
        self.pending.cancelled.store(true, Ordering::Release);
        self.pending.notify.notify_waiters();
    }
}

impl Default for BroadcastCoalescer {
    fn default() -> Self {
        Self::with_limits(
            DEFAULT_BROADCAST_COALESCING_WINDOW,
            DEFAULT_BROADCAST_MAX_COALESCING_DELAY,
        )
    }
}

impl BroadcastCoalescer {
    #[cfg(test)]
    fn with_window(window: Duration) -> Self {
        Self::with_limits(window, window * 4)
    }

    fn with_limits(window: Duration, max_delay: Duration) -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            window,
            max_delay,
            coalesced: AtomicU64::new(0),
        }
    }

    pub(crate) fn coalesced(&self) -> u64 {
        self.coalesced.load(Ordering::Relaxed)
    }

    /// Cancellation safe: if the leader is dropped, its guard removes the
    /// dead window and wakes followers to re-admit the latest buffered update.
    pub(crate) async fn run<F, Fut>(&self, broadcast: PushLogBroadcast, send: F) -> SharedResult
    where
        F: FnOnce(PushLogBroadcast) -> Fut,
        Fut: Future<Output = SharedResult>,
    {
        let mut candidate = broadcast;
        let mut send = Some(send);
        loop {
            let Some(incoming_version) = version(&candidate) else {
                // Without a decoded priority there is no safe proof that this
                // update subsumes another document head.
                return send.take().expect("send closure available")(candidate).await;
            };
            let key = BroadcastKey {
                collection_id: candidate.collection_id.clone(),
                doc_id: candidate.doc_id.clone(),
            };
            let (pending, leader) = {
                let mut all = self.pending.lock();
                if let Some(pending) = all.get(&key) {
                    let mut current_version = pending.version.lock();
                    if incoming_version > *current_version {
                        *pending.broadcast.lock() = candidate;
                        *current_version = incoming_version;
                        *pending.last_update.lock() = tokio::time::Instant::now();
                    }
                    self.coalesced.fetch_add(1, Ordering::Relaxed);
                    (Arc::clone(pending), false)
                } else {
                    let now = tokio::time::Instant::now();
                    let pending = Arc::new(PendingBroadcast {
                        broadcast: Mutex::new(candidate),
                        version: Mutex::new(incoming_version),
                        started_at: now,
                        last_update: Mutex::new(now),
                        result: Mutex::new(None),
                        cancelled: AtomicBool::new(false),
                        notify: Notify::new(),
                    });
                    all.insert(key.clone(), Arc::clone(&pending));
                    (pending, true)
                }
            };

            if leader {
                let mut guard = BroadcastLeaderGuard {
                    coalescer: self,
                    key: key.clone(),
                    pending: Arc::clone(&pending),
                    armed: true,
                };
                wait_for_quiet(
                    &pending.last_update,
                    pending.started_at,
                    self.window,
                    self.max_delay,
                )
                .await;
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
                let result = send.take().expect("send closure available")(latest).await;
                *pending.result.lock() = Some(result.clone());
                pending.notify.notify_waiters();
                guard.complete();
                return result;
            }

            loop {
                let notified = pending.notify.notified();
                tokio::pin!(notified);
                // Register before checking the result. `notify_waiters` does
                // not retain a permit, so polling only after the check can
                // miss the leader's one completion notification forever.
                notified.as_mut().enable();
                if let Some(result) = pending.result.lock().clone() {
                    return result;
                }
                if pending.cancelled.load(Ordering::Acquire) {
                    candidate = pending.broadcast.lock().clone();
                    break;
                }
                notified.await;
            }
        }
    }
}

pub(super) async fn wait_for_quiet(
    last_update: &Mutex<tokio::time::Instant>,
    started_at: tokio::time::Instant,
    window: Duration,
    max_delay: Duration,
) {
    let max_deadline = started_at + max_delay;
    loop {
        let deadline = (*last_update.lock() + window).min(max_deadline);
        tokio::time::sleep_until(deadline).await;
        let now = tokio::time::Instant::now();
        if now >= max_deadline || now >= *last_update.lock() + window {
            return;
        }
    }
}

fn version(broadcast: &PushLogBroadcast) -> Option<(u64, Cid)> {
    let priority = match defra_core::Block::from_dag_cbor(&broadcast.block) {
        Ok(block) => block.delta.priority(),
        Err(error) => {
            tracing::warn!(
                cid = %String::from_utf8_lossy(&broadcast.cid),
                %error,
                "broadcast head priority decode failed; bypassing document coalescing"
            );
            return None;
        }
    };
    let cid = match Cid::try_from(broadcast.cid.as_ref()) {
        Ok(cid) => cid,
        Err(error) => {
            tracing::warn!(%error, "broadcast CID decode failed; bypassing document coalescing");
            return None;
        }
    };
    Some((priority, cid))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use bytes::Bytes;
    use multihash_codetable::{Code, MultihashDigest};

    use super::*;

    fn broadcast(seed: &[u8]) -> PushLogBroadcast {
        use defra_core::{Block, CompositeDeltaPayload, CrdtDelta};

        let block = Block::new_with_options(
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: "schema".to_string(),
                priority: seed.iter().map(|byte| u64::from(*byte)).sum(),
                status: 1,
            }),
            vec![],
            vec![],
            None,
            None,
        );
        let block = Bytes::from(block.to_dag_cbor().unwrap());
        let cid = defra_core::block::generate_cid_from_bytes(&block).unwrap();
        PushLogBroadcast::new(
            "doc".to_string(),
            Bytes::from(cid.to_bytes()),
            "collection".to_string(),
            "creator".to_string(),
            block,
        )
    }

    fn undecodable_broadcast(seed: &[u8]) -> PushLogBroadcast {
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

    #[tokio::test(start_paused = true)]
    async fn sequential_update_resets_the_quiet_window() {
        let window = Duration::from_millis(40);
        let coalescer = Arc::new(BroadcastCoalescer::with_window(window));
        let sends = Arc::new(AtomicUsize::new(0));
        let leader = {
            let coalescer = Arc::clone(&coalescer);
            let sends = Arc::clone(&sends);
            tokio::spawn(async move {
                coalescer
                    .run(broadcast(b"1"), move |_| async move {
                        sends.fetch_add(1, Ordering::Relaxed);
                        Ok(BroadcastResult::Success)
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        tokio::time::advance(window / 2).await;
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
        tokio::task::yield_now().await;

        tokio::time::advance(window * 3 / 4).await;
        assert_eq!(sends.load(Ordering::Relaxed), 0);
        leader.await.unwrap().unwrap();
        follower.await.unwrap().unwrap();
        assert_eq!(sends.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_update_does_not_reset_the_quiet_window() {
        let window = Duration::from_millis(40);
        let coalescer = Arc::new(BroadcastCoalescer::with_window(window));
        let sends = Arc::new(AtomicUsize::new(0));
        let leader = {
            let coalescer = Arc::clone(&coalescer);
            let sends = Arc::clone(&sends);
            tokio::spawn(async move {
                coalescer
                    .run(broadcast(b"newer"), move |_| async move {
                        sends.fetch_add(1, Ordering::Relaxed);
                        Ok(BroadcastResult::Success)
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        tokio::time::advance(window / 2).await;
        let follower = {
            let coalescer = Arc::clone(&coalescer);
            tokio::spawn(async move {
                coalescer
                    .run(broadcast(b"old"), |_| async { unreachable!() })
                    .await
            })
        };
        while coalescer.coalesced() == 0 {
            tokio::task::yield_now().await;
        }

        tokio::time::advance(window / 2).await;
        tokio::task::yield_now().await;
        assert_eq!(sends.load(Ordering::Relaxed), 1);
        assert_eq!(leader.await.unwrap().unwrap(), BroadcastResult::Success);
        assert_eq!(follower.await.unwrap().unwrap(), BroadcastResult::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn sustained_updates_flush_at_the_max_delay() {
        let window = Duration::from_millis(250);
        let max_delay = Duration::from_secs(1);
        let coalescer = Arc::new(BroadcastCoalescer::with_limits(window, max_delay));
        let sends = Arc::new(AtomicUsize::new(0));
        let leader = {
            let coalescer = Arc::clone(&coalescer);
            let sends = Arc::clone(&sends);
            tokio::spawn(async move {
                coalescer
                    .run(broadcast(b"1"), move |_| async move {
                        sends.fetch_add(1, Ordering::Relaxed);
                        Ok(BroadcastResult::Success)
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;

        let mut followers = Vec::new();
        for seed in [b"22".as_slice(), b"333", b"4444", b"55555"] {
            tokio::time::advance(Duration::from_millis(200)).await;
            let coalescer = Arc::clone(&coalescer);
            followers.push(tokio::spawn(async move {
                coalescer
                    .run(broadcast(seed), |_| async { unreachable!() })
                    .await
            }));
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(200)).await;

        assert_eq!(leader.await.unwrap().unwrap(), BroadcastResult::Success);
        for follower in followers {
            assert_eq!(follower.await.unwrap().unwrap(), BroadcastResult::Success);
        }
        assert_eq!(sends.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn undecodable_heads_bypass_document_coalescing() {
        let coalescer = BroadcastCoalescer::with_window(Duration::from_millis(10));
        let sends = Arc::new(AtomicUsize::new(0));
        for seed in [b"first".as_slice(), b"second"] {
            let sends = Arc::clone(&sends);
            coalescer
                .run(undecodable_broadcast(seed), move |_| async move {
                    sends.fetch_add(1, Ordering::Relaxed);
                    Ok(BroadcastResult::Success)
                })
                .await
                .unwrap();
        }
        assert_eq!(sends.load(Ordering::Relaxed), 2);
        assert_eq!(coalescer.coalesced(), 0);
    }

    #[tokio::test]
    async fn follower_re_elects_after_leader_cancellation() {
        let coalescer = Arc::new(BroadcastCoalescer::with_window(Duration::from_millis(200)));
        let leader = {
            let coalescer = Arc::clone(&coalescer);
            tokio::spawn(async move {
                coalescer
                    .run(broadcast(b"leader"), |_| async { unreachable!() })
                    .await
            })
        };
        while coalescer.pending.lock().is_empty() {
            tokio::task::yield_now().await;
        }

        let sends = Arc::new(AtomicUsize::new(0));
        let sent_cid = Arc::new(Mutex::new(None));
        let follower = {
            let coalescer = Arc::clone(&coalescer);
            let sends = Arc::clone(&sends);
            let sent_cid = Arc::clone(&sent_cid);
            tokio::spawn(async move {
                coalescer
                    .run(broadcast(b"follower"), move |latest| async move {
                        sends.fetch_add(1, Ordering::Relaxed);
                        *sent_cid.lock() = Some(latest.cid);
                        Ok(BroadcastResult::Success)
                    })
                    .await
            })
        };
        while coalescer.coalesced() == 0 {
            tokio::task::yield_now().await;
        }
        let expected_cid = coalescer
            .pending
            .lock()
            .values()
            .next()
            .unwrap()
            .broadcast
            .lock()
            .cid
            .clone();

        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), follower)
                .await
                .expect("replacement leader must complete")
                .unwrap()
                .unwrap(),
            BroadcastResult::Success
        );
        assert_eq!(sends.load(Ordering::Relaxed), 1);
        assert_eq!(*sent_cid.lock(), Some(expected_cid));
        assert!(coalescer.pending.lock().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn many_followers_receive_completion_without_a_missed_wakeup() {
        const FOLLOWERS: usize = 256;
        let coalescer = Arc::new(BroadcastCoalescer::with_window(Duration::from_millis(250)));
        let leader = {
            let coalescer = Arc::clone(&coalescer);
            tokio::spawn(async move {
                coalescer
                    .run(broadcast(b"leader"), |_| async {
                        Ok(BroadcastResult::Success)
                    })
                    .await
            })
        };
        while coalescer.pending.lock().is_empty() {
            tokio::task::yield_now().await;
        }

        let mut followers = Vec::new();
        for seed in 0..FOLLOWERS {
            let coalescer = Arc::clone(&coalescer);
            followers.push(tokio::spawn(async move {
                coalescer
                    .run(broadcast(&seed.to_le_bytes()), |_| async { unreachable!() })
                    .await
            }));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while coalescer.coalesced() < FOLLOWERS as u64 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all followers must join the leader's window");

        assert_eq!(leader.await.unwrap().unwrap(), BroadcastResult::Success);
        for follower in followers {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), follower)
                    .await
                    .expect("follower must wake")
                    .unwrap()
                    .unwrap(),
                BroadcastResult::Success
            );
        }
    }
}
