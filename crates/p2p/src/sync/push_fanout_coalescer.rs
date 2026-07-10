//! Short-window coalescing before replicator peer fan-out (#1102).

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cid::Cid;
use parking_lot::Mutex;
use serde_json::Value as JsonValue;
use tokio::sync::Notify;

use super::broadcast_coalescer::{
    DEFAULT_BROADCAST_COALESCING_WINDOW, DEFAULT_BROADCAST_MAX_COALESCING_DELAY,
};

#[derive(Debug, Clone)]
pub(crate) struct PendingPush {
    pub(crate) cid: Cid,
    pub(crate) block: Bytes,
    pub(crate) doc_id: String,
    pub(crate) collection_id: String,
    pub(crate) creator: String,
    /// Whether unfiltered replicators need the complete DAG instead of only
    /// the head block.
    pub(crate) expand_unfiltered_dag: bool,
    /// Filter material also represents an obligation to send the complete DAG
    /// to matching filtered replicators.
    pub(crate) document: Option<JsonValue>,
}

impl PendingPush {
    fn version(&self) -> Option<(u64, Cid)> {
        let priority = match defra_core::Block::from_dag_cbor(&self.block) {
            Ok(block) => block.delta.priority(),
            Err(error) => {
                tracing::warn!(
                    cid = %self.cid,
                    %error,
                    "push fanout head priority decode failed; bypassing document coalescing"
                );
                return None;
            }
        };
        Some((priority, self.cid))
    }

    fn merge_same_version(&mut self, incoming: PendingPush) {
        self.expand_unfiltered_dag |= incoming.expand_unfiltered_dag;
        if incoming.document.is_some() {
            self.document = incoming.document;
        }
    }
}

struct Window {
    push: Mutex<PendingPush>,
    started_at: tokio::time::Instant,
    last_update: Mutex<tokio::time::Instant>,
    done: Mutex<bool>,
    cancelled: AtomicBool,
    notify: Notify,
}

pub(crate) struct PushFanoutCoalescer {
    pending: Mutex<HashMap<(String, String), Arc<Window>>>,
    coalesced: AtomicU64,
    window: Duration,
    max_delay: Duration,
}

struct FanoutLeaderGuard<'a> {
    coalescer: &'a PushFanoutCoalescer,
    key: (String, String),
    window: Arc<Window>,
    armed: bool,
}

impl FanoutLeaderGuard<'_> {
    fn complete(&mut self) {
        self.armed = false;
    }
}

impl Drop for FanoutLeaderGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut pending = self.coalescer.pending.lock();
        if pending
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.window))
        {
            pending.remove(&self.key);
        }
        drop(pending);
        self.window.cancelled.store(true, Ordering::Release);
        self.window.notify.notify_waiters();
    }
}

impl Default for PushFanoutCoalescer {
    fn default() -> Self {
        Self::with_limits(
            DEFAULT_BROADCAST_COALESCING_WINDOW,
            DEFAULT_BROADCAST_MAX_COALESCING_DELAY,
        )
    }
}

impl PushFanoutCoalescer {
    #[cfg(test)]
    fn with_window(window: Duration) -> Self {
        Self::with_limits(window, window * 4)
    }

    fn with_limits(window: Duration, max_delay: Duration) -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            coalesced: AtomicU64::new(0),
            window,
            max_delay,
        }
    }

    pub(crate) fn coalesced(&self) -> u64 {
        self.coalesced.load(Ordering::Relaxed)
    }

    /// Cancellation safe: if the leader is dropped, its guard removes the
    /// dead window and wakes followers to re-admit the latest buffered push.
    pub(crate) async fn run<F, Fut>(&self, push: PendingPush, send: F)
    where
        F: FnOnce(PendingPush) -> Fut,
        Fut: Future<Output = ()>,
    {
        let mut candidate = push;
        let mut send = Some(send);
        loop {
            if candidate.doc_id.is_empty() {
                send.take().expect("send closure available")(candidate).await;
                return;
            }
            let Some(push_version) = candidate.version() else {
                send.take().expect("send closure available")(candidate).await;
                return;
            };
            let key = (candidate.collection_id.clone(), candidate.doc_id.clone());
            let (window, leader) = {
                let mut pending = self.pending.lock();
                if let Some(window) = pending.get(&key) {
                    let mut current = window.push.lock();
                    match push_version.cmp(&current.version().expect("pending pushes decode")) {
                        std::cmp::Ordering::Greater => *current = candidate,
                        std::cmp::Ordering::Equal => current.merge_same_version(candidate),
                        std::cmp::Ordering::Less => {}
                    }
                    *window.last_update.lock() = tokio::time::Instant::now();
                    self.coalesced.fetch_add(1, Ordering::Relaxed);
                    (Arc::clone(window), false)
                } else {
                    let now = tokio::time::Instant::now();
                    let window = Arc::new(Window {
                        push: Mutex::new(candidate),
                        started_at: now,
                        last_update: Mutex::new(now),
                        done: Mutex::new(false),
                        cancelled: AtomicBool::new(false),
                        notify: Notify::new(),
                    });
                    pending.insert(key.clone(), Arc::clone(&window));
                    (window, true)
                }
            };

            if leader {
                let mut guard = FanoutLeaderGuard {
                    coalescer: self,
                    key: key.clone(),
                    window: Arc::clone(&window),
                    armed: true,
                };
                super::broadcast_coalescer::wait_for_quiet(
                    &window.last_update,
                    window.started_at,
                    self.window,
                    self.max_delay,
                )
                .await;
                {
                    let mut pending = self.pending.lock();
                    if pending
                        .get(&key)
                        .is_some_and(|current| Arc::ptr_eq(current, &window))
                    {
                        pending.remove(&key);
                    }
                }
                let latest = window.push.lock().clone();
                send.take().expect("send closure available")(latest).await;
                *window.done.lock() = true;
                window.notify.notify_waiters();
                guard.complete();
                return;
            }

            loop {
                let notified = window.notify.notified();
                tokio::pin!(notified);
                // Register before checking completion so the leader's single
                // notify_waiters call cannot race this follower into sleeping.
                notified.as_mut().enable();
                if *window.done.lock() {
                    return;
                }
                if window.cancelled.load(Ordering::Acquire) {
                    candidate = window.push.lock().clone();
                    break;
                }
                notified.await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use multihash_codetable::{Code, MultihashDigest};

    use super::*;

    fn push(seed: &[u8]) -> PendingPush {
        use defra_core::{Block, CompositeDeltaPayload, CrdtDelta};

        let block = Block::new_with_options(
            CrdtDelta::Composite(CompositeDeltaPayload {
                doc_id: b"doc".to_vec(),
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
        PendingPush {
            cid: defra_core::block::generate_cid_from_bytes(&block).unwrap(),
            block,
            doc_id: "doc".to_string(),
            collection_id: "collection".to_string(),
            creator: "creator".to_string(),
            expand_unfiltered_dag: false,
            document: Some(JsonValue::Null),
        }
    }

    fn undecodable_push(seed: &[u8]) -> PendingPush {
        PendingPush {
            cid: Cid::new_v1(0x55, Code::Sha2_256.digest(seed)),
            block: Bytes::copy_from_slice(seed),
            doc_id: "doc".to_string(),
            collection_id: "collection".to_string(),
            creator: "creator".to_string(),
            expand_unfiltered_dag: false,
            document: Some(JsonValue::Null),
        }
    }

    #[test]
    fn same_version_merges_unfiltered_and_filtered_dag_obligations() {
        let mut combined = push(b"same");
        combined.document = None;
        combined.expand_unfiltered_dag = true;

        combined.merge_same_version(push(b"same"));

        assert!(combined.expand_unfiltered_dag);
        assert!(combined.document.is_some());
    }

    #[tokio::test]
    async fn rapid_updates_create_one_peer_fanout() {
        let coalescer = Arc::new(PushFanoutCoalescer::with_window(Duration::from_millis(10)));
        let sends = Arc::new(AtomicUsize::new(0));
        let sent_cid = Arc::new(Mutex::new(None));
        let updates: Vec<_> = [b"1".as_slice(), b"2".as_slice(), b"3".as_slice()]
            .into_iter()
            .map(push)
            .collect();
        let expected_cid = updates
            .iter()
            .max_by_key(|update| update.version())
            .unwrap()
            .cid;
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
                    })
                    .await;
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(sends.load(Ordering::Relaxed), 1);
        assert_eq!(*sent_cid.lock(), Some(expected_cid));
        assert_eq!(coalescer.coalesced(), 2);
    }

    #[tokio::test]
    async fn sequential_update_resets_the_quiet_window() {
        let window = Duration::from_millis(40);
        let coalescer = Arc::new(PushFanoutCoalescer::with_window(window));
        let sends = Arc::new(AtomicUsize::new(0));
        let leader = {
            let coalescer = Arc::clone(&coalescer);
            let sends = Arc::clone(&sends);
            tokio::spawn(async move {
                coalescer
                    .run(push(b"first"), move |_| async move {
                        sends.fetch_add(1, Ordering::Relaxed);
                    })
                    .await;
            })
        };
        tokio::time::sleep(window / 2).await;
        let follower = {
            let coalescer = Arc::clone(&coalescer);
            let sends = Arc::clone(&sends);
            tokio::spawn(async move {
                coalescer
                    .run(push(b"second"), move |_| async move {
                        sends.fetch_add(1, Ordering::Relaxed);
                    })
                    .await;
            })
        };

        tokio::time::sleep(window * 3 / 4).await;
        assert_eq!(sends.load(Ordering::Relaxed), 0);
        leader.await.unwrap();
        follower.await.unwrap();
        assert_eq!(sends.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn sustained_updates_flush_at_the_max_delay() {
        let window = Duration::from_millis(250);
        let max_delay = Duration::from_secs(1);
        let coalescer = Arc::new(PushFanoutCoalescer::with_limits(window, max_delay));
        let sends = Arc::new(AtomicUsize::new(0));
        let leader = {
            let coalescer = Arc::clone(&coalescer);
            let sends = Arc::clone(&sends);
            tokio::spawn(async move {
                coalescer
                    .run(push(b"first"), move |_| async move {
                        sends.fetch_add(1, Ordering::Relaxed);
                    })
                    .await;
            })
        };
        tokio::task::yield_now().await;

        let mut followers = Vec::new();
        for seed in [b"2".as_slice(), b"3", b"4", b"5"] {
            tokio::time::advance(Duration::from_millis(200)).await;
            let coalescer = Arc::clone(&coalescer);
            followers.push(tokio::spawn(async move {
                coalescer
                    .run(push(seed), |_| async { unreachable!() })
                    .await;
            }));
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(200)).await;

        leader.await.unwrap();
        for follower in followers {
            follower.await.unwrap();
        }
        assert_eq!(sends.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn undecodable_heads_bypass_document_coalescing() {
        let coalescer = PushFanoutCoalescer::with_window(Duration::from_millis(10));
        let sends = Arc::new(AtomicUsize::new(0));
        for seed in [b"first".as_slice(), b"second"] {
            let sends = Arc::clone(&sends);
            coalescer
                .run(undecodable_push(seed), move |_| async move {
                    sends.fetch_add(1, Ordering::Relaxed);
                })
                .await;
        }
        assert_eq!(sends.load(Ordering::Relaxed), 2);
        assert_eq!(coalescer.coalesced(), 0);
    }

    #[tokio::test]
    async fn follower_re_elects_after_leader_cancellation() {
        let coalescer = Arc::new(PushFanoutCoalescer::with_window(Duration::from_millis(200)));
        let leader = {
            let coalescer = Arc::clone(&coalescer);
            tokio::spawn(async move {
                coalescer
                    .run(push(b"leader"), |_| async { unreachable!() })
                    .await;
            })
        };
        while coalescer.pending.lock().is_empty() {
            tokio::task::yield_now().await;
        }

        let sends = Arc::new(AtomicUsize::new(0));
        let follower = {
            let coalescer = Arc::clone(&coalescer);
            let sends = Arc::clone(&sends);
            tokio::spawn(async move {
                coalescer
                    .run(push(b"follower"), move |_| async move {
                        sends.fetch_add(1, Ordering::Relaxed);
                    })
                    .await;
            })
        };
        while coalescer.coalesced() == 0 {
            tokio::task::yield_now().await;
        }

        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), follower)
            .await
            .expect("replacement leader must complete")
            .unwrap();
        assert_eq!(sends.load(Ordering::Relaxed), 1);
        assert!(coalescer.pending.lock().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn many_followers_receive_completion_without_a_missed_wakeup() {
        const FOLLOWERS: usize = 256;
        let coalescer = Arc::new(PushFanoutCoalescer::with_window(Duration::from_millis(250)));
        let leader = {
            let coalescer = Arc::clone(&coalescer);
            tokio::spawn(async move {
                coalescer.run(push(b"leader"), |_| async {}).await;
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
                    .run(push(&seed.to_le_bytes()), |_| async { unreachable!() })
                    .await;
            }));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while coalescer.coalesced() < FOLLOWERS as u64 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all followers must join the leader's window");

        leader.await.unwrap();
        for follower in followers {
            tokio::time::timeout(Duration::from_secs(1), follower)
                .await
                .expect("follower must wake")
                .unwrap();
        }
    }
}
