//! Bounded, per-peer-fair admission queue for outbound replicator pushes.
//!
//! Resource contract (#1099): outbound push work is admitted here as compact
//! job specs (head block + identifiers only — never expanded DAG payloads)
//! before any task exists to execute it. A fixed worker pool drains the queue,
//! so resident outbound state is bounded by the queue caps plus the worker
//! count, independent of total write arrival count:
//!
//! - `queued_items <= item_capacity`
//! - `queued_bytes <= max(byte_capacity, one job)` — a job larger than the
//!   byte cap is admitted only when the queue is empty so it cannot wedge.
//! - one peer holds at most `min(per_peer_active_cap, worker_count)` workers,
//!   and ready peers are served round-robin, so a nonresponsive peer cannot
//!   starve healthy peers.
//! - every admission has an explicit outcome; rejection is counted and the
//!   caller reports it to the persisted retry ladder. Nothing is silently
//!   dropped and no waiting task is allocated.
//!
//! Coalescing replaces a queued job only for the identical `(peer, cid)`:
//! coalescing by document would drop distinct head blocks, and non-counter
//! field blocks must reach the receiver as heads (#1043 KMS DEK trigger).
//!
//! After a failed job a peer enters an escalating cooldown (reset on success):
//! its queued jobs park and workers serve other peers, so a dead peer costs
//! one timed-out probe per cooldown window instead of a full send timeout per
//! ring rotation. This keeps `max_concurrent_push_tasks = 1` deployments live
//! for healthy peers (Amy v0.6.7 canary, defra-agent#630).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use bytes::Bytes;
use cid::Cid;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::transport::PeerId;

/// Fixed per-job accounting overhead added to the payload/identifier bytes.
const PUSH_JOB_FIXED_OVERHEAD_BYTES: usize = 128;

/// First cooldown after a failed job; doubles per consecutive failure.
pub const DEFAULT_PUSH_FAILURE_COOLDOWN_BASE: Duration = Duration::from_secs(1);

/// Cooldown escalation cap: base << PUSH_FAILURE_COOLDOWN_MAX_SHIFT.
const PUSH_FAILURE_COOLDOWN_MAX_SHIFT: u32 = 6;

/// Compact description of one outbound push to one peer.
#[derive(Debug, Clone)]
pub struct PushJobSpec {
    pub peer_id: PeerId,
    pub doc_id: String,
    pub collection_id: String,
    pub creator: String,
    pub root_cid: Cid,
    /// The head block only. Workers expand the full DAG from the blockstore
    /// when `expand_dag` is set, so queued jobs never retain DAG payloads.
    pub head_block: Bytes,
    pub expand_dag: bool,
}

impl PushJobSpec {
    pub fn resident_bytes(&self) -> usize {
        self.head_block.len()
            + self.doc_id.len()
            + self.collection_id.len()
            + self.creator.len()
            + self.peer_id.to_string().len()
            + PUSH_JOB_FIXED_OVERHEAD_BYTES
    }
}

/// Every admission resolves to exactly one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Enqueued,
    /// An identical `(peer, cid)` job was already queued; it was replaced by
    /// the newer spec instead of queueing a duplicate.
    Coalesced,
    RejectedItems,
    RejectedBytes,
    Closed,
}

impl EnqueueOutcome {
    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::RejectedItems | Self::RejectedBytes)
    }
}

#[derive(Clone, Copy)]
struct PeerCooldown {
    until: Instant,
    consecutive_failures: u32,
}

#[derive(Default)]
struct Inner {
    /// Per-peer FIFO of queued jobs. A peer key is present in `ready` iff its
    /// deque is non-empty.
    queues: HashMap<String, VecDeque<PushJobSpec>>,
    ready: VecDeque<String>,
    active: HashMap<String, usize>,
    /// Peers backing off after failed jobs. Cleared on the peer's next
    /// success; expired idle entries are pruned during scheduling.
    cooldowns: HashMap<String, PeerCooldown>,
    queued_items: usize,
    queued_bytes: usize,
    active_jobs: usize,
    closed: bool,
}

/// Live per-peer occupancy so slot starvation is visible to operators
/// (defra-agent#630: a dead peer monopolizing the slots was invisible in
/// connection-level diagnostics).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PeerBacklogSnapshot {
    pub peer_id: String,
    pub queued_items: usize,
    pub queued_bytes: usize,
    pub active_jobs: usize,
    pub consecutive_failures: u32,
    pub cooldown_remaining_ms: u64,
}

/// Point-in-time view of the backlog for diagnostics and conformance tests.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PushBacklogSnapshot {
    pub queue_item_capacity: usize,
    pub queue_byte_capacity: usize,
    pub per_peer_active_cap: usize,
    pub worker_count: usize,
    pub queued_items: usize,
    pub queued_bytes: usize,
    pub active_jobs: usize,
    pub enqueued_total: u64,
    pub coalesced_total: u64,
    pub rejected_items_total: u64,
    pub rejected_bytes_total: u64,
    pub completed_total: u64,
    pub failed_total: u64,
    pub per_peer: Vec<PeerBacklogSnapshot>,
}

/// Bounded admission queue drained by a fixed worker pool.
pub struct PushBacklog {
    inner: Mutex<Inner>,
    notify: Notify,
    item_capacity: usize,
    byte_capacity: usize,
    per_peer_active_cap: usize,
    worker_count: usize,
    failure_cooldown_base: Duration,
    enqueued_total: AtomicU64,
    coalesced_total: AtomicU64,
    rejected_items_total: AtomicU64,
    rejected_bytes_total: AtomicU64,
    completed_total: AtomicU64,
    failed_total: AtomicU64,
}

impl PushBacklog {
    pub fn new(
        item_capacity: usize,
        byte_capacity: usize,
        per_peer_active_cap: usize,
        worker_count: usize,
    ) -> Arc<Self> {
        Self::with_failure_cooldown_base(
            item_capacity,
            byte_capacity,
            per_peer_active_cap,
            worker_count,
            DEFAULT_PUSH_FAILURE_COOLDOWN_BASE,
        )
    }

    pub fn with_failure_cooldown_base(
        item_capacity: usize,
        byte_capacity: usize,
        per_peer_active_cap: usize,
        worker_count: usize,
        failure_cooldown_base: Duration,
    ) -> Arc<Self> {
        let worker_count = worker_count.max(1);
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            notify: Notify::new(),
            item_capacity: item_capacity.max(1),
            byte_capacity: byte_capacity.max(1),
            per_peer_active_cap: per_peer_active_cap.max(1).min(worker_count),
            worker_count,
            failure_cooldown_base,
            enqueued_total: AtomicU64::new(0),
            coalesced_total: AtomicU64::new(0),
            rejected_items_total: AtomicU64::new(0),
            rejected_bytes_total: AtomicU64::new(0),
            completed_total: AtomicU64::new(0),
            failed_total: AtomicU64::new(0),
        })
    }

    pub fn worker_count(&self) -> usize {
        self.worker_count
    }

    /// Admit a job. Never blocks and never allocates a task.
    pub fn try_enqueue(&self, job: PushJobSpec) -> EnqueueOutcome {
        let cost = job.resident_bytes();
        let peer_key = job.peer_id.to_string();
        let mut inner = self.inner.lock();

        if inner.closed {
            return EnqueueOutcome::Closed;
        }

        if let Some(queue) = inner.queues.get_mut(&peer_key) {
            if let Some(existing) = queue
                .iter_mut()
                .find(|queued| queued.root_cid == job.root_cid)
            {
                let old_cost = existing.resident_bytes();
                let expand_dag = existing.expand_dag || job.expand_dag;
                *existing = job;
                existing.expand_dag = expand_dag;
                let new_cost = existing.resident_bytes();
                inner.queued_bytes = inner.queued_bytes - old_cost + new_cost;
                self.coalesced_total.fetch_add(1, Ordering::Relaxed);
                return EnqueueOutcome::Coalesced;
            }
        }

        if inner.queued_items >= self.item_capacity {
            self.rejected_items_total.fetch_add(1, Ordering::Relaxed);
            return EnqueueOutcome::RejectedItems;
        }
        // One peer may hold at most a quarter of the item budget, so a dead
        // peer's parked jobs cannot squat the whole queue and starve healthy
        // peers' admissions (defra-agent#630 req 1).
        let peer_quota = (self.item_capacity / 4).max(1);
        if inner
            .queues
            .get(&peer_key)
            .is_some_and(|queue| queue.len() >= peer_quota)
        {
            self.rejected_items_total.fetch_add(1, Ordering::Relaxed);
            return EnqueueOutcome::RejectedItems;
        }
        if inner.queued_items > 0 && inner.queued_bytes + cost > self.byte_capacity {
            self.rejected_bytes_total.fetch_add(1, Ordering::Relaxed);
            return EnqueueOutcome::RejectedBytes;
        }

        let was_empty = inner
            .queues
            .get(&peer_key)
            .map(|queue| queue.is_empty())
            .unwrap_or(true);
        inner
            .queues
            .entry(peer_key.clone())
            .or_default()
            .push_back(job);
        if was_empty {
            inner.ready.push_back(peer_key);
        }
        inner.queued_items += 1;
        inner.queued_bytes += cost;
        self.enqueued_total.fetch_add(1, Ordering::Relaxed);
        drop(inner);
        self.notify.notify_waiters();
        EnqueueOutcome::Enqueued
    }

    /// Next job whose peer is below its active cap and not cooling down,
    /// round-robin across ready peers. Parks until one is eligible (waking at
    /// the earliest cooldown expiry); `None` once the backlog is closed.
    pub async fn next_job(&self) -> Option<PushJobSpec> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Register the waiter before re-checking so a notify_waiters that
            // races the check below is not lost.
            notified.as_mut().enable();
            let next_wake = {
                let mut inner = self.inner.lock();
                if inner.closed {
                    return None;
                }
                match Self::pop_eligible(&mut inner, self.per_peer_active_cap, Instant::now()) {
                    Ok(job) => return Some(job),
                    Err(next_wake) => next_wake,
                }
            };
            match next_wake {
                Some(wake_at) => {
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep_until(wake_at) => {}
                    }
                }
                None => notified.await,
            }
        }
    }

    /// Pop the next eligible job, or return the earliest cooldown expiry among
    /// peers that were skipped only because they are cooling down.
    fn pop_eligible(
        inner: &mut Inner,
        per_peer_active_cap: usize,
        now: Instant,
    ) -> std::result::Result<PushJobSpec, Option<Instant>> {
        let mut next_wake: Option<Instant> = None;
        for _ in 0..inner.ready.len() {
            let Some(peer_key) = inner.ready.pop_front() else {
                break;
            };
            let at_cap = inner
                .active
                .get(&peer_key)
                .is_some_and(|count| *count >= per_peer_active_cap);
            if at_cap {
                inner.ready.push_back(peer_key);
                continue;
            }
            if let Some(cooldown) = inner.cooldowns.get(&peer_key) {
                if cooldown.until > now {
                    next_wake = Some(match next_wake {
                        Some(wake_at) => wake_at.min(cooldown.until),
                        None => cooldown.until,
                    });
                    inner.ready.push_back(peer_key);
                    continue;
                }
            }

            let queue = inner
                .queues
                .get_mut(&peer_key)
                .expect("ready peer has a queue");
            let job = queue.pop_front().expect("ready peer queue is non-empty");
            if queue.is_empty() {
                inner.queues.remove(&peer_key);
            } else {
                inner.ready.push_back(peer_key.clone());
            }
            inner.queued_items -= 1;
            inner.queued_bytes -= job.resident_bytes();
            *inner.active.entry(peer_key).or_insert(0) += 1;
            inner.active_jobs += 1;
            return Ok(job);
        }
        Err(next_wake)
    }

    /// Release the peer slot taken by `next_job`. A failure starts (or
    /// escalates) the peer's cooldown; a success clears it.
    pub fn job_done(&self, peer_id: &PeerId, succeeded: bool) {
        let peer_key = peer_id.to_string();
        {
            let mut inner = self.inner.lock();
            if let Some(count) = inner.active.get_mut(&peer_key) {
                *count -= 1;
                if *count == 0 {
                    inner.active.remove(&peer_key);
                }
            }
            inner.active_jobs = inner.active_jobs.saturating_sub(1);
            if succeeded {
                inner.cooldowns.remove(&peer_key);
            } else {
                let consecutive_failures = inner
                    .cooldowns
                    .get(&peer_key)
                    .map(|cooldown| cooldown.consecutive_failures)
                    .unwrap_or(0)
                    .saturating_add(1);
                let shift = (consecutive_failures - 1).min(PUSH_FAILURE_COOLDOWN_MAX_SHIFT);
                let cooldown = self.failure_cooldown_base.saturating_mul(1 << shift);
                inner.cooldowns.insert(
                    peer_key,
                    PeerCooldown {
                        until: Instant::now() + cooldown,
                        consecutive_failures,
                    },
                );
            }
            // Prune idle expired cooldowns so departed peers do not linger.
            let now = Instant::now();
            let inner = &mut *inner;
            let queues = &inner.queues;
            let active = &inner.active;
            inner.cooldowns.retain(|peer, cooldown| {
                cooldown.until > now || queues.contains_key(peer) || active.contains_key(peer)
            });
        }
        if succeeded {
            self.completed_total.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_total.fetch_add(1, Ordering::Relaxed);
        }
        self.notify.notify_waiters();
    }

    /// Stop admission and wake parked workers. Queued jobs are discarded:
    /// close is a shutdown-path operation and draining could take minutes of
    /// network sends, while the durable retry ladder already covers loss.
    pub fn close(&self) {
        {
            let mut inner = self.inner.lock();
            inner.closed = true;
            inner.queues.clear();
            inner.ready.clear();
            inner.queued_items = 0;
            inner.queued_bytes = 0;
        }
        self.notify.notify_waiters();
    }

    pub fn snapshot(&self) -> PushBacklogSnapshot {
        let inner = self.inner.lock();
        let now = Instant::now();
        let mut peers: std::collections::HashSet<&String> = inner.queues.keys().collect();
        peers.extend(inner.active.keys());
        peers.extend(inner.cooldowns.keys());
        let mut per_peer: Vec<PeerBacklogSnapshot> = peers
            .into_iter()
            .map(|peer| {
                let queued = inner.queues.get(peer);
                let cooldown = inner.cooldowns.get(peer);
                PeerBacklogSnapshot {
                    peer_id: peer.clone(),
                    queued_items: queued.map(|jobs| jobs.len()).unwrap_or(0),
                    queued_bytes: queued
                        .map(|jobs| jobs.iter().map(PushJobSpec::resident_bytes).sum())
                        .unwrap_or(0),
                    active_jobs: inner.active.get(peer).copied().unwrap_or(0),
                    consecutive_failures: cooldown
                        .map(|cooldown| cooldown.consecutive_failures)
                        .unwrap_or(0),
                    cooldown_remaining_ms: cooldown
                        .map(|cooldown| {
                            cooldown.until.saturating_duration_since(now).as_millis() as u64
                        })
                        .unwrap_or(0),
                }
            })
            .collect();
        per_peer.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        PushBacklogSnapshot {
            queue_item_capacity: self.item_capacity,
            queue_byte_capacity: self.byte_capacity,
            per_peer_active_cap: self.per_peer_active_cap,
            worker_count: self.worker_count,
            queued_items: inner.queued_items,
            queued_bytes: inner.queued_bytes,
            active_jobs: inner.active_jobs,
            enqueued_total: self.enqueued_total.load(Ordering::Relaxed),
            coalesced_total: self.coalesced_total.load(Ordering::Relaxed),
            rejected_items_total: self.rejected_items_total.load(Ordering::Relaxed),
            rejected_bytes_total: self.rejected_bytes_total.load(Ordering::Relaxed),
            completed_total: self.completed_total.load(Ordering::Relaxed),
            failed_total: self.failed_total.load(Ordering::Relaxed),
            per_peer,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use multihash_codetable::{Code, MultihashDigest};

    use super::*;

    fn job(peer: &str, cid_seed: &[u8]) -> PushJobSpec {
        PushJobSpec {
            peer_id: PeerId::new(peer.to_string()),
            doc_id: "doc".to_string(),
            collection_id: "collection".to_string(),
            creator: "creator".to_string(),
            root_cid: Cid::new_v1(0x55, Code::Sha2_256.digest(cid_seed)),
            head_block: Bytes::from_static(b"head-block"),
            expand_dag: false,
        }
    }

    #[test]
    fn enqueue_respects_item_capacity() {
        let backlog = PushBacklog::new(2, usize::MAX, 4, 4);
        assert_eq!(
            backlog.try_enqueue(job("a", b"1")),
            EnqueueOutcome::Enqueued
        );
        assert_eq!(
            backlog.try_enqueue(job("b", b"2")),
            EnqueueOutcome::Enqueued
        );
        assert_eq!(
            backlog.try_enqueue(job("c", b"3")),
            EnqueueOutcome::RejectedItems
        );

        let snap = backlog.snapshot();
        assert_eq!(snap.queued_items, 2);
        assert_eq!(snap.enqueued_total, 2);
        assert_eq!(snap.rejected_items_total, 1);
    }

    #[test]
    fn enqueue_respects_byte_capacity() {
        let cost = job("a", b"1").resident_bytes();
        let backlog = PushBacklog::new(1024, cost + cost / 2, 4, 4);
        assert_eq!(
            backlog.try_enqueue(job("a", b"1")),
            EnqueueOutcome::Enqueued
        );
        assert_eq!(
            backlog.try_enqueue(job("a", b"2")),
            EnqueueOutcome::RejectedBytes
        );

        let snap = backlog.snapshot();
        assert_eq!(snap.queued_items, 1);
        assert_eq!(snap.rejected_bytes_total, 1);
        assert!(snap.queued_bytes <= snap.queue_byte_capacity);
    }

    #[test]
    fn oversized_job_admitted_only_when_queue_is_empty() {
        let backlog = PushBacklog::new(1024, 1, 4, 4);
        assert_eq!(
            backlog.try_enqueue(job("a", b"1")),
            EnqueueOutcome::Enqueued
        );
        assert_eq!(
            backlog.try_enqueue(job("a", b"2")),
            EnqueueOutcome::RejectedBytes
        );
    }

    #[test]
    fn coalesce_replaces_same_peer_cid_only() {
        let backlog = PushBacklog::new(1024, usize::MAX, 4, 4);
        assert_eq!(
            backlog.try_enqueue(job("a", b"1")),
            EnqueueOutcome::Enqueued
        );

        let mut duplicate = job("a", b"1");
        duplicate.expand_dag = true;
        assert_eq!(backlog.try_enqueue(duplicate), EnqueueOutcome::Coalesced);
        // Same doc, different cid must NOT coalesce (#1043).
        assert_eq!(
            backlog.try_enqueue(job("a", b"2")),
            EnqueueOutcome::Enqueued
        );
        // Same cid, different peer must NOT coalesce.
        assert_eq!(
            backlog.try_enqueue(job("b", b"1")),
            EnqueueOutcome::Enqueued
        );

        let snap = backlog.snapshot();
        assert_eq!(snap.queued_items, 3);
        assert_eq!(snap.coalesced_total, 1);
    }

    #[tokio::test]
    async fn coalesced_job_keeps_expand_dag_obligation() {
        let backlog = PushBacklog::new(1024, usize::MAX, 4, 4);
        let mut full_dag = job("a", b"1");
        full_dag.expand_dag = true;
        backlog.try_enqueue(full_dag);
        backlog.try_enqueue(job("a", b"1"));

        let popped = backlog.next_job().await.expect("job queued");
        assert!(popped.expand_dag, "coalescing must not shrink the job");
    }

    #[tokio::test]
    async fn next_job_round_robins_across_peers() {
        let backlog = PushBacklog::new(1024, usize::MAX, 4, 4);
        backlog.try_enqueue(job("a", b"1"));
        backlog.try_enqueue(job("a", b"2"));
        backlog.try_enqueue(job("b", b"3"));

        let first = backlog.next_job().await.unwrap();
        let second = backlog.next_job().await.unwrap();
        let third = backlog.next_job().await.unwrap();
        assert_eq!(first.peer_id.to_string(), "a");
        assert_eq!(second.peer_id.to_string(), "b");
        assert_eq!(third.peer_id.to_string(), "a");
    }

    #[tokio::test]
    async fn per_peer_active_cap_holds_back_saturated_peer() {
        let backlog = PushBacklog::new(1024, usize::MAX, 1, 4);
        backlog.try_enqueue(job("slow", b"1"));
        backlog.try_enqueue(job("slow", b"2"));
        backlog.try_enqueue(job("healthy", b"3"));

        let slow_job = backlog.next_job().await.unwrap();
        assert_eq!(slow_job.peer_id.to_string(), "slow");

        // "slow" is at its cap: the next eligible job is the healthy peer's.
        let healthy_job = backlog.next_job().await.unwrap();
        assert_eq!(healthy_job.peer_id.to_string(), "healthy");

        // Nothing else is eligible until a slow slot frees.
        let parked = tokio::time::timeout(Duration::from_millis(50), backlog.next_job()).await;
        assert!(parked.is_err(), "slow peer above cap must not be served");

        backlog.job_done(&slow_job.peer_id, true);
        let released = tokio::time::timeout(Duration::from_millis(200), backlog.next_job())
            .await
            .expect("released slot must unblock the queued job")
            .unwrap();
        assert_eq!(released.peer_id.to_string(), "slow");
    }

    #[tokio::test]
    async fn close_wakes_workers_and_rejects_enqueue() {
        let backlog = PushBacklog::new(1024, usize::MAX, 4, 4);
        let waiter = {
            let backlog = Arc::clone(&backlog);
            tokio::spawn(async move { backlog.next_job().await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        backlog.close();

        let parked_result = tokio::time::timeout(Duration::from_millis(200), waiter)
            .await
            .expect("close must wake parked workers")
            .unwrap();
        assert!(parked_result.is_none());
        assert_eq!(backlog.try_enqueue(job("a", b"1")), EnqueueOutcome::Closed);
        assert_eq!(backlog.snapshot().queued_items, 0);
    }

    #[tokio::test]
    async fn snapshot_tracks_active_and_completion_counters() {
        let backlog = PushBacklog::new(1024, usize::MAX, 4, 4);
        backlog.try_enqueue(job("a", b"1"));
        backlog.try_enqueue(job("b", b"2"));

        let first = backlog.next_job().await.unwrap();
        assert_eq!(backlog.snapshot().active_jobs, 1);
        backlog.job_done(&first.peer_id, true);

        let second = backlog.next_job().await.unwrap();
        backlog.job_done(&second.peer_id, false);

        let snap = backlog.snapshot();
        assert_eq!(snap.active_jobs, 0);
        assert_eq!(snap.completed_total, 1);
        assert_eq!(snap.failed_total, 1);
        assert_eq!(snap.queued_items, 0);
        assert_eq!(snap.queued_bytes, 0);
    }

    /// Amy canary req 1 (defra-agent#630): one peer's backlog must not squat
    /// the whole global item budget.
    #[test]
    fn one_peer_cannot_fill_the_whole_queue() {
        let backlog = PushBacklog::new(8, usize::MAX, 4, 4);
        let mut dead_enqueued = 0;
        for index in 0..8u8 {
            if backlog.try_enqueue(job("dead", &[index])) == EnqueueOutcome::Enqueued {
                dead_enqueued += 1;
            }
        }
        assert_eq!(dead_enqueued, 2, "peer quota is a quarter of the item cap");
        assert_eq!(
            backlog.try_enqueue(job("healthy", b"h1")),
            EnqueueOutcome::Enqueued,
            "healthy peer must still be admitted"
        );
    }

    /// Amy canary req 3 (defra-agent#630): a failing peer must back off so
    /// its jobs stop re-occupying workers every rotation, while healthy peers
    /// keep flowing during the cooldown.
    #[tokio::test]
    async fn failed_peer_cools_down_while_healthy_peers_flow() {
        let backlog = PushBacklog::with_failure_cooldown_base(
            1024,
            usize::MAX,
            4,
            4,
            Duration::from_millis(80),
        );
        backlog.try_enqueue(job("dead", b"d1"));
        let dead_job = backlog.next_job().await.unwrap();
        backlog.job_done(&dead_job.peer_id, false);

        backlog.try_enqueue(job("dead", b"d2"));
        backlog.try_enqueue(job("healthy", b"h1"));

        // The healthy peer's job is served immediately even though the dead
        // peer's job was queued first.
        let first = backlog.next_job().await.unwrap();
        assert_eq!(first.peer_id.to_string(), "healthy");

        // The dead peer's job is withheld until its cooldown expires...
        let parked = tokio::time::timeout(Duration::from_millis(20), backlog.next_job()).await;
        assert!(parked.is_err(), "cooling peer must not be served");

        // ...then released without any further enqueue/notify.
        let released = tokio::time::timeout(Duration::from_millis(400), backlog.next_job())
            .await
            .expect("cooldown expiry must wake a parked worker")
            .unwrap();
        assert_eq!(released.peer_id.to_string(), "dead");
    }

    #[tokio::test]
    async fn cooldown_escalates_on_consecutive_failures_and_resets_on_success() {
        let backlog = PushBacklog::with_failure_cooldown_base(
            1024,
            usize::MAX,
            4,
            4,
            Duration::from_millis(10),
        );
        let peer = PeerId::new("flaky".to_string());
        backlog.try_enqueue(job("flaky", b"1"));
        let popped = backlog.next_job().await.unwrap();
        backlog.job_done(&popped.peer_id, false);
        backlog.job_done(&peer, false);

        let snap = backlog.snapshot();
        let entry = snap
            .per_peer
            .iter()
            .find(|entry| entry.peer_id == "flaky")
            .expect("cooling peer appears in per-peer snapshot");
        assert_eq!(entry.consecutive_failures, 2);
        assert!(entry.cooldown_remaining_ms > 0);

        backlog.job_done(&peer, true);
        let snap = backlog.snapshot();
        assert!(
            !snap.per_peer.iter().any(|entry| entry.peer_id == "flaky"),
            "success must clear the cooldown"
        );
    }

    #[tokio::test]
    async fn snapshot_reports_per_peer_backlog_occupancy() {
        let backlog = PushBacklog::new(1024, usize::MAX, 4, 4);
        backlog.try_enqueue(job("a", b"1"));
        backlog.try_enqueue(job("a", b"2"));
        backlog.try_enqueue(job("b", b"3"));
        let active = backlog.next_job().await.unwrap();

        let snap = backlog.snapshot();
        let a = snap
            .per_peer
            .iter()
            .find(|entry| entry.peer_id == "a")
            .expect("peer a present");
        assert_eq!(a.queued_items + a.active_jobs, 2);
        assert!(a.queued_bytes > 0);
        let b = snap
            .per_peer
            .iter()
            .find(|entry| entry.peer_id == "b")
            .expect("peer b present");
        assert_eq!(b.queued_items, 1);
        backlog.job_done(&active.peer_id, true);
    }

    #[test]
    fn caps_are_normalized_to_sane_minimums() {
        let backlog = PushBacklog::new(0, 0, 0, 0);
        let snap = backlog.snapshot();
        assert_eq!(snap.queue_item_capacity, 1);
        assert_eq!(snap.queue_byte_capacity, 1);
        assert_eq!(snap.per_peer_active_cap, 1);
        assert_eq!(snap.worker_count, 1);
    }
}
