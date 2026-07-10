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
//! Coalescing retains only the greatest `(priority, cid)` version for each
//! `(document, peer)`. Superseded active work may finish, but workers re-check
//! the version before encoding and before durable failure handoff, so it can
//! never recreate a stale retry obligation (#1102).
//!
//! Failed jobs enter an escalating `(peer, cid)` cooldown (reset on success or
//! retirement). Other CIDs for that peer remain eligible, avoiding the fixed
//! timeout cadence without turning one bad head into peer-wide head-of-line
//! blocking.

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
    pub(crate) encoded_payload: Option<Arc<super::push_encode_cache::PushPayload>>,
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

    fn key(&self) -> JobKey {
        let doc_id = if self.doc_id.is_empty() {
            format!("cid:{}", self.root_cid)
        } else {
            self.doc_id.clone()
        };
        JobKey {
            peer_id: self.peer_id.to_string(),
            collection_id: self.collection_id.clone(),
            doc_id,
        }
    }

    fn version(&self) -> HeadVersion {
        let priority = defra_core::Block::from_dag_cbor(&self.head_block)
            .map(|block| block.delta.priority())
            .unwrap_or(0);
        HeadVersion {
            priority,
            cid: self.root_cid,
        }
    }

    pub(crate) fn head_priority(&self) -> u64 {
        self.version().priority
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct JobKey {
    peer_id: String,
    collection_id: String,
    doc_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct HeadVersion {
    priority: u64,
    cid: Cid,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RetryKey {
    peer_id: String,
    cid: Cid,
}

/// Every admission resolves to exactly one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Enqueued,
    /// The same `(document, peer, version)` is already queued or active.
    /// Queued specs merge the full-DAG obligation without adding an item.
    Coalesced,
    /// The arriving head was older than the current `(document, peer)` head.
    RetiredStale,
    RejectedItems,
    RejectedBytes,
    Closed,
}

impl EnqueueOutcome {
    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::RejectedItems | Self::RejectedBytes)
    }
}

#[derive(Clone)]
struct RetryState {
    until: Instant,
    retry_count: u32,
    job_key: JobKey,
    version: HeadVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobCompletion {
    Succeeded,
    Failed,
    Retired,
}

#[derive(Default)]
struct Inner {
    /// Per-peer FIFO of queued jobs. A peer key is present in `ready` iff its
    /// deque is non-empty.
    queues: HashMap<String, VecDeque<PushJobSpec>>,
    ready: VecDeque<String>,
    active: HashMap<String, usize>,
    /// Greatest live version per `(document, peer)`, across queued and active
    /// work. Absence makes an active job stale.
    latest: HashMap<JobKey, HeadVersion>,
    /// Exact `(peer, cid)` retry state. A failed CID never parks unrelated
    /// work for the same peer.
    retries: HashMap<RetryKey, RetryState>,
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
    pub stale_head_retirements_total: u64,
    pub per_cid_retry_counts: Vec<CidRetrySnapshot>,
    pub per_peer: Vec<PeerBacklogSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CidRetrySnapshot {
    /// Root CID whose exact peer retry state is active.
    pub cid: String,
    /// Sum of per-peer retry counts currently retained for this CID.
    pub retry_count: u64,
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
    stale_head_retirements_total: AtomicU64,
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
            stale_head_retirements_total: AtomicU64::new(0),
        })
    }

    pub fn worker_count(&self) -> usize {
        self.worker_count
    }

    /// Admit a job. Never blocks and never allocates a task.
    pub fn try_enqueue(&self, job: PushJobSpec) -> EnqueueOutcome {
        let cost = job.resident_bytes();
        let peer_key = job.peer_id.to_string();
        let job_key = job.key();
        let version = job.version();
        let mut inner = self.inner.lock();

        if inner.closed {
            return EnqueueOutcome::Closed;
        }
        Self::prune_expired_retries(&mut inner, Instant::now());

        let persisted_version = inner
            .retries
            .values()
            .filter(|retry| retry.job_key == job_key)
            .map(|retry| retry.version)
            .max();
        if persisted_version.is_some_and(|persisted| persisted > version) {
            self.stale_head_retirements_total
                .fetch_add(1, Ordering::Relaxed);
            return EnqueueOutcome::RetiredStale;
        }
        let retired_retry_count = inner.retries.len();
        inner
            .retries
            .retain(|_, retry| retry.job_key != job_key || retry.version >= version);
        let retired_retry_count = retired_retry_count - inner.retries.len();
        if retired_retry_count > 0 {
            self.stale_head_retirements_total
                .fetch_add(retired_retry_count as u64, Ordering::Relaxed);
        }

        if let Some(current) = inner.latest.get(&job_key).copied() {
            match version.cmp(&current) {
                std::cmp::Ordering::Less => {
                    self.stale_head_retirements_total
                        .fetch_add(1, Ordering::Relaxed);
                    return EnqueueOutcome::RetiredStale;
                }
                std::cmp::Ordering::Equal => {
                    if let Some(existing) = inner
                        .queues
                        .get_mut(&peer_key)
                        .and_then(|queue| queue.iter_mut().find(|queued| queued.key() == job_key))
                    {
                        let old_cost = existing.resident_bytes();
                        let expand_dag = existing.expand_dag || job.expand_dag;
                        *existing = job;
                        existing.expand_dag = expand_dag;
                        let new_cost = existing.resident_bytes();
                        inner.queued_bytes = inner.queued_bytes - old_cost + new_cost;
                    }
                    self.coalesced_total.fetch_add(1, Ordering::Relaxed);
                    return EnqueueOutcome::Coalesced;
                }
                std::cmp::Ordering::Greater => {
                    Self::remove_queued_job(&mut inner, &peer_key, &job_key);
                    inner.latest.remove(&job_key);
                    inner.retries.remove(&RetryKey {
                        peer_id: peer_key.clone(),
                        cid: current.cid,
                    });
                    self.stale_head_retirements_total
                        .fetch_add(1, Ordering::Relaxed);
                }
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
        inner.latest.insert(job_key, version);
        self.enqueued_total.fetch_add(1, Ordering::Relaxed);
        drop(inner);
        self.notify.notify_waiters();
        EnqueueOutcome::Enqueued
    }

    fn remove_queued_job(inner: &mut Inner, peer_key: &str, job_key: &JobKey) {
        let removed = inner.queues.get_mut(peer_key).and_then(|queue| {
            let position = queue.iter().position(|queued| queued.key() == *job_key)?;
            queue.remove(position)
        });
        let Some(removed) = removed else {
            return;
        };

        inner.queued_items -= 1;
        inner.queued_bytes -= removed.resident_bytes();
        if inner.queues.get(peer_key).is_some_and(VecDeque::is_empty) {
            inner.queues.remove(peer_key);
            inner.ready.retain(|ready_peer| ready_peer != peer_key);
        }
    }

    fn prune_expired_retries(inner: &mut Inner, now: Instant) {
        let live_keys: std::collections::HashSet<JobKey> = inner.latest.keys().cloned().collect();
        inner
            .retries
            .retain(|_, retry| retry.until > now || live_keys.contains(&retry.job_key));
    }

    /// Whether this exact head remains the newest live obligation for its
    /// `(document, peer)` pair.
    pub fn is_current(&self, job: &PushJobSpec) -> bool {
        self.inner
            .lock()
            .latest
            .get(&job.key())
            .is_some_and(|version| *version == job.version())
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
            let queue = inner
                .queues
                .get_mut(&peer_key)
                .expect("ready peer has a queue");
            let eligible_position = queue.iter().position(|job| {
                inner
                    .retries
                    .get(&RetryKey {
                        peer_id: peer_key.clone(),
                        cid: job.root_cid,
                    })
                    .is_none_or(|retry| retry.until <= now)
            });
            let Some(eligible_position) = eligible_position else {
                for job in queue.iter() {
                    if let Some(retry) = inner.retries.get(&RetryKey {
                        peer_id: peer_key.clone(),
                        cid: job.root_cid,
                    }) {
                        next_wake = Some(match next_wake {
                            Some(wake_at) => wake_at.min(retry.until),
                            None => retry.until,
                        });
                    }
                }
                inner.ready.push_back(peer_key);
                continue;
            };
            let job = queue
                .remove(eligible_position)
                .expect("eligible job position exists");
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
    /// escalates) this `(peer, cid)` cooldown; success or retirement clears it.
    ///
    /// Must be called exactly once per job returned by `next_job`. A call
    /// with no active slot for the peer is a caller bug and is ignored so it
    /// cannot desync the accounting or double-charge the cooldown.
    pub fn job_done(&self, job: &PushJobSpec, completion: JobCompletion) {
        let peer_key = job.peer_id.to_string();
        let retry_key = RetryKey {
            peer_id: peer_key.clone(),
            cid: job.root_cid,
        };
        {
            let mut inner = self.inner.lock();
            let Some(count) = inner.active.get_mut(&peer_key) else {
                debug_assert!(false, "job_done without an active job for {peer_key}");
                tracing::debug!(
                    peer_id = %peer_key,
                    "job_done called without an active job; ignoring"
                );
                return;
            };
            *count -= 1;
            if *count == 0 {
                inner.active.remove(&peer_key);
            }
            inner.active_jobs = inner.active_jobs.saturating_sub(1);
            if completion == JobCompletion::Failed {
                let retry_count = inner
                    .retries
                    .get(&retry_key)
                    .map(|retry| retry.retry_count)
                    .unwrap_or(0)
                    .saturating_add(1);
                let shift = (retry_count - 1).min(PUSH_FAILURE_COOLDOWN_MAX_SHIFT);
                let cooldown = self.failure_cooldown_base.saturating_mul(1 << shift);
                inner.retries.insert(
                    retry_key,
                    RetryState {
                        until: Instant::now() + cooldown,
                        retry_count,
                        job_key: job.key(),
                        version: job.version(),
                    },
                );
            } else {
                inner.retries.remove(&retry_key);
            }
            let job_key = job.key();
            if inner
                .latest
                .get(&job_key)
                .is_some_and(|version| *version == job.version())
            {
                inner.latest.remove(&job_key);
            }
        }
        match completion {
            JobCompletion::Succeeded => {
                self.completed_total.fetch_add(1, Ordering::Relaxed);
            }
            JobCompletion::Failed => {
                self.failed_total.fetch_add(1, Ordering::Relaxed);
            }
            JobCompletion::Retired => {}
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
            inner.latest.clear();
            inner.retries.clear();
            inner.queued_items = 0;
            inner.queued_bytes = 0;
        }
        self.notify.notify_waiters();
    }

    pub fn snapshot(&self) -> PushBacklogSnapshot {
        let mut inner = self.inner.lock();
        let now = Instant::now();
        Self::prune_expired_retries(&mut inner, now);
        let mut peers: std::collections::HashSet<&String> = inner.queues.keys().collect();
        peers.extend(inner.active.keys());
        let retry_peers: std::collections::HashSet<&String> =
            inner.retries.keys().map(|key| &key.peer_id).collect();
        peers.extend(retry_peers);
        let mut per_peer: Vec<PeerBacklogSnapshot> = peers
            .into_iter()
            .map(|peer| {
                let queued = inner.queues.get(peer);
                let retries: Vec<&RetryState> = inner
                    .retries
                    .iter()
                    .filter_map(|(key, retry)| (key.peer_id == *peer).then_some(retry))
                    .collect();
                PeerBacklogSnapshot {
                    peer_id: peer.clone(),
                    queued_items: queued.map(|jobs| jobs.len()).unwrap_or(0),
                    queued_bytes: queued
                        .map(|jobs| jobs.iter().map(PushJobSpec::resident_bytes).sum())
                        .unwrap_or(0),
                    active_jobs: inner.active.get(peer).copied().unwrap_or(0),
                    consecutive_failures: retries
                        .iter()
                        .map(|retry| retry.retry_count)
                        .max()
                        .unwrap_or(0),
                    cooldown_remaining_ms: retries
                        .iter()
                        .map(|retry| retry.until.saturating_duration_since(now).as_millis() as u64)
                        .max()
                        .unwrap_or(0),
                }
            })
            .collect();
        per_peer.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        let mut retries_by_cid: HashMap<Cid, u64> = HashMap::new();
        for (key, retry) in &inner.retries {
            *retries_by_cid.entry(key.cid).or_default() += u64::from(retry.retry_count);
        }
        let mut per_cid_retry_counts: Vec<CidRetrySnapshot> = retries_by_cid
            .into_iter()
            .map(|(cid, retry_count)| CidRetrySnapshot {
                cid: cid.to_string(),
                retry_count,
            })
            .collect();
        per_cid_retry_counts.sort_by(|a, b| a.cid.cmp(&b.cid));
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
            stale_head_retirements_total: self.stale_head_retirements_total.load(Ordering::Relaxed),
            per_cid_retry_counts,
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
            doc_id: format!("doc-{}", hex::encode(cid_seed)),
            collection_id: "collection".to_string(),
            creator: "creator".to_string(),
            root_cid: Cid::new_v1(0x55, Code::Sha2_256.digest(cid_seed)),
            head_block: Bytes::from_static(b"head-block"),
            expand_dag: false,
            encoded_payload: None,
        }
    }

    fn versioned_job(peer: &str, doc_id: &str, priority: u64) -> PushJobSpec {
        use defra_core::{Block, CompositeDeltaPayload, CrdtDelta};

        let block = Block::new_with_options(
            CrdtDelta::Composite(CompositeDeltaPayload {
                doc_id: doc_id.as_bytes().to_vec(),
                schema_version_id: "schema".to_string(),
                priority,
                status: 1,
            }),
            vec![],
            vec![],
            None,
            None,
        );
        let head_block = Bytes::from(block.to_dag_cbor().unwrap());
        PushJobSpec {
            peer_id: PeerId::new(peer.to_string()),
            doc_id: doc_id.to_string(),
            collection_id: "collection".to_string(),
            creator: "creator".to_string(),
            root_cid: defra_core::block::generate_cid_from_bytes(&head_block).unwrap(),
            head_block,
            expand_dag: false,
            encoded_payload: None,
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

    #[tokio::test]
    async fn coalesce_retires_older_head_for_same_document_peer() {
        let backlog = PushBacklog::new(1024, usize::MAX, 4, 4);
        let old = versioned_job("a", "doc", 1);
        assert_eq!(backlog.try_enqueue(old.clone()), EnqueueOutcome::Enqueued);

        let mut duplicate = old;
        duplicate.expand_dag = true;
        assert_eq!(backlog.try_enqueue(duplicate), EnqueueOutcome::Coalesced);
        let newest = versioned_job("a", "doc", 2);
        assert_eq!(
            backlog.try_enqueue(newest.clone()),
            EnqueueOutcome::Enqueued
        );
        assert_eq!(
            backlog.try_enqueue(versioned_job("a", "doc", 1)),
            EnqueueOutcome::RetiredStale
        );
        assert_eq!(
            backlog.try_enqueue(versioned_job("b", "doc", 1)),
            EnqueueOutcome::Enqueued
        );

        let snap = backlog.snapshot();
        assert_eq!(snap.queued_items, 2);
        assert_eq!(snap.coalesced_total, 1);
        assert_eq!(snap.stale_head_retirements_total, 2);
        let popped = backlog.next_job().await.unwrap();
        if popped.peer_id.to_string() == "a" {
            assert_eq!(popped.root_cid, newest.root_cid);
        }
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

        backlog.job_done(&slow_job, JobCompletion::Succeeded);
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
        backlog.job_done(&first, JobCompletion::Succeeded);

        let second = backlog.next_job().await.unwrap();
        backlog.job_done(&second, JobCompletion::Failed);

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

    /// A failed `(peer, cid)` parks only that CID. A different CID for the same
    /// peer and healthy peers both keep flowing during the cooldown.
    #[tokio::test]
    async fn failed_cid_cools_down_without_blocking_other_work() {
        let backlog = PushBacklog::with_failure_cooldown_base(
            1024,
            usize::MAX,
            4,
            4,
            Duration::from_millis(80),
        );
        let failed = job("dead", b"d1");
        backlog.try_enqueue(failed.clone());
        let dead_job = backlog.next_job().await.unwrap();
        backlog.job_done(&dead_job, JobCompletion::Failed);

        backlog.try_enqueue(failed);
        backlog.try_enqueue(job("dead", b"d2"));
        backlog.try_enqueue(job("healthy", b"h1"));

        let first = backlog.next_job().await.unwrap();
        assert_eq!(first.root_cid, job("dead", b"d2").root_cid);
        backlog.job_done(&first, JobCompletion::Succeeded);
        let second = backlog.next_job().await.unwrap();
        assert_eq!(second.peer_id.to_string(), "healthy");
        backlog.job_done(&second, JobCompletion::Succeeded);

        let parked = tokio::time::timeout(Duration::from_millis(20), backlog.next_job()).await;
        assert!(parked.is_err(), "failed CID must remain parked");

        let released = tokio::time::timeout(Duration::from_millis(400), backlog.next_job())
            .await
            .expect("cooldown expiry must wake a parked worker")
            .unwrap();
        assert_eq!(released.peer_id.to_string(), "dead");
        assert_eq!(released.root_cid, job("dead", b"d1").root_cid);
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

        for _ in 0..2 {
            backlog.try_enqueue(job("flaky", b"1"));
            let popped = tokio::time::timeout(Duration::from_secs(2), backlog.next_job())
                .await
                .expect("job available once any cooldown expires")
                .unwrap();
            backlog.job_done(&popped, JobCompletion::Failed);
        }

        let snap = backlog.snapshot();
        let entry = snap
            .per_peer
            .iter()
            .find(|entry| entry.peer_id == "flaky")
            .expect("cooling peer appears in per-peer snapshot");
        assert_eq!(entry.consecutive_failures, 2);
        assert!(entry.cooldown_remaining_ms > 0);

        backlog.try_enqueue(job("flaky", b"1"));
        let popped = tokio::time::timeout(Duration::from_secs(2), backlog.next_job())
            .await
            .expect("job available once the cooldown expires")
            .unwrap();
        backlog.job_done(&popped, JobCompletion::Succeeded);
        let snap = backlog.snapshot();
        assert!(
            !snap.per_peer.iter().any(|entry| entry.peer_id == "flaky"),
            "success must clear the cooldown"
        );
    }

    /// A `job_done` with no matching active job (caller bug) must not desync
    /// the accounting or charge a cooldown.
    #[tokio::test]
    #[cfg_attr(
        debug_assertions,
        should_panic(expected = "job_done without an active job")
    )]
    async fn spurious_job_done_is_ignored() {
        let backlog = PushBacklog::new(1024, usize::MAX, 4, 4);
        backlog.try_enqueue(job("a", b"1"));
        let popped = backlog.next_job().await.unwrap();
        backlog.job_done(&popped, JobCompletion::Succeeded);
        assert_eq!(backlog.snapshot().active_jobs, 0);

        backlog.job_done(&popped, JobCompletion::Failed);
        let snap = backlog.snapshot();
        assert_eq!(snap.active_jobs, 0);
        assert_eq!(
            snap.failed_total, 0,
            "spurious call must not count a failure"
        );
        assert!(!snap.per_peer.iter().any(|entry| entry.peer_id == "a"));
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
        backlog.job_done(&active, JobCompletion::Succeeded);
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
