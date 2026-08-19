//! Pending DAG tracking for Bitswap synchronization.

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::time::{Duration, Instant};

use cid::Cid;

use crate::ExplicitReplayAuthorization;

/// Time-to-live for a pending DAG entry.
///
/// Entries older than this are evicted during insertion to prevent
/// indefinitely stale DAGs from accumulating.
pub const PENDING_DAG_TTL: Duration = Duration::from_secs(300);

/// Metadata for a pending DAG sync waiting for Bitswap to complete.
#[derive(Debug, Clone)]
pub struct PendingDag {
    /// Document ID from the original PushLog message
    pub doc_id: String,
    /// Collection ID from the original PushLog message
    pub collection_id: String,
    /// Monotonic priority decoded from the announced composite/collection
    /// head. `None` disables scope retirement because subsumption is unknown.
    pub head_priority: Option<u64>,
    /// Creator from the original PushLog message
    pub creator: String,
    /// CIDs still missing (gets smaller as blocks arrive via Bitswap)
    #[allow(dead_code)] // Used for tracking Bitswap progress
    pub missing: HashSet<Cid>,
    /// The peer that originally provided this DAG (e.g. DocSync reply sender).
    /// Always included in the Bitswap provider list during retries so the
    /// blocks can be fetched even if the peer isn't in connected_peers().
    pub source_peer: Option<String>,
    /// True when the DAG originated from an explicit replicator push.
    pub is_explicit_replicator: bool,
    /// Capability-based explicit replay authorization carried by two-stream pushes.
    pub explicit_replay_authorization: Option<ExplicitReplayAuthorization>,
    /// Whether a success reply can rely on this entry for eventual recovery.
    pub is_recovery_registered: bool,
    /// When this entry was inserted (for TTL eviction).
    pub inserted_at: Instant,
    /// How many times `retry_pending_dag` has been invoked for this root.
    ///
    /// Surfaced in diagnostic logs so the single aggregated WARN on terminal
    /// failure can carry an attempt count without scraping intermediate noise.
    pub attempts: u32,
    /// Number of Bitswap/CAR fetch rounds that exhausted providers without
    /// yielding a complete DAG for this root.
    pub fetch_failures: u32,
    /// Most recent provider-exhaustion error for this root, if any.
    pub last_fetch_error: Option<String>,
    /// Earliest instant a fetch dispatch may be claimed for this root.
    /// Every dispatch site must win `try_claim_pending_dag_dispatch`,
    /// which advances this by `retry_backoff(dispatches)` — the receiver's
    /// re-arm pacing (#1112 inbound half, #1116 stage 2).
    pub next_retry_at: tokio::time::Instant,
    /// Fetch dispatches claimed for this root (drives the backoff rung).
    pub dispatches: u32,
}

/// Pending roots and the reverse index used to route arriving blocks only to
/// roots that are waiting for them.
#[derive(Debug, Default)]
pub(super) struct PendingDagRegistry {
    roots: HashMap<Cid, PendingDag>,
    waiters: HashMap<Cid, HashSet<Cid>>,
    roots_by_source: HashMap<String, usize>,
    current_by_source_scope: HashMap<PendingScopeKey, (HeadVersion, Cid)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PendingScopeKey {
    source_peer: String,
    collection_id: String,
    doc_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct HeadVersion {
    priority: u64,
    cid: Cid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScopeHeadDecision {
    Independent,
    Current,
    CoveredByCurrent,
    Supersedes(Cid),
}

impl Deref for PendingDagRegistry {
    type Target = HashMap<Cid, PendingDag>;

    fn deref(&self) -> &Self::Target {
        &self.roots
    }
}

impl PendingDagRegistry {
    pub(super) fn scope_head_is_refresh_or_newer(
        &self,
        root_cid: Cid,
        source_peer: Option<&str>,
        collection_id: &str,
        doc_id: &str,
        head_priority: Option<u64>,
    ) -> bool {
        let (Some(source_peer), Some(priority)) = (source_peer, head_priority) else {
            return false;
        };
        let key = PendingScopeKey {
            source_peer: source_peer.to_owned(),
            collection_id: collection_id.to_owned(),
            doc_id: doc_id.to_owned(),
        };
        let version = HeadVersion {
            priority,
            cid: root_cid,
        };
        self.current_by_source_scope
            .get(&key)
            .is_some_and(|(current, current_root)| *current_root == root_cid || version > *current)
    }

    fn scope_key(dag: &PendingDag) -> Option<PendingScopeKey> {
        let source_peer = dag.source_peer.as_ref()?;
        dag.head_priority?;
        Some(PendingScopeKey {
            source_peer: source_peer.clone(),
            collection_id: dag.collection_id.clone(),
            doc_id: dag.doc_id.clone(),
        })
    }

    fn head_version(root_cid: Cid, dag: &PendingDag) -> Option<HeadVersion> {
        Some(HeadVersion {
            priority: dag.head_priority?,
            cid: root_cid,
        })
    }

    pub(super) fn scope_head_decision(&self, root_cid: Cid, dag: &PendingDag) -> ScopeHeadDecision {
        let (Some(key), Some(version)) = (Self::scope_key(dag), Self::head_version(root_cid, dag))
        else {
            return ScopeHeadDecision::Independent;
        };
        match self.current_by_source_scope.get(&key) {
            None => ScopeHeadDecision::Current,
            Some((current, current_root)) if *current_root == root_cid => {
                ScopeHeadDecision::Current
            }
            Some((current, _)) if version <= *current => ScopeHeadDecision::CoveredByCurrent,
            Some((_, current_root)) => ScopeHeadDecision::Supersedes(*current_root),
        }
    }

    pub(super) fn get_mut(&mut self, root_cid: &Cid) -> Option<&mut PendingDag> {
        self.roots.get_mut(root_cid)
    }

    pub(super) fn insert(&mut self, root_cid: Cid, dag: PendingDag) -> Option<PendingDag> {
        let previous = self.remove(&root_cid);
        for missing_cid in &dag.missing {
            self.waiters
                .entry(*missing_cid)
                .or_default()
                .insert(root_cid);
        }
        if let Some(source_peer) = &dag.source_peer {
            *self.roots_by_source.entry(source_peer.clone()).or_default() += 1;
        }
        if let (Some(key), Some(version)) =
            (Self::scope_key(&dag), Self::head_version(root_cid, &dag))
        {
            self.current_by_source_scope
                .insert(key, (version, root_cid));
        }
        self.roots.insert(root_cid, dag);
        previous
    }

    pub(super) fn remove(&mut self, root_cid: &Cid) -> Option<PendingDag> {
        let dag = self.roots.remove(root_cid)?;
        for missing_cid in &dag.missing {
            self.remove_waiter(missing_cid, root_cid);
        }
        if let Some(source_peer) = &dag.source_peer {
            let remove_source = self
                .roots_by_source
                .get_mut(source_peer)
                .is_some_and(|count| {
                    debug_assert!(*count > 0);
                    *count = count.saturating_sub(1);
                    *count == 0
                });
            if remove_source {
                self.roots_by_source.remove(source_peer);
            }
        }
        if let Some(key) = Self::scope_key(&dag) {
            if self
                .current_by_source_scope
                .get(&key)
                .is_some_and(|(_, current_root)| current_root == root_cid)
            {
                self.current_by_source_scope.remove(&key);
            }
        }
        Some(dag)
    }

    pub(super) fn source_count(&self, source_peer: &str) -> usize {
        self.roots_by_source
            .get(source_peer)
            .copied()
            .unwrap_or_default()
    }

    pub(super) fn replace_missing(&mut self, root_cid: &Cid, missing: HashSet<Cid>) -> bool {
        let Some(previous) = self.roots.get(root_cid).map(|dag| dag.missing.clone()) else {
            return false;
        };

        for missing_cid in previous.difference(&missing) {
            self.remove_waiter(missing_cid, root_cid);
        }
        for missing_cid in missing.difference(&previous) {
            self.waiters
                .entry(*missing_cid)
                .or_default()
                .insert(*root_cid);
        }
        self.roots
            .get_mut(root_cid)
            .expect("root checked above")
            .missing = missing;
        true
    }

    pub(super) fn waiting_roots(&self, missing_cid: &Cid) -> Vec<Cid> {
        self.waiters
            .get(missing_cid)
            .map(|roots| roots.iter().copied().collect())
            .unwrap_or_default()
    }

    pub(super) fn has_waiters(&self, missing_cid: &Cid) -> bool {
        self.waiters.contains_key(missing_cid)
    }

    pub(super) fn advance_waiters(
        &mut self,
        received_cid: &Cid,
        newly_missing: &[Cid],
    ) -> Vec<Cid> {
        let waiting_roots = self.waiters.remove(received_cid).unwrap_or_default();
        let mut emptied = Vec::new();

        for root_cid in waiting_roots {
            let Some(dag) = self.roots.get_mut(&root_cid) else {
                continue;
            };
            if !dag.missing.remove(received_cid) {
                continue;
            }
            for missing_cid in newly_missing {
                if dag.missing.insert(*missing_cid) {
                    self.waiters
                        .entry(*missing_cid)
                        .or_default()
                        .insert(root_cid);
                }
            }
            if dag.missing.is_empty() {
                emptied.push(root_cid);
            }
        }

        emptied
    }

    fn remove_waiter(&mut self, missing_cid: &Cid, root_cid: &Cid) {
        let remove_entry = self.waiters.get_mut(missing_cid).is_some_and(|roots| {
            roots.remove(root_cid);
            roots.is_empty()
        });
        if remove_entry {
            self.waiters.remove(missing_cid);
        }
    }
}

/// Base delay before the first scheduled re-dispatch of a pending root.
pub const PENDING_RETRY_BASE: Duration = Duration::from_secs(2);
/// Ceiling for the per-root dispatch backoff.
pub const PENDING_RETRY_CAP: Duration = Duration::from_secs(60);

/// Capped exponential backoff for pending-DAG fetch dispatches: 2s, 4s,
/// 8s, 16s, 32s, then 60s forever.
pub fn retry_backoff(dispatches: u32) -> Duration {
    let shifted = PENDING_RETRY_BASE.saturating_mul(1u32 << dispatches.min(5));
    shifted.min(PENDING_RETRY_CAP)
}
