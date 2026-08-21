//! Per-peer selective CAR grant cache for outbound pushes.
//!
//! The cache closes the pre-send race. Post-ack/restart recovery does not rely
//! on its lifetime: the CAR serve path re-derives exact-root authority from the
//! durable replicator configuration and DB-backed root classification.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cid::Cid;
use parking_lot::Mutex;

use crate::transport::PeerId;

/// Covers the receiver's worst-case bounded DAG fetch retry budget (roughly
/// seven minutes with four providers) after it has acknowledged the PushLog.
const POST_ACK_RECOVERY_WINDOW: Duration = Duration::from_secs(10 * 60);
const MAX_SELECTIVE_CAR_GRANTS: usize = 65_536;
const MAX_SELECTIVE_CAR_GRANTS_PER_PEER: usize = 4096;

#[derive(Debug)]
struct PushGrant {
    root_cid: Cid,
    /// Active pushes have no expiry. Dropping the registration starts the
    /// bounded post-ack recovery window instead of revoking access immediately.
    expires_at: Option<Instant>,
}

#[derive(Debug)]
pub(in crate::sync) struct SelectiveCarAccess {
    next_id: AtomicU64,
    recovery_window: Duration,
    grants: Mutex<HashMap<PeerId, HashMap<u64, PushGrant>>>,
}

impl Default for SelectiveCarAccess {
    fn default() -> Self {
        Self {
            next_id: AtomicU64::new(0),
            recovery_window: POST_ACK_RECOVERY_WINDOW,
            grants: Mutex::new(HashMap::new()),
        }
    }
}

impl SelectiveCarAccess {
    pub(super) fn register(
        self: &Arc<Self>,
        peer_id: PeerId,
        root_cid: Cid,
    ) -> Option<SelectiveCarAccessGuard> {
        let grant_id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let mut grants = self.grants.lock();
        Self::remove_expired(&mut grants, Instant::now());
        let peer_grants = grants.get(&peer_id).map_or(0, HashMap::len);
        let total_grants: usize = grants.values().map(HashMap::len).sum();
        if peer_grants >= MAX_SELECTIVE_CAR_GRANTS_PER_PEER
            || total_grants >= MAX_SELECTIVE_CAR_GRANTS
        {
            tracing::warn!(
                peer_id = %peer_id,
                root_cid = %root_cid,
                peer_grants,
                total_grants,
                "Selective CAR authority capacity reached; retaining durable head marker"
            );
            return None;
        }
        grants.entry(peer_id.clone()).or_default().insert(
            grant_id,
            PushGrant {
                root_cid,
                expires_at: None,
            },
        );

        Some(SelectiveCarAccessGuard {
            access: Arc::clone(self),
            peer_id,
            grant_id,
        })
    }

    pub(super) fn allows_root(&self, peer_id: &PeerId, root_cid: &Cid) -> bool {
        let mut grants = self.grants.lock();
        Self::remove_expired(&mut grants, Instant::now());
        grants.get(peer_id).is_some_and(|peer_grants| {
            peer_grants
                .values()
                .any(|grant| grant.root_cid == *root_cid)
        })
    }

    fn finish_push(&self, peer_id: &PeerId, grant_id: u64) {
        let mut grants = self.grants.lock();
        let Some(grant) = grants
            .get_mut(peer_id)
            .and_then(|peer_grants| peer_grants.get_mut(&grant_id))
        else {
            return;
        };
        grant.expires_at = Some(Instant::now() + self.recovery_window);
        Self::remove_expired(&mut grants, Instant::now());
    }

    fn remove_expired(grants: &mut HashMap<PeerId, HashMap<u64, PushGrant>>, now: Instant) {
        grants.retain(|_, peer_grants| {
            peer_grants.retain(|_, grant| grant.expires_at.is_none_or(|expiry| expiry > now));
            !peer_grants.is_empty()
        });
    }

    #[cfg(test)]
    fn with_recovery_window(recovery_window: Duration) -> Self {
        Self {
            next_id: AtomicU64::new(0),
            recovery_window,
            grants: Mutex::new(HashMap::new()),
        }
    }
}

/// Cloneable capability used by replay/retry senders outside the coordinator.
/// Holding a grant proves that a head hint cannot be emitted before its rooted
/// selective-CAR authority is installed.
#[derive(Clone)]
pub struct HeadHintCarAuthority {
    access: Arc<SelectiveCarAccess>,
}

impl HeadHintCarAuthority {
    pub(super) fn new(access: Arc<SelectiveCarAccess>) -> Self {
        Self { access }
    }

    pub fn register(&self, peer_id: PeerId, root_cid: Cid) -> Option<HeadHintCarGrant> {
        self.access
            .register(peer_id, root_cid)
            .map(HeadHintCarGrant)
    }
}

#[must_use = "the CAR grant must cover the corresponding PushLog attempt"]
pub struct HeadHintCarGrant(#[allow(dead_code)] SelectiveCarAccessGuard);

pub(super) struct SelectiveCarAccessGuard {
    access: Arc<SelectiveCarAccess>,
    peer_id: PeerId,
    grant_id: u64,
}

impl Drop for SelectiveCarAccessGuard {
    fn drop(&mut self) {
        self.access.finish_push(&self.peer_id, self.grant_id);
    }
}

#[cfg(test)]
mod tests {
    use multihash_codetable::{Code, MultihashDigest};

    use super::*;

    fn cid(label: &[u8]) -> Cid {
        Cid::new_v1(0x71, Code::Sha2_256.digest(label))
    }

    #[test]
    fn completed_push_grants_post_ack_recovery_for_its_peer_and_root() {
        let access = Arc::new(SelectiveCarAccess::default());
        let peer = PeerId::new("peer-a".to_string());
        let other_peer = PeerId::new("peer-b".to_string());
        let root = cid(b"root");
        let child = cid(b"child");
        let unrelated = cid(b"unrelated");

        let guard = access.register(peer.clone(), root).unwrap();
        drop(guard);

        assert!(access.allows_root(&peer, &root));
        assert!(!access.allows_root(&peer, &unrelated));
        assert!(!access.allows_root(&peer, &child));
        assert!(!access.allows_root(&other_peer, &root));
    }

    #[test]
    fn post_ack_grant_expires_after_recovery_window() {
        let access = Arc::new(SelectiveCarAccess::with_recovery_window(Duration::ZERO));
        let peer = PeerId::new("peer".to_string());
        let root = cid(b"root");
        let guard = access.register(peer.clone(), root).unwrap();
        assert!(access.allows_root(&peer, &root));

        drop(guard);
        assert!(!access.allows_root(&peer, &root));
    }

    #[test]
    fn overlapping_push_grants_finish_independently() {
        let access = Arc::new(SelectiveCarAccess::with_recovery_window(Duration::ZERO));
        let peer = PeerId::new("peer".to_string());
        let root = cid(b"root");
        let first = access.register(peer.clone(), root).unwrap();
        let second = access.register(peer.clone(), root).unwrap();
        drop(first);
        assert!(access.allows_root(&peer, &root));

        drop(second);
        assert!(!access.allows_root(&peer, &root));
    }

    #[test]
    fn active_authority_is_bounded_and_overflow_is_actionable() {
        let access = Arc::new(SelectiveCarAccess::default());
        let peer = PeerId::new("peer".to_string());
        let root = cid(b"root");
        let mut grants = Vec::new();
        for _ in 0..MAX_SELECTIVE_CAR_GRANTS_PER_PEER {
            grants.push(access.register(peer.clone(), root).unwrap());
        }
        assert!(access.register(peer, root).is_none());
        assert_eq!(grants.len(), MAX_SELECTIVE_CAR_GRANTS_PER_PEER);
    }
}
