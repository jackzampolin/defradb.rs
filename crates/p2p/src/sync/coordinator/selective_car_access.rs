//! Per-peer selective CAR grants for outbound pushes and post-ack recovery.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cid::Cid;
use parking_lot::Mutex;

use crate::transport::PeerId;

/// Covers the receiver's worst-case bounded DAG fetch retry budget (roughly
/// seven minutes with four providers) after it has acknowledged the PushLog.
const POST_ACK_RECOVERY_WINDOW: Duration = Duration::from_secs(10 * 60);

#[derive(Debug)]
struct PushGrant {
    root_cid: Cid,
    cids: HashSet<Cid>,
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
        pushed_cids: impl IntoIterator<Item = Cid>,
    ) -> SelectiveCarAccessGuard {
        let mut cids: HashSet<Cid> = pushed_cids.into_iter().collect();
        cids.insert(root_cid);
        let grant_id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let mut grants = self.grants.lock();
        Self::remove_expired(&mut grants, Instant::now());
        grants.entry(peer_id.clone()).or_default().insert(
            grant_id,
            PushGrant {
                root_cid,
                cids,
                expires_at: None,
            },
        );

        SelectiveCarAccessGuard {
            access: Arc::clone(self),
            peer_id,
            grant_id,
        }
    }

    pub(super) fn allows(&self, peer_id: &PeerId, root_cid: &Cid, wanted_cid: &Cid) -> bool {
        let mut grants = self.grants.lock();
        Self::remove_expired(&mut grants, Instant::now());
        grants.get(peer_id).is_some_and(|peer_grants| {
            peer_grants
                .values()
                .any(|grant| grant.root_cid == *root_cid && grant.cids.contains(wanted_cid))
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

        let guard = access.register(peer.clone(), root, [root, child]);
        drop(guard);

        assert!(access.allows(&peer, &root, &child));
        assert!(!access.allows(&peer, &root, &unrelated));
        assert!(!access.allows(&peer, &child, &root));
        assert!(!access.allows(&other_peer, &root, &child));
    }

    #[test]
    fn post_ack_grant_expires_after_recovery_window() {
        let access = Arc::new(SelectiveCarAccess::with_recovery_window(Duration::ZERO));
        let peer = PeerId::new("peer".to_string());
        let root = cid(b"root");
        let child = cid(b"child");

        let guard = access.register(peer.clone(), root, [root, child]);
        assert!(access.allows(&peer, &root, &child));

        drop(guard);
        assert!(!access.allows(&peer, &root, &child));
    }

    #[test]
    fn overlapping_push_grants_finish_independently() {
        let access = Arc::new(SelectiveCarAccess::with_recovery_window(Duration::ZERO));
        let peer = PeerId::new("peer".to_string());
        let root = cid(b"root");
        let child = cid(b"child");

        let first = access.register(peer.clone(), root, [root, child]);
        let second = access.register(peer.clone(), root, [root, child]);
        drop(first);
        assert!(access.allows(&peer, &root, &child));

        drop(second);
        assert!(!access.allows(&peer, &root, &child));
    }
}
