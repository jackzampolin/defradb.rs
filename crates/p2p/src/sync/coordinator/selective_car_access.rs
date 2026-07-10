//! Per-peer selective CAR grants bounded by active outbound pushes.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use cid::Cid;
use parking_lot::Mutex;

use crate::transport::PeerId;

#[derive(Debug)]
struct PushGrant {
    cids: HashSet<Cid>,
}

#[derive(Debug, Default)]
pub(in crate::sync) struct SelectiveCarAccess {
    next_id: AtomicU64,
    grants: Mutex<HashMap<PeerId, HashMap<u64, PushGrant>>>,
}

impl SelectiveCarAccess {
    pub(super) fn register(
        self: &Arc<Self>,
        peer_id: PeerId,
        pushed_cids: impl IntoIterator<Item = Cid>,
    ) -> SelectiveCarAccessGuard {
        let cids: HashSet<Cid> = pushed_cids.into_iter().collect();
        let grant_id = self.next_id.fetch_add(1, Ordering::Relaxed);

        self.grants
            .lock()
            .entry(peer_id.clone())
            .or_default()
            .insert(grant_id, PushGrant { cids });

        SelectiveCarAccessGuard {
            access: Arc::clone(self),
            peer_id,
            grant_id,
        }
    }

    pub(super) fn allows(&self, peer_id: &PeerId, root_cid: &Cid, wanted_cid: &Cid) -> bool {
        self.grants.lock().get(peer_id).is_some_and(|grants| {
            grants
                .values()
                .any(|grant| grant.cids.contains(root_cid) && grant.cids.contains(wanted_cid))
        })
    }

    fn remove(&self, peer_id: &PeerId, grant_id: u64) {
        let mut grants = self.grants.lock();
        let Some(peer_grants) = grants.get_mut(peer_id) else {
            return;
        };
        peer_grants.remove(&grant_id);
        if peer_grants.is_empty() {
            grants.remove(peer_id);
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
        self.access.remove(&self.peer_id, self.grant_id);
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
    fn active_push_grants_only_its_peer_and_dag() {
        let access = Arc::new(SelectiveCarAccess::default());
        let peer = PeerId::new("peer-a".to_string());
        let other_peer = PeerId::new("peer-b".to_string());
        let root = cid(b"root");
        let child = cid(b"child");
        let unrelated = cid(b"unrelated");

        let guard = access.register(peer.clone(), [root, child]);

        assert!(access.allows(&peer, &root, &child));
        assert!(!access.allows(&peer, &root, &unrelated));
        assert!(!access.allows(&peer, &unrelated, &child));
        assert!(!access.allows(&other_peer, &root, &child));

        drop(guard);
        assert!(!access.allows(&peer, &root, &child));
    }

    #[test]
    fn overlapping_push_grants_revoke_independently() {
        let access = Arc::new(SelectiveCarAccess::default());
        let peer = PeerId::new("peer".to_string());
        let root = cid(b"root");
        let child = cid(b"child");

        let first = access.register(peer.clone(), [root, child]);
        let second = access.register(peer.clone(), [root, child]);
        drop(first);
        assert!(access.allows(&peer, &root, &child));

        drop(second);
        assert!(!access.allows(&peer, &root, &child));
    }
}
