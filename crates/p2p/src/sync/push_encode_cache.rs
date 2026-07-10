//! Weak-lifetime encode cache for outbound PushLog fan-out (#1102).
//!
//! The index is keyed by root CID and stores only `Weak` entries. Queued and
//! active peer jobs own the strong references, so an entry disappears when
//! the last peer acknowledges or its job is retired.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use bytes::Bytes;
use cid::Cid;
use parking_lot::Mutex;
use tokio::sync::OnceCell;

use super::push_backlog::PushJobSpec;
use crate::message::PushLogRequest;

#[derive(Debug)]
pub(crate) struct PushPayload {
    pub(crate) root_cid: Cid,
    pub(crate) doc_id: String,
    pub(crate) collection_id: String,
    pub(crate) creator: String,
    pub(crate) head_block: Bytes,
    pub(crate) root_request: OnceCell<Option<(Cid, PushLogRequest)>>,
    pub(crate) dependency_requests: OnceCell<Arc<Vec<(Cid, PushLogRequest)>>>,
}

impl PushPayload {
    pub(crate) fn from_job(job: &PushJobSpec) -> Self {
        Self {
            root_cid: job.root_cid,
            doc_id: job.doc_id.clone(),
            collection_id: job.collection_id.clone(),
            creator: job.creator.clone(),
            head_block: job.head_block.clone(),
            root_request: OnceCell::new(),
            dependency_requests: OnceCell::new(),
        }
    }

    fn matches(&self, job: &PushJobSpec) -> bool {
        self.root_cid == job.root_cid
            && self.doc_id == job.doc_id
            && self.collection_id == job.collection_id
            && self.creator == job.creator
            && self.head_block == job.head_block
    }
}

#[derive(Default)]
pub(crate) struct PushEncodeCache {
    entries: Mutex<HashMap<Cid, Weak<PushPayload>>>,
    hits: AtomicU64,
}

impl PushEncodeCache {
    pub(crate) fn acquire(&self, job: &PushJobSpec) -> Arc<PushPayload> {
        let mut entries = self.entries.lock();
        entries.retain(|_, entry| entry.strong_count() > 0);
        if let Some(payload) = entries.get(&job.root_cid).and_then(Weak::upgrade) {
            if payload.matches(job) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return payload;
            }
        }

        let payload = Arc::new(PushPayload::from_job(job));
        entries.insert(job.root_cid, Arc::downgrade(&payload));
        payload
    }

    pub(crate) fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub(crate) fn live_entries(&self) -> usize {
        let mut entries = self.entries.lock();
        entries.retain(|_, entry| entry.strong_count() > 0);
        entries.len()
    }
}

#[cfg(test)]
mod tests {
    use multihash_codetable::{Code, MultihashDigest};

    use super::*;
    use crate::transport::PeerId;

    fn job(peer: &str) -> PushJobSpec {
        PushJobSpec::new(
            PeerId::new(peer.to_string()),
            "doc".to_string(),
            "collection".to_string(),
            "creator".to_string(),
            Cid::new_v1(0x55, Code::Sha2_256.digest(b"root")),
            Bytes::from_static(b"head"),
            false,
        )
    }

    #[test]
    fn entry_is_shared_by_cid_and_dropped_with_last_peer() {
        let cache = PushEncodeCache::default();
        let first = cache.acquire(&job("a"));
        let second = cache.acquire(&job("b"));
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.live_entries(), 1);

        drop(first);
        drop(second);
        assert_eq!(cache.live_entries(), 0);
    }
}
