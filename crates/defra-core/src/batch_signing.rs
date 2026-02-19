//! Batch CID collection and Merkle root computation for batch signing.
//!
//! During block creation, individual CIDs are collected into a session.
//! After all documents in a batch are created, the collected CIDs are
//! used to compute a Merkle root which is then signed as a single batch.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Mutex;

use cid::Cid;
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Merkle root computation
// ---------------------------------------------------------------------------

/// Compute a SHA256 Merkle root over a set of CIDs.
///
/// 1. Sort CIDs by their string representation (deterministic ordering).
/// 2. SHA256-hash each CID's binary encoding to produce leaf hashes.
/// 3. Binary tree reduction: pair adjacent hashes → SHA256(left || right).
///    Odd elements pass through to the next level unchanged.
///
/// Returns `None` if the input slice is empty.
pub fn compute_merkle_root(cids: &[Cid]) -> Option<[u8; 32]> {
    if cids.is_empty() {
        return None;
    }

    let mut sorted: Vec<&Cid> = cids.iter().collect();
    sorted.sort_by_key(|a| a.to_bytes());

    let mut hashes: Vec<[u8; 32]> = sorted
        .iter()
        .map(|c| {
            let mut hasher = Sha256::new();
            hasher.update(c.to_bytes());
            hasher.finalize().into()
        })
        .collect();

    while hashes.len() > 1 {
        let mut next = Vec::with_capacity(hashes.len().div_ceil(2));
        let mut i = 0;
        while i + 1 < hashes.len() {
            let mut hasher = Sha256::new();
            hasher.update(hashes[i]);
            hasher.update(hashes[i + 1]);
            next.push(hasher.finalize().into());
            i += 2;
        }
        if i < hashes.len() {
            next.push(hashes[i]);
        }
        hashes = next;
    }

    Some(hashes[0])
}

// ---------------------------------------------------------------------------
// Global batch collector  (same pattern as IDENTITY_STORE in signing.rs)
// ---------------------------------------------------------------------------

static BATCH_COLLECTORS: std::sync::OnceLock<Mutex<HashMap<String, Vec<Cid>>>> =
    std::sync::OnceLock::new();

fn collectors() -> &'static Mutex<HashMap<String, Vec<Cid>>> {
    BATCH_COLLECTORS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Start (or reset) a batch collection session for `session_key`.
pub fn batch_start(session_key: &str) {
    if let Ok(mut map) = collectors().lock() {
        map.insert(session_key.to_string(), Vec::new());
    }
}

/// Append a CID to the session identified by `session_key`.
/// No-op if the session does not exist.
pub fn batch_collect_cid(session_key: &str, cid: Cid) {
    if let Ok(mut map) = collectors().lock() {
        if let Some(vec) = map.get_mut(session_key) {
            vec.push(cid);
        }
    }
}

/// Remove and return all collected CIDs for `session_key`.
pub fn batch_take_cids(session_key: &str) -> Option<Vec<Cid>> {
    collectors().lock().ok()?.remove(session_key)
}

// ---------------------------------------------------------------------------
// Thread-local session key  (same pattern as CURRENT_SIGNING_CONFIG)
// ---------------------------------------------------------------------------

thread_local! {
    static BATCH_SESSION_KEY: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Set the batch session key for the current thread.
pub fn set_batch_session_key(key: Option<String>) {
    BATCH_SESSION_KEY.with(|k| {
        *k.borrow_mut() = key;
    });
}

/// Get the batch session key for the current thread.
pub fn get_batch_session_key() -> Option<String> {
    BATCH_SESSION_KEY.with(|k| k.borrow().clone())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use cid::multihash::{Code, MultihashDigest};

    fn make_cid(data: &[u8]) -> Cid {
        let hash = Code::Sha2_256.digest(data);
        Cid::new_v1(0x71, hash) // dag-cbor codec
    }

    #[test]
    fn merkle_root_empty() {
        assert!(compute_merkle_root(&[]).is_none());
    }

    #[test]
    fn merkle_root_single() {
        let cid = make_cid(b"hello");
        let root = compute_merkle_root(&[cid]).unwrap();
        // Single element: root == SHA256(cid.to_bytes())
        let mut hasher = Sha256::new();
        hasher.update(cid.to_bytes());
        let expected: [u8; 32] = hasher.finalize().into();
        assert_eq!(root, expected);
    }

    #[test]
    fn merkle_root_deterministic() {
        let a = make_cid(b"a");
        let b = make_cid(b"b");
        let root1 = compute_merkle_root(&[a, b]).unwrap();
        let root2 = compute_merkle_root(&[b, a]).unwrap();
        assert_eq!(root1, root2, "order should not matter (CIDs are sorted)");
    }

    #[test]
    fn merkle_root_different_inputs() {
        let a = make_cid(b"a");
        let b = make_cid(b"b");
        let c = make_cid(b"c");
        let root_ab = compute_merkle_root(&[a, b]).unwrap();
        let root_ac = compute_merkle_root(&[a, c]).unwrap();
        assert_ne!(root_ab, root_ac);
    }

    #[test]
    fn batch_collect_roundtrip() {
        let key = "test-session-roundtrip";
        batch_start(key);
        let c1 = make_cid(b"doc1");
        let c2 = make_cid(b"doc2");
        batch_collect_cid(key, c1);
        batch_collect_cid(key, c2);
        let cids = batch_take_cids(key).unwrap();
        assert_eq!(cids.len(), 2);
        assert!(batch_take_cids(key).is_none(), "session should be consumed");
    }

    #[test]
    fn batch_collect_noop_without_start() {
        let key = "nonexistent-session";
        let c = make_cid(b"stray");
        batch_collect_cid(key, c);
        assert!(batch_take_cids(key).is_none());
    }

    #[test]
    fn thread_local_session_key() {
        set_batch_session_key(Some("my-key".to_string()));
        assert_eq!(get_batch_session_key(), Some("my-key".to_string()));
        set_batch_session_key(None);
        assert_eq!(get_batch_session_key(), None);
    }
}
