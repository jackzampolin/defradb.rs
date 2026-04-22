//! Per-peer Bitswap block-request filter.
//!
//! Enforces per-collection access control on outbound Bitswap responses.
//! Without this filter a node running in `AccessMode::Controlled` would
//! still serve any stored block to any connected peer, bypassing the
//! `ReplicatorRegistry` that gates ingress.
//!
//! Matches Go DefraDB's `bitswap.WithPeerBlockRequestFilter(hasAccess)`
//! wiring in `go-p2p/peer.go:146`.
//!
//! # Decision flow
//!
//! 1. `AccessMode::Open` → allow all.
//! 2. Block not in store → deny (prevents existence leaks too).
//! 3. Block decodes as a `Signature` → allow (signature blocks carry no
//!    collection data and are required to verify sibling blocks).
//! 4. Block decodes as a CRDT `Block` with a definition delta → allow
//!    (schema/collection definitions are broadcast to all replicators).
//! 5. Block decodes as a CRDT `Block` with a data delta → allow iff the
//!    requesting peer is registered as a replicator for the block's
//!    collection.
//! 6. Anything else (lens/wasm/chunk blocks, decode errors) → deny by
//!    default. Callers that need a permissive path for Rust-specific
//!    block kinds should extend the match arms below.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use cid::Cid;
use defra_core::{Block as DefraBlock, Signature};
use iroh_bitswap::Store;
use libp2p::PeerId;

use super::{AccessMode, ReplicatorRegistry};

/// Build a filter closure that satisfies iroh-bitswap's
/// `PeerBlockRequestFilter` trait.
///
/// The returned closure owns clones of `registry` and `store`, so the
/// caller can construct it once and hand it to `BitswapConfig`.
pub fn make_peer_block_access_filter<S>(
    mode: AccessMode,
    registry: Arc<ReplicatorRegistry>,
    store: S,
) -> impl Fn(&PeerId, &Cid) -> Pin<Box<dyn Future<Output = bool> + Send + 'static>> + Send + Sync + 'static
where
    S: Store + Clone + Send + Sync + 'static,
{
    move |peer_id: &PeerId, cid: &Cid| {
        let peer_id = *peer_id;
        let cid = *cid;
        let registry = Arc::clone(&registry);
        let store = store.clone();
        Box::pin(async move { check_access(mode, &registry, &store, &peer_id, &cid).await })
    }
}

async fn check_access<S: Store>(
    mode: AccessMode,
    registry: &ReplicatorRegistry,
    store: &S,
    peer_id: &PeerId,
    cid: &Cid,
) -> bool {
    if mode.is_open() {
        return true;
    }

    let block = match store.get(cid).await {
        Ok(b) => b,
        Err(_) => {
            // Miss or I/O error: deny without leaking block presence.
            // Matches Go's error path in hasAccess (returns false on
            // blockstore.Get failure).
            return false;
        }
    };
    let data = block.data();

    // Signature blocks are required for peers to verify data they already
    // have, and carry no authored data themselves. Always serve.
    if Signature::from_dag_cbor(data).is_ok() {
        return true;
    }

    let defra_block = match DefraBlock::from_dag_cbor(data) {
        Ok(b) => b,
        Err(_) => return false,
    };

    if defra_block.delta.is_definition() {
        return true;
    }

    let collection_id = match defra_block.delta.schema_version_id() {
        Some(id) => id,
        None => return false,
    };

    let peer_str = peer_id.to_string();
    registry.is_replicator(collection_id, &peer_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use iroh_bitswap::{Block, Store};
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Debug, Default, Clone)]
    struct InMemoryStore {
        inner: Arc<Mutex<HashMap<Cid, Vec<u8>>>>,
    }

    impl InMemoryStore {
        fn put(&self, cid: Cid, data: Vec<u8>) {
            self.inner.lock().unwrap().insert(cid, data);
        }
    }

    #[async_trait]
    impl Store for InMemoryStore {
        async fn get_size(&self, cid: &Cid) -> anyhow::Result<usize> {
            self.inner
                .lock()
                .unwrap()
                .get(cid)
                .map(|b| b.len())
                .ok_or_else(|| anyhow::anyhow!("not found"))
        }

        async fn get(&self, cid: &Cid) -> anyhow::Result<Block> {
            let data = self
                .inner
                .lock()
                .unwrap()
                .get(cid)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("not found"))?;
            Ok(Block::new(data.into(), *cid))
        }

        async fn has(&self, cid: &Cid) -> anyhow::Result<bool> {
            Ok(self.inner.lock().unwrap().contains_key(cid))
        }
    }

    fn cid_for(data: &[u8]) -> Cid {
        defra_core::block::generate_cid_from_bytes(data).unwrap()
    }

    fn make_data_block(collection_id: &str) -> (Cid, Vec<u8>) {
        use defra_core::{CompositeDeltaPayload, CrdtDelta};

        let payload = CompositeDeltaPayload {
            doc_id: b"doc1".to_vec(),
            priority: 1,
            schema_version_id: collection_id.to_string(),
            status: 1,
        };
        let delta = CrdtDelta::Composite(payload);
        let block = DefraBlock::new(delta, Vec::new(), Vec::new());
        let bytes = block.to_dag_cbor().unwrap();
        let cid = cid_for(&bytes);
        (cid, bytes)
    }

    #[tokio::test]
    async fn open_mode_allows_all() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        let (cid, bytes) = make_data_block("users");
        store.put(cid, bytes);

        let peer = PeerId::random();
        let allowed = check_access(AccessMode::Open, &registry, &store, &peer, &cid).await;
        assert!(allowed);
    }

    #[tokio::test]
    async fn controlled_mode_denies_unknown_peer() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        let (cid, bytes) = make_data_block("users");
        store.put(cid, bytes);

        let peer = PeerId::random();
        let allowed = check_access(AccessMode::Controlled, &registry, &store, &peer, &cid).await;
        assert!(!allowed, "unknown peer must be denied in Controlled mode");
    }

    #[tokio::test]
    async fn controlled_mode_allows_registered_replicator() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        let (cid, bytes) = make_data_block("users");
        store.put(cid, bytes);

        let peer = PeerId::random();
        registry.add_replicator("users", &peer.to_string());

        let allowed = check_access(AccessMode::Controlled, &registry, &store, &peer, &cid).await;
        assert!(allowed, "registered replicator must be served");
    }

    #[tokio::test]
    async fn controlled_mode_denies_replicator_for_other_collection() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        let (cid, bytes) = make_data_block("users");
        store.put(cid, bytes);

        let peer = PeerId::random();
        // Peer is a replicator for a different collection.
        registry.add_replicator("posts", &peer.to_string());

        let allowed = check_access(AccessMode::Controlled, &registry, &store, &peer, &cid).await;
        assert!(
            !allowed,
            "replicator for a different collection must not fetch this one"
        );
    }

    #[tokio::test]
    async fn missing_block_denies_without_leaking() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        let missing = cid_for(b"never-stored");

        let peer = PeerId::random();
        registry.add_replicator("users", &peer.to_string());

        let allowed =
            check_access(AccessMode::Controlled, &registry, &store, &peer, &missing).await;
        assert!(!allowed, "missing block must be denied");
    }

    #[tokio::test]
    async fn signature_block_is_served_without_registry_check() {
        use defra_core::{Signature as DefraSig, SignatureHeader, SignatureType};
        let header = SignatureHeader::new(SignatureType::EdDSA, vec![9, 9, 9]);
        let sig = DefraSig::new(header, vec![1, 2, 3, 4]);
        let bytes = sig.to_dag_cbor().unwrap();
        let cid = cid_for(&bytes);

        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        store.put(cid, bytes);

        let peer = PeerId::random();
        let allowed = check_access(AccessMode::Controlled, &registry, &store, &peer, &cid).await;
        assert!(
            allowed,
            "signature blocks must be served even in Controlled mode"
        );
    }
}
