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
//! 6. Block decodes as a lens IPLD shape (config / module / WASM /
//!    chunk) → allow. Schema-migration artifacts carry no user data
//!    and mirror Go's `hasAccess` passthrough at `internal/db/p2p/
//!    p2p.go:335-348`.
//! 7. Anything else (decode errors on all known shapes) → deny.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use cid::Cid;
use defra_core::{is_lens_block, Block as DefraBlock, Signature};
use iroh_bitswap::Store;
use libp2p::PeerId;
use tracing::debug;

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
        Err(error) => {
            // Miss or I/O error: deny without leaking block presence.
            // Matches Go's error path in hasAccess (returns false on
            // blockstore.Get failure).
            debug!(
                cid = %cid,
                peer = %peer_id,
                %error,
                "bitswap request denied: block unavailable from store"
            );
            return false;
        }
    };
    let data = block.data();

    // Signature blocks are required for peers to verify data they already
    // have, and carry no authored data themselves. Always serve.
    if Signature::from_dag_cbor(data).is_ok() {
        return true;
    }

    match DefraBlock::from_dag_cbor(data) {
        Ok(defra_block) => {
            if defra_block.delta.is_definition() {
                return true;
            }
            let Some(collection_id) = defra_block.delta.schema_version_id() else {
                debug!(
                    cid = %cid,
                    peer = %peer_id,
                    "bitswap request denied: data delta missing collection id"
                );
                return false;
            };
            let peer_str = peer_id.to_string();
            if registry.is_replicator(collection_id, &peer_str) {
                return true;
            }
            debug!(
                cid = %cid,
                peer = %peer_id,
                collection = %collection_id,
                "bitswap request denied: peer not a replicator for collection"
            );
            false
        }
        Err(_) => {
            // Not a CRDT block. Go's hasAccess lets lens IPLD artifacts
            // (config / module / WASM / chunks) through unconditionally —
            // they carry no user data. Match that or deny.
            if is_lens_block(data) {
                return true;
            }
            debug!(
                cid = %cid,
                peer = %peer_id,
                "bitswap request denied: block is neither CRDT, signature, nor lens"
            );
            false
        }
    }
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

    /// Schema/collection definitions are broadcast to every replicator
    /// regardless of per-collection registration. A peer that has no
    /// entry in the registry must still be able to fetch them.
    #[tokio::test]
    async fn definition_delta_is_served_without_registry_check() {
        use defra_core::{CollectionSetDeltaPayload, CrdtDelta};

        let delta = CrdtDelta::CollectionSet(CollectionSetDeltaPayload::new(1));
        assert!(
            delta.is_definition(),
            "test precondition: CollectionSet is a definition delta"
        );
        let block = DefraBlock::new(delta, Vec::new(), Vec::new());
        let bytes = block.to_dag_cbor().unwrap();
        let cid = cid_for(&bytes);

        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        store.put(cid, bytes);

        let peer = PeerId::random();
        let allowed = check_access(AccessMode::Controlled, &registry, &store, &peer, &cid).await;
        assert!(
            allowed,
            "definition deltas must be served even to non-replicator peers"
        );
    }

    /// Go parity: lens schema-migration blocks (`lens` / `modules` /
    /// `wasmBytes` / `chunks`) must be served to any peer in Controlled
    /// mode — they carry no user data and have to propagate so schema
    /// migrations converge across the network.
    #[tokio::test]
    async fn lens_config_block_is_served_without_registry_check() {
        use defra_core::{build_lens_ipld_blocks, CidBlock};

        let wasm = b"wasm-bytes-test-payload".to_vec();
        let (_config_cid, blocks): (_, Vec<CidBlock>) =
            build_lens_ipld_blocks(&wasm, false, &[]).unwrap();

        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        for (cid, bytes) in &blocks {
            store.put(*cid, bytes.clone());
        }

        let peer = PeerId::random();
        for (cid, _) in &blocks {
            let allowed = check_access(AccessMode::Controlled, &registry, &store, &peer, cid).await;
            assert!(
                allowed,
                "lens block at {cid} must be served without replicator trust"
            );
        }
    }
}
