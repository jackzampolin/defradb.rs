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
//! 3. Classifier allows signature / definition / lens blocks.
//! 4. Data blocks get metadata from the classifier.
//! 5. Replicators for the block's stable collection id bypass ACP.
//! 6. Non-replicators resolve identity and run the late-bound ACP read gate.
//! 7. Unknown blocks or ACP errors deny.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use cid::Cid;
use iroh_bitswap::Store;
use libp2p::PeerId;
use tracing::debug;

use super::{AccessMode, BlockClass, BlockClassifier, LateBoundServeAcp, ReplicatorRegistry};

/// Build a filter closure that satisfies iroh-bitswap's
/// `PeerBlockRequestFilter` trait.
///
/// The returned closure owns clones of `registry` and `store`, so the
/// caller can construct it once and hand it to `BitswapConfig`.
pub fn make_peer_block_access_filter<S>(
    mode: AccessMode,
    registry: Arc<ReplicatorRegistry>,
    store: S,
    classifier: Arc<dyn BlockClassifier>,
    serve_acp: Arc<LateBoundServeAcp>,
) -> impl Fn(&PeerId, &Cid) -> Pin<Box<dyn Future<Output = bool> + Send + 'static>> + Send + Sync + 'static
where
    S: Store + Clone + Send + Sync + 'static,
{
    move |peer_id: &PeerId, cid: &Cid| {
        let peer_id = *peer_id;
        let cid = *cid;
        let registry = Arc::clone(&registry);
        let classifier = Arc::clone(&classifier);
        let serve_acp = Arc::clone(&serve_acp);
        let store = store.clone();
        Box::pin(async move {
            check_access(
                mode,
                &registry,
                &store,
                classifier.as_ref(),
                serve_acp.as_ref(),
                &peer_id,
                &cid,
            )
            .await
        })
    }
}

async fn check_access<S: Store>(
    mode: AccessMode,
    registry: &ReplicatorRegistry,
    store: &S,
    classifier: &dyn BlockClassifier,
    serve_acp: &LateBoundServeAcp,
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

    match classifier.classify(cid, data).await {
        BlockClass::Allow => true,
        BlockClass::Deny => {
            debug!(cid = %cid, peer = %peer_id, "bitswap request denied by block classifier");
            false
        }
        BlockClass::Data(meta) => {
            let peer_str = peer_id.to_string();
            if registry.is_filtered_replicator(&meta.collection_id, &peer_str) {
                debug!(
                    cid = %cid,
                    peer = %peer_id,
                    collection = %meta.collection_id,
                    "bitswap request denied: filtered replicator data-block access uses direct push"
                );
                return false;
            }
            if registry.is_replicator(&meta.collection_id, &peer_str) {
                return true;
            }

            let Some(serve) = serve_acp.get() else {
                debug!(
                    cid = %cid,
                    peer = %peer_id,
                    collection = %meta.collection_id,
                    "bitswap request denied: serve ACP gate not installed"
                );
                return false;
            };
            let transport_peer_id = crate::transport::PeerId::from(peer_id);
            let identity = match serve.resolver.resolve(&transport_peer_id).await {
                Some(did) => acp::Identity::Authenticated(did),
                None => acp::Identity::Anonymous,
            };
            if serve.gate.may_read(&identity, &meta).await {
                return true;
            }
            debug!(
                cid = %cid,
                peer = %peer_id,
                collection = %meta.collection_id,
                identity = %identity,
                "bitswap request denied: ACP read gate denied"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitswap::{
        AllowAllBlockReadGate, BlockAcpMeta, DefaultBlockClassifier, LateBoundServeAcp, ServeAcp,
    };
    use crate::peer_identity::AnonymousResolver;
    use crate::replicator::{ReplicationFilter, ReplicationFilters, ReplicatorInfo};
    use async_trait::async_trait;
    use defra_core::Block as DefraBlock;
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

    async fn check_access_with_defaults<S: Store>(
        mode: AccessMode,
        registry: &ReplicatorRegistry,
        store: &S,
        peer_id: &PeerId,
        cid: &Cid,
    ) -> bool {
        let classifier = DefaultBlockClassifier;
        let serve_acp = LateBoundServeAcp::new();
        check_access(mode, registry, store, &classifier, &serve_acp, peer_id, cid).await
    }

    async fn check_access_with_data_classifier<S: Store>(
        mode: AccessMode,
        registry: &ReplicatorRegistry,
        store: &S,
        peer_id: &PeerId,
        cid: &Cid,
        collection_id: &str,
    ) -> bool {
        let classifier = StaticClassifier(BlockClass::Data(BlockAcpMeta {
            collection_id: collection_id.to_string(),
            is_branchable: false,
            policy: None,
            doc_ids: vec!["doc1".to_string()],
        }));
        let serve_acp = LateBoundServeAcp::new();
        check_access(mode, registry, store, &classifier, &serve_acp, peer_id, cid).await
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
        let allowed =
            check_access_with_defaults(AccessMode::Open, &registry, &store, &peer, &cid).await;
        assert!(allowed);
    }

    #[tokio::test]
    async fn controlled_mode_denies_unknown_peer() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        let (cid, bytes) = make_data_block("users");
        store.put(cid, bytes);

        let peer = PeerId::random();
        let allowed =
            check_access_with_defaults(AccessMode::Controlled, &registry, &store, &peer, &cid)
                .await;
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

        let allowed = check_access_with_data_classifier(
            AccessMode::Controlled,
            &registry,
            &store,
            &peer,
            &cid,
            "users",
        )
        .await;
        assert!(allowed, "registered replicator must be served");
    }

    #[tokio::test]
    async fn controlled_mode_denies_filtered_replicator_data_block_requests() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        let (cid, bytes) = make_data_block("users");
        store.put(cid, bytes);

        let peer = PeerId::random();
        let mut filters = ReplicationFilters::new();
        filters.insert(
            "users".to_string(),
            ReplicationFilter::new("agent_did", serde_json::json!("did:key:z6M")),
        );
        registry.set_replicator_info(ReplicatorInfo::from_raw_with_filters(
            peer.to_string(),
            vec!["users".to_string()],
            vec![],
            filters,
        ));

        let allowed = check_access_with_data_classifier(
            AccessMode::Controlled,
            &registry,
            &store,
            &peer,
            &cid,
            "users",
        )
        .await;
        assert!(
            !allowed,
            "filtered replicators receive matching full DAGs by direct push, not collection-wide Bitswap"
        );
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

        let allowed = check_access_with_data_classifier(
            AccessMode::Controlled,
            &registry,
            &store,
            &peer,
            &cid,
            "users",
        )
        .await;
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
            check_access_with_defaults(AccessMode::Controlled, &registry, &store, &peer, &missing)
                .await;
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
        let allowed =
            check_access_with_defaults(AccessMode::Controlled, &registry, &store, &peer, &cid)
                .await;
        assert!(
            allowed,
            "signature blocks must be served even in Controlled mode"
        );
    }

    /// Schema/collection definitions are broadcast to every peer regardless of
    /// per-collection registration (they carry no user data and must propagate
    /// for schema convergence). A peer with no registry entry can fetch them.
    #[tokio::test]
    async fn definition_delta_is_served_without_registry_check() {
        use defra_core::{CollectionDefinitionDeltaPayload, CrdtDelta};

        let delta = CrdtDelta::CollectionDefinition(CollectionDefinitionDeltaPayload::new(1));
        assert!(
            delta.is_definition(),
            "test precondition: CollectionDefinition is a definition delta"
        );
        let block = DefraBlock::new(delta, Vec::new(), Vec::new());
        let bytes = block.to_dag_cbor().unwrap();
        let cid = cid_for(&bytes);

        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        store.put(cid, bytes);

        let peer = PeerId::random();
        let allowed =
            check_access_with_defaults(AccessMode::Controlled, &registry, &store, &peer, &cid)
                .await;
        assert!(
            allowed,
            "definition deltas must be served even to non-replicator peers"
        );
    }

    /// Go parity: a `CollectionSet` block (circular-relation group root) is NOT
    /// a definition — it has no collection-version id, cannot be ACP-scoped, and
    /// is never transferred over P2P (circular-relation groups converge by local
    /// reconstruction). A non-replicator, unauthorized peer must be DENIED.
    #[tokio::test]
    async fn collection_set_block_is_denied_to_non_replicator() {
        use defra_core::{CollectionSetDeltaPayload, CrdtDelta};

        let delta = CrdtDelta::CollectionSet(CollectionSetDeltaPayload::new(1));
        assert!(
            !delta.is_definition(),
            "CollectionSet must not be classified as a servable definition"
        );
        let block = DefraBlock::new(delta, Vec::new(), Vec::new());
        let bytes = block.to_dag_cbor().unwrap();
        let cid = cid_for(&bytes);

        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        store.put(cid, bytes);

        let peer = PeerId::random();
        let allowed =
            check_access_with_defaults(AccessMode::Controlled, &registry, &store, &peer, &cid)
                .await;
        assert!(
            !allowed,
            "CollectionSet blocks must not be served to a non-replicator peer"
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
            let allowed =
                check_access_with_defaults(AccessMode::Controlled, &registry, &store, &peer, cid)
                    .await;
            assert!(
                allowed,
                "lens block at {cid} must be served without replicator trust"
            );
        }
    }

    #[tokio::test]
    async fn controlled_mode_uses_late_bound_gate_for_non_replicator() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        let cid = cid_for(b"opaque-data-block");
        store.put(cid, b"opaque-data-block".to_vec());
        let classifier = StaticClassifier(BlockClass::Data(BlockAcpMeta {
            collection_id: "users".to_string(),
            is_branchable: true,
            policy: Some(("policy1".to_string(), "user".to_string())),
            doc_ids: vec!["doc1".to_string()],
        }));
        let serve_acp = LateBoundServeAcp::new();
        serve_acp.set(ServeAcp {
            resolver: Arc::new(AnonymousResolver),
            gate: Arc::new(AllowAllBlockReadGate),
        });

        let peer = PeerId::random();
        let allowed = check_access(
            AccessMode::Controlled,
            &registry,
            &store,
            &classifier,
            &serve_acp,
            &peer,
            &cid,
        )
        .await;
        assert!(
            allowed,
            "non-replicator data blocks must be served when the late-bound gate grants read"
        );
    }

    #[tokio::test]
    async fn controlled_mode_replicator_bypasses_uninstalled_gate() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let store = InMemoryStore::default();
        let cid = cid_for(b"opaque-data-block");
        store.put(cid, b"opaque-data-block".to_vec());
        let classifier = StaticClassifier(BlockClass::Data(BlockAcpMeta {
            collection_id: "users".to_string(),
            is_branchable: true,
            policy: Some(("policy1".to_string(), "user".to_string())),
            doc_ids: vec!["doc1".to_string()],
        }));
        let serve_acp = LateBoundServeAcp::new();

        let peer = PeerId::random();
        registry.add_replicator("users", &peer.to_string());
        let allowed = check_access(
            AccessMode::Controlled,
            &registry,
            &store,
            &classifier,
            &serve_acp,
            &peer,
            &cid,
        )
        .await;
        assert!(
            allowed,
            "replicator passthrough must not depend on serve ACP initialization"
        );
    }

    #[derive(Clone)]
    struct StaticClassifier(BlockClass);

    #[async_trait]
    impl BlockClassifier for StaticClassifier {
        async fn classify(&self, _cid: &Cid, _data: &[u8]) -> BlockClass {
            self.0.clone()
        }
    }
}
