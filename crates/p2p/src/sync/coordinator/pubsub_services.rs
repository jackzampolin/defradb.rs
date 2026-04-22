//! `pubsub_rpc`-based DocSync / BranchableSync services (#828).
//!
//! Wraps two [`TopicHandler`]s — one per base topic — with message handlers
//! that serve DocSync heads and BranchableSync heads to peers over
//! gossipsub. Owned by the `SyncCoordinator`; instantiated when the
//! transport exposes a libp2p peer identity (so the response sub-topic
//! name can be computed with the `<base>/<peer>/_response` pattern).
//!
//! Iroh transports don't expose a libp2p peer id and their gossipsub
//! equivalent doesn't route raw bytes through this layer, so the
//! coordinator skips starting these services on iroh and falls back to
//! the two-stream DocSync/Branchable paths.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use crate::bitswap::{AccessMode, ReplicatorRegistry};
use crate::message::pubsub as wire;
use crate::pubsub_rpc::{MessageHandler, TopicHandler};
use crate::sync::head_provider::DocumentHeadProvider;
use crate::sync::peer_state::PeerStateTracker;

pub(super) const DOC_SYNC_TOPIC: &str = "doc-sync";
pub(super) const BRANCHABLE_SYNC_TOPIC: &str = "sync-branchable";

/// Bundle of pubsub_rpc services for a single SyncCoordinator.
///
/// `Arc<TopicHandler>` is clone-safe and internally reference-counted;
/// handlers are shared between the inbound dispatcher and the outbound
/// publish path.
pub(crate) struct PubsubServices {
    pub(super) doc_sync: Arc<TopicHandler>,
    pub(super) branchable_sync: Arc<TopicHandler>,
}

impl PubsubServices {
    /// Build services with message handlers that serve heads locally.
    ///
    /// Returns `None` when `local_peer_id_str` cannot be parsed as a
    /// libp2p peer id — the pubsub_rpc layer is libp2p-specific, so on
    /// non-libp2p transports the coordinator silently skips these
    /// services (the two-stream path remains functional).
    pub(super) fn try_new(
        local_peer_id_str: &str,
        head_provider: Arc<dyn DocumentHeadProvider>,
        replicators: Arc<ReplicatorRegistry>,
        peer_state: Arc<PeerStateTracker>,
        access_mode: AccessMode,
        subscribed_collections: Arc<tokio::sync::RwLock<std::collections::HashSet<String>>>,
    ) -> Option<Self> {
        let self_peer: libp2p::PeerId = local_peer_id_str.parse().ok()?;
        let shared = Arc::new(HandlerContext {
            head_provider,
            replicators,
            peer_state,
            access_mode,
            subscribed_collections,
            local_peer_id: local_peer_id_str.to_string(),
        });
        let doc_sync = Arc::new(TopicHandler::new(
            DOC_SYNC_TOPIC,
            self_peer,
            Arc::new(DocSyncHandler {
                ctx: Arc::clone(&shared),
            }),
        ));
        let branchable_sync = Arc::new(TopicHandler::new(
            BRANCHABLE_SYNC_TOPIC,
            self_peer,
            Arc::new(BranchableSyncHandler {
                ctx: Arc::clone(&shared),
            }),
        ));
        Some(Self {
            doc_sync,
            branchable_sync,
        })
    }

    /// Return the TopicHandler for `topic` if this services bundle owns it.
    /// Matches either the base topic exactly or any sub-topic that starts
    /// with `<base>/` (the response sub-topics).
    pub(super) fn handler_for_topic(&self, topic: &str) -> Option<&Arc<TopicHandler>> {
        if topic == DOC_SYNC_TOPIC || topic.starts_with(concat!("doc-sync", "/")) {
            Some(&self.doc_sync)
        } else if topic == BRANCHABLE_SYNC_TOPIC
            || topic.starts_with(concat!("sync-branchable", "/"))
        {
            Some(&self.branchable_sync)
        } else {
            None
        }
    }
}

/// Shared state captured by both message handlers.
struct HandlerContext {
    head_provider: Arc<dyn DocumentHeadProvider>,
    replicators: Arc<ReplicatorRegistry>,
    peer_state: Arc<PeerStateTracker>,
    access_mode: AccessMode,
    subscribed_collections: Arc<tokio::sync::RwLock<std::collections::HashSet<String>>>,
    local_peer_id: String,
}

impl HandlerContext {
    /// Returns `true` when `peer` is authorised to ask for doc-sync heads.
    /// Mirrors `SyncCoordinator::check_peer_is_replicator` at
    /// `coordinator/access.rs:125`: Open mode always allows; Controlled
    /// mode allows peers that are either registered as replicators for
    /// at least one collection or currently connected.
    fn peer_may_doc_sync(&self, peer: &libp2p::PeerId) -> bool {
        if self.access_mode.is_open() {
            return true;
        }
        let peer_str = peer.to_string();
        self.replicators.is_any_replicator(&peer_str) || self.peer_state.is_connected(&peer_str)
    }

    /// Returns `true` when `peer` is authorised to ask for branchable heads
    /// of `collection_id`. Collection-scoped protocols always require the
    /// collection to be locally subscribed (see coordinator docs at
    /// `mod.rs:30-37`).
    async fn peer_may_branchable_sync(&self, peer: &libp2p::PeerId, collection_id: &str) -> bool {
        if !self
            .subscribed_collections
            .read()
            .await
            .contains(collection_id)
        {
            return false;
        }
        if self.access_mode.is_open() {
            return true;
        }
        let peer_str = peer.to_string();
        self.replicators.is_replicator(collection_id, &peer_str)
            || self.peer_state.is_connected(&peer_str)
    }
}

struct DocSyncHandler {
    ctx: Arc<HandlerContext>,
}

#[async_trait]
impl MessageHandler for DocSyncHandler {
    async fn handle(&self, from: libp2p::PeerId, data: Vec<u8>) -> Result<Vec<u8>, String> {
        if !self.ctx.peer_may_doc_sync(&from) {
            return Err(format!("peer {from} not authorised for doc-sync"));
        }

        let req: wire::DocSyncRequest =
            ciborium::from_reader(data.as_slice()).map_err(|e| format!("decode request: {e}"))?;

        let mut results = Vec::new();
        for doc_id in &req.doc_ids {
            match self.ctx.head_provider.get_document_heads(doc_id).await {
                Ok(heads) if !heads.is_empty() => {
                    results.push(wire::DocSyncItem {
                        doc_id: doc_id.clone(),
                        heads: heads.iter().map(|cid| cid.to_bytes()).collect(),
                    });
                }
                Ok(_) => {} // no heads — skip, per Go's `if len(result.Heads) > 0` guard
                Err(e) => {
                    warn!(doc_id = %doc_id, error = %e, "doc-sync: head lookup failed");
                }
            }
        }

        let reply = wire::DocSyncReply {
            results,
            sender: self.ctx.local_peer_id.clone(),
        };
        let mut bytes = Vec::new();
        ciborium::into_writer(&reply, &mut bytes).map_err(|e| format!("encode reply: {e}"))?;
        Ok(bytes)
    }
}

struct BranchableSyncHandler {
    ctx: Arc<HandlerContext>,
}

#[async_trait]
impl MessageHandler for BranchableSyncHandler {
    async fn handle(&self, from: libp2p::PeerId, data: Vec<u8>) -> Result<Vec<u8>, String> {
        let req: wire::BranchableSyncRequest =
            ciborium::from_reader(data.as_slice()).map_err(|e| format!("decode request: {e}"))?;

        if !self
            .ctx
            .peer_may_branchable_sync(&from, &req.collection_id)
            .await
        {
            return Err(format!(
                "peer {from} not authorised for branchable-sync of {}",
                req.collection_id
            ));
        }

        let heads = match self
            .ctx
            .head_provider
            .get_collection_heads(&req.collection_id)
            .await
        {
            Ok(heads) => heads.iter().map(|cid| cid.to_bytes()).collect(),
            Err(e) => {
                warn!(
                    collection_id = %req.collection_id,
                    error = %e,
                    "branchable-sync: collection head lookup failed"
                );
                Vec::new()
            }
        };

        let reply = wire::BranchableSyncReply {
            collection_id: req.collection_id,
            heads,
            sender: self.ctx.local_peer_id.clone(),
        };
        let mut bytes = Vec::new();
        ciborium::into_writer(&reply, &mut bytes).map_err(|e| format!("encode reply: {e}"))?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::head_provider::NoOpHeadProvider;

    fn a_libp2p_peer() -> libp2p::PeerId {
        libp2p::PeerId::from_public_key(&libp2p::identity::Keypair::generate_ed25519().public())
    }

    #[tokio::test]
    async fn handler_for_topic_routes_doc_sync_base() {
        let peer = a_libp2p_peer();
        let services = PubsubServices::try_new(
            &peer.to_string(),
            Arc::new(NoOpHeadProvider),
            Arc::new(ReplicatorRegistry::new()),
            Arc::new(PeerStateTracker::new()),
            AccessMode::Open,
            Arc::new(tokio::sync::RwLock::new(std::collections::HashSet::new())),
        )
        .expect("local peer parses");

        assert!(services.handler_for_topic(DOC_SYNC_TOPIC).is_some());
        assert!(services
            .handler_for_topic("doc-sync/abc/_response")
            .is_some());
        assert!(services.handler_for_topic(BRANCHABLE_SYNC_TOPIC).is_some());
        assert!(services
            .handler_for_topic("sync-branchable/xyz/_response")
            .is_some());
        assert!(services.handler_for_topic("unrelated").is_none());
        // A subtopic prefix match on just the base name (no slash) must not
        // cross-route to the other handler.
        assert!(services.handler_for_topic("doc-syncoid").is_none());
    }

    #[tokio::test]
    async fn try_new_returns_none_for_non_libp2p_peer_id() {
        let services = PubsubServices::try_new(
            "iroh-node-12345",
            Arc::new(NoOpHeadProvider),
            Arc::new(ReplicatorRegistry::new()),
            Arc::new(PeerStateTracker::new()),
            AccessMode::Open,
            Arc::new(tokio::sync::RwLock::new(std::collections::HashSet::new())),
        );
        assert!(services.is_none());
    }

    #[tokio::test]
    async fn doc_sync_handler_returns_heads_for_known_doc() {
        use async_trait::async_trait;
        use cid::Cid;

        struct HeadProviderStub;
        #[async_trait]
        impl DocumentHeadProvider for HeadProviderStub {
            async fn get_document_heads(&self, doc_id: &str) -> crate::error::Result<Vec<Cid>> {
                if doc_id == "known" {
                    Ok(vec![Cid::try_from(
                        "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku",
                    )
                    .unwrap()])
                } else {
                    Ok(Vec::new())
                }
            }
            async fn get_collection_heads(&self, _col: &str) -> crate::error::Result<Vec<Cid>> {
                Ok(Vec::new())
            }
        }

        let peer = a_libp2p_peer();
        let services = PubsubServices::try_new(
            &peer.to_string(),
            Arc::new(HeadProviderStub),
            Arc::new(ReplicatorRegistry::new()),
            Arc::new(PeerStateTracker::new()),
            AccessMode::Open,
            Arc::new(tokio::sync::RwLock::new(std::collections::HashSet::new())),
        )
        .unwrap();

        let req = wire::DocSyncRequest::new(vec!["known".into(), "missing".into()]);
        let mut req_bytes = Vec::new();
        ciborium::into_writer(&req, &mut req_bytes).unwrap();

        let remote = a_libp2p_peer();
        let outcome = services
            .doc_sync
            .deliver_gossip_message(DOC_SYNC_TOPIC, remote, req_bytes)
            .await;

        use crate::pubsub_rpc::DeliveryOutcome;
        let response = match outcome {
            DeliveryOutcome::Respond(r) => r,
            other => panic!("expected Respond, got {other:?}"),
        };
        assert_eq!(response.topic, format!("doc-sync/{remote}/_response"));
    }

    #[tokio::test]
    async fn doc_sync_handler_restricted_rejects_non_replicator() {
        let peer = a_libp2p_peer();
        let services = PubsubServices::try_new(
            &peer.to_string(),
            Arc::new(NoOpHeadProvider),
            Arc::new(ReplicatorRegistry::new()),
            Arc::new(PeerStateTracker::new()),
            AccessMode::Controlled,
            Arc::new(tokio::sync::RwLock::new(std::collections::HashSet::new())),
        )
        .unwrap();

        let req = wire::DocSyncRequest::new(vec!["a".into()]);
        let mut req_bytes = Vec::new();
        ciborium::into_writer(&req, &mut req_bytes).unwrap();

        let outcome = services
            .doc_sync
            .deliver_gossip_message(DOC_SYNC_TOPIC, a_libp2p_peer(), req_bytes)
            .await;

        // Handler error wraps into an `err` field in the response envelope.
        use crate::pubsub_rpc::{DeliveryOutcome, InternalResponse};
        let response = match outcome {
            DeliveryOutcome::Respond(r) => r,
            other => panic!("expected Respond, got {other:?}"),
        };
        let envelope = InternalResponse::from_cbor(&response.bytes).unwrap();
        assert!(envelope.err.contains("not authorised"));
    }
}
