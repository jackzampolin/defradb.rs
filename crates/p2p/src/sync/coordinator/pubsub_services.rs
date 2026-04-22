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

    // End-to-end byte-parity test: Go-produced request bytes are fed to
    // our DocSyncHandler and the resulting response envelope must be
    // byte-identical to the one Go's `go-libp2p-pubsub-rpc` would emit.
    // Fixtures generated by `testdata/gen_pubsub_rpc_fixture/main.go`.
    //
    // This exercises the full inbound pipeline: request decode →
    // bounded deserialization → head lookup → reply encode
    // (fxamacker/cbor ↔ ciborium) → dag-cbor envelope (ipld.Marshal ↔
    // ciborium canonical order) → per-peer response topic naming.
    #[tokio::test]
    async fn go_parity_doc_sync_round_trip() {
        use async_trait::async_trait;
        use cid::Cid;

        // From `go run testdata/gen_pubsub_rpc_fixture/main.go`:
        //   request.doc_ids:  ["docA"]
        //   sender:           "12D3KooWRustPeer"
        //   head-for-docA:    CIDv1(raw, sha256("docA-head"))
        let go_request_hex = "a166646f634944738164646f6341";
        let go_head_cid_hex =
            "01551220ac695a770969c9a3ae934d1e3325839c60bfe0e0d265e5494748ea46939d19a6";
        let go_envelope_hex = "a4624944783b6261666b726569626579647533777a736d73677a687473747967366169623667377565667575637075786e7570726e75377366796d6c366a68776d63457272606444617461585ba267726573756c747381a265646f63494464646f634165686561647381582401551220ac695a770969c9a3ae934d1e3325839c60bfe0e0d265e5494748ea46939d19a66673656e64657270313244334b6f6f5752757374506565726446726f6d40";

        let head_bytes = hex::decode(go_head_cid_hex).unwrap();
        let head_cid_expected =
            Cid::try_from(head_bytes.as_slice()).expect("fixture head is a valid CID");

        struct DocAHeadProvider {
            head: Cid,
        }
        #[async_trait]
        impl DocumentHeadProvider for DocAHeadProvider {
            async fn get_document_heads(&self, doc_id: &str) -> crate::error::Result<Vec<Cid>> {
                if doc_id == "docA" {
                    Ok(vec![self.head])
                } else {
                    Ok(Vec::new())
                }
            }
            async fn get_collection_heads(&self, _col: &str) -> crate::error::Result<Vec<Cid>> {
                Ok(Vec::new())
            }
        }

        // Build the handler directly so we control the sender string (the Go
        // fixture uses "12D3KooWRustPeer" verbatim; libp2p::PeerId parsing
        // isn't exercised on this path).
        let ctx = Arc::new(HandlerContext {
            head_provider: Arc::new(DocAHeadProvider {
                head: head_cid_expected,
            }),
            replicators: Arc::new(ReplicatorRegistry::new()),
            peer_state: Arc::new(PeerStateTracker::new()),
            access_mode: AccessMode::Open,
            subscribed_collections: Arc::new(tokio::sync::RwLock::new(
                std::collections::HashSet::new(),
            )),
            local_peer_id: "12D3KooWRustPeer".to_string(),
        });
        let handler = DocSyncHandler {
            ctx: Arc::clone(&ctx),
        };

        // Exercise the handler directly.
        let req_bytes = hex::decode(go_request_hex).unwrap();
        let remote = a_libp2p_peer();
        let reply_bytes = MessageHandler::handle(&handler, remote, req_bytes.clone())
            .await
            .expect("handler");

        // Build the envelope the same way TopicHandler would: ID from
        // derive_request_id(req_bytes), Err/From empty.
        use crate::pubsub_rpc::{derive_request_id, InternalResponse};
        let envelope = InternalResponse {
            id: derive_request_id(&req_bytes).to_string(),
            err: String::new(),
            data: reply_bytes,
            from: Vec::new(),
        };
        let envelope_bytes = envelope.to_cbor().expect("encode envelope");

        let got_hex = hex::encode(&envelope_bytes);
        assert_eq!(
            got_hex,
            go_envelope_hex,
            "Rust response envelope must be byte-identical to Go's fixture\n  \
             Rust len={}, Go len={}",
            envelope_bytes.len(),
            go_envelope_hex.len() / 2
        );
    }

    // Reverse direction: a Go-produced envelope (with its inner
    // docSyncReply) must decode cleanly on the Rust side and yield the
    // expected DocSyncReply struct. This validates the receiving half
    // of the parity claim (caller-side of a pubsub_rpc exchange).
    #[tokio::test]
    async fn go_parity_doc_sync_envelope_decodes_on_rust() {
        use crate::pubsub_rpc::InternalResponse;

        let go_envelope_hex = "a4624944783b6261666b726569626579647533777a736d73677a687473747967366169623667377565667575637075786e7570726e75377366796d6c366a68776d63457272606444617461585ba267726573756c747381a265646f63494464646f634165686561647381582401551220ac695a770969c9a3ae934d1e3325839c60bfe0e0d265e5494748ea46939d19a66673656e64657270313244334b6f6f5752757374506565726446726f6d40";

        let bytes = hex::decode(go_envelope_hex).unwrap();
        let env = InternalResponse::from_cbor(&bytes).expect("envelope decode");
        assert_eq!(env.err, "");
        assert!(
            env.from.is_empty(),
            "wire From field is empty (receiver fills from transport source)"
        );
        assert_eq!(
            env.id, "bafkreibeydu3wzsmsgzhtstyg6aib6g7uefuucpuxnuprnu7sfyml6jhwm",
            "envelope id must match CIDv1(raw, sha256(request_bytes))"
        );

        // env.data is a nested CBOR-encoded DocSyncReply.
        let reply: wire::DocSyncReply =
            ciborium::from_reader(env.data.as_slice()).expect("inner reply decode");
        assert_eq!(reply.sender, "12D3KooWRustPeer");
        assert_eq!(reply.results.len(), 1);
        let item = &reply.results[0];
        assert_eq!(item.doc_id, "docA");
        assert_eq!(item.heads.len(), 1);

        // And the head parses as a CID.
        let head = cid::Cid::try_from(item.heads[0].as_slice()).expect("head is CID");
        assert_eq!(head.codec(), 0x55, "head is CIDv1 raw");
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
