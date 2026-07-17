//! Concrete `KmsService` implementation.
//!
//! Composes a `KeyStore`, zero or more `KeyTransport`s, and one
//! `AccessPolicy`. Cross-peer fetch fans out across transports; replies
//! are ECIES envelopes addressed to the requester's per-request
//! ephemeral pubkey.

use async_trait::async_trait;
use identity::Did;
use std::sync::{Arc, RwLock};

use crate::context::RequestContext;
use crate::error::{Error, Result};
use crate::policy::AccessPolicy;
use crate::results::KeyResults;
use crate::service::{KmsService, PeerIdentity};
use crate::store::{KeyStore, StoredKey};
use crate::transport::{EncodedFetchRequest, KeyTransport};
use crate::types::{EncryptionCid, KeyScope, PolicyDecision};
use crate::wire::{FetchEncryptionKeyReply, FetchEncryptionKeyRequest};

#[cfg(not(target_arch = "wasm32"))]
fn spawn_task<F>(future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}

#[cfg(target_arch = "wasm32")]
fn spawn_task<F>(future: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

/// Default `KmsService` implementation.
pub struct DefraKms {
    store: Arc<dyn KeyStore>,
    transports: Vec<Arc<dyn KeyTransport>>,
    policy: Arc<dyn AccessPolicy>,
    doc_resolver: Arc<dyn BlockDocIDResolver>,
    /// Fallback identity used when `RequestContext::user_identity` is None
    /// (gossip-triggered syncs with no caller).
    node_identity: Did,
    /// This node's transport-level peer id, bound into the ECIES AAD of
    /// served replies (Go's `makeAssociatedData`). Empty until the node
    /// wiring sets it from the P2P transport.
    local_peer_id: RwLock<String>,
    /// Test-only deterministic ephemeral. `None` in prod ⇒ fresh per call.
    test_ephemeral: RwLock<Option<x25519_dalek::StaticSecret>>,
}

impl DefraKms {
    /// Construct a new `DefraKms`. Transports may be empty (single-node).
    pub fn new(
        store: Arc<dyn KeyStore>,
        transports: Vec<Arc<dyn KeyTransport>>,
        policy: Arc<dyn AccessPolicy>,
        doc_resolver: Arc<dyn BlockDocIDResolver>,
        node_identity: Did,
    ) -> Self {
        Self {
            store,
            transports,
            policy,
            doc_resolver,
            node_identity,
            local_peer_id: RwLock::new(String::new()),
            test_ephemeral: RwLock::new(None),
        }
    }

    fn principal<'a>(&'a self, ctx: &'a RequestContext) -> &'a Did {
        ctx.user_identity().unwrap_or(&self.node_identity)
    }

    fn local_peer_id(&self) -> String {
        self.local_peer_id
            .read()
            .map(|g| g.clone())
            .unwrap_or_default()
    }

    fn ephemeral(&self) -> x25519_dalek::StaticSecret {
        self.test_ephemeral
            .read()
            .ok()
            .and_then(|g| g.clone())
            .unwrap_or_else(|| crypto::generate_x25519().expect("OsRng"))
    }

    /// Test hook: inject a deterministic ephemeral so a unit test can
    /// decrypt replies produced by a `FakeTransport`.
    #[cfg(test)]
    pub(crate) fn set_ephemeral_for_test(&self, eph: x25519_dalek::StaticSecret) {
        if let Ok(mut g) = self.test_ephemeral.write() {
            *g = Some(eph);
        }
    }

    /// Local release decision for the DEK stored under `cid`.
    ///
    /// Encryption blocks carry no identity (Go #4838): ownership is
    /// resolved through the block-CID -> DocID index, and the key is
    /// released if the actor may read ANY owning document (an encryption
    /// block can be co-owned by several documents). When ownership is not
    /// yet recorded — e.g. a block just fetched during replication, before
    /// its merge registers ownership — the node cannot authorize locally
    /// and defers to the owner via a remote fetch.
    async fn local_release_decision(
        &self,
        actor: Option<&Did>,
        cid: &EncryptionCid,
    ) -> Result<LocalRelease> {
        let doc_ids = self.doc_resolver.doc_ids_for_block(cid).await?;
        if doc_ids.is_empty() {
            return Ok(LocalRelease::OwnershipUnknown);
        }
        for doc_id in doc_ids {
            let scope = KeyScope::Document {
                doc_id,
                field: None,
            };
            if matches!(
                self.policy.check_release(actor, &scope).await?,
                PolicyDecision::Allow
            ) {
                return Ok(LocalRelease::Allow);
            }
        }
        Ok(LocalRelease::Deny)
    }

    /// Serve-side release decision: the owner node has recorded ownership,
    /// so unknown ownership is treated as a denial (never leak a key we
    /// cannot attribute).
    async fn may_serve(&self, actor: Option<&Did>, cid: &EncryptionCid) -> Result<bool> {
        Ok(matches!(
            self.local_release_decision(actor, cid).await?,
            LocalRelease::Allow
        ))
    }
}

/// Outcome of a local DEK release check.
enum LocalRelease {
    /// The actor may read an owning document; release the key.
    Allow,
    /// Ownership is known but the actor may read none of the owners.
    Deny,
    /// No owning document is recorded locally yet; defer to a remote fetch.
    OwnershipUnknown,
}

/// Resolves which documents own a block, via the node's
/// block-CID -> DocID index (mirrors Go's `ResolveBlockDocIDs`).
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait BlockDocIDResolver: defra_core::thread_bounds::MaybeSendSync {
    async fn doc_ids_for_block(&self, cid: &EncryptionCid) -> Result<Vec<String>>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl KmsService for DefraKms {
    async fn get_keys(&self, ctx: &RequestContext, cids: &[EncryptionCid]) -> Result<KeyResults> {
        let (results, tx) = KeyResults::new(cids.len().max(1));
        // Local policy check uses the caller's actual identity (may be None
        // for anonymous callers). The node-identity fallback is only correct
        // on the WIRE path for gossip-triggered syncs (per Go PR #4778), not
        // on the local pass.
        let user_actor: Option<Did> = ctx.user_identity().cloned();

        // Local pass: serve any CIDs we hold.
        let mut remote: Vec<EncryptionCid> = Vec::new();
        for cid in cids.iter().copied() {
            match self.store.get(&cid).await? {
                Some(stored) => {
                    match self
                        .local_release_decision(user_actor.as_ref(), &cid)
                        .await?
                    {
                        LocalRelease::Allow => {
                            let _ = tx.send(Ok((cid, stored.key))).await;
                        }
                        LocalRelease::Deny => {
                            let _ = tx
                                .send(Err(Error::AccessDenied {
                                    reason: "policy denied".into(),
                                }))
                                .await;
                        }
                        // We hold the block but not its ownership yet (e.g. a
                        // replication fetch before merge). Defer to the owner.
                        LocalRelease::OwnershipUnknown => remote.push(cid),
                    }
                }
                None => remote.push(cid),
            }
        }

        // Cross-peer fan-out for misses.
        if !remote.is_empty() {
            if self.transports.is_empty() {
                // No transports configured — surface unavailability per
                // missing CID so callers don't block waiting for a reply
                // that can never come.
                for _ in &remote {
                    let _ = tx.send(Err(Error::KeyUnavailable)).await;
                }
            } else {
                // Wire path: fall back to node identity for gossip-triggered
                // syncs where ctx has no user identity (mirrors Go's
                // behavior in internal/kms/pubsub.go per PR #4778).
                let wire_actor = self.principal(ctx).clone();
                let eph = self.ephemeral();
                let pub_bytes = x25519_dalek::PublicKey::from(&eph).as_bytes().to_vec();
                let req = FetchEncryptionKeyRequest {
                    identity: wire_actor.to_string().into_bytes(),
                    links: remote.iter().map(|c| c.to_bytes()).collect(),
                    ephemeral_public_key: pub_bytes,
                };
                let payload =
                    serde_cbor::to_vec(&req).map_err(|e| Error::WireEncode(e.to_string()))?;
                let encoded = EncodedFetchRequest {
                    payload,
                    request_id: uuid::Uuid::new_v4().to_string(),
                };
                let remote_set: std::collections::HashSet<EncryptionCid> =
                    remote.iter().copied().collect();

                for transport in &self.transports {
                    let mut rx = transport.send_request(encoded.clone()).await?;
                    let tx = tx.clone();
                    let store = self.store.clone();
                    let eph_clone = eph.clone();
                    let remote_set = remote_set.clone();
                    let our_eph_pub = x25519_dalek::PublicKey::from(&eph_clone)
                        .as_bytes()
                        .to_vec();
                    spawn_task(async move {
                        while let Some(transport_result) = rx.recv().await {
                            let (reply, responder_peer_id) = match transport_result {
                                Ok(reply) => reply,
                                Err(error) => {
                                    let _ = tx.send(Err(error)).await;
                                    continue;
                                }
                            };
                            tracing::debug!(
                                responder = %responder_peer_id,
                                links = reply.links.len(),
                                blocks = reply.blocks.len(),
                                resp_eph_len = reply.ephemeral_public_key.len(),
                                "KMS get_keys: received reply from transport"
                            );
                            // AAD binds OUR (requester) ephemeral pubkey and the
                            // RESPONDER's peer id, matching the serve side.
                            let aad = crate::ecies_envelope::make_associated_data(
                                &our_eph_pub,
                                &responder_peer_id,
                            );
                            let mut returned = std::collections::HashSet::new();
                            for (cid_bytes, block_env) in
                                reply.links.iter().zip(reply.blocks.iter())
                            {
                                let Ok(cid) = cid::Cid::try_from(cid_bytes.as_slice()) else {
                                    tracing::warn!("KMS reply contained malformed CID; skipping");
                                    continue;
                                };
                                if !remote_set.contains(&cid) {
                                    // Expected case — reply for a CID we
                                    // didn't ask for. Silent skip.
                                    tracing::debug!(
                                        cid = %cid,
                                        "KMS get_keys: reply CID not in requested set; skipping"
                                    );
                                    continue;
                                }
                                returned.insert(cid);
                                let block_bytes = match crate::ecies_envelope::unwrap_with_private(
                                    block_env,
                                    &eph_clone,
                                    &reply.ephemeral_public_key,
                                    &aad,
                                ) {
                                    Ok(b) => b,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            cid = %cid,
                                            "ECIES unwrap failed on KMS reply"
                                        );
                                        continue;
                                    }
                                };
                                let block =
                                    match defra_core::Encryption::from_dag_cbor(&block_bytes) {
                                        Ok(b) => b,
                                        Err(e) => {
                                            tracing::warn!(
                                                error = %e,
                                                cid = %cid,
                                                "Encryption block decode failed on KMS reply"
                                            );
                                            continue;
                                        }
                                    };
                                if block.key.len() != 32 {
                                    tracing::warn!(
                                        cid = %cid,
                                        len = block.key.len(),
                                        "Encryption block has invalid key length; skipping"
                                    );
                                    continue;
                                }
                                let mut key = [0u8; 32];
                                key.copy_from_slice(&block.key);
                                let _ = store
                                    .put(
                                        cid,
                                        StoredKey {
                                            key,
                                            block_bytes: block_bytes.clone(),
                                        },
                                    )
                                    .await;
                                tracing::debug!(
                                    cid = %cid,
                                    "KMS get_keys: DEK unwrapped and delivered to caller"
                                );
                                let _ = tx.send(Ok((cid, key))).await;
                            }
                            if returned.len() < remote_set.len() {
                                let _ = tx
                                    .send(Err(Error::AccessDenied {
                                        reason: "peer did not release one or more requested keys"
                                            .into(),
                                    }))
                                    .await;
                            }
                        }
                    });
                }
            }
        }

        // Drop our handle so the channel closes once spawned transport tasks finish.
        // Without this, the receiver would block indefinitely on the local sender.
        drop(tx);
        Ok(results)
    }

    async fn generate_key(
        &self,
        _ctx: &RequestContext,
        scope: KeyScope,
    ) -> Result<(EncryptionCid, [u8; 32])> {
        let (cid, stored) = self.store.generate(&scope).await?;
        Ok((cid, stored.key))
    }

    async fn serve_request(
        &self,
        _from: PeerIdentity,
        req: FetchEncryptionKeyRequest,
    ) -> Result<FetchEncryptionKeyReply> {
        let actor: Option<Did> = std::str::from_utf8(&req.identity)
            .ok()
            .and_then(|s| s.parse().ok());
        if actor.is_none() && !req.identity.is_empty() {
            tracing::warn!(
                identity_bytes_len = req.identity.len(),
                "KMS request identity field is non-empty but failed DID parse; treating as anonymous"
            );
        }

        let mut out_links: Vec<Vec<u8>> = Vec::new();
        let mut out_blocks: Vec<Vec<u8>> = Vec::new();

        // ONE responder ephemeral per reply, shared across all served blocks
        // (matches Go). Its PUBLIC key is carried in the reply field below.
        let responder_eph = crypto::generate_x25519().map_err(|e| Error::Crypto(e.to_string()))?;
        let peer_id = self.local_peer_id();
        let aad = crate::ecies_envelope::make_associated_data(&req.ephemeral_public_key, &peer_id);

        for cid_bytes in req.links {
            let cid = match cid::Cid::try_from(cid_bytes.as_slice()) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let Some(stored) = self.store.get(&cid).await? else {
                continue;
            };
            if self.may_serve(actor.as_ref(), &cid).await? {
                tracing::debug!(cid = %cid, "KMS serve_request: DEK release GRANTED");
            } else {
                tracing::warn!(cid = %cid, "KMS serve_request: DEK release DENIED");
                continue;
            }
            let wrapped = crate::ecies_envelope::wrap_for_requester(
                &stored.block_bytes,
                &req.ephemeral_public_key,
                &responder_eph,
                &aad,
            )?;
            out_links.push(cid.to_bytes());
            out_blocks.push(wrapped);
        }

        Ok(FetchEncryptionKeyReply {
            links: out_links,
            blocks: out_blocks,
            ephemeral_public_key: x25519_dalek::PublicKey::from(&responder_eph)
                .as_bytes()
                .to_vec(),
        })
    }

    fn set_local_peer_id(&self, id: String) {
        if let Ok(mut g) = self.local_peer_id.write() {
            *g = id;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::RequestContext;
    use crate::memory_store::MemoryKeyStore;
    use crate::transport::{
        EncodedFetchRequest, IncomingHandler, KeyTransport, TransportReplyStream,
    };
    use crate::types::KeyScope;
    use std::sync::Arc;

    struct AnyDocResolver;
    #[async_trait::async_trait]
    impl BlockDocIDResolver for AnyDocResolver {
        async fn doc_ids_for_block(&self, _: &EncryptionCid) -> crate::Result<Vec<String>> {
            Ok(vec!["bae-test-doc".to_string()])
        }
    }

    fn any_doc_resolver() -> Arc<dyn BlockDocIDResolver> {
        Arc::new(AnyDocResolver)
    }

    struct AllowAll;
    #[async_trait::async_trait]
    impl crate::policy::AccessPolicy for AllowAll {
        async fn check_release(
            &self,
            _: Option<&identity::Did>,
            _: &KeyScope,
        ) -> crate::Result<crate::PolicyDecision> {
            Ok(crate::PolicyDecision::Allow)
        }
        async fn check_node_release(
            &self,
            _: Option<&identity::Did>,
            _: &KeyScope,
        ) -> crate::Result<crate::PolicyDecision> {
            Ok(crate::PolicyDecision::Allow)
        }
    }

    struct DenyAll;
    #[async_trait::async_trait]
    impl crate::policy::AccessPolicy for DenyAll {
        async fn check_release(
            &self,
            _: Option<&identity::Did>,
            _: &KeyScope,
        ) -> crate::Result<crate::PolicyDecision> {
            Ok(crate::PolicyDecision::Deny)
        }
        async fn check_node_release(
            &self,
            _: Option<&identity::Did>,
            _: &KeyScope,
        ) -> crate::Result<crate::PolicyDecision> {
            Ok(crate::PolicyDecision::Deny)
        }
    }

    fn node_did() -> identity::Did {
        "did:key:znode".parse().unwrap()
    }

    #[tokio::test]
    async fn generate_returns_cid_and_plain_key() {
        let store: Arc<dyn crate::KeyStore> = Arc::new(MemoryKeyStore::new());
        let policy: Arc<dyn crate::policy::AccessPolicy> = Arc::new(AllowAll);
        let kms = DefraKms::new(store, vec![], policy, any_doc_resolver(), node_did());
        let ctx = RequestContext::anonymous();
        let (cid, key) = kms
            .generate_key(
                &ctx,
                KeyScope::Document {
                    doc_id: "d1".into(),
                    field: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(key.len(), 32);
        // After generate, get_keys returns the same key locally.
        let results = kms.get_keys(&ctx, &[cid]).await.unwrap();
        let map = results.wait_all().await.unwrap();
        assert_eq!(map[&cid], key);
    }

    #[tokio::test]
    async fn get_keys_missing_returns_unavailable() {
        let store: Arc<dyn crate::KeyStore> = Arc::new(MemoryKeyStore::new());
        let policy: Arc<dyn crate::policy::AccessPolicy> = Arc::new(AllowAll);
        let kms = DefraKms::new(store, vec![], policy, any_doc_resolver(), node_did());
        let ctx = RequestContext::anonymous();
        let cid: crate::EncryptionCid =
            "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
                .parse()
                .unwrap();
        let results = kms.get_keys(&ctx, &[cid]).await.unwrap();
        let mut rx = results.into_receiver();
        let first = rx.recv().await.unwrap();
        assert!(matches!(first, Err(crate::Error::KeyUnavailable)));
    }

    #[tokio::test]
    async fn get_keys_local_with_deny_policy_returns_access_denied() {
        let store: Arc<dyn crate::KeyStore> = Arc::new(MemoryKeyStore::new());
        let allow: Arc<dyn crate::policy::AccessPolicy> = Arc::new(AllowAll);
        let kms_gen = DefraKms::new(store.clone(), vec![], allow, any_doc_resolver(), node_did());
        let ctx = RequestContext::anonymous();
        let (cid, _) = kms_gen
            .generate_key(
                &ctx,
                KeyScope::Document {
                    doc_id: "d1".into(),
                    field: None,
                },
            )
            .await
            .unwrap();

        let deny: Arc<dyn crate::policy::AccessPolicy> = Arc::new(DenyAll);
        let kms_check = DefraKms::new(store, vec![], deny, any_doc_resolver(), node_did());
        let results = kms_check.get_keys(&ctx, &[cid]).await.unwrap();
        let mut rx = results.into_receiver();
        let first = rx.recv().await.unwrap();
        assert!(matches!(first, Err(crate::Error::AccessDenied { .. })));
    }

    #[tokio::test]
    async fn serve_request_returns_ecies_wrapped_block_bytes() {
        let store: Arc<dyn crate::KeyStore> = Arc::new(MemoryKeyStore::new());
        let policy: Arc<dyn crate::policy::AccessPolicy> = Arc::new(AllowAll);
        let kms = DefraKms::new(
            store.clone(),
            vec![],
            policy,
            any_doc_resolver(),
            node_did(),
        );
        kms.set_local_peer_id("peer-1".into());
        let ctx = RequestContext::anonymous();
        let (cid, _) = kms
            .generate_key(
                &ctx,
                KeyScope::Document {
                    doc_id: "d1".into(),
                    field: None,
                },
            )
            .await
            .unwrap();

        let requester = crypto::generate_x25519().unwrap();
        let req_pub = x25519_dalek::PublicKey::from(&requester)
            .as_bytes()
            .to_vec();
        let req = crate::wire::FetchEncryptionKeyRequest {
            identity: b"did:key:zalice".to_vec(),
            links: vec![cid.to_bytes()],
            ephemeral_public_key: req_pub.clone(),
        };
        let from = crate::service::PeerIdentity {
            peer_id: "peer-1".into(),
        };
        let reply = kms.serve_request(from, req).await.unwrap();
        assert_eq!(reply.links.len(), 1);
        assert_eq!(reply.blocks.len(), 1);
        let aad = crate::ecies_envelope::make_associated_data(&req_pub, "peer-1");
        let unwrapped = crate::unwrap_with_private(
            &reply.blocks[0],
            &requester,
            &reply.ephemeral_public_key,
            &aad,
        )
        .unwrap();
        let block = defra_core::Encryption::from_dag_cbor(&unwrapped).unwrap();

        assert_eq!(block.key.len(), 32);
    }

    #[tokio::test]
    async fn serve_request_skips_unknown_cids() {
        let store: Arc<dyn crate::KeyStore> = Arc::new(MemoryKeyStore::new());
        let policy: Arc<dyn crate::policy::AccessPolicy> = Arc::new(AllowAll);
        let kms = DefraKms::new(store, vec![], policy, any_doc_resolver(), node_did());
        let requester = crypto::generate_x25519().unwrap();
        let unknown: crate::EncryptionCid =
            "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
                .parse()
                .unwrap();
        let req = crate::wire::FetchEncryptionKeyRequest {
            identity: b"did:key:zalice".to_vec(),
            links: vec![unknown.to_bytes()],
            ephemeral_public_key: x25519_dalek::PublicKey::from(&requester)
                .as_bytes()
                .to_vec(),
        };
        let from = crate::service::PeerIdentity {
            peer_id: "peer-1".into(),
        };
        let reply = kms.serve_request(from, req).await.unwrap();
        assert!(reply.links.is_empty());
        assert!(reply.blocks.is_empty());
    }

    struct FakeTransport {
        reply: tokio::sync::Mutex<
            Option<crate::Result<(crate::wire::FetchEncryptionKeyReply, String)>>,
        >,
    }
    #[async_trait::async_trait]
    impl KeyTransport for FakeTransport {
        fn name(&self) -> &'static str {
            "fake"
        }
        async fn send_request(
            &self,
            _: EncodedFetchRequest,
        ) -> crate::Result<TransportReplyStream> {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            if let Some(r) = self.reply.lock().await.take() {
                let _ = tx.send(r).await;
            }
            Ok(rx)
        }
        fn install_handler(&self, _: Arc<dyn IncomingHandler>) {}
    }

    #[tokio::test]
    async fn get_keys_fans_out_when_local_miss() {
        // Peer KMS produces a reply for some CID.
        let peer_store: Arc<dyn crate::KeyStore> = Arc::new(MemoryKeyStore::new());
        let peer_policy: Arc<dyn crate::policy::AccessPolicy> = Arc::new(AllowAll);
        let peer_kms = DefraKms::new(
            peer_store,
            vec![],
            peer_policy,
            any_doc_resolver(),
            node_did(),
        );
        peer_kms.set_local_peer_id("peer".into());
        let ctx = RequestContext::anonymous();
        let (peer_cid, _) = peer_kms
            .generate_key(
                &ctx,
                KeyScope::Document {
                    doc_id: "d1".into(),
                    field: None,
                },
            )
            .await
            .unwrap();

        let requester = crypto::generate_x25519().unwrap();
        let req = crate::wire::FetchEncryptionKeyRequest {
            identity: b"did:key:znode".to_vec(),
            links: vec![peer_cid.to_bytes()],
            ephemeral_public_key: x25519_dalek::PublicKey::from(&requester)
                .as_bytes()
                .to_vec(),
        };
        let reply = peer_kms
            .serve_request(
                crate::service::PeerIdentity {
                    peer_id: "peer".into(),
                },
                req,
            )
            .await
            .unwrap();

        // Local empty KMS with a fake transport carrying the reply + the
        // responder peer id (same one the serve side bound into the AAD).
        let fake = FakeTransport {
            reply: tokio::sync::Mutex::new(Some(Ok((reply, "peer".to_string())))),
        };
        let store: Arc<dyn crate::KeyStore> = Arc::new(MemoryKeyStore::new());
        let policy: Arc<dyn crate::policy::AccessPolicy> = Arc::new(AllowAll);
        let kms = DefraKms::new(
            store,
            vec![Arc::new(fake)],
            policy,
            any_doc_resolver(),
            node_did(),
        );
        kms.set_ephemeral_for_test(requester);

        let results = kms.get_keys(&ctx, &[peer_cid]).await.unwrap();
        let map = results.wait_all().await.unwrap();
        assert_eq!(map.len(), 1);
        assert!(map.contains_key(&peer_cid));
    }

    #[tokio::test]
    async fn get_keys_propagates_transport_timeout() {
        let fake = FakeTransport {
            reply: tokio::sync::Mutex::new(Some(Err(crate::Error::KeyUnavailable))),
        };
        let store: Arc<dyn crate::KeyStore> = Arc::new(MemoryKeyStore::new());
        let policy: Arc<dyn crate::policy::AccessPolicy> = Arc::new(AllowAll);
        let kms = DefraKms::new(
            store,
            vec![Arc::new(fake)],
            policy,
            any_doc_resolver(),
            node_did(),
        );
        let cid: crate::EncryptionCid =
            "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
                .parse()
                .unwrap();

        let result = kms
            .get_keys(&RequestContext::anonymous(), &[cid])
            .await
            .unwrap()
            .wait_all()
            .await;

        assert!(matches!(result, Err(crate::Error::KeyUnavailable)));
    }

    #[tokio::test]
    async fn get_keys_maps_empty_peer_reply_to_access_denied() {
        let fake = FakeTransport {
            reply: tokio::sync::Mutex::new(Some(Ok((
                crate::FetchEncryptionKeyReply {
                    links: vec![],
                    blocks: vec![],
                    ephemeral_public_key: vec![],
                },
                "peer".to_string(),
            )))),
        };
        let store: Arc<dyn crate::KeyStore> = Arc::new(MemoryKeyStore::new());
        let policy: Arc<dyn crate::policy::AccessPolicy> = Arc::new(AllowAll);
        let kms = DefraKms::new(
            store,
            vec![Arc::new(fake)],
            policy,
            any_doc_resolver(),
            node_did(),
        );
        let cid: crate::EncryptionCid =
            "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
                .parse()
                .unwrap();

        let result = kms
            .get_keys(&RequestContext::anonymous(), &[cid])
            .await
            .unwrap()
            .wait_all()
            .await;

        assert!(matches!(result, Err(crate::Error::AccessDenied { .. })));
    }
}
