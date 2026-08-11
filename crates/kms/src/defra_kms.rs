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
    /// Node identity sent on cross-peer requests and authenticated by peers.
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

    fn verified_key(cid: &EncryptionCid, block_bytes: &[u8]) -> Result<[u8; 32]> {
        let block = defra_core::Encryption::from_dag_cbor(block_bytes)
            .map_err(|error| Error::Crypto(format!("decode encryption block: {error}")))?;
        let computed_cid = block
            .generate_cid()
            .map_err(|error| Error::Crypto(format!("compute encryption block CID: {error}")))?;
        if computed_cid != *cid {
            return Err(Error::Crypto(format!(
                "encryption block CID mismatch: expected {cid}, got {computed_cid}"
            )));
        }
        block.key.as_slice().try_into().map_err(|_| {
            Error::Crypto(format!(
                "encryption block {cid} has invalid key length {}",
                block.key.len()
            ))
        })
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
        delegated_actor: Option<(&Did, &str)>,
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
            if let Some((delegate, collection_id)) = delegated_actor {
                if matches!(
                    self.policy
                        .check_delegated_release(delegate, &scope, collection_id)
                        .await?,
                    PolicyDecision::Allow
                ) {
                    return Ok(LocalRelease::Allow);
                }
            }
        }
        Ok(LocalRelease::Deny)
    }

    /// Serve-side release decision: the owner node has recorded ownership,
    /// so unknown ownership is treated as a denial (never leak a key we
    /// cannot attribute).
    async fn may_serve(
        &self,
        actor: Option<&Did>,
        cid: &EncryptionCid,
        delegated_actor: Option<(&Did, &str)>,
    ) -> Result<bool> {
        Ok(matches!(
            self.local_release_decision(actor, cid, delegated_actor)
                .await?,
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
                    let key = match Self::verified_key(&cid, &stored.block_bytes) {
                        Ok(key) => key,
                        Err(error) => {
                            tracing::warn!(
                                cid = %cid,
                                error = %error,
                                "Ignoring invalid locally stored encryption block"
                            );
                            remote.push(cid);
                            continue;
                        }
                    };
                    match self
                        .local_release_decision(user_actor.as_ref(), &cid, None)
                        .await?
                    {
                        LocalRelease::Allow => {
                            let _ = tx.send(Ok((cid, key))).await;
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
                // Cross-peer DEK access is authorized as the requesting node.
                // Go responders still consume this legacy wire field, while
                // Rust responders bind it to the authenticated transport peer.
                let wire_actor = self.node_identity.clone();
                let eph = self.ephemeral();
                let pub_bytes = x25519_dalek::PublicKey::from(&eph).as_bytes().to_vec();
                let req = FetchEncryptionKeyRequest {
                    identity: wire_actor.to_string().into_bytes(),
                    links: remote.iter().map(|c| c.to_bytes()).collect(),
                    ephemeral_public_key: pub_bytes,
                    explicit_replay_capability: ctx.explicit_replay_capability().map(str::to_owned),
                };
                let payload =
                    defra_core::cbor::to_vec(&req).map_err(|e| Error::WireEncode(e.to_string()))?;
                let encoded = EncodedFetchRequest {
                    payload,
                    request_id: uuid::Uuid::new_v4().to_string(),
                };
                let remote_set: std::collections::HashSet<EncryptionCid> =
                    remote.iter().copied().collect();

                let (transport_tx, mut transport_rx) =
                    tokio::sync::mpsc::channel(self.transports.len().max(1) * 16);
                for transport in &self.transports {
                    let mut rx = match transport.send_request(encoded.clone()).await {
                        Ok(rx) => rx,
                        Err(error) => {
                            let _ = transport_tx.send(Err(error)).await;
                            continue;
                        }
                    };
                    let transport_tx = transport_tx.clone();
                    spawn_task(async move {
                        loop {
                            tokio::select! {
                                _ = transport_tx.closed() => break,
                                result = rx.recv() => {
                                    let Some(result) = result else {
                                        break;
                                    };
                                    if transport_tx.send(result).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    });
                }
                drop(transport_tx);

                let tx = tx.clone();
                let store = self.store.clone();
                let our_eph_pub = x25519_dalek::PublicKey::from(&eph).as_bytes().to_vec();
                spawn_task(async move {
                    let mut returned = std::collections::HashSet::new();
                    let mut denied = None;
                    let mut unavailable = None;
                    while let Some(transport_result) = transport_rx.recv().await {
                        let (reply, responder_peer_id) = match transport_result {
                            Ok(reply) => reply,
                            Err(error @ Error::AccessDenied { .. }) => {
                                denied = Some(error);
                                continue;
                            }
                            Err(error) => {
                                unavailable = Some(error);
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
                        if reply.links.len() != reply.blocks.len() {
                            let error = Error::Crypto(format!(
                                "KMS reply links/blocks length mismatch: {} links, {} blocks",
                                reply.links.len(),
                                reply.blocks.len()
                            ));
                            tracing::warn!(error = %error, "Rejecting malformed KMS reply");
                            unavailable = Some(error);
                            continue;
                        }
                        // AAD binds OUR (requester) ephemeral pubkey and the
                        // RESPONDER's peer id, matching the serve side.
                        let aad = crate::ecies_envelope::make_associated_data(
                            &our_eph_pub,
                            &responder_peer_id,
                        );
                        for (cid_bytes, block_env) in reply.links.iter().zip(reply.blocks.iter()) {
                            let Ok(cid) = cid::Cid::try_from(cid_bytes.as_slice()) else {
                                tracing::warn!("KMS reply contained malformed CID; skipping");
                                unavailable =
                                    Some(Error::Crypto("KMS reply contained malformed CID".into()));
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
                            if returned.contains(&cid) {
                                continue;
                            }
                            let block_bytes = match crate::ecies_envelope::unwrap_with_private(
                                block_env,
                                &eph,
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
                                    unavailable = Some(Error::Crypto(format!(
                                        "unwrap encryption block {cid}: {e}"
                                    )));
                                    continue;
                                }
                            };
                            let key = match Self::verified_key(&cid, &block_bytes) {
                                Ok(key) => key,
                                Err(error) => {
                                    tracing::warn!(
                                        error = %error,
                                        cid = %cid,
                                        "Rejecting invalid encryption block from KMS peer"
                                    );
                                    unavailable = Some(error);
                                    continue;
                                }
                            };
                            if let Err(error) = store
                                .put(
                                    cid,
                                    StoredKey {
                                        key,
                                        block_bytes: block_bytes.clone(),
                                    },
                                )
                                .await
                            {
                                tracing::warn!(
                                    error = %error,
                                    cid = %cid,
                                    "Failed to store verified encryption block from KMS peer"
                                );
                                unavailable = Some(error);
                                continue;
                            }
                            returned.insert(cid);
                            tracing::debug!(
                                cid = %cid,
                                "KMS get_keys: DEK unwrapped and delivered to caller"
                            );
                            if tx.send(Ok((cid, key))).await.is_err() {
                                return;
                            }
                        }
                        if returned.len() == remote_set.len() {
                            break;
                        }
                    }
                    if returned.len() < remote_set.len() {
                        let _ = tx
                            .send(Err(unavailable.or(denied).unwrap_or(Error::KeyUnavailable)))
                            .await;
                    }
                });
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
        from: PeerIdentity,
        req: FetchEncryptionKeyRequest,
    ) -> Result<FetchEncryptionKeyReply> {
        let claimed_actor: Option<Did> = std::str::from_utf8(&req.identity)
            .ok()
            .and_then(|s| s.parse().ok());
        if claimed_actor.is_none() && !req.identity.is_empty() {
            tracing::warn!(
                identity_bytes_len = req.identity.len(),
                "KMS request identity field is non-empty but failed DID parse"
            );
        }
        if claimed_actor.as_ref() != from.authenticated_did.as_ref() {
            tracing::warn!(
                peer_id = %from.peer_id,
                "KMS request identity does not match authenticated peer identity"
            );
        }
        let actor = from.authenticated_did;
        let delegated_actor =
            from.explicit_replay_authorization
                .as_ref()
                .and_then(|authorization| {
                    authorization
                        .authorizer_did
                        .parse::<Did>()
                        .ok()
                        .map(|did| (did, authorization.collection_id.as_str()))
                });

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
            if self
                .may_serve(
                    actor.as_ref(),
                    &cid,
                    delegated_actor
                        .as_ref()
                        .map(|(did, collection_id)| (did, *collection_id)),
                )
                .await?
            {
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
#[path = "defra_kms_tests.rs"]
mod tests;
