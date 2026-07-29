use super::*;
use crate::context::RequestContext;
use crate::memory_store::MemoryKeyStore;
use crate::transport::{EncodedFetchRequest, IncomingHandler, KeyTransport, TransportReplyStream};
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
    let cid: crate::EncryptionCid = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
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
    reply:
        tokio::sync::Mutex<Option<crate::Result<(crate::wire::FetchEncryptionKeyReply, String)>>>,
}
#[async_trait::async_trait]
impl KeyTransport for FakeTransport {
    fn name(&self) -> &'static str {
        "fake"
    }
    async fn send_request(&self, _: EncodedFetchRequest) -> crate::Result<TransportReplyStream> {
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
async fn get_keys_ignores_failed_transport_when_another_resolves_key() {
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
    let failed = FakeTransport {
        reply: tokio::sync::Mutex::new(Some(Err(crate::Error::KeyUnavailable))),
    };
    let resolved = FakeTransport {
        reply: tokio::sync::Mutex::new(Some(Ok((reply, "peer".to_string())))),
    };
    let store: Arc<dyn crate::KeyStore> = Arc::new(MemoryKeyStore::new());
    let policy: Arc<dyn crate::policy::AccessPolicy> = Arc::new(AllowAll);
    let kms = DefraKms::new(
        store,
        vec![Arc::new(failed), Arc::new(resolved)],
        policy,
        any_doc_resolver(),
        node_did(),
    );
    kms.set_ephemeral_for_test(requester);

    let map = kms
        .get_keys(&ctx, &[peer_cid])
        .await
        .unwrap()
        .wait_all()
        .await
        .unwrap();

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
    let cid: crate::EncryptionCid = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
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
async fn get_keys_maps_empty_peer_reply_to_unavailable() {
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
    let cid: crate::EncryptionCid = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
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
async fn get_keys_rejects_reply_length_mismatch() {
    let cid: crate::EncryptionCid = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
        .parse()
        .unwrap();
    let fake = FakeTransport {
        reply: tokio::sync::Mutex::new(Some(Ok((
            crate::FetchEncryptionKeyReply {
                links: vec![cid.to_bytes()],
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

    let result = kms
        .get_keys(&RequestContext::anonymous(), &[cid])
        .await
        .unwrap()
        .wait_all()
        .await;

    assert!(matches!(result, Err(crate::Error::Crypto(_))));
}

#[tokio::test]
async fn get_keys_rejects_encryption_block_with_mismatched_cid() {
    let requester = crypto::generate_x25519().unwrap();
    let requester_pub = x25519_dalek::PublicKey::from(&requester)
        .as_bytes()
        .to_vec();
    let responder = crypto::generate_x25519().unwrap();
    let actual_block = defra_core::Encryption::new(vec![7u8; 32]);
    let actual_bytes = actual_block.to_dag_cbor().unwrap();
    let requested_cid = defra_core::Encryption::new(vec![8u8; 32])
        .generate_cid()
        .unwrap();
    let aad = crate::ecies_envelope::make_associated_data(&requester_pub, "peer");
    let envelope =
        crate::wrap_for_requester(&actual_bytes, &requester_pub, &responder, &aad).unwrap();
    let reply = crate::FetchEncryptionKeyReply {
        links: vec![requested_cid.to_bytes()],
        blocks: vec![envelope],
        ephemeral_public_key: x25519_dalek::PublicKey::from(&responder)
            .as_bytes()
            .to_vec(),
    };
    let fake = FakeTransport {
        reply: tokio::sync::Mutex::new(Some(Ok((reply, "peer".to_string())))),
    };
    let store: Arc<dyn crate::KeyStore> = Arc::new(MemoryKeyStore::new());
    let inspect_store = Arc::clone(&store);
    let policy: Arc<dyn crate::policy::AccessPolicy> = Arc::new(AllowAll);
    let kms = DefraKms::new(
        store,
        vec![Arc::new(fake)],
        policy,
        any_doc_resolver(),
        node_did(),
    );
    kms.set_ephemeral_for_test(requester);

    let result = kms
        .get_keys(&RequestContext::anonymous(), &[requested_cid])
        .await
        .unwrap()
        .wait_all()
        .await;

    assert!(matches!(result, Err(crate::Error::Crypto(_))));
    assert!(inspect_store.get(&requested_cid).await.unwrap().is_none());
}
