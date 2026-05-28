//! Libp2p gossipsub transport for KMS request/reply.
//!
//! Wire-compatible at the topic level with Go's `internal/kms/pubsub.go`:
//! bare CBOR on topic `"encryption"`, ECIES-encrypted reply blocks. No
//! request-id envelope — requests and replies are matched cryptographically
//! (the requester's ephemeral pubkey is embedded in each ECIES envelope).

use kms::{
    EncodedFetchRequest, FetchEncryptionKeyReply, FetchEncryptionKeyRequest, IncomingHandler,
    KeyTransport, PeerIdentity, Result as KmsResult, TransportReplyStream,
};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tracing::warn;

use crate::host::P2PHostHandle;
use crate::topics::{DefraTopic, ENCRYPTION_TOPIC};

/// Libp2p gossipsub-backed `KeyTransport`.
///
/// **M1 limitation:** holds a single in-flight reply slot. Each `get_keys`
/// call produces one transport iteration, and `DefraKms` does not issue
/// concurrent `get_keys` against the same transport. M2 broadens this to N
/// slots via request-id correlation.
pub struct Libp2pPubsubTransport {
    handle: P2PHostHandle,
    handler: RwLock<Option<Arc<dyn IncomingHandler>>>,
    in_flight: RwLock<Option<mpsc::Sender<FetchEncryptionKeyReply>>>,
}

impl Libp2pPubsubTransport {
    /// Construct, subscribe to ENCRYPTION_TOPIC, and register raw routing.
    /// The returned `Arc` is the same instance the host will dispatch incoming
    /// messages to (call `dispatch_incoming` from the SyncCoordinator event
    /// handler — see Task G3).
    pub async fn new(handle: P2PHostHandle) -> KmsResult<Arc<Self>> {
        handle
            .subscribe(DefraTopic::Encryption)
            .await
            .map_err(|e| kms::Error::Internal(format!("subscribe encryption topic: {e}")))?;
        handle
            .register_pubsub_rpc_topic(ENCRYPTION_TOPIC.to_string())
            .await
            .map_err(|e| kms::Error::Internal(format!("register raw routing: {e}")))?;
        Ok(Arc::new(Self {
            handle,
            handler: RwLock::new(None),
            in_flight: RwLock::new(None),
        }))
    }

    /// Called by the sync coordinator when a `GossipRawMessage` arrives on
    /// ENCRYPTION_TOPIC. Try to decode as a reply first (if a request is
    /// in flight); otherwise treat as an incoming request and dispatch to
    /// the installed handler.
    pub async fn dispatch_incoming(&self, from: PeerIdentity, payload: Vec<u8>) {
        // Reply path: if there's an in-flight slot, attempt reply decode.
        if let Some(tx) = self.in_flight.read().ok().and_then(|g| g.clone()) {
            if let Ok(reply) = serde_cbor::from_slice::<FetchEncryptionKeyReply>(&payload) {
                let _ = tx.send(reply).await;
                return;
            }
        }

        // Otherwise treat as a request.
        let handler = self.handler.read().ok().and_then(|g| g.clone());
        let Some(handler) = handler else {
            warn!("KMS request arrived but no handler installed; dropping");
            return;
        };
        let req: FetchEncryptionKeyRequest = match serde_cbor::from_slice(&payload) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "failed to decode KMS request");
                return;
            }
        };
        let reply = match handler.handle(from, req).await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "KMS handler errored");
                return;
            }
        };
        if reply.blocks.is_empty() {
            // No keys to serve (deny-all / unknown CIDs) — don't pollute the
            // topic with empty replies.
            return;
        }
        let bytes = match serde_cbor::to_vec(&reply) {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "failed to encode KMS reply");
                return;
            }
        };
        if let Err(e) = self
            .handle
            .publish_raw(ENCRYPTION_TOPIC.to_string(), bytes)
            .await
        {
            warn!(error = %e, "failed to publish KMS reply on encryption topic");
        }
    }
}

#[async_trait::async_trait]
impl KeyTransport for Libp2pPubsubTransport {
    fn name(&self) -> &'static str {
        "libp2p-pubsub"
    }

    async fn send_request(&self, req: EncodedFetchRequest) -> KmsResult<TransportReplyStream> {
        let (tx, rx) = mpsc::channel(16);
        if let Ok(mut slot) = self.in_flight.write() {
            *slot = Some(tx);
        }
        self.handle
            .publish_raw(ENCRYPTION_TOPIC.to_string(), req.payload)
            .await
            .map_err(|e| kms::Error::Internal(format!("publish KMS request: {e}")))?;
        Ok(rx)
    }

    fn install_handler(&self, handler: Arc<dyn IncomingHandler>) {
        if let Ok(mut slot) = self.handler.write() {
            *slot = Some(handler);
        }
    }
}
