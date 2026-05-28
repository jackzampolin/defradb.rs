//! Pubsub-gossip transport for KMS request/reply, generic over any
//! [`P2PTransport`] that supports raw gossip (`publish_raw` +
//! `register_pubsub_rpc_topic`).
//!
//! Wire-compatible at the topic level with Go's `internal/kms/pubsub.go`:
//! bare CBOR on topic `"encryption"`, ECIES-encrypted reply blocks. No
//! request-id envelope — requests and replies are matched cryptographically.

use kms::{
    EncodedFetchRequest, FetchEncryptionKeyReply, FetchEncryptionKeyRequest, IncomingHandler,
    KeyTransport, PeerIdentity, Result as KmsResult, TransportReplyStream,
};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tracing::warn;

use crate::topics::{DefraTopic, ENCRYPTION_TOPIC};
use crate::transport::P2PTransport;

/// Gossip-backed `KeyTransport`, generic over the underlying P2P transport.
///
/// **M1 limitation:** single in-flight reply slot. Each `get_keys` call
/// produces one transport iteration, and `DefraKms` does not issue
/// concurrent `get_keys` against the same transport. A later milestone
/// broadens this to N slots via request-id correlation.
pub struct PubsubKeyTransport<T: P2PTransport> {
    transport: T,
    handler: RwLock<Option<Arc<dyn IncomingHandler>>>,
    in_flight: RwLock<Option<mpsc::Sender<FetchEncryptionKeyReply>>>,
}

impl<T: P2PTransport> PubsubKeyTransport<T> {
    /// Construct, subscribe to ENCRYPTION_TOPIC, register raw routing.
    pub async fn new(transport: T) -> KmsResult<Arc<Self>> {
        transport
            .subscribe(DefraTopic::Encryption)
            .await
            .map_err(|e| kms::Error::Internal(format!("subscribe encryption topic: {e}")))?;
        transport
            .register_pubsub_rpc_topic(ENCRYPTION_TOPIC.to_string())
            .await
            .map_err(|e| kms::Error::Internal(format!("register raw routing: {e}")))?;
        Ok(Arc::new(Self {
            transport,
            handler: RwLock::new(None),
            in_flight: RwLock::new(None),
        }))
    }

    /// Called by the sync coordinator when a `GossipRawMessage` arrives on
    /// ENCRYPTION_TOPIC. Reply-decode-first; else dispatch as a request.
    pub async fn dispatch_incoming(&self, from: PeerIdentity, payload: Vec<u8>) {
        if let Some(tx) = self.in_flight.read().ok().and_then(|g| g.clone()) {
            if let Ok(reply) = serde_cbor::from_slice::<FetchEncryptionKeyReply>(&payload) {
                let _ = tx.send(reply).await;
                return;
            }
        }
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
            .transport
            .publish_raw(ENCRYPTION_TOPIC.to_string(), bytes)
            .await
        {
            warn!(error = %e, "failed to publish KMS reply on encryption topic");
        }
    }
}

#[async_trait::async_trait]
impl<T: P2PTransport> KeyTransport for PubsubKeyTransport<T> {
    fn name(&self) -> &'static str {
        "pubsub"
    }

    async fn send_request(&self, req: EncodedFetchRequest) -> KmsResult<TransportReplyStream> {
        let (tx, rx) = mpsc::channel(16);
        if let Ok(mut slot) = self.in_flight.write() {
            *slot = Some(tx);
        }
        self.transport
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
