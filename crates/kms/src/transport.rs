//! Transport abstraction for cross-peer DEK fetches.
//!
//! M1 ships `PubsubKeyTransport<T>` (in `crates/p2p/src/kms/`), one impl
//! generic over the `P2PTransport` abstraction so it rides both libp2p and
//! iroh gossip. The KMS layer composes one or more transports and fans out
//! fetch requests across them.

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::service::PeerIdentity;
use crate::wire::{FetchEncryptionKeyReply, FetchEncryptionKeyRequest};

/// A CBOR-encoded `FetchEncryptionKeyRequest` ready to publish on a
/// `KeyTransport`. KMS wire is bare CBOR (matching Go's
/// `internal/kms/pubsub.go`) — there is no signature envelope here.
///
/// `request_id` is local-only for tracing/correlation; NOT on the wire.
#[derive(Debug, Clone)]
pub struct EncodedFetchRequest {
    /// CBOR-encoded `FetchEncryptionKeyRequest` payload.
    pub payload: Vec<u8>,
    /// Local tracing id (NOT on the wire).
    pub request_id: String,
}

/// Receiver channel for incoming replies to a fetch request.
///
/// Closes when the transport stops listening for replies (timeout
/// elapsed, peer set exhausted). Callers drain until close.
pub type TransportReplyStream = mpsc::Receiver<FetchEncryptionKeyReply>;

/// Handler invoked by a transport when an inbound request arrives.
/// `DefraKms` installs itself as the handler at startup.
#[async_trait]
pub trait IncomingHandler: Send + Sync {
    /// Process an incoming request and produce a reply.
    async fn handle(
        &self,
        from: PeerIdentity,
        req: FetchEncryptionKeyRequest,
    ) -> Result<FetchEncryptionKeyReply>;
}

/// Pluggable transport for KMS request/reply.
#[async_trait]
pub trait KeyTransport: Send + Sync {
    /// Carrier name, used in tracing and metrics (e.g. `"libp2p-pubsub"`).
    fn name(&self) -> &'static str;

    /// Publish a fetch request and return a stream of replies. Caller is
    /// responsible for any retry/timeout policy on the returned stream.
    async fn send_request(&self, req: EncodedFetchRequest) -> Result<TransportReplyStream>;

    /// Install the local handler invoked when a peer sends a fetch request.
    /// Idempotent: replacing a previously installed handler is allowed (M1
    /// transports keep only one).
    fn install_handler(&self, handler: Arc<dyn IncomingHandler>);
}

#[cfg(test)]
mod tests {
    use super::*;
    fn assert_object_safe<T: ?Sized + Send + Sync>() {}

    #[test]
    fn key_transport_is_object_safe() {
        assert_object_safe::<dyn KeyTransport>();
    }
}
