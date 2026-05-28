//! Transport abstraction for cross-peer DEK fetches.
//!
//! M1 ships `Libp2pPubsubTransport` (in `crates/p2p/src/kms/`). M2 adds
//! `IrohStreamTransport`. The KMS layer composes one or more transports
//! and fans out fetch requests across them.

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::service::PeerIdentity;
use crate::wire::{FetchEncryptionKeyReply, FetchEncryptionKeyRequest};

/// A request ready to send on a `KeyTransport`. Already CBOR-encoded.
///
/// `request_id` is for tracing only — Go's wire format has no request-id
/// envelope, so peer correlation happens cryptographically (only the
/// requester's ephemeral private key can decrypt the reply).
#[derive(Debug, Clone)]
pub struct SignedFetchRequest {
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
    async fn send_request(&self, req: SignedFetchRequest) -> Result<TransportReplyStream>;

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
