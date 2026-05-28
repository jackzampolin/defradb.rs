//! Top-level KMS service trait.

use async_trait::async_trait;

use crate::context::RequestContext;
use crate::error::Result;
use crate::results::KeyResults;
use crate::types::{EncryptionCid, KeyScope};
use crate::wire::{FetchEncryptionKeyReply, FetchEncryptionKeyRequest};

/// Identity of an incoming peer at the transport boundary. Used by
/// `KmsService::serve_request` for tracing and audit (NOT for authorization —
/// the wire `identity` field on the request carries the authorization principal).
#[derive(Debug, Clone)]
pub struct PeerIdentity {
    /// Transport-level peer id (libp2p `PeerId` stringified, or iroh node id).
    pub peer_id: String,
}

/// Top-level KMS surface. One concrete implementation (`DefraKms`) lives
/// alongside test-only `NoopKms`; future milestones may add `RemoteHttpKms`,
/// `SubscriberKms`, etc.
#[async_trait]
pub trait KmsService: Send + Sync {
    /// Fetch DEKs for the given Encryption-block CIDs. Returns a streaming
    /// aggregator because keys may arrive from peers asynchronously.
    /// `ctx` carries the caller identity (set at HTTP/CLI entry points).
    async fn get_keys(
        &self,
        ctx: &RequestContext,
        cids: &[EncryptionCid],
    ) -> Result<KeyResults>;

    /// Generate a fresh DEK for the given scope, persist it locally, and
    /// return both the CID of the on-disk `Encryption` block AND the plain
    /// key bytes the caller will use to encrypt data. Returning both avoids
    /// a redundant `get_keys` round trip on the write path.
    async fn generate_key(
        &self,
        ctx: &RequestContext,
        scope: KeyScope,
    ) -> Result<(EncryptionCid, [u8; 32])>;

    /// Handle an incoming peer key request. Invoked by `KeyTransport`
    /// adapters (e.g. `Libp2pPubsubTransport`) when a request arrives on
    /// the wire.
    async fn serve_request(
        &self,
        from: PeerIdentity,
        req: FetchEncryptionKeyRequest,
    ) -> Result<FetchEncryptionKeyReply>;
}

#[cfg(test)]
mod tests {
    use super::*;
    fn assert_object_safe<T: ?Sized + Send + Sync>() {}

    #[test]
    fn kms_service_is_object_safe() {
        assert_object_safe::<dyn KmsService>();
    }
}
