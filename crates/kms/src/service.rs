//! Top-level KMS service trait.

use async_trait::async_trait;
use defra_core::thread_bounds::MaybeSendSync;

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
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait KmsService: MaybeSendSync {
    /// Fetch DEKs for the given Encryption-block CIDs. Returns a streaming
    /// aggregator because keys may arrive from peers asynchronously.
    /// `ctx` carries the caller identity (set at HTTP/CLI entry points).
    async fn get_keys(&self, ctx: &RequestContext, cids: &[EncryptionCid]) -> Result<KeyResults>;

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
    /// adapters (e.g. `PubsubKeyTransport`) when a request arrives on
    /// the wire.
    async fn serve_request(
        &self,
        from: PeerIdentity,
        req: FetchEncryptionKeyRequest,
    ) -> Result<FetchEncryptionKeyReply>;

    /// Set this node's transport-level peer id. Bound into the ECIES AAD on
    /// served replies (per Go's `makeAssociatedData`). Default no-op for
    /// implementations that don't serve over a transport (e.g. `NoopKms`).
    fn set_local_peer_id(&self, _id: String) {}
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
